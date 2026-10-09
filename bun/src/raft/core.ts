/**
 * A Raft group as a deterministic state machine, as docs/raft.md specifies.
 *
 * {@link RaftCore} does no I/O. The driver feeds it `tick`, `step` and
 * `propose`, persists what `takeDirty` reports before sending `takeOutbox`,
 * and applies `takeCommitted` in order. Messages are the JSON payloads of
 * section 4, the same the Rust broker's `cluster::raft::Core` sends.
 */

export const HEARTBEAT_MS = 150;
export const ELECTION_MIN_MS = 1000;
export const ELECTION_MAX_MS = 2000;
const MAX_BATCH = 256;

export type Entry = { i: number; term: number; kind: string; data: unknown };
export type Role = "follower" | "precandidate" | "candidate" | "leader";
export type Snapshot = { index: number; term: number; voters: string[]; state: unknown };
export type Dirty = { hardState: boolean; truncatedFrom: number | null; appended: Entry[]; snapshot: Snapshot | null };
export type Msg = Record<string, unknown> & { t: string; g: string };

const sorted = (v: string[]) => [...new Set(v)].sort();
const num = (v: unknown, d = 0) => (typeof v === "number" ? v : d);

export class RaftCore {
  term = 0;
  votedFor: string | null = null;
  role: Role = "follower";
  leader: string | null = null;
  commit = 0;
  applied = 0;
  private log: Entry[] = [];
  private snapIndex = 0;
  private snapTerm = 0;
  private snapVoters: string[] = [];
  private initialVoters: string[];
  private votes = new Set<string>();
  private nextIndex = new Map<string, number>();
  private matchIndex = new Map<string, number>();
  private heardLeaderAt = 0;
  private electionDeadline = 0;
  private heartbeatDue = 0;
  private rng: bigint;
  private outbox: Array<[string, Msg]> = [];
  private dirty: Dirty = { hardState: false, truncatedFrom: null, appended: [], snapshot: null };
  private installed: Snapshot | null = null;
  /** The state machine as a snapshot, for a follower behind the compacted log. */
  snapshotSource: (() => unknown) | null = null;
  /**
   * Highest index known to be on this member's disk. A leader counts itself
   * toward a commit only up to here. `null` treats every entry as durable,
   * as the in-memory tests do.
   */
  private durable: number | null = null;

  constructor(
    readonly id: string,
    readonly group: string,
    voters: string[],
    seed: number,
  ) {
    this.initialVoters = sorted(voters);
    this.rng = BigInt(seed) | 1n;
    this.resetElection(0);
  }

  restore(term: number, votedFor: string | null, snapshot: Snapshot | null, log: Entry[], applied: number) {
    this.term = term;
    this.votedFor = votedFor;
    if (snapshot) {
      this.snapIndex = snapshot.index;
      this.snapTerm = snapshot.term;
      this.snapVoters = sorted(snapshot.voters);
    }
    this.log = log.filter((e) => e.i > this.snapIndex);
    this.applied = Math.min(Math.max(applied, this.snapIndex), this.lastIndex());
    this.commit = this.applied;
  }

  private rand(): number {
    // xorshift64*, as the Rust core.
    const mask = (1n << 64n) - 1n;
    let x = this.rng;
    x ^= x >> 12n;
    x ^= (x << 25n) & mask;
    x ^= x >> 27n;
    this.rng = x;
    return Number(((x * 0x2545f4914f6cdd1dn) & mask) % 1000000007n);
  }

  private resetElection(now: number) {
    this.electionDeadline = now + ELECTION_MIN_MS + (this.rand() % (ELECTION_MAX_MS - ELECTION_MIN_MS));
  }

  lastIndex() {
    return this.log.length ? this.log[this.log.length - 1]!.i : this.snapIndex;
  }

  lastTerm() {
    return this.log.length ? this.log[this.log.length - 1]!.term : this.snapTerm;
  }

  termAt(index: number): number | null {
    if (index === 0) return 0;
    if (index === this.snapIndex) return this.snapTerm;
    if (index < this.snapIndex) return null;
    return this.log[index - this.snapIndex - 1]?.term ?? null;
  }

  private entry(index: number): Entry | null {
    if (index <= this.snapIndex) return null;
    return this.log[index - this.snapIndex - 1] ?? null;
  }

  voters(): string[] {
    for (let i = this.log.length - 1; i >= 0; i--) {
      const e = this.log[i]!;
      if (e.kind === "config") {
        const v = (e.data as { voters?: unknown })?.voters;
        if (Array.isArray(v)) return sorted(v.map(String));
      }
    }
    return this.snapVoters.length ? [...this.snapVoters] : [...this.initialVoters];
  }

  private quorum() {
    return Math.floor(this.voters().length / 2) + 1;
  }

  private peers() {
    return this.voters().filter((v) => v !== this.id);
  }

  private send(to: string, msg: Msg) {
    this.outbox.push([to, msg]);
  }

  takeOutbox() {
    const out = this.outbox;
    this.outbox = [];
    return out;
  }

  takeDirty(): Dirty {
    const d = this.dirty;
    this.dirty = { hardState: false, truncatedFrom: null, appended: [], snapshot: null };
    return d;
  }

  takeInstalled(): Snapshot | null {
    const s = this.installed;
    this.installed = null;
    return s;
  }

  takeCommitted(): Entry[] {
    const out: Entry[] = [];
    while (this.applied < this.commit) {
      const e = this.entry(this.applied + 1);
      if (!e) break;
      out.push(e);
      this.applied++;
    }
    return out;
  }

  private becomeFollower(term: number, leader: string | null, now: number) {
    if (term > this.term) {
      this.term = term;
      this.votedFor = null;
      this.dirty.hardState = true;
    }
    this.role = "follower";
    this.leader = leader;
    this.votes.clear();
    this.resetElection(now);
  }

  private appendLocal(kind: string, data: unknown): number {
    const e: Entry = { i: this.lastIndex() + 1, term: this.term, kind, data };
    this.dirty.appended.push(e);
    this.log.push(e);
    return e.i;
  }

  /** Append a command as leader. Returns its index, or null when not leader. */
  propose(kind: string, data: unknown, now: number): number | null {
    if (this.role !== "leader") return null;
    const index = this.appendLocal(kind, data);
    this.maybeCommit();
    // Sent with the next tick, so a burst shares one append per peer.
    this.heartbeatDue = Math.min(this.heartbeatDue, now);
    return index;
  }

  /** The driver's disk writer finished writing through `index`. */
  setDurable(index: number) {
    this.durable = this.durable === null ? index : Math.max(this.durable, index);
    this.maybeCommit();
  }

  /** Entries from `index + 1` on were replaced and are not yet on disk. */
  capDurable(index: number) {
    if (this.durable !== null) this.durable = Math.min(this.durable, index);
  }

  private durableLast() {
    return this.durable === null ? this.lastIndex() : Math.min(this.durable, this.lastIndex());
  }

  /** Move the voters one member at a time toward `wanted`. Leader only. */
  reconfigure(wanted: string[], now: number) {
    if (this.role !== "leader") return;
    if (this.log.some((e) => e.kind === "config" && e.i > this.commit)) return;
    const current = this.voters();
    const want = sorted(wanted);
    if (want.length === 0 || JSON.stringify(current) === JSON.stringify(want)) return;
    let next = [...current];
    const add = want.find((w) => !current.includes(w));
    if (add) next.push(add);
    else {
      const remove = current.find((c) => !want.includes(c));
      if (remove) next = next.filter((c) => c !== remove);
    }
    this.appendLocal("config", { voters: sorted(next) });
    const last = this.lastIndex();
    for (const p of this.peers()) {
      if (!this.nextIndex.has(p)) this.nextIndex.set(p, last);
      if (!this.matchIndex.has(p)) this.matchIndex.set(p, 0);
    }
    this.maybeCommit();
    this.broadcastAppend(now);
  }

  /** Start the election timer from `now`: a group opened after the node started. */
  startTimer(now: number) {
    this.resetElection(now);
  }

  /** Campaign at the next tick, when there is no leader: the member a new queue should be led by. */
  expedite() {
    if (this.role !== "leader" && this.leader === null) this.electionDeadline = 0;
  }

  tick(now: number) {
    if (this.role === "leader") {
      if (now >= this.heartbeatDue) this.broadcastAppend(now);
    } else if (now >= this.electionDeadline && this.voters().includes(this.id)) {
      this.startPreVote(now);
    }
  }

  private startPreVote(now: number) {
    this.role = "precandidate";
    this.leader = null;
    this.votes = new Set([this.id]);
    this.resetElection(now);
    if (this.votes.size >= this.quorum()) return this.startElection(now);
    const msg = { t: "vote", g: this.group, term: this.term + 1, cand: this.id, lli: this.lastIndex(), llt: this.lastTerm(), pre: true };
    for (const p of this.peers()) this.send(p, msg);
  }

  private startElection(now: number) {
    this.role = "candidate";
    this.term++;
    this.votedFor = this.id;
    this.dirty.hardState = true;
    this.leader = null;
    this.votes = new Set([this.id]);
    this.resetElection(now);
    if (this.votes.size >= this.quorum()) return this.becomeLeader(now);
    const msg = { t: "vote", g: this.group, term: this.term, cand: this.id, lli: this.lastIndex(), llt: this.lastTerm(), pre: false };
    for (const p of this.peers()) this.send(p, msg);
  }

  private becomeLeader(now: number) {
    this.role = "leader";
    this.leader = this.id;
    this.nextIndex.clear();
    this.matchIndex.clear();
    const next = this.lastIndex() + 1;
    for (const p of this.peers()) {
      this.nextIndex.set(p, next);
      this.matchIndex.set(p, 0);
    }
    // A noop in the new term lets earlier entries commit.
    this.appendLocal("noop", null);
    this.maybeCommit();
    this.broadcastAppend(now);
  }

  private broadcastAppend(now: number) {
    this.heartbeatDue = now + HEARTBEAT_MS;
    for (const p of this.peers()) this.sendAppend(p);
  }

  private sendAppend(peer: string) {
    const next = this.nextIndex.get(peer) ?? this.lastIndex() + 1;
    if (next <= this.snapIndex) {
      const state = this.snapshotSource ? this.snapshotSource() : null;
      const voters = this.snapVoters.length ? this.snapVoters : this.voters();
      return this.send(peer, { t: "snap", g: this.group, term: this.term, leader: this.id, lii: this.snapIndex, lit: this.snapTerm, voters, state });
    }
    const pli = next - 1;
    const plt = this.termAt(pli) ?? 0;
    const entries: Entry[] = [];
    for (let i = next; i <= this.lastIndex() && entries.length < MAX_BATCH; i++) {
      const e = this.entry(i);
      if (e) entries.push(e);
    }
    this.send(peer, { t: "append", g: this.group, term: this.term, leader: this.id, pli, plt, entries, commit: this.commit });
    // Pipelined: the next append starts after what is now in flight. A
    // rejection rewinds to the follower's hint.
    if (entries.length > 0) this.nextIndex.set(peer, next + entries.length);
  }

  private maybeCommit() {
    if (this.role !== "leader") return;
    const matched = this.voters()
      .map((v) => (v === this.id ? this.durableLast() : (this.matchIndex.get(v) ?? 0)))
      .sort((a, b) => b - a);
    const n = matched[this.quorum() - 1];
    // Only entries of the current term commit by counting (Raft 5.4.2).
    if (n !== undefined && n > this.commit && this.termAt(n) === this.term) this.commit = n;
  }

  /** Handle one message from `from`. */
  step(from: string, msg: Msg, now: number) {
    const t = msg.t;
    const term = num(msg.term);
    const pre = msg.pre === true;
    const preVote = (t === "vote" || t === "vote_r") && pre;
    if (!preVote && term > this.term && ["vote", "vote_r", "append", "append_r", "snap", "snap_r"].includes(t)) {
      this.becomeFollower(term, t === "append" || t === "snap" ? from : null, now);
    }
    if (t === "vote") this.onVote(from, msg, term, pre, now);
    else if (t === "vote_r") this.onVoteReply(from, msg, term, pre, now);
    else if (t === "append") this.onAppend(from, msg, term, now);
    else if (t === "append_r") this.onAppendReply(from, msg, term);
    else if (t === "snap") this.onSnap(from, msg, term, now);
    else if (t === "snap_r") this.onSnapReply(from, msg, term);
  }

  private logOk(lli: number, llt: number) {
    return llt > this.lastTerm() || (llt === this.lastTerm() && lli >= this.lastIndex());
  }

  private onVote(from: string, msg: Msg, term: number, pre: boolean, now: number) {
    const cand = typeof msg.cand === "string" ? msg.cand : from;
    const lli = num(msg.lli);
    const llt = num(msg.llt);
    let granted: boolean;
    if (pre) {
      // A leader, or a follower that heard one recently, refuses: the cluster is not leaderless.
      const leaderLive = this.role === "leader" || (this.leader !== null && now < this.heardLeaderAt + ELECTION_MIN_MS);
      granted = term > this.term && this.logOk(lli, llt) && !leaderLive;
    } else {
      granted = term === this.term && (this.votedFor === null || this.votedFor === cand) && this.logOk(lli, llt);
    }
    if (granted && !pre) {
      this.votedFor = cand;
      this.dirty.hardState = true;
      this.resetElection(now);
    }
    const replyTerm = pre ? (granted ? term : Math.max(term, this.term)) : this.term;
    this.send(from, { t: "vote_r", g: this.group, term: replyTerm, granted, pre });
  }

  private onVoteReply(from: string, msg: Msg, term: number, pre: boolean, now: number) {
    if (msg.granted !== true) return;
    if (pre) {
      if (this.role !== "precandidate" || term !== this.term + 1) return;
      this.votes.add(from);
      if (this.votes.size >= this.quorum()) this.startElection(now);
    } else {
      if (this.role !== "candidate" || term !== this.term) return;
      this.votes.add(from);
      if (this.votes.size >= this.quorum()) this.becomeLeader(now);
    }
  }

  private onAppend(from: string, msg: Msg, term: number, now: number) {
    if (term < this.term) {
      return this.send(from, { t: "append_r", g: this.group, term: this.term, ok: false, hint: this.lastIndex() + 1 });
    }
    if (this.role !== "follower" || this.leader !== from) {
      this.role = "follower";
      this.leader = from;
      this.votes.clear();
    }
    this.heardLeaderAt = now;
    this.resetElection(now);
    const pli = num(msg.pli);
    const plt = num(msg.plt);
    const leaderCommit = num(msg.commit);
    if (pli > this.lastIndex()) {
      return this.send(from, { t: "append_r", g: this.group, term: this.term, ok: false, hint: this.lastIndex() + 1 });
    }
    if (pli >= this.snapIndex && this.termAt(pli) !== plt) {
      // Skip back over the whole conflicting term.
      const bad = this.termAt(pli) ?? 0;
      let hint = pli;
      while (hint > this.snapIndex + 1 && this.termAt(hint - 1) === bad) hint--;
      return this.send(from, { t: "append_r", g: this.group, term: this.term, ok: false, hint: Math.max(hint, this.snapIndex + 1) });
    }
    const entries = Array.isArray(msg.entries) ? (msg.entries as Entry[]) : [];
    let lastNew = pli;
    for (const e of entries) {
      lastNew = e.i;
      if (e.i <= this.snapIndex) continue;
      const have = this.termAt(e.i);
      if (have === e.term) continue;
      if (have !== null) {
        // Conflict: drop it and everything after.
        this.log.length = e.i - this.snapIndex - 1;
        this.dirty.appended = this.dirty.appended.filter((a) => a.i < e.i);
        this.dirty.truncatedFrom = this.dirty.truncatedFrom === null ? e.i : Math.min(this.dirty.truncatedFrom, e.i);
      }
      const copy: Entry = { i: e.i, term: e.term, kind: e.kind, data: e.data ?? null };
      this.dirty.appended.push(copy);
      this.log.push(copy);
    }
    if (leaderCommit > this.commit) this.commit = Math.max(this.commit, Math.min(leaderCommit, lastNew));
    this.send(from, { t: "append_r", g: this.group, term: this.term, ok: true, match: lastNew });
  }

  private onAppendReply(from: string, msg: Msg, term: number) {
    if (this.role !== "leader" || term !== this.term) return;
    if (msg.ok === true) {
      const m = num(msg.match);
      const prev = this.matchIndex.get(from) ?? 0;
      if (m > prev) this.matchIndex.set(from, m);
      this.nextIndex.set(from, Math.max(this.nextIndex.get(from) ?? 0, Math.max(m, prev) + 1));
      this.maybeCommit();
      if (this.nextIndex.get(from)! <= this.lastIndex()) this.sendAppend(from);
    } else {
      const hint = Math.max(1, num(msg.hint, 1));
      this.nextIndex.set(from, Math.min(hint, this.lastIndex() + 1));
      this.sendAppend(from);
    }
  }

  private onSnap(from: string, msg: Msg, term: number, now: number) {
    if (term < this.term) return this.send(from, { t: "snap_r", g: this.group, term: this.term, lii: 0 });
    this.role = "follower";
    this.leader = from;
    this.heardLeaderAt = now;
    this.resetElection(now);
    const lii = num(msg.lii);
    const lit = num(msg.lit);
    if (lii > this.commit) {
      const voters = Array.isArray(msg.voters) ? (msg.voters as unknown[]).map(String) : [];
      const snap: Snapshot = { index: lii, term: lit, voters, state: msg.state ?? null };
      if (this.termAt(lii) === lit) this.log = this.log.filter((e) => e.i > lii);
      else this.log = [];
      this.snapIndex = lii;
      this.snapTerm = lit;
      this.snapVoters = sorted(voters);
      this.commit = lii;
      this.applied = lii;
      this.dirty.snapshot = snap;
      this.installed = snap;
    }
    this.send(from, { t: "snap_r", g: this.group, term: this.term, lii });
  }

  private onSnapReply(from: string, msg: Msg, term: number) {
    if (this.role !== "leader" || term !== this.term) return;
    const lii = num(msg.lii);
    if (lii > (this.matchIndex.get(from) ?? 0)) this.matchIndex.set(from, lii);
    this.nextIndex.set(from, lii + 1);
    this.maybeCommit();
  }

  /** Replace the applied prefix of the log with a snapshot of `state`. */
  compact(state: unknown): Snapshot | null {
    return this.compactAt(this.applied, state);
  }

  /** Like `compact`, at `index`: where the driver's state machine was when it produced `state`. */
  compactAt(index: number, state: unknown): Snapshot | null {
    if (index <= this.snapIndex || index > this.applied) return null;
    const term = this.termAt(index);
    if (term === null) return null;
    const voters = this.votersAt(index);
    this.log = this.log.filter((e) => e.i > index);
    this.snapIndex = index;
    this.snapTerm = term;
    this.snapVoters = voters;
    return { index, term, voters, state };
  }

  private votersAt(index: number): string[] {
    for (let i = this.log.length - 1; i >= 0; i--) {
      const e = this.log[i]!;
      if (e.i > index || e.kind !== "config") continue;
      const v = (e.data as { voters?: unknown })?.voters;
      if (Array.isArray(v)) return sorted(v.map(String));
    }
    return this.snapVoters.length ? [...this.snapVoters] : [...this.initialVoters];
  }

  entries(): readonly Entry[] {
    return this.log;
  }

  snapshotPoint(): { index: number; term: number } {
    return { index: this.snapIndex, term: this.snapTerm };
  }
}

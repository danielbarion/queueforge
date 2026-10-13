/**
 * The Raft driver for Bun: runs the `meta` and `quorum` groups of
 * docs/raft.md on `RaftCore`, with the same files as the Rust broker
 * (`raft/<group>/state.json`, `log.jsonl`, `snapshot.json`), and one
 * `q:v2:<hex-vhost>:<hex-name>` group per quorum queue when the cluster has them
 * (`raft/q-<hash>/`, with `group.json` naming the group).
 *
 * A round ticks both cores, writes what they report dirty, then sends. A
 * leader's own `append` and `snap` go out before its fsync; it counts
 * itself toward a commit only once the write is done (`setDurable`), and a
 * round that sends nothing else runs that fsync on the thread pool. Votes
 * and append replies wait for the write they depend on. Committed entries
 * are applied in log order by `apply`, and a round's proposals resolve together
 * after that.
 */
import { closeSync, existsSync, fsync, fsyncSync, mkdirSync, openSync, readdirSync, readFileSync, renameSync, rmSync, writeSync } from "node:fs";
import { join } from "node:path";
import { RaftCore, type Entry, type Msg, type Snapshot } from "./core.ts";

export const META = "meta";
export const QUORUM = "quorum";
const GROUPS = [META, QUORUM] as const;
/** `meta`, `quorum`, or `q:v2:<hex-vhost>:<hex-name>` for one quorum queue. */
export type Group = string;
const TICK_MS = 20;
const PROPOSE_TIMEOUT_MS = 5000;
const APPLY_RETRY_MIN_MS = 100;
const APPLY_RETRY_MAX_MS = 1000;

/** The group of one quorum queue, as docs/raft.md section 2 names it. */
export function queueGroup(vhost: string, name: string): Group {
  // Existing groups keep their persisted names; only new queues use v2.
  return `q:v2:${Buffer.from(vhost, "utf8").toString("hex")}:${Buffer.from(name, "utf8").toString("hex")}`;
}

/**
 * A quorum queue group with one voter, `owner`. Only the worker processes of
 * one physical broker create these (`Consensus.groupForNewQueue`): siblings
 * share its disk, so more voters there add fsyncs and IPC but no safety, and
 * RabbitMQ on one node runs a quorum queue with one member too. The voter is
 * part of the name, so it is persisted and replicated with the queue row.
 */
export function soloQueueGroup(owner: string, vhost: string, name: string): Group {
  return `q:s1:${Buffer.from(owner, "utf8").toString("hex")}:${Buffer.from(vhost, "utf8").toString("hex")}:${Buffer.from(name, "utf8").toString("hex")}`;
}

/** The only voter of a `soloQueueGroup`, or null for any other group. */
export function soloVoter(group: Group): string | null {
  if (!group.startsWith("q:s1:")) return null;
  const hex = group.slice(5, group.indexOf(":", 5));
  return hex ? Buffer.from(hex, "hex").toString("utf8") : null;
}

/** Applied entries kept before a snapshot replaces them (section 7). */
export function compactAfter(group: string) {
  return group === META ? 50_000 : 10_000;
}

/** Directory of a group under `raft/`. A queue group's name is hashed, as names may hold any byte. */
function groupDir(group: Group): string {
  if (group === META || group === QUORUM) return group;
  let h = 0xcbf29ce484222325n;
  for (const b of new TextEncoder().encode(group)) h = ((h ^ BigInt(b)) * 0x100000001b3n) & 0xffffffffffffffffn;
  return `q-${h.toString(16).padStart(16, "0")}`;
}

/** What the broker supplies: sending, applying and snapshot state. */
export type RaftHooks = {
  send(to: string, payload: Msg): void;
  apply(group: Group, entry: Entry): Promise<void>;
  install(group: Group, state: unknown): Promise<void>;
  state(group: Group): unknown;
  leaderChanged(group: Group, leader: string | null): void;
  /** A message names a queue group this node does not run: start it when the queue exists. */
  wantGroup?(group: Group): boolean;
};

type Waiter = { term: number; deadline: number; resolve: () => void; reject: (err: Error) => void } | { term: number; deadline: number; remote: { from: string; rid: string } };

type Pending = {
  group: Group;
  rid: string;
  kind: string;
  data: unknown;
  resolve: () => void;
  reject: (err: Error) => void;
  deadline: number;
  sentTo: string | null;
};

export class Disk {
  private log: number | null = null;
  private unsynced = false;
  /** Bumped when the log file is replaced: a sync of the old one proves nothing. */
  private gen = 0;
  /** The fd a background fsync is running on. It is closed only once that returns. */
  private syncingFd: number | null = null;
  private closeAfterSync = false;
  constructor(private dir: string) {
    mkdirSync(dir, { recursive: true });
  }

  load(): { term: number; vote: string | null; snapshot: Snapshot | null; log: Entry[] } {
    let term = 0;
    let vote: string | null = null;
    try {
      const hard = JSON.parse(readFileSync(join(this.dir, "state.json"), "utf8")) as { term?: number; vote?: string | null };
      term = Number(hard.term ?? 0);
      vote = hard.vote ?? null;
    } catch {
      /* a new member */
    }
    let snapshot: Snapshot | null = null;
    try {
      snapshot = JSON.parse(readFileSync(join(this.dir, "snapshot.json"), "utf8")) as Snapshot;
    } catch {
      /* no snapshot yet */
    }
    const log: Entry[] = [];
    if (existsSync(join(this.dir, "log.jsonl"))) {
      for (const line of readFileSync(join(this.dir, "log.jsonl"), "utf8").split("\n")) {
        if (!line) continue;
        let e: Entry;
        try {
          e = JSON.parse(line) as Entry;
        } catch {
          break; // a torn last line from a crash mid-write
        }
        while (log.length && log[log.length - 1]!.i >= e.i) log.pop();
        log.push(e);
      }
    }
    return { term, vote, snapshot, log };
  }

  private atomic(name: string, text: string) {
    const tmp = join(this.dir, `${name}.tmp`);
    const fd = openSync(tmp, "w");
    writeSync(fd, text);
    fsyncSync(fd);
    closeSync(fd);
    renameSync(tmp, join(this.dir, name));
    try {
      const d = openSync(this.dir, "r");
      fsyncSync(d);
      closeSync(d);
    } catch {
      /* directory fsync is best effort on some platforms */
    }
  }

  saveHard(term: number, vote: string | null) {
    this.atomic("state.json", JSON.stringify({ term, vote }));
  }

  writeEntries(entries: Entry[]) {
    if (this.log === null) this.log = openSync(join(this.dir, "log.jsonl"), "a");
    writeSync(this.log, entries.map((e) => JSON.stringify(e)).join("\n") + "\n");
    this.unsynced = true;
  }

  /** @returns Whether it ran an fsync. */
  sync(): boolean {
    // A background sync still running has not covered its entries yet.
    if (this.log === null || (!this.unsynced && this.syncingFd === null)) return false;
    this.unsynced = false;
    fsyncSync(this.log);
    return true;
  }

  /** Something was written since the last sync started. */
  get dirty(): boolean {
    return this.unsynced && this.log !== null;
  }

  get syncing(): boolean {
    return this.syncingFd !== null;
  }

  /**
   * fsync the log on the thread pool. `done(true)` means every entry written
   * before this call is on disk; false, that it failed or the file was
   * replaced since. Only one runs at a time.
   */
  syncAside(done: (ok: boolean) => void) {
    const fd = this.log!;
    const gen = this.gen;
    this.unsynced = false;
    this.syncingFd = fd;
    fsync(fd, (err) => {
      this.syncingFd = null;
      if (this.closeAfterSync) {
        this.closeAfterSync = false;
        closeSync(fd);
      }
      const ok = !err && gen === this.gen;
      if (err && gen === this.gen) this.unsynced = true;
      done(ok);
    });
  }

  private closeLog() {
    this.gen++;
    if (this.log === null) return;
    // Closing under a running fsync could hand its fd number to another file.
    if (this.log === this.syncingFd) this.closeAfterSync = true;
    else closeSync(this.log);
    this.log = null;
  }

  rewrite(entries: readonly Entry[]) {
    this.closeLog();
    this.unsynced = false;
    this.atomic("log.jsonl", entries.map((e) => JSON.stringify(e)).join("\n") + (entries.length ? "\n" : ""));
  }

  saveSnapshot(s: Snapshot, entries: readonly Entry[]) {
    this.atomic("snapshot.json", JSON.stringify(s));
    this.rewrite(entries);
  }

  close() {
    this.closeLog();
  }
}

function writeFileSynced(path: string, text: string) {
  const fd = openSync(path, "w");
  writeSync(fd, text);
  fsyncSync(fd);
  closeSync(fd);
}

type GroupState = {
  name: Group;
  core: RaftCore;
  disk: Disk;
  source: unknown;
  waiters: Map<number, Waiter>;
  lastLeader: string | null;
  sinceSnapshot: number;
  /** While leader: the index of the term's first entry. Announced only once it commits. */
  leadFrom: number | null;
  /** The leader as the applier last announced it, after every apply before it. */
  announced: string | null;
};

export class RaftNode {
  private groups = new Map<Group, GroupState>();
  private pending: Pending[] = [];
  private nextRid = 0;
  private wanted: string[];
  private start = performance.now();
  private timer: ReturnType<typeof setInterval> | null = null;
  private scheduled = false;
  private applying: Promise<void> = Promise.resolve();
  private stopped = false;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private wakeRetry: (() => void) | null = null;

  constructor(
    readonly id: string,
    private dir: string,
    voters: string[],
    private hooks: RaftHooks,
  ) {
    this.wanted = [...new Set(voters)].sort();
    for (const name of GROUPS) this.open(name);
    // Queue groups this node ran before a restart.
    for (const entry of existsSync(dir) ? readdirSync(dir) : []) {
      if (!entry.startsWith("q-")) continue;
      try {
        const named = JSON.parse(readFileSync(join(dir, entry, "group.json"), "utf8")) as { group?: unknown };
        if (typeof named.group === "string") this.open(named.group);
      } catch {
        /* a directory left half made by a crash */
      }
    }
    this.timer = setInterval(() => this.round(), TICK_MS);
  }

  private open(name: Group): GroupState {
    const existing = this.groups.get(name);
    if (existing) return existing;
    const path = join(this.dir, groupDir(name));
    const fresh = !existsSync(path);
    const disk = new Disk(path);
    if (fresh && name !== META && name !== QUORUM) writeFileSynced(join(path, "group.json"), JSON.stringify({ group: name }));
    const seed = (Date.now() ^ hashId(name) ^ hashId(this.id)) >>> 0;
    const core = new RaftCore(this.id, name, this.votersOf(name), seed || 1);
    const { term, vote, snapshot, log } = disk.load();
    const g: GroupState = { name, core, disk, source: snapshot?.state ?? null, waiters: new Map(), lastLeader: null, sinceSnapshot: 0, leadFrom: null, announced: null };
    if (snapshot) this.enqueueApply(() => this.hooks.install(name, snapshot.state));
    core.restore(term, vote, snapshot, log, snapshot?.index ?? 0);
    core.setDurable(core.lastIndex());
    core.snapshotSource = () => g.source;
    core.startTimer(this.now());
    this.groups.set(name, g);
    return g;
  }

  /** Start a queue group. Starting one that runs is a no-op. */
  addGroup(name: Group) {
    this.open(name);
    this.schedule();
  }

  /** Stop a queue group and remove its files: its queue was deleted. */
  dropGroup(name: Group) {
    const g = this.groups.get(name);
    if (!g || name === META || name === QUORUM) return;
    this.groups.delete(name);
    for (const w of g.waiters.values()) this.fail(g, w, "queue deleted");
    this.pending = this.pending.filter((p) => {
      if (p.group !== name) return true;
      p.reject(new Error("queue deleted"));
      return false;
    });
    g.disk.close();
    rmSync(join(this.dir, groupDir(name)), { recursive: true, force: true });
  }

  /** Have this member campaign in `name` now, when it has no leader. */
  expedite(name: Group) {
    this.groups.get(name)?.core.expedite();
    this.schedule();
  }

  /** Queue groups this member leads, by leader id, for the `balanced` locator. */
  queueLeaders(): Map<string, number> {
    const counts = new Map<string, number>();
    for (const g of this.groups.values()) {
      const leader = g.name === META || g.name === QUORUM ? null : this.leader(g.name);
      if (!leader) continue;
      counts.set(leader, (counts.get(leader) ?? 0) + 1);
    }
    return counts;
  }

  groupNames(): Group[] {
    return [...this.groups.keys()];
  }

  /** The voters of a group, for the management API's `members`. */
  members(name: Group): string[] {
    return this.groups.get(name)?.core.voters() ?? [];
  }

  stop() {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (this.retryTimer) clearTimeout(this.retryTimer);
    this.retryTimer = null;
    this.wakeRetry?.();
    this.wakeRetry = null;
  }

  private now() {
    return Math.floor(performance.now() - this.start);
  }

  private schedule() {
    if (this.scheduled || this.stopped) return;
    this.scheduled = true;
    setImmediate(() => {
      this.scheduled = false;
      this.round();
    });
  }

  /** One received `raft` payload. */
  step(from: string, msg: Msg) {
    let g = this.groups.get(String(msg.g));
    if (!g && typeof msg.g === "string" && msg.g.startsWith("q:") && this.hooks.wantGroup?.(msg.g)) g = this.open(msg.g);
    if (msg.t === "propose") {
      if (!g) return;
      if (g.core.role === "leader") {
        const index = g.core.propose(String(msg.kind), msg.data, this.now());
        if (index !== null) g.waiters.set(index, { term: g.core.term, deadline: Date.now() + PROPOSE_TIMEOUT_MS, remote: { from, rid: String(msg.rid) } });
      } else {
        this.hooks.send(from, { t: "propose_r", g: g.name, rid: String(msg.rid), ok: false, error: "not leader", ...(g.core.leader ? { leader: g.core.leader } : {}) });
      }
    } else if (msg.t === "propose_r") {
      const i = this.pending.findIndex((p) => p.rid === msg.rid);
      if (i < 0) return;
      if (msg.ok === true) this.pending.splice(i, 1)[0]!.resolve();
      else this.pending[i]!.sentTo = null;
    } else if (g) {
      g.core.step(from, msg, this.now());
    }
    this.schedule();
  }

  /** Replicate a command; resolves once it is applied (here, or on the leader when forwarded). */
  propose(group: Group, kind: string, data: unknown): Promise<void> {
    const g = this.groups.get(group);
    if (!g) return Promise.reject(new Error(`no raft group ${group}`));
    return new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + PROPOSE_TIMEOUT_MS;
      if (g.core.role === "leader") {
        const index = g.core.propose(kind, data, this.now());
        if (index !== null) {
          g.waiters.set(index, { term: g.core.term, deadline, resolve, reject });
          this.schedule();
          return;
        }
      }
      this.pending.push({ group, rid: `${this.id}-${++this.nextRid}`, kind, data, resolve, reject, deadline, sentTo: null });
      this.schedule();
    });
  }

  /** The group's leader, once it has applied everything committed before its term. */
  leader(group: Group): string | null {
    const g = this.groups.get(group);
    if (!g) return null;
    // Another process never hears from a one-voter group: its voter leads.
    const solo = soloVoter(group);
    if (solo && solo !== this.id) return solo;
    return g.announced ?? null;
  }

  /** Voters a group moves toward: its own for a one-voter group, else the cluster's. */
  private votersOf(group: Group): string[] {
    const solo = soloVoter(group);
    return solo ? [solo] : this.wanted;
  }

  setVoters(voters: string[]) {
    this.wanted = [...new Set(voters)].sort();
    this.schedule();
  }

  private enqueueApply(fn: () => Promise<void>) {
    this.applying = this.applying.then(async () => {
      let delay = APPLY_RETRY_MIN_MS;
      while (!this.stopped) {
        try {
          await fn();
          return;
        } catch (err) {
          // Keep this operation at the head: later applies, announcements and
          // confirmations must not overtake a failed state-machine install.
          if (delay === APPLY_RETRY_MIN_MS) console.error("raft apply; retrying", err);
          if (this.stopped) return;
          await new Promise<void>((resolve) => {
            this.wakeRetry = resolve;
            this.retryTimer = setTimeout(() => {
              this.retryTimer = null;
              this.wakeRetry = null;
              resolve();
            }, delay);
          });
          delay = Math.min(delay * 2, APPLY_RETRY_MAX_MS);
        }
      }
    });
  }

  private round() {
    if (this.stopped) return;
    const now = this.now();
    for (const g of this.groups.values()) {
      g.core.tick(now);
      if (g.core.role === "leader") g.core.reconfigure(this.votersOf(g.name), now);
    }
    this.forwardPending(now);
    for (const g of this.groups.values()) this.flush(g);
  }

  private forwardPending(now: number) {
    const pending = this.pending;
    this.pending = [];
    for (const p of pending) {
      if (Date.now() >= p.deadline) {
        p.reject(new Error("raft proposal timed out"));
        continue;
      }
      const g = this.groups.get(p.group);
      if (!g) {
        p.reject(new Error(`no raft group ${p.group}`));
        continue;
      }
      if (g.core.role === "leader") {
        const index = g.core.propose(p.kind, p.data, now);
        if (index !== null) g.waiters.set(index, { term: g.core.term, deadline: p.deadline, resolve: p.resolve, reject: p.reject });
        continue;
      }
      const leader = g.core.leader ?? soloVoter(p.group);
      if (leader && leader !== this.id && p.sentTo !== leader) {
        this.hooks.send(leader, { t: "propose", g: p.group, rid: p.rid, kind: p.kind, data: p.data });
        p.sentTo = leader;
      }
      this.pending.push(p);
    }
  }

  private flush(g: GroupState) {
    const outbox = g.core.takeOutbox();
    const late: Array<[string, Msg]> = [];
    for (const [to, msg] of outbox) {
      if (msg.t === "append" || msg.t === "snap") this.hooks.send(to, msg);
      else late.push([to, msg]);
    }
    const dirty = g.core.takeDirty();
    if (dirty.snapshot) {
      g.source = dirty.snapshot.state;
      g.disk.saveSnapshot(dirty.snapshot, g.core.entries());
    } else if (dirty.truncatedFrom !== null) {
      g.core.capDurable(dirty.truncatedFrom - 1);
      g.disk.rewrite(g.core.entries());
    } else if (dirty.appended.length) {
      g.disk.writeEntries(dirty.appended);
    }
    if (dirty.hardState) g.disk.saveHard(g.core.term, g.core.votedFor);
    if (late.length === 0 && dirty.truncatedFrom === null && !dirty.snapshot && g.core.role === "leader") {
      // Nothing sent here waits on this write: a leader counts itself once it lands.
      this.syncAside(g);
    } else {
      g.disk.sync();
      g.core.setDurable(g.core.lastIndex());
    }
    for (const [to, msg] of late) this.hooks.send(to, msg);

    const installed = g.core.takeInstalled();
    if (installed) {
      g.sinceSnapshot = 0;
      this.enqueueApply(() => this.hooks.install(g.name, installed.state));
    }
    const committed = g.core.takeCommitted();
    if (committed.length) {
      const waiters: Array<Waiter | null> = [];
      for (const e of committed) {
        const w = g.waiters.get(e.i);
        g.waiters.delete(e.i);
        const waiter = w && w.term === e.term ? w : null;
        if (w && !waiter) this.fail(g, w, "entry replaced by a new leader");
        waiters.push(waiter);
      }
      // One job per round: waiters are answered together, so the publishers
      // they release stage their frames in the same turns and share a write.
      // A failed apply is retried from that entry; earlier ones stay applied.
      let next = 0;
      this.enqueueApply(async () => {
        const ready: Array<[Waiter, number]> = [];
        try {
          for (; next < committed.length; next++) {
            const e = committed[next]!;
            await this.hooks.apply(g.name, e);
            if (waiters[next]) ready.push([waiters[next]!, e.i]);
            if (++g.sinceSnapshot >= compactAfter(g.name)) {
              g.sinceSnapshot = 0;
              const state = this.hooks.state(g.name);
              const snap = g.core.compactAt(e.i, state);
              if (snap) {
                g.source = snap.state;
                g.disk.saveSnapshot(snap, g.core.entries());
              }
            }
          }
        } finally {
          for (const [w, index] of ready) this.confirm(g, w, index);
        }
      });
    }
    const now = Date.now();
    for (const [i, w] of g.waiters) {
      if (now < w.deadline) continue;
      g.waiters.delete(i);
      this.fail(g, w, "raft proposal timed out");
    }
    // A new leader serves only after everything committed before its term is
    // applied (Raft section 8): until its noop commits it is announced as none.
    if (g.core.role === "leader") {
      if (g.leadFrom === null) g.leadFrom = g.core.lastIndex();
    } else {
      g.leadFrom = null;
    }
    const usable = g.leadFrom !== null && g.core.commit < g.leadFrom ? null : g.core.leader;
    if (usable !== g.lastLeader) {
      g.lastLeader = usable;
      // Announced after the applies queued above, so they land first.
      this.enqueueApply(async () => {
        g.announced = usable;
        this.hooks.leaderChanged(g.name, usable);
      });
    }
  }

  /**
   * Make the leader's log durable without blocking the loop. Entries written
   * while one sync runs share the next; its completion runs another round.
   */
  private syncAside(g: GroupState) {
    if (g.disk.syncing) return;
    if (!g.disk.dirty) {
      g.core.setDurable(g.core.lastIndex());
      return;
    }
    const through = g.core.lastIndex();
    g.disk.syncAside((ok) => {
      if (ok) g.core.setDurable(through);
      else if (!this.stopped) console.error("raft log fsync failed; retrying");
      this.schedule();
    });
  }

  private confirm(g: GroupState, w: Waiter, index: number) {
    if ("remote" in w) this.hooks.send(w.remote.from, { t: "propose_r", g: g.name, rid: w.remote.rid, ok: true, index });
    else w.resolve();
  }

  private fail(g: GroupState, w: Waiter, why: string) {
    if ("remote" in w) this.hooks.send(w.remote.from, { t: "propose_r", g: g.name, rid: w.remote.rid, ok: false, error: why });
    else w.reject(new Error(why));
  }
}

function hashId(id: string) {
  let h = 0x811c9dc5;
  for (let i = 0; i < id.length; i++) h = Math.imul(h ^ id.charCodeAt(i), 0x01000193);
  return h >>> 0;
}

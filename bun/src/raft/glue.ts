/**
 * Turning Raft on for Bun, and applying its commits (docs/raft.md,
 * sections 5, 6 and 8).
 *
 * Every hello advertises `features: ["raft"]`. The flag behaves as a
 * RabbitMQ feature flag: a cluster created by this build turns it on by
 * itself once every voter supports it (each advertises `raft_auto`, written
 * when its data directory was new). A node upgraded from a build without Raft
 * waits for `PUT /api/feature-flags/raft/enable`, which every member then
 * applies (`op: "feature"`, and `raft_on` in later hellos). Either way
 * `raft/enabled` is written and never removed. Until then quorum queues keep
 * majority-ack replication and metadata is pushed. `QUEUEFORGE_RAFT=0` keeps a
 * node on that version 1 behaviour.
 */
import { existsSync, fsyncSync, mkdirSync, openSync, closeSync, writeSync } from "node:fs";
import { join } from "node:path";
import type { Broker } from "../broker/index.ts";
import { decodeQuorumAppend } from "../wire.ts";
import type { Entry, Msg } from "./core.ts";
import { META, QUORUM, RaftNode, queueGroup, type Group } from "./node.ts";

export function raftSupported() {
  return process.env.QUEUEFORGE_RAFT !== "0";
}

/** One Raft group per quorum queue, as RabbitMQ runs. `QUEUEFORGE_RAFT_QGROUPS=0` keeps the shared group. */
export function queueGroupsSupported() {
  return raftSupported() && process.env.QUEUEFORGE_RAFT_QGROUPS !== "0";
}

function writeMarker(dir: string, name: string) {
  mkdirSync(dir, { recursive: true });
  const fd = openSync(join(dir, name), "w");
  writeSync(fd, "raft\n");
  fsyncSync(fd);
  closeSync(fd);
}

/** Record that this data directory was created by a build with Raft, so the cluster may turn it on by itself. */
export function markFreshRaft(dataDir: string) {
  if (raftSupported()) writeMarker(join(dataDir, "raft"), "auto");
}

/** Raft state of one broker: who advertised it, and the running node. */
export class Consensus {
  node: RaftNode | null = null;
  private advertised = new Set<string>();
  /** Peers whose data directory allows turning Raft on without an operator. */
  private autoPeers = new Set<string>();
  /** Peers that run one Raft group per quorum queue. */
  private qgroupPeers = new Set<string>();
  /** Peers that take one `enqueue` for several queues (`queues`). */
  manyPeers = new Set<string>();
  /** Quorum messages the quorum group holds, by `vhost\0queue\0id`, as their `enq` data. */
  live = new Map<string, Record<string, unknown>>();

  constructor(
    private broker: Broker,
    private send: (to: string, payload: Msg) => void,
  ) {}

  private dir() {
    return join(this.broker.cfg.dataDir, "raft");
  }

  /** "disabled" until every voter advertised Raft; "unsupported" with it switched off. */
  flag(): "enabled" | "disabled" | "unsupported" {
    if (!raftSupported()) return "unsupported";
    return this.node ? "enabled" : "disabled";
  }

  start() {
    if (raftSupported() && existsSync(join(this.dir(), "enabled"))) this.enable();
  }

  /** The `features` list for a hello payload. */
  features(): string[] {
    if (!raftSupported()) return ["enqueue_many"];
    const out = ["raft", "enqueue_many"];
    if (this.node || existsSync(join(this.dir(), "auto"))) out.push("raft_auto");
    if (this.node) out.push("raft_on");
    if (queueGroupsSupported()) out.push("raft_qgroups");
    return out;
  }

  /**
   * The group a new quorum queue should get: its own when Raft is on and
   * every voter runs queue groups, else the shared `quorum` group (null).
   * A mixed cluster with Rust or an older Bun stays on the shared group.
   */
  groupForNewQueue(vhost: string, name: string): Group | null {
    if (!this.node || !queueGroupsSupported()) return null;
    const self = this.broker.cfg.nodeId;
    if (!this.broker.cfg.members.every((m) => m.id === self || this.qgroupPeers.has(m.id))) return null;
    return queueGroup(vhost, name);
  }

  /** Every voter, this node included, can run Raft. */
  allSupport(): boolean {
    if (!raftSupported()) return false;
    const self = this.broker.cfg.nodeId;
    return this.broker.cfg.members.every((m) => m.id === self || this.advertised.has(m.id));
  }

  /**
   * Turn Raft on because an operator or a member asked.
   *
   * @returns False when this node or a voter cannot run it. True when it is on.
   */
  enableNow(): boolean {
    if (this.node) return true;
    if (!this.allSupport()) return false;
    writeMarker(this.dir(), "enabled");
    console.log("queueforge-bun: the raft feature flag was enabled");
    this.enable();
    return this.node != null;
  }

  stop() {
    this.node?.stop();
  }

  noteFeatures(peer: string, payload: Record<string, unknown> | undefined) {
    if (!peer) return;
    const list = Array.isArray(payload?.features) ? (payload!.features as unknown[]) : [];
    if (list.includes("enqueue_many")) this.manyPeers.add(peer);
    else this.manyPeers.delete(peer);
    if (!raftSupported()) return;
    if (!list.includes("raft")) return;
    this.advertised.add(peer);
    if (list.includes("raft_auto")) this.autoPeers.add(peer);
    if (list.includes("raft_qgroups")) this.qgroupPeers.add(peer);
    else this.qgroupPeers.delete(peer);
    // A member already runs Raft: the cluster turned the flag on.
    if (list.includes("raft_on") && !this.node) {
      writeMarker(this.dir(), "enabled");
      this.enable();
      return;
    }
    this.maybeEnable();
  }

  membersChanged() {
    if (this.node) this.node.setVoters(this.broker.cfg.members.map((m) => m.id));
    else this.maybeEnable();
  }

  private maybeEnable() {
    if (this.node || !this.allSupport()) return;
    if (!existsSync(join(this.dir(), "auto"))) return;
    const self = this.broker.cfg.nodeId;
    if (!this.broker.cfg.members.every((m) => m.id === self || this.autoPeers.has(m.id))) return;
    writeMarker(this.dir(), "enabled");
    console.log("queueforge-bun: every voter supports raft; enabling it");
    this.enable();
  }

  private enable() {
    const voters = this.broker.cfg.members.map((m) => m.id);
    if (!voters.length) voters.push(this.broker.cfg.nodeId);
    this.node = new RaftNode(this.broker.cfg.nodeId, this.dir(), voters, {
      send: this.send,
      apply: (group, entry) => this.apply(group, entry),
      install: (group, state) => this.install(group, state),
      state: (group) => this.state(group),
      leaderChanged: (group) => {
        if (group === QUORUM) this.broker.promoteIfLeader();
        else if (group !== META) {
          const q = this.queueOfGroup(group);
          if (q) this.broker.promoteIfLeader(q);
        }
      },
      wantGroup: (group) => this.queueOfGroup(group) != null,
    });
    // Groups whose queue was deleted while this node was down.
    queueMicrotask(() => this.dropOrphans());
  }

  private queueOfGroup(group: Group) {
    for (const q of this.broker.queues.values()) if (q.raftGroup === group) return q;
    return null;
  }

  /** Stop queue groups no queue names, once the meta log has replayed. */
  dropOrphans() {
    const node = this.node;
    if (!node) return;
    setTimeout(() => {
      for (const group of node.groupNames()) {
        if (group !== META && group !== QUORUM && !this.queueOfGroup(group)) node.dropGroup(group);
      }
    }, 5000).unref?.();
  }

  step(from: string, payload: Msg) {
    this.node?.step(from, payload);
  }

  private async apply(group: Group, entry: Entry) {
    if (entry.kind === "noop" || entry.kind === "config") return;
    const data = (entry.data ?? {}) as Record<string, unknown>;
    if (group === META) {
      // Membership commits through the meta log (docs/raft.md, section 7).
      if (entry.kind === "members") {
        if (Array.isArray(entry.data)) this.broker.cluster?.installMembers(entry.data as Array<{ id?: string; addr?: string }>);
        return;
      }
      this.broker.applyRemote(entry.kind, data);
      return;
    }
    if (entry.kind === "sappend") {
      const q = this.queueOfGroup(group);
      if (q) this.broker.applyStreamAppend(q, entry.i, data);
      return;
    }
    if (entry.kind === "enq") this.applyEnq(data);
    else if (entry.kind === "drop") {
      const vhost = String(data.vhost ?? "/");
      const queue = String(data.queue ?? "");
      const ids = Array.isArray(data.ids) ? (data.ids as unknown[]).map(String) : [String(data.id ?? "")];
      const leader = this.broker.isQuorumLeader(this.broker.queues.get(this.broker.key(vhost, queue)));
      for (const id of ids) {
        this.live.delete(`${vhost}\0${queue}\0${id}`);
        this.broker.noteConsumed(vhost, queue, id);
        // The leader delivered this body; its ack settles it there.
        if (!leader) this.broker.dropLocal(vhost, queue, id);
      }
    } else if (entry.kind === "purge") {
      const vhost = String(data.vhost ?? "/");
      const queue = String(data.queue ?? "");
      const prefix = `${vhost}\0${queue}\0`;
      for (const key of [...this.live.keys()]) if (key.startsWith(prefix)) this.live.delete(key);
      const q = this.broker.queues.get(this.broker.key(vhost, queue));
      if (q && !this.broker.isQuorumLeader(q)) q.replicas.clear();
    }
  }

  /** Store a committed quorum message here, once. */
  private applyEnq(data: Record<string, unknown>) {
    const d = decodeQuorumAppend(data);
    const key = `${d.vhost}\0${d.queue}\0${d.messageId}`;
    if (this.live.has(key)) return;
    this.live.set(key, data);
    const q = this.broker.queues.get(this.broker.key(d.vhost, d.queue));
    if (!q) return;
    // A replay after a restart finds the body already recovered from the store.
    if (q.replicas.has(d.messageId) || q.unacked.has(d.messageId)) return;
    if (q.ready.length && q.ready.extract((m) => m.id === d.messageId).length) {
      /* recovered copy dropped; the committed one is stored below */
    }
    this.broker.enqueueLocal(q, {
      body: d.body,
      exchange: d.exchange,
      routingKey: d.routingKey,
      headers: d.headers,
      propRaw: d.propRaw,
      persistent: d.persistent,
      priority: d.priority,
      expiration: d.expiration,
      id: d.messageId,
    }, 0);
  }

  /** Is a live key (`vhost\0queue\0id`) held by `group`? */
  private holds(group: Group, key: string): boolean {
    const [vhost, queue] = key.split("\0");
    const q = this.broker.queues.get(this.broker.key(vhost!, queue!));
    return (q?.raftGroup || QUORUM) === group;
  }

  private async install(group: Group, state: unknown) {
    if (group === META) {
      this.broker.installMetaSnapshot(state);
      return;
    }
    const stream = this.queueOfGroup(group);
    if (stream?.argsParsed.queueType === "stream") {
      this.broker.installStreamSnapshot(stream, state);
      return;
    }
    // The snapshot replaces the group's bodies as well as its tracking map.
    // Rebuild in snapshot order: retaining local copies would preserve stale
    // follower order, and absent queues must become empty too.
    const queues = ((state ?? {}) as { queues?: Array<{ messages?: Array<Record<string, unknown>> }> }).queues ?? [];
    const messages = new Map<string, Record<string, unknown>>();
    for (const queue of queues) for (const data of queue.messages ?? []) {
      const d = decodeQuorumAppend(data);
      const key = `${d.vhost}\0${d.queue}\0${d.messageId}`;
      const q = this.broker.queues.get(this.broker.key(d.vhost, d.queue));
      if (q?.argsParsed.queueType === "quorum" && this.holds(group, key)) messages.set(key, data);
    }
    for (const key of [...this.live.keys()]) {
      if (!this.holds(group, key)) continue;
      if (!messages.has(key)) {
        const [vhost, queue, id] = key.split("\0");
        this.broker.noteConsumed(vhost!, queue!, id!);
      }
      this.live.delete(key);
    }
    for (const q of this.broker.queues.values()) {
      if (q.argsParsed.queueType !== "quorum" || (q.raftGroup || QUORUM) !== group) continue;
      const bodies = [...q.ready.extract(() => true), ...q.replicas.values(), ...q.unacked.values()];
      q.replicas.clear();
      q.unacked.clear();
      const rows = new Set<number>();
      for (const m of bodies) {
        if (m.rowId != null) rows.add(m.rowId);
        // Removing a retained body is only replacement, not consumption.
        if (!messages.has(`${q.vhost}\0${q.name}\0${m.id}`)) this.broker.noteConsumed(q.vhost, q.name, m.id);
      }
      for (const row of rows) this.broker.store.deleteMessage(row);
    }
    for (const data of messages.values()) this.applyEnq(data);
  }

  private state(group: Group): unknown {
    if (group === META) return this.broker.snapshot();
    const stream = this.queueOfGroup(group);
    if (stream?.argsParsed.queueType === "stream") return this.broker.streamSnapshot(stream);
    const byQueue = new Map<string, { vhost: string; queue: string; messages: unknown[] }>();
    for (const [key, data] of this.live) {
      if (!this.holds(group, key)) continue;
      const vhost = String(data.vhost ?? "/");
      const queue = String(data.queue ?? "");
      const k = `${vhost}\0${queue}`;
      if (!byQueue.has(k)) byQueue.set(k, { vhost, queue, messages: [] });
      byQueue.get(k)!.messages.push(data);
    }
    return { queues: [...byQueue.values()] };
  }
}

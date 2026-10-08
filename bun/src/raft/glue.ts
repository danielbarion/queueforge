/**
 * Turning Raft on for Bun, and applying its commits (docs/raft.md,
 * sections 5, 6 and 8).
 *
 * Every hello advertises `features: ["raft"]`. Once each voter has done so,
 * `raft/enabled` is written to the data directory and the groups start.
 * Until then quorum queues keep majority-ack replication and metadata is
 * pushed. `QUEUEFORGE_RAFT=0` keeps a node on that version 1 behaviour.
 */
import { existsSync, fsyncSync, mkdirSync, openSync, closeSync, writeSync } from "node:fs";
import { join } from "node:path";
import type { Broker } from "../broker/index.ts";
import { decodeQuorumAppend } from "../wire.ts";
import type { Entry, Msg } from "./core.ts";
import { META, QUORUM, RaftNode, type Group } from "./node.ts";

export function raftSupported() {
  return process.env.QUEUEFORGE_RAFT !== "0";
}

/** The `features` list for a hello payload. */
export function helloFeatures(): string[] {
  return raftSupported() ? ["raft"] : [];
}

/** Raft state of one broker: who advertised it, and the running node. */
export class Consensus {
  node: RaftNode | null = null;
  private advertised = new Set<string>();
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

  stop() {
    this.node?.stop();
  }

  noteFeatures(peer: string, payload: Record<string, unknown> | undefined) {
    if (!peer || !raftSupported()) return;
    const list = Array.isArray(payload?.features) ? (payload!.features as unknown[]) : [];
    if (!list.includes("raft")) return;
    this.advertised.add(peer);
    this.maybeEnable();
  }

  membersChanged() {
    if (this.node) this.node.setVoters(this.broker.cfg.members.map((m) => m.id));
    else this.maybeEnable();
  }

  private maybeEnable() {
    if (this.node || !raftSupported()) return;
    const self = this.broker.cfg.nodeId;
    if (!this.broker.cfg.members.every((m) => m.id === self || this.advertised.has(m.id))) return;
    mkdirSync(this.dir(), { recursive: true });
    const path = join(this.dir(), "enabled");
    const fd = openSync(path, "w");
    writeSync(fd, "raft\n");
    fsyncSync(fd);
    closeSync(fd);
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
      },
    });
  }

  step(from: string, payload: Msg) {
    this.node?.step(from, payload);
  }

  private async apply(group: Group, entry: Entry) {
    if (entry.kind === "noop" || entry.kind === "config") return;
    const data = (entry.data ?? {}) as Record<string, unknown>;
    if (group === META) {
      if (entry.kind === "members") return;
      this.broker.applyRemote(entry.kind, data);
      return;
    }
    if (entry.kind === "enq") this.applyEnq(data);
    else if (entry.kind === "drop") {
      const vhost = String(data.vhost ?? "/");
      const queue = String(data.queue ?? "");
      const ids = Array.isArray(data.ids) ? (data.ids as unknown[]).map(String) : [String(data.id ?? "")];
      const leader = this.broker.isQuorumLeader();
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
      if (!this.broker.isQuorumLeader()) {
        const q = this.broker.queues.get(this.broker.key(vhost, queue));
        if (q) q.replicas.clear();
      }
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

  private async install(group: Group, state: unknown) {
    if (group === META) {
      try {
        this.broker.applySnapshot(state as ReturnType<Broker["snapshot"]>);
      } catch {
        /* a snapshot from the other implementation keeps what it cannot read */
      }
      return;
    }
    this.live.clear();
    const queues = ((state ?? {}) as { queues?: Array<{ messages?: Array<Record<string, unknown>> }> }).queues ?? [];
    for (const q of queues) for (const m of q.messages ?? []) this.applyEnq(m);
  }

  private state(group: Group): unknown {
    if (group === META) return this.broker.snapshot();
    const byQueue = new Map<string, { vhost: string; queue: string; messages: unknown[] }>();
    for (const data of this.live.values()) {
      const vhost = String(data.vhost ?? "/");
      const queue = String(data.queue ?? "");
      const k = `${vhost}\0${queue}`;
      if (!byQueue.has(k)) byQueue.set(k, { vhost, queue, messages: [] });
      byQueue.get(k)!.messages.push(data);
    }
    return { queues: [...byQueue.values()] };
  }
}

/**
 * Durable-majority quorum enqueue. enqueueQuorum calls durableMajority.
 *
 * These functions are the Broker methods. Loading this file installs them.
 */
import { Broker } from "./class.ts";
import { fieldEq, fieldStr, replaceHeaderTable, writeTable, type Field } from "../codec.ts";
import type { Config } from "../config.ts";
import { ChanError } from "../errors.ts";
import { Store, type BindRow, type ExRow, type QueueRow } from "../store.ts";
import { encodeQuorumAppend } from "../wire.ts";
import { durableMajority, type MemberCopy } from "../quorum-confirm.ts";
import { rabbitPasswordHashMatches } from "./auth.ts";
import { parseArgs, deathHeaders, propsWithDeath, argsFromFields } from "./args.ts";
import { topicMatches, headersMatch, fnv1a, headerList, overflowOf, liveFrom, pickConsumer, queueHome } from "./routing.ts";
import { matchOne, policyItem, policyFromBody, fillPolicyArgs } from "./policy-data.ts";
import { BUILTIN, emptyProm, type Consumer, type LiveMsg, type MgmtChannel, type MgmtConnection, type MgmtConsumer, type Policy, type Prom, type QArgs, type QueueLive, type TopicPerm } from "./model.ts";


/**
 * Confirm a quorum publish only after a durable majority.
 *
 * @param q Quorum queue that receives the local copy.
 * @param src Body and routing fields. This method assigns the message id.
 * @returns True only when the local enqueue succeeded and `durableMajority` accepts the peer copies. Otherwise the local copy and the peer copies are dropped and the return is false. Fewer reachable peers than a majority returns false before the append.
 */
export async function enqueueQuorum(this: Broker,
  q: QueueLive,
  src: {
    body: Uint8Array;
    exchange: string;
    routingKey: string;
    headers: Array<[string, Field]>;
    propRaw: Uint8Array;
    persistent: boolean;
    priority: number;
    expiration: string;
  },
): Promise<boolean> {
  const members = this.cfg.members.length || 1;
  const majority = Math.floor(members / 2) + 1;
  const peers = this.cluster ? this.cluster.peerIds().filter((id) => id !== this.cfg.nodeId) : [];
  if (peers.length + 1 < majority) return false;
  const qid = `q-${this.cfg.nodeId}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const payload = encodeQuorumAppend({
    vhost: q.vhost,
    queue: q.name,
    messageId: qid,
    body: src.body,
    exchange: src.exchange,
    routingKey: src.routingKey,
    persistent: src.persistent,
  });
  const acked: string[] = [];
  const copies: MemberCopy[] = [];
  if (this.cluster) {
    for (const id of peers) {
      try {
        await this.cluster.call(id, "quorum_append", payload);
        acked.push(id);
        copies.push("durable");
      } catch {
        copies.push("memory");
      }
    }
  }
  const ok = this.enqueueLocal(q, { ...src, id: qid }, 0);
  if (ok) this.store.noteQuorumConfirm();
  if (ok) await this.store.whenDurable();
  copies.push(ok ? "durable" : "memory");
  if (!ok || !durableMajority(members, copies)) {
    await Promise.all(acked.map((id) => this.cluster!.call(id, "quorum_drop", { vhost: q.vhost, queue: q.name, id: qid }).catch(() => null)));
    if (ok) this.dropLocal(q.vhost, q.name, qid);
    return false;
  }
  return true;
}

/**
 * Name the current quorum leader.
 *
 * @returns The lowest sorted member id that is up when a majority of members are up. With no members, returns this node's id. Without a live majority, returns the lowest configured id even if that node is down.
 */
export function quorumLeader(this: Broker): string {
  const ids = this.cfg.members.map((member) => member.id);
  if (!ids.length) return this.cfg.nodeId;
  ids.sort();
  const up = new Set<string>([this.cfg.nodeId]);
  for (const id of this.cluster?.peerIds() ?? []) up.add(id);
  const live = ids.filter((id) => up.has(id));
  const majority = Math.floor(ids.length / 2) + 1;
  if (live.length < majority) return ids[0]!;
  return live[0]!;
}

/**
 * Report whether this process may confirm quorum publishes.
 *
 * @returns True when the cluster has no members, or when `quorumLeader` is this node. A follower must not serve the ready queue.
 */
export function isQuorumLeader(this: Broker): boolean {
  return !this.cfg.members.length || this.quorumLeader() === this.cfg.nodeId;
}

/** Move follower copies into the ready queue once this process is the live leader. */

/**
 * Move follower copies into the ready queue once this process is leader.
 *
 * @returns Nothing. A follower returns immediately and leaves `replicas` in place. Each promoted queue is pumped.
 */
export function promoteIfLeader(this: Broker) {
  if (!this.isQuorumLeader()) return;
  for (const q of this.queues.values()) {
    if (q.argsParsed.queueType !== "quorum" || q.replicas.length === 0) continue;
    q.ready.push(...q.replicas);
    q.replicas = [];
    this.pump(q);
  }
}

/**
 * Ask other members to drop one quorum message.
 *
 * @param q Queue whose vhost and name identify the message.
 * @param id Message id to drop. This process is not dropped here.
 * @returns Nothing. A peer error is ignored. The caller drops the local copy separately.
 */
export async function quorumDrop(this: Broker, q: QueueLive, id: string) {
  const peers = this.cluster?.peerIds().filter((peer) => peer !== this.cfg.nodeId) ?? [];
  await Promise.all(peers.map((peer) => this.cluster!.call(peer, "quorum_drop", { vhost: q.vhost, queue: q.name, id }).catch(() => null)));
}

/**
 * Remove one message from this process.
 *
 * @param vhost Vhost of the queue.
 * @param queue Queue name. A missing queue still records the id as consumed.
 * @param id Message id removed from ready, replicas, and unacked.
 * @returns Nothing. A stored row for that id is deleted.
 */
export function dropLocal(this: Broker, vhost: string, queue: string, id: string) {
  this.noteConsumed(vhost, queue, id);
  const q = this.queues.get(this.key(vhost, queue));
  if (!q) return;
  const ready = q.ready.filter((m) => m.id === id);
  q.ready = q.ready.filter((m) => m.id !== id);
  const replicas = q.replicas.filter((m) => m.id === id);
  q.replicas = q.replicas.filter((m) => m.id !== id);
  for (const m of ready.concat(replicas)) if (m.rowId != null) this.store.deleteMessage(m.rowId);
  const held = q.unacked.get(id);
  if (held) {
    q.unacked.delete(id);
    if (held.rowId != null) this.store.deleteMessage(held.rowId);
  }
}

/**
 * Remember a message id that must not be delivered again.
 *
 * @param vhost Vhost of the queue.
 * @param queue Queue name.
 * @param id Message id. An empty id is ignored. A duplicate triple is ignored.
 * @returns Nothing. The list is what a later snapshot asks peers to drop.
 */
export function noteConsumed(this: Broker, vhost: string, queue: string, id: string) {
  if (!id || this.consumed.some((item) => item.vhost === vhost && item.queue === queue && item.id === id)) return;
  this.consumed.push({ vhost, queue, id });
}

/**
 * Drop messages a peer already consumed.
 *
 * @param items Records with `vhost`, `queue`, and `id`. Undefined is an empty list. A record missing any of the three is skipped.
 * @returns Nothing. Each complete record is noted and removed locally.
 */
export function applyConsumed(this: Broker, items: Array<{ vhost?: string; queue?: string; id?: string }> | undefined) {
  for (const item of items ?? []) {
    const vhost = String(item.vhost ?? "");
    const queue = String(item.queue ?? "");
    const id = String(item.id ?? "");
    if (!vhost || !queue || !id) continue;
    this.noteConsumed(vhost, queue, id);
    this.dropLocal(vhost, queue, id);
  }
}

Broker.prototype.enqueueQuorum = enqueueQuorum;
Broker.prototype.quorumLeader = quorumLeader;
Broker.prototype.isQuorumLeader = isQuorumLeader;
Broker.prototype.promoteIfLeader = promoteIfLeader;
Broker.prototype.quorumDrop = quorumDrop;
Broker.prototype.dropLocal = dropLocal;
Broker.prototype.noteConsumed = noteConsumed;
Broker.prototype.applyConsumed = applyConsumed;

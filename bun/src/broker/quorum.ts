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
import { QUORUM } from "../raft/node.ts";
import { durableMajority, selectQuorumPeers, type MemberCopy } from "../quorum-confirm.ts";
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
  if (this.cluster?.consensus?.node) return this.enqueueQuorumRaft(q, src);
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
  const neededFromPeers = Math.max(0, majority - 1);
  // A peer the majority does not need stays out of this publish once it is
  // already holding EXTRA_APPEND_CAP appends. The confirm does not wait for
  // that peer, and the skipped call is not counted as a durable copy.
  const chosen = selectQuorumPeers(peers, this.appendInflight, neededFromPeers);
  for (const id of chosen) this.appendInflight.set(id, (this.appendInflight.get(id) ?? 0) + 1);
  const release = (id: string) => {
    const left = (this.appendInflight.get(id) ?? 1) - 1;
    if (left <= 0) this.appendInflight.delete(id);
    else this.appendInflight.set(id, left);
  };
  // Peer fsync and the local fsync cover the same append. Waiting for the
  // peers to finish before starting the local one added a second group-commit
  // interval to every quorum confirm.
  const peersDone = new Promise<void>((resolve) => {
    if (!this.cluster || chosen.length === 0 || neededFromPeers === 0) {
      for (const id of chosen) release(id);
      resolve();
      return;
    }
    const cluster = this.cluster;
    let settled = 0;
    let finished = false;
    const finish = () => {
      if (finished) return;
      if (acked.length >= neededFromPeers || settled >= chosen.length) {
        finished = true;
        resolve();
      }
    };
    for (const id of chosen) {
      cluster.call(id, "quorum_append", payload).then(
        () => {
          acked.push(id);
          copies.push("durable");
          settled++;
          release(id);
          finish();
        },
        () => {
          copies.push("memory");
          settled++;
          release(id);
          finish();
        },
      );
    }
  });
  // The body stays at the head until both fsyncs finish, so a consumer cannot
  // take it before the peer copy exists.
  const confirmGate = { hold: true };
  const localTask = (async () => {
    const stored = this.enqueueLocal(q, { ...src, id: qid, confirmGate }, 0);
    if (stored) await this.store.whenDurable();
    return stored;
  })();
  const [, ok] = await Promise.all([peersDone, localTask]);
  copies.push(ok ? "durable" : "memory");
  if (!ok || !durableMajority(members, copies)) {
    await Promise.all(acked.map((id) => this.cluster!.call(id, "quorum_drop", { vhost: q.vhost, queue: q.name, id: qid }).catch(() => null)));
    if (ok) this.dropLocal(q.vhost, q.name, qid);
    this.pump(q);
    return false;
  }
  confirmGate.hold = false;
  this.pump(q);
  return true;
}

/**
 * Raft quorum publish: store the copy here and commit an `enq` in the
 * quorum group (docs/raft.md, section 6). The confirm is the commit.
 *
 * @returns True once the entry committed; false (and no copy) otherwise.
 */
export async function enqueueQuorumRaft(this: Broker, q: QueueLive, src: Parameters<Broker["enqueueQuorum"]>[1]): Promise<boolean> {
  const consensus = this.cluster!.consensus;
  const node = consensus.node!;
  const qid = `q-${this.cfg.nodeId}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const payload = encodeQuorumAppend({
    vhost: q.vhost,
    queue: q.name,
    messageId: qid,
    body: src.body,
    exchange: src.exchange,
    routingKey: src.routingKey,
    persistent: src.persistent,
  }) as Record<string, unknown>;
  const key = `${q.vhost}\0${q.name}\0${qid}`;
  // Known before the commit applies here, so that apply is a no-op.
  consensus.live.set(key, payload);
  const confirmGate = { hold: true };
  if (!this.enqueueLocal(q, { ...src, id: qid, confirmGate }, 0)) {
    consensus.live.delete(key);
    return false;
  }
  try {
    await node.propose(QUORUM, "enq", payload);
  } catch {
    consensus.live.delete(key);
    this.dropLocal(q.vhost, q.name, qid);
    this.pump(q);
    return false;
  }
  confirmGate.hold = false;
  this.pump(q);
  return true;
}

/**
 * Name the current quorum leader.
 *
 * @returns The lowest sorted member id that is up when a majority of members are up. With no members, returns this node's id. Without a live majority, returns the lowest configured id even if that node is down.
 */
export function quorumLeader(this: Broker): string {
  // With Raft on, the quorum group's election names it.
  const node = this.cluster?.consensus?.node;
  if (node) return node.leader(QUORUM) ?? "";
  const ids = this.cfg.members.map((member) => member.id);
  if (!ids.length) return this.cfg.nodeId;
  ids.sort();
  const known = new Set(ids);
  const up = new Set<string>([this.cfg.nodeId]);
  for (const id of this.cluster?.peerIds() ?? []) if (known.has(id)) up.add(id);
  const live = ids.filter((id) => up.has(id));
  const majority = Math.floor(ids.length / 2) + 1;
  if (live.length < majority) return ids[0]!;
  return live[0]!;
}

/**
 * Wait, up to two election timeouts, for the quorum group to have a leader.
 *
 * @returns Nothing. Without Raft there is always a leader, so it returns at once. A consume or get during an election would otherwise go to an empty home.
 */
export async function waitQuorumLeader(this: Broker): Promise<void> {
  const node = this.cluster?.consensus?.node;
  if (!node) return;
  const deadline = Date.now() + 4000;
  // A leader that is not connected is the one that just died; a new one is being elected.
  const usable = () => {
    const leader = node.leader(QUORUM);
    return leader !== null && (leader === this.cfg.nodeId || this.cluster!.peers.has(leader));
  };
  while (!usable() && Date.now() < deadline) await Bun.sleep(20);
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
  if (this.quorumHold || !this.isQuorumLeader()) return;
  for (const q of this.queues.values()) {
    if (q.argsParsed.queueType !== "quorum" || q.replicas.size === 0) continue;
    for (const replica of q.replicas.values()) q.ready.push(replica);
    q.replicas.clear();
    this.pump(q);
  }
}

const UNREACHABLE_STRIKES = 5;

/**
 * End the hold once every other member is heard or has refused five dials.
 *
 * @returns Nothing. Unconsumed replicas are promoted and `/readyz` flips when the hold ends.
 */
export function releaseQuorumHold(this: Broker) {
  if (!this.quorumHold) return;
  const others = this.cfg.members.filter((member) => member.id !== this.cfg.nodeId);
  const done = others.every((member) => this.heardPeers.has(member.id) || this.downPeers.has(member.id));
  if (others.length > 0 && !done) return;
  this.quorumHold = false;
  this.promoteIfLeader();
  this.ready = true;
}

/**
 * Count one peer hello toward quorum catchup.
 *
 * @param id Peer node id. Empty and self are ignored. The hold ends when every other member is heard or down, then unconsumed replicas are promoted and `/readyz` flips.
 * @returns Nothing.
 */
export function noteQuorumPeer(this: Broker, id: string) {
  if (!this.quorumHold || !id || id === this.cfg.nodeId) return;
  this.heardPeers.add(id);
  this.downStrikes.delete(id);
  this.releaseQuorumHold();
}

/**
 * Count one refused dial toward quorum catchup.
 *
 * @param id Peer node id. A peer already heard is ignored. The fifth refusal marks it down.
 * @returns Nothing.
 */
export function noteQuorumDown(this: Broker, id: string) {
  if (!this.quorumHold || !id || id === this.cfg.nodeId) return;
  if (this.heardPeers.has(id) || this.downPeers.has(id)) return;
  const n = (this.downStrikes.get(id) ?? 0) + 1;
  this.downStrikes.set(id, n);
  if (n < UNREACHABLE_STRIKES) return;
  this.downPeers.add(id);
  this.releaseQuorumHold();
}

/**
 * Forget refused dials after a successful connect.
 *
 * @param id Peer node id. The peer stays required until its hello arrives.
 * @returns Nothing.
 */
export function clearQuorumStrikes(this: Broker, id: string) {
  if (id) this.downStrikes.delete(id);
}

/**
 * Ask other members to drop one quorum message.
 *
 * @param q Queue whose vhost and name identify the message.
 * @param id Message id to drop. This process is not dropped here.
 * @returns True when every peer accepted the drop. A peer error returns false so the caller can retry. No peers returns true.
 */
export async function quorumDrop(this: Broker, q: QueueLive, id: string): Promise<boolean> {
  const node = this.cluster?.consensus?.node;
  if (node) {
    // Committed before the body is written (docs/raft.md, section 6).
    return node.propose(QUORUM, "drop", { vhost: q.vhost, queue: q.name, ids: [id] }).then(
      () => true,
      () => false,
    );
  }
  const peers = this.cluster?.peerIds().filter((peer) => peer !== this.cfg.nodeId) ?? [];
  if (peers.length === 0) return true;
  const results = await Promise.all(
    peers.map((peer) =>
      this.cluster!.call(peer, "quorum_drop", { vhost: q.vhost, queue: q.name, id }).then(
        () => true,
        () => false,
      ),
    ),
  );
  return results.every(Boolean);
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
  // Follower copies live in `replicas`. An empty ready set has nothing to scan.
  const ready = q.ready.length === 0 ? [] : q.ready.extract((m) => m.id === id);
  const replica = q.replicas.get(id);
  if (replica) q.replicas.delete(id);
  for (const m of replica ? ready.concat(replica) : ready) if (m.rowId != null) this.store.deleteMessage(m.rowId);
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
  if (!id) return;
  this.consumedIds.add(vhost, queue, id);
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
Broker.prototype.enqueueQuorumRaft = enqueueQuorumRaft;
Broker.prototype.quorumLeader = quorumLeader;
Broker.prototype.isQuorumLeader = isQuorumLeader;
Broker.prototype.waitQuorumLeader = waitQuorumLeader;
Broker.prototype.promoteIfLeader = promoteIfLeader;
Broker.prototype.noteQuorumPeer = noteQuorumPeer;
Broker.prototype.releaseQuorumHold = releaseQuorumHold;
Broker.prototype.noteQuorumDown = noteQuorumDown;
Broker.prototype.clearQuorumStrikes = clearQuorumStrikes;
Broker.prototype.quorumDrop = quorumDrop;
Broker.prototype.dropLocal = dropLocal;
Broker.prototype.noteConsumed = noteConsumed;
Broker.prototype.applyConsumed = applyConsumed;

declare module "./class.ts" {
  interface Broker {
    enqueueQuorum: typeof enqueueQuorum;
    enqueueQuorumRaft: typeof enqueueQuorumRaft;
    quorumLeader: typeof quorumLeader;
    isQuorumLeader: typeof isQuorumLeader;
    waitQuorumLeader: typeof waitQuorumLeader;
    promoteIfLeader: typeof promoteIfLeader;
    noteQuorumPeer: typeof noteQuorumPeer;
    releaseQuorumHold: typeof releaseQuorumHold;
    noteQuorumDown: typeof noteQuorumDown;
    clearQuorumStrikes: typeof clearQuorumStrikes;
    quorumDrop: typeof quorumDrop;
    dropLocal: typeof dropLocal;
    noteConsumed: typeof noteConsumed;
    applyConsumed: typeof applyConsumed;
  }
}

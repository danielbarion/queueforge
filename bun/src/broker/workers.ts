/**
 * One broker as many workers (cores mode): answers that must cover every
 * worker, not just the one a client reached.
 *
 * Each worker has the whole topology but serves only the queues homed on it,
 * the connections handed to it, and the direct reply-to consumers on those.
 * Management, passive declares, connection limits and replies ask the worker
 * that holds the state, or every worker, over the cluster link. None of this
 * runs on the publish, deliver or ack paths.
 */
import { Broker } from "./class.ts";
import type { MgmtChannel, MgmtConsumer, QueueLive } from "./model.ts";
import type { MgmtConnection } from "./model.ts";

/** Live counts of one queue, as its serving worker reports them. */
export type QueueStats = { vhost: string; name: string; ready: number; unacked: number; consumers: number };

/** Management rows of one worker, without the close callbacks. */
export type MgmtRows = {
  connections: Array<Omit<MgmtConnection, "close">>;
  channels: MgmtChannel[];
  consumers: MgmtConsumer[];
};

/** Whether this process holds the messages of `q`. */
function serves(this: Broker, q: QueueLive): boolean {
  if (q.argsParsed.queueType === "quorum") return this.isQuorumLeader(q);
  return this.isLocalHome(q.home);
}

/** The worker that holds the messages of `q`, when it is another one of this broker's workers. */
export function servingWorker(this: Broker, q: QueueLive): string | null {
  if (!this.workerPool() || !this.cluster || q.argsParsed.queueType === "stream") return null;
  const at = q.argsParsed.queueType === "quorum" ? this.quorumLeader(q) : q.home;
  if (!at || at === this.cfg.nodeId || !this.cluster.peers.has(at)) return null;
  return at;
}

/**
 * Ready and unacked messages and consumers of a queue, from the worker that serves it.
 *
 * @param touch Count the call as use of the queue for `x-expires` there.
 * @returns This process's own counts outside cores mode, for a queue served
 * here, or when the serving worker does not answer.
 */
export async function queueCounts(this: Broker, q: QueueLive, touch: boolean): Promise<{ messages: number; consumers: number; unacked: number }> {
  const at = this.servingWorker(q);
  if (at) {
    try {
      const r = (await this.cluster!.call(at, "stats", { vhost: q.vhost, queue: q.name, touch })) as Record<string, unknown> | null;
      if (r) return { messages: Number(r.messages_ready ?? 0), consumers: Number(r.consumer_count ?? 0), unacked: Number(r.messages_unacknowledged ?? 0) };
    } catch {
      /* fall back to what this worker knows */
    }
  }
  if (touch) q.lastUsed = Date.now();
  return { messages: q.ready.length, consumers: q.consumers.length, unacked: q.unacked.size };
}

/** Counts of the queues this worker serves, optionally in one vhost. */
export function servedQueueStats(this: Broker, vhost?: string): QueueStats[] {
  const out: QueueStats[] = [];
  for (const q of this.queues.values()) {
    if (vhost != null && q.vhost !== vhost) continue;
    if (q.argsParsed.queueType === "stream" || !serves.call(this, q)) continue;
    out.push({ vhost: q.vhost, name: q.name, ready: q.ready.length, unacked: q.unacked.size, consumers: q.consumers.length });
  }
  return out;
}

/**
 * Counts of every queue as their serving workers see them, keyed by
 * `this.key(vhost, name)`. Empty outside cores mode: callers use their own.
 */
export async function workerQueueStats(this: Broker, vhost?: string): Promise<Map<string, QueueStats>> {
  const out = new Map<string, QueueStats>();
  if (!this.workerPool() || !this.cluster) return out;
  for (const { reply } of await this.cluster.callAll("queue_stats", vhost == null ? {} : { vhost })) {
    if (!Array.isArray(reply)) continue;
    for (const s of reply as QueueStats[]) out.set(this.key(s.vhost, s.name), s);
  }
  return out;
}

/** Open connections and channels of one user, and connections of one vhost, on this worker. */
export function localConnCounts(this: Broker, user: string, vhost: string) {
  let userConns = 0;
  let vhostConns = 0;
  for (const c of this.mgmtConnections.values()) {
    if (c.user === user) userConns++;
    if (c.vhost === vhost) vhostConns++;
  }
  let userChannels = 0;
  for (const c of this.mgmtChannels.values()) if (c.user === user) userChannels++;
  return { user: userConns, vhost: vhostConns, channels: userChannels };
}

/** Totals of {@link localConnCounts} over every worker. */
async function connCounts(this: Broker, user: string, vhost: string) {
  const sum = this.localConnCounts(user, vhost);
  if (!this.workerPool() || !this.cluster) return sum;
  for (const { reply } of await this.cluster.callAll("conn_counts", { user, vhost }, 1000)) {
    const r = reply as Partial<typeof sum> | null;
    sum.user += Number(r?.user ?? 0);
    sum.vhost += Number(r?.vhost ?? 0);
    sum.channels += Number(r?.channels ?? 0);
  }
  return sum;
}

/**
 * `connectionAllowed` across every worker of this broker. Asks the others
 * only when the user or the vhost has a connection limit.
 */
export async function connectionAllowedAll(this: Broker, user: string, vhost: string): Promise<boolean> {
  if (!this.connectionAllowed(user, vhost)) return false;
  const userMax = this.userConnLimit.get(user);
  const vhostMax = this.vhostConnLimit.get(vhost);
  if ((userMax == null && vhostMax == null) || !this.workerPool()) return true;
  const n = await connCounts.call(this, user, vhost);
  if (userMax != null && n.user >= userMax) return false;
  if (vhostMax != null && n.vhost >= vhostMax) return false;
  return true;
}

/** `channelAllowed` across every worker. Asks the others only when the user has a channel limit. */
export async function channelAllowedAll(this: Broker, user: string): Promise<boolean> {
  if (!this.channelAllowed(user)) return false;
  const max = this.userChanLimit.get(user);
  if (max == null || !this.workerPool()) return true;
  return (await connCounts.call(this, user, "")).channels < max;
}

/** This worker's management rows. */
export function localMgmtRows(this: Broker): MgmtRows {
  return { connections: this.listMgmtConnections(), channels: this.listMgmtChannels(), consumers: this.listMgmtConsumers() };
}

/** Management rows of every worker: this one's, then the others'. */
export async function allMgmtRows(this: Broker): Promise<MgmtRows> {
  const out = this.localMgmtRows();
  if (!this.workerPool() || !this.cluster) return out;
  for (const { reply } of await this.cluster.callAll("mgmt_rows", {})) {
    const r = reply as Partial<MgmtRows> | null;
    out.connections.push(...(r?.connections ?? []));
    out.channels.push(...(r?.channels ?? []));
    out.consumers.push(...(r?.consumers ?? []));
  }
  out.connections.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

/** Close a connection on whichever worker holds it. */
export async function closeMgmtConnectionAll(this: Broker, name: string): Promise<boolean> {
  if (this.closeMgmtConnection(name)) return true;
  if (!this.workerPool() || !this.cluster) return false;
  return (await this.cluster.callAll("close_connection", { name })).some(({ reply }) => reply === true);
}

/** Hand a reply to the direct reply-to consumer on this worker. False when it has gone. */
export function deliverReply(this: Broker, routingKey: string, body: Uint8Array, propRaw: Uint8Array): boolean {
  const sink = this.replySinks.get(routingKey);
  if (!sink) return false;
  if (sink({ routingKey, body, propRaw })) return true;
  this.replySinks.delete(routingKey);
  return false;
}

Broker.prototype.servingWorker = servingWorker;
Broker.prototype.queueCounts = queueCounts;
Broker.prototype.servedQueueStats = servedQueueStats;
Broker.prototype.workerQueueStats = workerQueueStats;
Broker.prototype.localConnCounts = localConnCounts;
Broker.prototype.connectionAllowedAll = connectionAllowedAll;
Broker.prototype.channelAllowedAll = channelAllowedAll;
Broker.prototype.localMgmtRows = localMgmtRows;
Broker.prototype.allMgmtRows = allMgmtRows;
Broker.prototype.closeMgmtConnectionAll = closeMgmtConnectionAll;
Broker.prototype.deliverReply = deliverReply;

declare module "./class.ts" {
  interface Broker {
    servingWorker: typeof servingWorker;
    queueCounts: typeof queueCounts;
    servedQueueStats: typeof servedQueueStats;
    workerQueueStats: typeof workerQueueStats;
    localConnCounts: typeof localConnCounts;
    connectionAllowedAll: typeof connectionAllowedAll;
    channelAllowedAll: typeof channelAllowedAll;
    localMgmtRows: typeof localMgmtRows;
    allMgmtRows: typeof allMgmtRows;
    closeMgmtConnectionAll: typeof closeMgmtConnectionAll;
    deliverReply: typeof deliverReply;
  }
}

/**
 * Management connection, channel, consumer, and limit rows.
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


/** Broker.openMgmtConnection. The parameters and return value are unchanged from the previous class method. */
export function openMgmtConnection(this: Broker, input: {
  user: string;
  vhost: string;
  peerHost: string;
  peerPort: number;
  close: () => void;
}): string {
  const name = `conn-${++this.connSeq}`;
  this.mgmtConnections.set(name, {
    name,
    user: input.user,
    vhost: input.vhost,
    peer_host: input.peerHost,
    peer_port: input.peerPort,
    channels: 0,
    connected_at: Math.floor(Date.now() / 1000),
    close: input.close,
  });
  return name;
}

/** Broker.setMgmtChannels. The parameters and return value are unchanged from the previous class method. */
export function setMgmtChannels(this: Broker, name: string, channels: number) {
  const row = this.mgmtConnections.get(name);
  if (row) row.channels = channels;
}

/** Broker.listMgmtConnections. The parameters and return value are unchanged from the previous class method. */
export function listMgmtConnections(this: Broker) {
  return [...this.mgmtConnections.values()]
    .map(({ close: _close, ...row }) => row)
    .sort((a, b) => a.name.localeCompare(b.name));
}

/** Broker.closeMgmtConnection. The parameters and return value are unchanged from the previous class method. */
export function closeMgmtConnection(this: Broker, name: string): boolean {
  const row = this.mgmtConnections.get(name);
  if (!row) return false;
  row.close();
  return true;
}

/** Broker.forgetMgmtConnection. The parameters and return value are unchanged from the previous class method. */
export function forgetMgmtConnection(this: Broker, name: string) {
  this.mgmtConnections.delete(name);
  this.clearMgmtChildren(name);
}

/** Broker.syncMgmtChannels. The parameters and return value are unchanged from the previous class method. */
export function syncMgmtChannels(this: Broker, conn: string, user: string, vhost: string, peerHost: string, peerPort: number, numbers: number[]) {
  for (const [name, row] of this.mgmtChannels) {
    if (row.connection === conn) this.mgmtChannels.delete(name);
  }
  for (const number of numbers) {
    const name = `${conn}:${number}`;
    this.mgmtChannels.set(name, { name, connection: conn, user, vhost, number, peer_host: peerHost, peer_port: peerPort });
  }
  const live = this.mgmtConnections.get(conn);
  if (live) live.channels = numbers.length;
}

/** Broker.clearMgmtChildren. The parameters and return value are unchanged from the previous class method. */
export function clearMgmtChildren(this: Broker, conn: string) {
  for (const [name, row] of this.mgmtChannels) {
    if (row.connection === conn) this.mgmtChannels.delete(name);
  }
  this.mgmtConsumers = this.mgmtConsumers.filter((c) => c.connection !== conn);
}

/** Broker.noteMgmtConsumer. The parameters and return value are unchanged from the previous class method. */
export function noteMgmtConsumer(this: Broker, row: MgmtConsumer) {
  this.mgmtConsumers = this.mgmtConsumers.filter(
    (c) => !(c.connection === row.connection && c.channel === row.channel && c.consumer_tag === row.consumer_tag),
  );
  this.mgmtConsumers.push(row);
}

/** Broker.forgetMgmtConsumer. The parameters and return value are unchanged from the previous class method. */
export function forgetMgmtConsumer(this: Broker, conn: string, channel: number, tag: string) {
  this.mgmtConsumers = this.mgmtConsumers.filter(
    (c) => !(c.connection === conn && c.channel === channel && c.consumer_tag === tag),
  );
}

/** Broker.forgetMgmtChannelConsumers. The parameters and return value are unchanged from the previous class method. */
export function forgetMgmtChannelConsumers(this: Broker, conn: string, channel: number) {
  this.mgmtConsumers = this.mgmtConsumers.filter((c) => !(c.connection === conn && c.channel === channel));
}

/** Broker.listMgmtChannels. The parameters and return value are unchanged from the previous class method. */
export function listMgmtChannels(this: Broker) {
  return [...this.mgmtChannels.values()].sort((a, b) => a.name.localeCompare(b.name));
}

/** Broker.getMgmtChannel. The parameters and return value are unchanged from the previous class method. */
export function getMgmtChannel(this: Broker, name: string) {
  return this.mgmtChannels.get(name) ?? null;
}

/** Broker.listMgmtConsumers. The parameters and return value are unchanged from the previous class method. */
export function listMgmtConsumers(this: Broker, vhost?: string, queue?: string) {
  return this.mgmtConsumers.filter((c) => (vhost == null || c.vhost === vhost) && (queue == null || c.queue === queue));
}

/** Broker.connectionAllowed. The parameters and return value are unchanged from the previous class method. */
export function connectionAllowed(this: Broker, user: string, vhost: string): boolean {
  const userMax = this.userConnLimit.get(user);
  if (userMax != null && [...this.mgmtConnections.values()].filter((c) => c.user === user).length >= userMax) return false;
  const vhostMax = this.vhostConnLimit.get(vhost);
  if (vhostMax != null && [...this.mgmtConnections.values()].filter((c) => c.vhost === vhost).length >= vhostMax) return false;
  return true;
}

/** Broker.channelAllowed. The parameters and return value are unchanged from the previous class method. */
export function channelAllowed(this: Broker, user: string): boolean {
  const max = this.userChanLimit.get(user);
  if (max == null) return true;
  return [...this.mgmtChannels.values()].filter((c) => c.user === user).length < max;
}

/** Broker.queueAllowed. The parameters and return value are unchanged from the previous class method. */
export function queueAllowed(this: Broker, vhost: string, current: number): boolean {
  const max = this.vhostQueueLimit.get(vhost);
  return max == null || current < max;
}

/** Broker.setUserLimit. The parameters and return value are unchanged from the previous class method. */
export function setUserLimit(this: Broker, user: string, connections: number | null, channels: number | null) {
  if (connections == null) this.userConnLimit.delete(user);
  else this.userConnLimit.set(user, connections);
  if (channels == null) this.userChanLimit.delete(user);
  else this.userChanLimit.set(user, channels);
}

/** Broker.setVhostLimit. The parameters and return value are unchanged from the previous class method. */
export function setVhostLimit(this: Broker, vhost: string, connections: number | null, queues: number | null) {
  if (connections == null) this.vhostConnLimit.delete(vhost);
  else this.vhostConnLimit.set(vhost, connections);
  if (queues == null) this.vhostQueueLimit.delete(vhost);
  else this.vhostQueueLimit.set(vhost, queues);
}

/** Broker.listUserLimits. The parameters and return value are unchanged from the previous class method. */
export function listUserLimits(this: Broker) {
  const users = new Set([...this.userConnLimit.keys(), ...this.userChanLimit.keys()]);
  return [...users].sort().map((user) => ({
    user,
    "max-connections": this.userConnLimit.get(user) ?? null,
    "max-channels": this.userChanLimit.get(user) ?? null,
  }));
}

/** Broker.listVhostLimits. The parameters and return value are unchanged from the previous class method. */
export function listVhostLimits(this: Broker) {
  const vhosts = new Set([...this.vhostConnLimit.keys(), ...this.vhostQueueLimit.keys()]);
  return [...vhosts].sort().map((vhost) => ({
    vhost,
    "max-connections": this.vhostConnLimit.get(vhost) ?? null,
    "max-queues": this.vhostQueueLimit.get(vhost) ?? null,
  }));
}

/** Broker.putTopicPerm. The parameters and return value are unchanged from the previous class method. */
export function putTopicPerm(this: Broker, perm: TopicPerm) {
  try {
    new RegExp(perm.write);
    new RegExp(perm.read);
  } catch {
    throw new Error("write and read must be valid patterns");
  }
  this.topicPerms = this.topicPerms.filter(
    (p) => !(p.user === perm.user && p.vhost === perm.vhost && p.exchange === perm.exchange),
  );
  this.topicPerms.push(perm);
}

/** Broker.deleteTopicPerm. The parameters and return value are unchanged from the previous class method. */
export function deleteTopicPerm(this: Broker, user: string, vhost: string, exchange: string): boolean {
  const before = this.topicPerms.length;
  this.topicPerms = this.topicPerms.filter((p) => !(p.user === user && p.vhost === vhost && p.exchange === exchange));
  return this.topicPerms.length !== before;
}

/** Broker.listTopicPerms. The parameters and return value are unchanged from the previous class method. */
export function listTopicPerms(this: Broker, user?: string) {
  return this.topicPerms
    .filter((p) => user == null || p.user === user)
    .sort((a, b) => a.user.localeCompare(b.user) || a.vhost.localeCompare(b.vhost) || a.exchange.localeCompare(b.exchange));
}

/** Broker.topicWriteAllowed. The parameters and return value are unchanged from the previous class method. */
export function topicWriteAllowed(this: Broker, user: string, vhost: string, exchange: string, routingKey: string): boolean {
  const perm = this.topicPerms.find((p) => p.user === user && p.vhost === vhost && p.exchange === exchange);
  if (!perm) return true;
  try {
    return new RegExp(perm.write).test(routingKey);
  } catch {
    return false;
  }
}

/** Broker.topicReadAllowed. The parameters and return value are unchanged from the previous class method. */
export function topicReadAllowed(this: Broker, user: string, vhost: string, exchange: string, routingKey: string): boolean {
  const perm = this.topicPerms.find((p) => p.user === user && p.vhost === vhost && p.exchange === exchange);
  if (!perm) return true;
  try {
    return new RegExp(perm.read).test(routingKey);
  } catch {
    return false;
  }
}

Broker.prototype.openMgmtConnection = openMgmtConnection;
Broker.prototype.setMgmtChannels = setMgmtChannels;
Broker.prototype.listMgmtConnections = listMgmtConnections;
Broker.prototype.closeMgmtConnection = closeMgmtConnection;
Broker.prototype.forgetMgmtConnection = forgetMgmtConnection;
Broker.prototype.syncMgmtChannels = syncMgmtChannels;
Broker.prototype.clearMgmtChildren = clearMgmtChildren;
Broker.prototype.noteMgmtConsumer = noteMgmtConsumer;
Broker.prototype.forgetMgmtConsumer = forgetMgmtConsumer;
Broker.prototype.forgetMgmtChannelConsumers = forgetMgmtChannelConsumers;
Broker.prototype.listMgmtChannels = listMgmtChannels;
Broker.prototype.getMgmtChannel = getMgmtChannel;
Broker.prototype.listMgmtConsumers = listMgmtConsumers;
Broker.prototype.connectionAllowed = connectionAllowed;
Broker.prototype.channelAllowed = channelAllowed;
Broker.prototype.queueAllowed = queueAllowed;
Broker.prototype.setUserLimit = setUserLimit;
Broker.prototype.setVhostLimit = setVhostLimit;
Broker.prototype.listUserLimits = listUserLimits;
Broker.prototype.listVhostLimits = listVhostLimits;
Broker.prototype.putTopicPerm = putTopicPerm;
Broker.prototype.deleteTopicPerm = deleteTopicPerm;
Broker.prototype.listTopicPerms = listTopicPerms;
Broker.prototype.topicWriteAllowed = topicWriteAllowed;
Broker.prototype.topicReadAllowed = topicReadAllowed;

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


/**
 * Record one open AMQP connection for the management API.
 *
 * @param input User, vhost, peer address, and the function that closes the socket.
 * @returns The connection name `conn-` plus a counter. The close function is kept and is not returned by the list method.
 */
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

/**
 * Store the open channel count for one connection.
 *
 * @param name Connection name from `openMgmtConnection`. An unknown name is ignored.
 * @param channels Count to store. It is not checked against the live channel table.
 * @returns Nothing.
 */
export function setMgmtChannels(this: Broker, name: string, channels: number) {
  const row = this.mgmtConnections.get(name);
  if (row) row.channels = channels;
}

/**
 * List management connection rows.
 *
 * @returns Rows sorted by name, without the close function. The caller can mutate a returned row and that changes the broker's record.
 */
export function listMgmtConnections(this: Broker) {
  return [...this.mgmtConnections.values()]
    .map(({ close: _close, ...row }) => row)
    .sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Close one connection from the management API.
 *
 * @param name Connection name.
 * @returns True when the close function was called. False when the name is unknown. The row is not removed here.
 */
export function closeMgmtConnection(this: Broker, name: string): boolean {
  const row = this.mgmtConnections.get(name);
  if (!row) return false;
  row.close();
  return true;
}

/**
 * Drop a connection and its channels and consumers.
 *
 * @param name Connection name. An unknown name still clears children with that name.
 * @returns Nothing. The socket is not closed.
 */
export function forgetMgmtConnection(this: Broker, name: string) {
  this.mgmtConnections.delete(name);
  this.clearMgmtChildren(name);
}

/**
 * Replace the channel rows for one connection.
 *
 * @param conn Connection name.
 * @param user User stored on each new channel row.
 * @param vhost Vhost stored on each new channel row.
 * @param peerHost Peer host stored on each new channel row.
 * @param peerPort Peer port stored on each new channel row.
 * @param numbers Channel numbers that are open now. Previous rows for `conn` are deleted first.
 * @returns Nothing. The connection's channel count becomes `numbers.length` when that connection exists.
 */
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

/**
 * Remove channel and consumer rows for one connection.
 *
 * @param conn Connection name.
 * @returns Nothing. The connection row itself stays.
 */
export function clearMgmtChildren(this: Broker, conn: string) {
  for (const [name, row] of this.mgmtChannels) {
    if (row.connection === conn) this.mgmtChannels.delete(name);
  }
  this.mgmtConsumers = this.mgmtConsumers.filter((c) => c.connection !== conn);
}

/**
 * Record one consumer for the management API.
 *
 * @param row Consumer row. An earlier row with the same connection, channel, and tag is removed first.
 * @returns Nothing.
 */
export function noteMgmtConsumer(this: Broker, row: MgmtConsumer) {
  this.mgmtConsumers = this.mgmtConsumers.filter(
    (c) => !(c.connection === row.connection && c.channel === row.channel && c.consumer_tag === row.consumer_tag),
  );
  this.mgmtConsumers.push(row);
}

/**
 * Remove one management consumer row.
 *
 * @param conn Connection name.
 * @param channel Channel number.
 * @param tag Consumer tag. Other tags on that channel stay.
 * @returns Nothing.
 */
export function forgetMgmtConsumer(this: Broker, conn: string, channel: number, tag: string) {
  this.mgmtConsumers = this.mgmtConsumers.filter(
    (c) => !(c.connection === conn && c.channel === channel && c.consumer_tag === tag),
  );
}

/**
 * Remove every management consumer on one channel.
 *
 * @param conn Connection name.
 * @param channel Channel number.
 * @returns Nothing. Other channels on that connection stay.
 */
export function forgetMgmtChannelConsumers(this: Broker, conn: string, channel: number) {
  this.mgmtConsumers = this.mgmtConsumers.filter((c) => !(c.connection === conn && c.channel === channel));
}

/**
 * List management channel rows.
 *
 * @returns Rows sorted by name. The array is a copy. The row objects are the live records.
 */
export function listMgmtChannels(this: Broker) {
  return [...this.mgmtChannels.values()].sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Read one management channel row.
 *
 * @param name Channel name, `connection` plus `:` plus the channel number.
 * @returns The row, or null when the name is absent.
 */
export function getMgmtChannel(this: Broker, name: string) {
  return this.mgmtChannels.get(name) ?? null;
}

/**
 * List management consumer rows.
 *
 * @param vhost When set, only that vhost is returned. Omitted includes every vhost.
 * @param queue When set, only that queue is returned. Omitted includes every queue.
 * @returns The matching rows. The array is a copy of the filter, and the row objects are live.
 */
export function listMgmtConsumers(this: Broker, vhost?: string, queue?: string) {
  return this.mgmtConsumers.filter((c) => (vhost == null || c.vhost === vhost) && (queue == null || c.queue === queue));
}

/**
 * Test the user and vhost connection limits.
 *
 * @param user User whose `max-connections` limit is checked.
 * @param vhost Vhost whose `max-connections` limit is checked.
 * @returns False when either limit is set and the current open count has reached it. A missing limit is unlimited.
 */
export function connectionAllowed(this: Broker, user: string, vhost: string): boolean {
  const userMax = this.userConnLimit.get(user);
  if (userMax != null && [...this.mgmtConnections.values()].filter((c) => c.user === user).length >= userMax) return false;
  const vhostMax = this.vhostConnLimit.get(vhost);
  if (vhostMax != null && [...this.mgmtConnections.values()].filter((c) => c.vhost === vhost).length >= vhostMax) return false;
  return true;
}

/**
 * Test the user's channel limit.
 *
 * @param user User whose `max-channels` limit is checked.
 * @returns True when the user has no limit or is still under it. The count is the management channel table, not the AMQP session table.
 */
export function channelAllowed(this: Broker, user: string): boolean {
  const max = this.userChanLimit.get(user);
  if (max == null) return true;
  return [...this.mgmtChannels.values()].filter((c) => c.user === user).length < max;
}

/**
 * Test the vhost queue limit.
 *
 * @param vhost Vhost whose `max-queues` limit is checked.
 * @param current Queues already in that vhost. This method does not count them.
 * @returns True when the vhost has no limit or `current` is still below it.
 */
export function queueAllowed(this: Broker, vhost: string, current: number): boolean {
  const max = this.vhostQueueLimit.get(vhost);
  return max == null || current < max;
}

/**
 * Set or clear one user's connection and channel limits.
 *
 * @param user User name.
 * @param connections Maximum connections, or null to clear that limit.
 * @param channels Maximum channels, or null to clear that limit.
 * @returns Nothing. Existing connections are not closed.
 */
export function setUserLimit(this: Broker, user: string, connections: number | null, channels: number | null) {
  if (connections == null) this.userConnLimit.delete(user);
  else this.userConnLimit.set(user, connections);
  if (channels == null) this.userChanLimit.delete(user);
  else this.userChanLimit.set(user, channels);
}

/**
 * Set or clear one vhost's connection and queue limits.
 *
 * @param vhost Vhost name.
 * @param connections Maximum connections, or null to clear that limit.
 * @param queues Maximum queues, or null to clear that limit.
 * @returns Nothing. Existing queues are not deleted.
 */
export function setVhostLimit(this: Broker, vhost: string, connections: number | null, queues: number | null) {
  if (connections == null) this.vhostConnLimit.delete(vhost);
  else this.vhostConnLimit.set(vhost, connections);
  if (queues == null) this.vhostQueueLimit.delete(vhost);
  else this.vhostQueueLimit.set(vhost, queues);
}

/**
 * List users that have a connection or channel limit.
 *
 * @returns One row per user, sorted by name. A limit that was cleared is null.
 */
export function listUserLimits(this: Broker) {
  const users = new Set([...this.userConnLimit.keys(), ...this.userChanLimit.keys()]);
  return [...users].sort().map((user) => ({
    user,
    "max-connections": this.userConnLimit.get(user) ?? null,
    "max-channels": this.userChanLimit.get(user) ?? null,
  }));
}

/**
 * List vhosts that have a connection or queue limit.
 *
 * @returns One row per vhost, sorted by name. A limit that was cleared is null.
 */
export function listVhostLimits(this: Broker) {
  const vhosts = new Set([...this.vhostConnLimit.keys(), ...this.vhostQueueLimit.keys()]);
  return [...vhosts].sort().map((vhost) => ({
    vhost,
    "max-connections": this.vhostConnLimit.get(vhost) ?? null,
    "max-queues": this.vhostQueueLimit.get(vhost) ?? null,
  }));
}

/**
 * Replace one topic permission.
 *
 * @param perm User, vhost, exchange, and the write and read patterns. A pattern that is not a valid regular expression throws before the table changes.
 * @returns Nothing. The previous row for that user, vhost, and exchange is removed.
 */
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

/**
 * Remove one topic permission.
 *
 * @param user User name.
 * @param vhost Vhost name.
 * @param exchange Exchange name.
 * @returns True when a row was removed. False when that triple was absent.
 */
export function deleteTopicPerm(this: Broker, user: string, vhost: string, exchange: string): boolean {
  const before = this.topicPerms.length;
  this.topicPerms = this.topicPerms.filter((p) => !(p.user === user && p.vhost === vhost && p.exchange === exchange));
  return this.topicPerms.length !== before;
}

/**
 * List topic permissions.
 *
 * @param user When set, only that user is returned. Omitted includes every user.
 * @returns Rows sorted by user, vhost, then exchange.
 */
export function listTopicPerms(this: Broker, user?: string) {
  return this.topicPerms
    .filter((p) => user == null || p.user === user)
    .sort((a, b) => a.user.localeCompare(b.user) || a.vhost.localeCompare(b.vhost) || a.exchange.localeCompare(b.exchange));
}

/**
 * Test a topic write pattern.
 *
 * @param user User name.
 * @param vhost Vhost name.
 * @param exchange Exchange name. No matching permission returns true.
 * @param routingKey Key tested against the write pattern.
 * @returns True when the pattern matches. An invalid stored pattern returns false.
 */
export function topicWriteAllowed(this: Broker, user: string, vhost: string, exchange: string, routingKey: string): boolean {
  const perm = this.topicPerms.find((p) => p.user === user && p.vhost === vhost && p.exchange === exchange);
  if (!perm) return true;
  try {
    return new RegExp(perm.write).test(routingKey);
  } catch {
    return false;
  }
}

/**
 * Test a topic read pattern.
 *
 * @param user User name.
 * @param vhost Vhost name.
 * @param exchange Exchange name. No matching permission returns true.
 * @param routingKey Key tested against the read pattern.
 * @returns True when the pattern matches. An invalid stored pattern returns false.
 */
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

declare module "./class.ts" {
  interface Broker {
    openMgmtConnection: typeof openMgmtConnection;
    setMgmtChannels: typeof setMgmtChannels;
    listMgmtConnections: typeof listMgmtConnections;
    closeMgmtConnection: typeof closeMgmtConnection;
    forgetMgmtConnection: typeof forgetMgmtConnection;
    syncMgmtChannels: typeof syncMgmtChannels;
    clearMgmtChildren: typeof clearMgmtChildren;
    noteMgmtConsumer: typeof noteMgmtConsumer;
    forgetMgmtConsumer: typeof forgetMgmtConsumer;
    forgetMgmtChannelConsumers: typeof forgetMgmtChannelConsumers;
    listMgmtChannels: typeof listMgmtChannels;
    getMgmtChannel: typeof getMgmtChannel;
    listMgmtConsumers: typeof listMgmtConsumers;
    connectionAllowed: typeof connectionAllowed;
    channelAllowed: typeof channelAllowed;
    queueAllowed: typeof queueAllowed;
    setUserLimit: typeof setUserLimit;
    setVhostLimit: typeof setVhostLimit;
    listUserLimits: typeof listUserLimits;
    listVhostLimits: typeof listVhostLimits;
    putTopicPerm: typeof putTopicPerm;
    deleteTopicPerm: typeof deleteTopicPerm;
    listTopicPerms: typeof listTopicPerms;
    topicWriteAllowed: typeof topicWriteAllowed;
    topicReadAllowed: typeof topicReadAllowed;
  }
}

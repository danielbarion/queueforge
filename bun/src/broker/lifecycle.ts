/**
 * Startup load, access checks, and queue home.
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
 * Load vhosts, users, topology, and stored messages into memory.
 *
 * @returns Nothing. Exclusive queues are skipped. A classic queue whose home is another node is kept as a proxy and its messages are not loaded. `ready` is true only after this returns.
 */
export function load(this: Broker) {
  this.store.ensureVhost("/");
  for (const name of this.store.listVhosts()) this.vhosts.add(name);
  if (!this.vhosts.has("/")) this.vhosts.add("/");
  for (const u of this.store.listUsers()) this.users.set(u.name, { hash: u.hash, tags: u.tags });
  this.perms = this.store.listPerms();
  for (const e of this.store.listExchanges()) this.exchanges.set(this.key(e.vhost, e.name), e);
  for (const vh of this.vhosts) this.ensureBuiltins(vh);
  for (const q of this.store.listQueues()) {
    if (q.exclusive) continue;
    const quorum = parseArgs(q.args).queueType === "quorum";
    if (!quorum && q.home && this.cfg.nodeId && q.home !== this.cfg.nodeId) {
      this.queues.set(this.key(q.vhost, q.name), this.makeQueue(q, true));
      continue;
    }
    this.queues.set(this.key(q.vhost, q.name), this.makeQueue(q, false));
  }
  this.bindings = this.store.listBindings();
  for (const p of this.store.listPolicies() as Policy[]) this.policies.push(p);
  this.applyPolicies();
  for (const row of this.store.listMessages()) {
    const q = this.queues.get(this.key(row.vhost, row.queue));
    if (!q || (q.argsParsed.queueType !== "quorum" && q.home && this.cfg.nodeId && q.home !== this.cfg.nodeId)) continue;
    const meta = JSON.parse(row.meta) as {
      id?: string;
      exchange: string;
      routingKey: string;
      headers: Array<[string, Field]>;
      propRaw: string;
      persistent: boolean;
      priority: number;
      expiresAt: number | null;
      redelivered: boolean;
    };
    q.ready.push({
      id: meta.id || `d-${row.id}`,
      rowId: row.id,
      body: row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body as ArrayBuffer),
      exchange: meta.exchange,
      routingKey: meta.routingKey,
      headers: meta.headers ?? [],
      propRaw: new Uint8Array(Buffer.from(meta.propRaw, "base64")),
      persistent: meta.persistent,
      priority: meta.priority ?? 0,
      expiresAt: meta.expiresAt,
      redelivered: !!meta.redelivered,
    });
  }
  this.ready = true;
}

/**
 * Build the live queue record for a stored row.
 *
 * @param q Stored queue row, including arguments and home.
 * @param proxy Passed through. The home on the row is kept either way.
 * @returns A live queue with empty ready, replica, unacked, and consumer lists. Arguments are parsed once here.
 */
export function makeQueue(this: Broker, q: QueueRow, proxy: boolean): QueueLive {
  return {
    ...q,
    home: proxy ? q.home : q.home,
    argsParsed: parseArgs(q.args),
    declaredArgs: { ...q.args },
    ready: [],
    replicas: [],
    unacked: new Map(),
    consumers: [],
    rr: 0,
    lastUsed: Date.now(),
  };
}

/**
 * Create the built-in exchanges for one vhost.
 *
 * @param vhost Vhost to add. It is inserted even when it already exists.
 * @returns Nothing. An exchange that is already present is not replaced.
 */
export function ensureBuiltins(this: Broker, vhost: string) {
  this.vhosts.add(vhost);
  this.store.ensureVhost(vhost);
  for (const [name, kind, internal] of BUILTIN) {
    const row: ExRow = { vhost, name, kind, durable: true, autoDelete: false, internal, alternate: null };
    const k = this.key(vhost, name);
    if (!this.exchanges.has(k)) {
      this.exchanges.set(k, row);
      this.store.putExchange(row);
    }
  }
}

/**
 * Check a management or AMQP password.
 *
 * @param user User name. An unknown user returns false.
 * @param password Plain password. A hash that starts with `$` uses Bun's verifier. Any other hash uses the RabbitMQ password-hash check.
 * @returns True when the password matches. A mismatch returns false and does not throw.
 */
export async function verify(this: Broker, user: string, password: string): Promise<boolean> {
  const row = this.users.get(user);
  if (!row) return false;
  if (row.hash.startsWith("$")) return Bun.password.verify(password, row.hash);
  return rabbitPasswordHashMatches(password, row.hash);
}

/**
 * Test one configure, write, or read permission.
 *
 * @param user User name.
 * @param vhost Vhost the permission must name.
 * @param kind Which permission pattern to test.
 * @param resource Resource name. The default `.*` matches a pattern that is `.*`.
 * @returns True when the user's pattern matches `resource`. No permission, or a pattern that is not a valid regular expression, returns false.
 */
export function can(this: Broker, user: string, vhost: string, kind: "configure" | "write" | "read", resource = ".*"): boolean {
  const perm = this.perms.find((p) => p.user === user && p.vhost === vhost);
  if (!perm) return false;
  try {
    return new RegExp(perm[kind]).test(resource);
  } catch {
    return false;
  }
}

/**
 * Report whether a user has any permission row on a vhost.
 *
 * @param user User name.
 * @param vhost Vhost name.
 * @returns True when a permission row exists. The configure, write, and read patterns are not tested.
 */
export function hasVhostAccess(this: Broker, user: string, vhost: string): boolean {
  return this.perms.some((p) => p.user === user && p.vhost === vhost);
}

/**
 * Choose the node that owns a queue.
 *
 * @param vhost Vhost joined into the home hash.
 * @param name Queue name.
 * @param exclusive True keeps the queue on this node.
 * @returns Null when the broker has no cluster members. Otherwise the member id, or this node's id for an exclusive queue.
 */
export function homeOf(this: Broker, vhost: string, name: string, exclusive: boolean): string | null {
  if (this.cfg.members.length === 0) return null;
  if (exclusive) return this.cfg.nodeId || null;
  return queueHome(vhost, name, this.cfg.members);
}

/**
 * Report whether this process should serve a queue.
 *
 * @param home Home node id, or null when the queue is local.
 * @returns True when `home` is null, this node has no id, or `home` is this node. A follower must forward instead.
 */
export function isLocalHome(this: Broker, home: string | null): boolean {
  return !home || !this.cfg.nodeId || home === this.cfg.nodeId;
}

Broker.prototype.load = load;
Broker.prototype.makeQueue = makeQueue;
Broker.prototype.ensureBuiltins = ensureBuiltins;
Broker.prototype.verify = verify;
Broker.prototype.can = can;
Broker.prototype.hasVhostAccess = hasVhostAccess;
Broker.prototype.homeOf = homeOf;
Broker.prototype.isLocalHome = isLocalHome;

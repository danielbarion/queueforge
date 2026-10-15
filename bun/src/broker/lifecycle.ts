/**
 * Startup load, access checks, and queue home.
 *
 * These functions are the Broker methods. Loading this file installs them.
 */
import { Broker } from "./class.ts";
import { principalAllows, principalHasVhost, type Principal } from "../auth/backends.ts";
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
import { BUILTIN, EMPTY_BODY, EMPTY_HEADERS, emptyProm, ReadyQueue, type Consumer, type LiveMsg, type MgmtChannel, type MgmtConnection, type MgmtConsumer, type Policy, type Prom, type QArgs, type QueueLive, type TopicPerm } from "./model.ts";


/**
 * Load vhosts, users, topology, and stored messages into memory.
 *
 * @returns Nothing. Exclusive queues are skipped. A classic queue whose home is another node is kept as a proxy and its messages are not loaded. `ready` stays false when recovered quorum messages still need peer hellos.
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
  this.e2e = this.store.listExchangeBindings();
  for (const p of this.store.listPolicies() as Policy[]) this.policies.push(p);
  for (const row of this.store.listParameters("operator-policies")) this.operatorPolicies.push(JSON.parse(row.value) as Policy);
  this.applyPolicies();
  this.loadLimits();
  this.loadTopicPerms();
  const otherMembers = this.cfg.members.some((member) => member.id !== this.cfg.nodeId);
  let recoveredQuorum = false;
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
    const bytes = row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body as ArrayBuffer);
    const quorum = q.argsParsed.queueType === "quorum";
    const live = {
      id: meta.id || `d-${row.id}`,
      rowId: row.id,
      body: quorum ? EMPTY_BODY : bytes,
      bodyBytes: quorum ? bytes.byteLength : undefined,
      exchange: quorum ? "" : meta.exchange,
      routingKey: quorum ? "" : meta.routingKey,
      headers: quorum ? EMPTY_HEADERS : (meta.headers ?? []),
      propRaw: quorum ? EMPTY_BODY : new Uint8Array(Buffer.from(meta.propRaw, "base64")),
      persistent: meta.persistent,
      priority: meta.priority ?? 0,
      expiresAt: meta.expiresAt,
      redelivered: !!meta.redelivered,
      slim: quorum,
    };
    // Park quorum copies until peer hellos apply the consumed set. Putting
    // them on ready here made basic.get succeed at the first readyz byte.
    if (quorum && otherMembers) {
      recoveredQuorum = true;
      q.replicas.set(live.id, live);
    } else {
      q.ready.push(live);
    }
  }
  this.quorumHold = recoveredQuorum;
  this.ready = !recoveredQuorum;
}

/**
 * Build the live queue record for a stored row.
 *
 * @param q Stored queue row, including arguments and home.
 * @param proxy Passed through. The home on the row is kept either way.
 * @returns A live queue with empty ready, replica, unacked, and consumer lists. Arguments are parsed once here.
 */
export function makeQueue(this: Broker, q: QueueRow, proxy: boolean): QueueLive {
  const argsParsed = parseArgs(q.args);
  // The durable log is written on this process only once a queue that can use it lives here.
  if (!proxy && q.durable && argsParsed.queueType === "classic") this.store.prepareLog();
  return {
    ...q,
    home: proxy ? q.home : q.home,
    argsParsed,
    declaredArgs: { ...q.args },
    ready: new ReadyQueue(),
    replicas: new Map(),
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
 * @param password Plain password. The stored hash is a RabbitMQ password-hash.
 * @returns True when the password matches. A mismatch returns false and does not throw.
 */
export async function verify(this: Broker, user: string, password: string): Promise<boolean> {
  const row = this.users.get(user);
  if (row) return rabbitPasswordHashMatches(password, row.hash);
  // The next backends, in RabbitMQ's auth_backends order: OAuth 2.0, then LDAP.
  if (this.oauth && password.split(".").length === 3) {
    const got = await this.oauth.login(password);
    if (got) {
      this.principals.set(user || got.sub, got.principal);
      return true;
    }
  }
  if (this.ldap && !row) {
    const p = await this.ldap.login(user, password);
    if (p) {
      this.principals.set(user, p);
      return true;
    }
  }
  return false;
}

/** The OAuth or LDAP login behind `user`, when the user is not in the internal store. */
export function principalOf(this: Broker, user: string): Principal | null {
  if (this.users.has(user)) return null;
  return this.principals.get(user) ?? null;
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
  const principal = this.principalOf(user);
  if (principal) return principalAllows(principal, vhost, kind, resource);
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
  const principal = this.principalOf(user);
  if (principal) return principalHasVhost(principal, vhost);
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
 * Choose the home of a new queue, as RabbitMQ's `x-queue-leader-locator` does.
 *
 * @param locator `client-local` is this node. `balanced` is the member that
 *   homes the fewest queues, ties to the lowest id. Anything else uses the
 *   home hash, which every implementation computes the same way.
 * @returns Null when the broker has no cluster members.
 */
export function placeQueue(this: Broker, vhost: string, name: string, exclusive: boolean, locator: unknown): string | null {
  if (this.cfg.members.length === 0) return null;
  if (exclusive || locator === "client-local") return this.cfg.nodeId || null;
  if (locator === "balanced") {
    const counts = new Map<string, number>(this.cfg.members.map((m) => [m.id, 0]));
    for (const q of this.queues.values()) {
      if (q.home && counts.has(q.home)) counts.set(q.home, counts.get(q.home)! + 1);
    }
    const ids = [...counts.keys()].sort();
    return ids.reduce((best, id) => (counts.get(id)! < counts.get(best)! ? id : best), ids[0]!);
  }
  return this.homeOf(vhost, name, false);
}

/**
 * Every member is a worker the cores supervisor started for this broker
 * (`n0`..`nN-1`, see `childPlans`). Fixed at start and the same on every
 * worker, so they all apply the same placement rule.
 */
export function workerPool(this: Broker): boolean {
  return process.env.QUEUEFORGE_CHILD === "1" && !!this.cfg.brokerId && this.cfg.members.length > 1
    && this.cfg.members.every((m) => /^n\d+$/.test(m.id));
}

/** Pending homes handed out by `assignHome` whose declare has not landed yet. */
const assigned = new Map<string, { home: string; at: number }>();

/**
 * On the placement worker (`n0`): the home for a new classic queue among the
 * workers. A known queue keeps its home; a name already handed out gets the
 * same answer, so concurrent declares on two workers agree; otherwise the
 * worker with the fewest classic queues, counting pending ones, ties to the
 * lowest id. The name hash left the busiest of 32 workers with 9 of 128
 * queues, so one process set the pace while others idled.
 */
export function assignHome(this: Broker, vhost: string, name: string): string {
  const key = this.key(vhost, name);
  const known = this.queues.get(key);
  if (known?.home) return known.home;
  const now = Date.now();
  const pending = assigned.get(key);
  if (pending && now - pending.at < 60_000) return pending.home;
  const counts = new Map<string, number>(this.cfg.members.map((m) => [m.id, 0]));
  for (const q of this.queues.values()) {
    if (q.home && counts.has(q.home) && q.argsParsed.queueType === "classic") counts.set(q.home, counts.get(q.home)! + 1);
  }
  for (const [k, a] of assigned) {
    if (now - a.at >= 60_000 || this.queues.has(k)) assigned.delete(k);
    else if (counts.has(a.home)) counts.set(a.home, counts.get(a.home)! + 1);
  }
  const ids = [...counts.keys()].sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
  const home = ids.reduce((best, id) => (counts.get(id)! < counts.get(best)! ? id : best), ids[0]!);
  assigned.set(key, { home, at: now });
  return home;
}

/**
 * `placeQueue` for a declare. Among workers with no explicit locator, `n0`
 * assigns the home (`assignHome`); if it cannot be reached the shared hash
 * decides, as in any other cluster.
 */
export async function placeQueueAsync(this: Broker, vhost: string, name: string, exclusive: boolean, locator: unknown): Promise<string | null> {
  if (exclusive || locator != null || !this.workerPool()) return this.placeQueue(vhost, name, exclusive, locator);
  const placer = "n0";
  if (this.cfg.nodeId === placer) return this.assignHome(vhost, name);
  try {
    const home = await this.cluster!.call(placer, "place_queue", { vhost, name });
    if (typeof home === "string" && this.cfg.members.some((m) => m.id === home)) return home;
  } catch {
    /* the placement worker is down: use the hash */
  }
  return this.homeOf(vhost, name, false);
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
Broker.prototype.workerPool = workerPool;
Broker.prototype.assignHome = assignHome;
Broker.prototype.placeQueueAsync = placeQueueAsync;
Broker.prototype.makeQueue = makeQueue;
Broker.prototype.ensureBuiltins = ensureBuiltins;
Broker.prototype.verify = verify;
Broker.prototype.principalOf = principalOf;
Broker.prototype.can = can;
Broker.prototype.hasVhostAccess = hasVhostAccess;
Broker.prototype.homeOf = homeOf;
Broker.prototype.placeQueue = placeQueue;
Broker.prototype.isLocalHome = isLocalHome;

declare module "./class.ts" {
  interface Broker {
    load: typeof load;
    workerPool: typeof workerPool;
    assignHome: typeof assignHome;
    placeQueueAsync: typeof placeQueueAsync;
    makeQueue: typeof makeQueue;
    ensureBuiltins: typeof ensureBuiltins;
    verify: typeof verify;
    principalOf: typeof principalOf;
    can: typeof can;
    hasVhostAccess: typeof hasVhostAccess;
    homeOf: typeof homeOf;
    placeQueue: typeof placeQueue;
    isLocalHome: typeof isLocalHome;
  }
}

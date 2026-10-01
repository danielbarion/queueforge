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


/** Broker.load. The parameters and return value are unchanged from the previous class method. */
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

/** Broker.makeQueue. The parameters and return value are unchanged from the previous class method. */
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

/** Broker.ensureBuiltins. The parameters and return value are unchanged from the previous class method. */
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

/** Broker.verify. The parameters and return value are unchanged from the previous class method. */
export async function verify(this: Broker, user: string, password: string): Promise<boolean> {
  const row = this.users.get(user);
  if (!row) return false;
  if (row.hash.startsWith("$")) return Bun.password.verify(password, row.hash);
  return rabbitPasswordHashMatches(password, row.hash);
}

/** Broker.can. The parameters and return value are unchanged from the previous class method. */
export function can(this: Broker, user: string, vhost: string, kind: "configure" | "write" | "read", resource = ".*"): boolean {
  const perm = this.perms.find((p) => p.user === user && p.vhost === vhost);
  if (!perm) return false;
  try {
    return new RegExp(perm[kind]).test(resource);
  } catch {
    return false;
  }
}

/** Broker.hasVhostAccess. The parameters and return value are unchanged from the previous class method. */
export function hasVhostAccess(this: Broker, user: string, vhost: string): boolean {
  return this.perms.some((p) => p.user === user && p.vhost === vhost);
}

/** Broker.homeOf. The parameters and return value are unchanged from the previous class method. */
export function homeOf(this: Broker, vhost: string, name: string, exclusive: boolean): string | null {
  if (this.cfg.members.length === 0) return null;
  if (exclusive) return this.cfg.nodeId || null;
  return queueHome(vhost, name, this.cfg.members);
}

/** Broker.isLocalHome. The parameters and return value are unchanged from the previous class method. */
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

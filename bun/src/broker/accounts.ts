/**
 * Users, permissions, and definition import and export.
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


/** Broker.putUser. The parameters and return value are unchanged from the previous class method. */
export async function putUser(this: Broker, name: string, password: string | null, tags: string[], create: boolean) {
  const existing = this.users.get(name);
  if (!existing && !password) throw new Error("password required");
  const hash = password ? await Bun.password.hash(password, { algorithm: "argon2id" }) : existing!.hash;
  this.users.set(name, { hash, tags: tags.length ? tags : existing?.tags ?? [] });
  this.store.putUser({ name, hash, tags: this.users.get(name)!.tags });
  await this.cluster?.replicate("user", { name, hash, tags: this.users.get(name)!.tags });
  return create;
}

/** Broker.deleteUser. The parameters and return value are unchanged from the previous class method. */
export async function deleteUser(this: Broker, name: string) {
  this.users.delete(name);
  this.perms = this.perms.filter((p) => p.user !== name);
  this.store.deleteUser(name);
  await this.cluster?.replicate("delete_user", { name });
}

/** Broker.putPerm. The parameters and return value are unchanged from the previous class method. */
export async function putPerm(this: Broker, p: { user: string; vhost: string; configure: string; write: string; read: string }) {
  this.perms = this.perms.filter((x) => !(x.user === p.user && x.vhost === p.vhost));
  this.perms.push(p);
  this.store.putPerm(p);
  await this.cluster?.replicate("permission", p);
}

/** Broker.deletePerm. The parameters and return value are unchanged from the previous class method. */
export async function deletePerm(this: Broker, user: string, vhost: string) {
  this.perms = this.perms.filter((p) => !(p.user === user && p.vhost === vhost));
  this.store.deletePerm(user, vhost);
  await this.cluster?.replicate("delete_permission", { user, vhost });
}

/** Broker.exportDefinitions. The parameters and return value are unchanged from the previous class method. */
export function exportDefinitions(this: Broker) {
  const argMap = (args: Array<[string, Field]>) => {
    const out: Record<string, string | number> = {};
    for (const [k, v] of args) {
      if (v.t === "I" || v.t === "l") out[k] = v.v;
      else if (v.t === "S" || v.t === "s") out[k] = v.v;
    }
    return out;
  };
  return {
    rabbit_version: "3.13.0",
    queueforge_version: "0.1.0",
    users: [...this.users.entries()].map(([name, u]) => ({
      name,
      password_hash: u.hash,
      tags: u.tags.join(","),
    })),
    vhosts: [...this.vhosts].map((name) => ({ name })),
    permissions: this.perms.map((p) => ({ ...p })),
    exchanges: [...this.exchanges.values()]
      .filter((e) => e.name !== "")
      .map((e) => ({
        name: e.name,
        vhost: e.vhost,
        type: e.kind === "default" ? "direct" : e.kind,
        durable: e.durable,
        auto_delete: e.autoDelete,
        internal: e.internal,
      })),
    queues: [...this.queues.values()]
      .filter((q) => !q.exclusive)
      .map((q) => ({
        name: q.name,
        vhost: q.vhost,
        durable: q.durable,
        exclusive: false,
        auto_delete: q.autoDelete,
        type: q.argsParsed.queueType,
        arguments: q.args,
      })),
    bindings: this.bindings.map((b) => ({
      source: b.exchange,
      vhost: b.vhost,
      destination: b.queue,
      destination_type: "queue",
      routing_key: b.routingKey,
      arguments: argMap(b.args),
    })),
    policies: this.policies.map((p) => ({
      vhost: p.vhost,
      name: p.name,
      pattern: p.pattern,
      "apply-to": p.applyTo,
      priority: p.priority,
      definition: {
        ...(p.messageTtl != null ? { "message-ttl": p.messageTtl } : {}),
        ...(p.dlx ? { "dead-letter-exchange": p.dlx } : {}),
        ...(p.dlxKey ? { "dead-letter-routing-key": p.dlxKey } : {}),
        ...(p.maxLength != null ? { "max-length": p.maxLength } : {}),
        ...(p.maxLengthBytes != null ? { "max-length-bytes": p.maxLengthBytes } : {}),
        ...(p.expiresMs != null ? { expires: p.expiresMs } : {}),
        ...(p.overflow ? { overflow: p.overflow } : {}),
        ...(p.dlxStrategy ? { "dead-letter-strategy": p.dlxStrategy } : {}),
        ...(p.deliveryLimit != null ? { "delivery-limit": p.deliveryLimit } : {}),
        ...(p.alternate ? { "alternate-exchange": p.alternate } : {}),
      },
    })),
  };
}

/** Broker.importDefinitions. The parameters and return value are unchanged from the previous class method. */
export async function importDefinitions(this: Broker, body: {
  users?: Array<{ name: string; password?: string; password_hash?: string; tags?: string | string[] }>;
  vhosts?: Array<{ name: string }>;
  permissions?: Array<{ user: string; vhost: string; configure: string; write: string; read: string }>;
  exchanges?: Array<{ name: string; vhost: string; type?: string; durable?: boolean; auto_delete?: boolean; internal?: boolean }>;
  queues?: Array<{ name: string; vhost: string; durable?: boolean; exclusive?: boolean; auto_delete?: boolean; type?: string; arguments?: Record<string, string | number> }>;
  bindings?: Array<{ source: string; vhost: string; destination: string; destination_type?: string; routing_key?: string; arguments?: Record<string, string | number> }>;
  policies?: Array<{ vhost: string; name: string; pattern: string; "apply-to"?: string; priority?: number; definition?: Record<string, string | number> }>;
}) {
  for (const vh of body.vhosts ?? []) this.ensureBuiltins(vh.name);
  for (const u of body.users ?? []) {
    const rawTags = u.tags ?? "";
    const tags = Array.isArray(rawTags) ? rawTags : rawTags.split(/[,\s]+/).filter(Boolean);
    if (u.password) await this.putUser(u.name, u.password, tags, true);
    else if (u.password_hash) {
      this.users.set(u.name, { hash: u.password_hash, tags });
      this.store.putUser({ name: u.name, hash: u.password_hash, tags });
    }
  }
  for (const p of body.permissions ?? []) await this.putPerm(p);
  for (const ex of body.exchanges ?? []) {
    if (!ex.name || ex.name.startsWith("amq.")) continue;
    await this.declareExchange(ex.vhost, ex.name, ex.type ?? "direct", !!ex.durable, !!ex.auto_delete, !!ex.internal, null);
  }
  for (const p of body.policies ?? []) {
    const apply = p["apply-to"] ?? "all";
    if (apply !== "queues" && apply !== "exchanges" && apply !== "all") continue;
    const def = p.definition ?? {};
    const num = (k: string) => {
      const v = def[k];
      if (v == null || v === "") return null;
      const n = Number(v);
      return Number.isFinite(n) && n > 0 ? n : null;
    };
    const str = (k: string) => {
      const v = def[k];
      return v == null || v === "" ? null : String(v);
    };
    this.upsertPolicy({
      vhost: p.vhost,
      name: p.name,
      pattern: p.pattern,
      applyTo: apply,
      priority: p.priority ?? 0,
      messageTtl: num("message-ttl"),
      expiresMs: num("expires"),
      dlx: str("dead-letter-exchange"),
      dlxKey: str("dead-letter-routing-key"),
      maxLength: num("max-length"),
      maxLengthBytes: num("max-length-bytes"),
      overflow: overflowOf(str("overflow")),
      dlxStrategy: str("dead-letter-strategy") === "at-least-once" ? "at-least-once" : str("dead-letter-strategy") === "at-most-once" ? "at-most-once" : null,
      deliveryLimit: num("delivery-limit"),
      alternate: str("alternate-exchange"),
    });
  }
  for (const q of body.queues ?? []) {
    if (q.exclusive) continue;
    const args = { ...(q.arguments ?? {}) };
    if (q.type && args["x-queue-type"] == null) args["x-queue-type"] = q.type;
    await this.declareQueue({
      vhost: q.vhost,
      name: q.name,
      durable: !!q.durable,
      exclusive: false,
      autoDelete: !!q.auto_delete,
      passive: false,
      args,
    });
  }
  for (const b of body.bindings ?? []) {
    if (b.destination_type && b.destination_type !== "queue") continue;
    if (!b.source) continue;
    const args = Object.entries(b.arguments ?? {}).map(([k, v]) =>
      [k, typeof v === "number" ? { t: "I" as const, v } : { t: "S" as const, v: String(v) }] as [string, Field],
    );
    await this.bind(b.vhost, b.source, b.destination, b.routing_key ?? "", args);
  }
}

/** Broker.listUsers. The parameters and return value are unchanged from the previous class method. */
export function listUsers(this: Broker) {
  return [...this.users.entries()].map(([name, u]) => ({ name, tags: u.tags }));
}

/** Broker.userTags. The parameters and return value are unchanged from the previous class method. */
export function userTags(this: Broker, name: string) {
  return this.users.get(name)?.tags ?? [];
}

/** Broker.sweep. The parameters and return value are unchanged from the previous class method. */
export function sweep(this: Broker) {
  const now = Date.now();
  for (const q of [...this.queues.values()]) {
    if (!this.isLocalHome(q.home)) continue;
    this.pump(q);
    const expires = q.argsParsed.expiresMs;
    if (expires != null && q.consumers.length === 0 && now - q.lastUsed >= expires) {
      void this.deleteQueue(q.vhost, q.name).catch(() => {});
    }
  }
}

Broker.prototype.putUser = putUser;
Broker.prototype.deleteUser = deleteUser;
Broker.prototype.putPerm = putPerm;
Broker.prototype.deletePerm = deletePerm;
Broker.prototype.exportDefinitions = exportDefinitions;
Broker.prototype.importDefinitions = importDefinitions;
Broker.prototype.listUsers = listUsers;
Broker.prototype.userTags = userTags;
Broker.prototype.sweep = sweep;

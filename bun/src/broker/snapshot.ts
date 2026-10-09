/**
 * Cluster snapshot and remote declare application.
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
import { matchOne, policyItem, policyFromBody, fillPolicyArgs, capOperatorArgs } from "./policy-data.ts";
import type { RuntimeParam } from "./params.ts";
import { BUILTIN, emptyProm, type Consumer, type LiveMsg, type MgmtChannel, type MgmtConnection, type MgmtConsumer, type Policy, type Prom, type QArgs, type QueueLive, type TopicPerm } from "./model.ts";


/**
 * Apply one replicated topology record.
 *
 * @param kind Record kind, such as `exchange`, `queue`, `binding`, `user`, or `vhost`. An unknown kind is ignored.
 * @param payload Fields for that kind. A queue or exchange that is already present is not replaced, except a durable row is still stored.
 * @returns Nothing. The caller must not apply a message body with this method.
 */
/** Turn a Rust or Bun queue record into the local row. Rust sends `queue_type` and `auto_delete`. */
function queueFromWire(payload: Record<string, unknown>): QueueRow {
  const argsIn = (payload.args ?? {}) as Record<string, unknown>;
  const args: Record<string, string | number> = {};
  const take = (from: string, to: string) => {
    const value = argsIn[from];
    if (typeof value === "string" || typeof value === "number") args[to] = value;
  };
  take("x-message-ttl", "x-message-ttl");
  take("message_ttl_ms", "x-message-ttl");
  take("x-expires", "x-expires");
  take("expires_ms", "x-expires");
  take("x-max-length", "x-max-length");
  take("max_length", "x-max-length");
  take("x-max-length-bytes", "x-max-length-bytes");
  take("max_length_bytes", "x-max-length-bytes");
  take("x-overflow", "x-overflow");
  take("overflow", "x-overflow");
  take("x-dead-letter-exchange", "x-dead-letter-exchange");
  take("dead_letter_exchange", "x-dead-letter-exchange");
  take("x-dead-letter-routing-key", "x-dead-letter-routing-key");
  take("dead_letter_routing_key", "x-dead-letter-routing-key");
  take("x-max-priority", "x-max-priority");
  take("max_priority", "x-max-priority");
  take("x-delivery-limit", "x-delivery-limit");
  take("delivery_limit", "x-delivery-limit");
  take("x-max-age", "x-max-age");
  const qtype = String(argsIn["x-queue-type"] ?? argsIn.queue_type ?? "").toLowerCase();
  if (qtype === "quorum" || qtype === "classic" || qtype === "stream") args["x-queue-type"] = qtype;
  // Rust's names for the rest of its arguments.
  if (typeof argsIn.max_age_ms === "number" && args["x-max-age"] == null) args["x-max-age"] = `${Math.ceil(argsIn.max_age_ms / 1000)}s`;
  if (typeof argsIn.leader_locator === "string") args["x-queue-leader-locator"] = argsIn.leader_locator;
  if (argsIn.single_active === true || argsIn["x-single-active-consumer"] === true) args["x-single-active-consumer"] = "true";
  if (argsIn.dead_letter_strategy === "at-least-once") args["x-dead-letter-strategy"] = "at-least-once";
  for (const [key, value] of Object.entries(argsIn)) {
    if (key.startsWith("x-") && (typeof value === "string" || typeof value === "number") && args[key] == null) args[key] = value;
  }
  const home = payload.home == null || payload.home === "" ? null : String(payload.home);
  return {
    vhost: String(payload.vhost ?? "/"),
    name: String(payload.name ?? payload.queue ?? ""),
    durable: payload.durable !== false,
    exclusive: payload.exclusive === true,
    autoDelete: payload.autoDelete === true || payload.auto_delete === true,
    args,
    home,
    raftGroup: typeof payload.raftGroup === "string" ? payload.raftGroup : null,
  };
}

/**
 * A replicated policy row. Rust sends its `Policy` struct (snake_case,
 * `apply_to`); Bun sends its own camelCase row. Both map to Bun's `Policy`.
 */
export function policyFromWire(p: Record<string, unknown>): Policy & { definition: Record<string, unknown> } {
  const definition = (p.definition ?? {}) as Record<string, unknown>;
  p = { ...Object.fromEntries(Object.entries(definition).map(([key, value]) => [({ "message-ttl": "message_ttl_ms", "expires": "expires_ms", "dead-letter-exchange": "dead_letter_exchange", "dead-letter-routing-key": "dead_letter_routing_key", "max-length": "max_length", "max-length-bytes": "max_length_bytes", "dead-letter-strategy": "dead_letter_strategy", "delivery-limit": "delivery_limit", "alternate-exchange": "alternate_exchange" } as Record<string, string>)[key] ?? key, value])), ...p };
  const num = (v: unknown) => (v == null ? null : Number(v));
  const str = (v: unknown) => (v == null ? null : String(v));
  const applyTo = String(p.applyTo ?? p.apply_to ?? "all");
  return {
    vhost: String(p.vhost ?? "/"),
    name: String(p.name ?? ""),
    pattern: String(p.pattern ?? ""),
    applyTo: applyTo === "queues" || applyTo === "exchanges" ? applyTo : "all",
    priority: Number(p.priority ?? 0),
    messageTtl: num(p.messageTtl ?? p.message_ttl_ms),
    expiresMs: num(p.expiresMs ?? p.expires_ms),
    dlx: str(p.dlx ?? p.dead_letter_exchange),
    dlxKey: str(p.dlxKey ?? p.dead_letter_routing_key),
    maxLength: num(p.maxLength ?? p.max_length),
    maxLengthBytes: num(p.maxLengthBytes ?? p.max_length_bytes),
    overflow: (str(p.overflow) as Policy["overflow"]) ?? null,
    dlxStrategy: (str(p.dlxStrategy ?? p.dead_letter_strategy) as Policy["dlxStrategy"]) ?? null,
    deliveryLimit: num(p.deliveryLimit ?? p.delivery_limit),
    alternate: str(p.alternate ?? p.alternate_exchange),
    definition,
  };
}

/**
 * The wire form of a policy: Rust's `Policy` field names, which Rust parses
 * and `policyFromWire` reads back.
 */
export function policyToWire(p: Policy) {
  return {
    vhost: p.vhost,
    name: p.name,
    pattern: p.pattern,
    apply_to: p.applyTo,
    priority: p.priority,
    message_ttl_ms: p.messageTtl,
    expires_ms: p.expiresMs,
    dead_letter_exchange: p.dlx,
    dead_letter_routing_key: p.dlxKey,
    max_length: p.maxLength,
    max_length_bytes: p.maxLengthBytes,
    overflow: p.overflow,
    dead_letter_strategy: p.dlxStrategy,
    delivery_limit: p.deliveryLimit,
    alternate_exchange: p.alternate,
    definition: { ...policyItem(p).definition, ...((p as Policy & { definition?: Record<string, unknown> }).definition ?? {}) },
  };
}

export function applyRemote(this: Broker, kind: string, payload: Record<string, unknown>) {
  if (kind === "exchange") {
    const e = payload as ExRow;
    this.exchanges.set(this.key(e.vhost, e.name), e);
    if (e.durable) this.store.putExchange(e);
  } else if (kind === "delete_exchange") {
    const vhost = String(payload.vhost);
    const name = String(payload.name);
    this.exchanges.delete(this.key(vhost, name));
    this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.exchange === name));
    this.store.deleteExchange(vhost, name);
  } else if (kind === "queue" || kind === "declare_queue") {
    const row = queueFromWire(payload);
    if (!this.queues.has(this.key(row.vhost, row.name))) {
      this.queues.set(this.key(row.vhost, row.name), this.makeQueue(row, !this.isLocalHome(row.home)));
    }
    if (row.durable) this.store.putQueue(row);
    if (row.raftGroup) this.startQueueGroup(row.raftGroup, payload.raftLeader === this.cfg.nodeId);
  } else if (kind === "delete_queue") {
    const vhost = String(payload.vhost);
    const name = String(payload.name ?? payload.queue);
    const group = this.queues.get(this.key(vhost, name))?.raftGroup;
    if (group) this.cluster?.consensus?.node?.dropGroup(group);
    this.queues.delete(this.key(vhost, name));
    this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.queue === name));
    this.store.deleteQueue(vhost, name);
  } else if (kind === "binding") {
    const b = payload as BindRow;
    // Applying twice (a replay, or a snapshot after a push) keeps one row.
    const same = this.bindings.some(
      (x) => x.vhost === b.vhost && x.exchange === b.exchange && x.queue === b.queue && x.routingKey === b.routingKey && JSON.stringify(x.args ?? null) === JSON.stringify(b.args ?? null),
    );
    if (!same) {
      this.bindings.push(b);
      this.store.putBinding(b);
    }
  } else if (kind === "policy") {
    this.upsertPolicy(policyFromWire(payload));
  } else if (kind === "delete_policy") {
    this.deletePolicy(String(payload.vhost ?? "/"), String(payload.name ?? ""));
  } else if (kind === "unbind") {
    const vhost = String(payload.vhost);
    const exchange = String(payload.exchange);
    const queue = String(payload.queue);
    const routingKey = String(payload.routingKey);
    const argsJson = payload.args ? JSON.stringify(payload.args) : null;
    this.bindings = this.bindings.filter((b) => {
      if (b.vhost !== vhost || b.exchange !== exchange || b.queue !== queue || b.routingKey !== routingKey) return true;
      if (argsJson == null) return false;
      return JSON.stringify(b.args) !== argsJson;
    });
    this.store.deleteBinding(vhost, exchange, queue, routingKey, argsJson);
  } else if (kind === "user") {
    const name = String(payload.name);
    const hash = String(payload.hash);
    const tags = (payload.tags as string[]) ?? [];
    this.users.set(name, { hash, tags });
    this.store.putUser({ name, hash, tags });
  } else if (kind === "delete_user") {
    const name = String(payload.name);
    this.users.delete(name);
    this.perms = this.perms.filter((p) => p.user !== name);
    this.store.deleteUser(name);
  } else if (kind === "permission") {
    const p = payload as { user: string; vhost: string; configure: string; write: string; read: string };
    this.perms = this.perms.filter((x) => !(x.user === p.user && x.vhost === p.vhost));
    this.perms.push(p);
    this.store.putPerm(p);
  } else if (kind === "delete_permission") {
    this.perms = this.perms.filter((p) => !(p.user === payload.user && p.vhost === payload.vhost));
    this.store.deletePerm(String(payload.user), String(payload.vhost));
  } else if (kind === "user_limits" || kind === "vhost_limits") {
    this.applyLimitsRow(kind, payload);
  } else if (kind === "topic_permission") {
    try {
      this.putTopicPerm(payload as unknown as TopicPerm);
    } catch {
      /* the sender validated the patterns */
    }
  } else if (kind === "delete_topic_permission") {
    this.deleteTopicPerm(String(payload.user), String(payload.vhost), String(payload.exchange));
  } else if (kind === "parameter") {
    try {
      this.putRuntimeParam(payload as unknown as RuntimeParam);
    } catch {
      /* the sender validated the value */
    }
  } else if (kind === "delete_parameter") {
    this.deleteRuntimeParam(String(payload.component), String(payload.vhost), String(payload.name));
  } else if (kind === "global_parameter") {
    this.putGlobalParam(String(payload.name), payload.value);
  } else if (kind === "delete_global_parameter") {
    this.deleteGlobalParam(String(payload.name));
  } else if (kind === "vhost") {
    this.ensureBuiltins(String(payload.name));
  } else if (kind === "delete_vhost") {
    this.vhosts.delete(String(payload.name));
    this.store.deleteVhost(String(payload.name));
  }
}

/**
 * Copy the topology a peer can import.
 *
 * @returns Users, vhosts, permissions, exchanges, queue rows, bindings, and consumed ids. Ready message bodies are not included. Exclusive queues are included.
 */
export function snapshot(this: Broker) {
  return {
    users: [...this.users.entries()].map(([name, u]) => ({ name, hash: u.hash, password_hash: u.hash, tags: u.tags })),
    vhosts: [...this.vhosts],
    permissions: this.perms,
    exchanges: [...this.exchanges.values()],
    queues: [...this.queues.values()].map((q) => ({
      vhost: q.vhost,
      name: q.name,
      durable: q.durable,
      exclusive: q.exclusive,
      autoDelete: q.autoDelete,
      args: q.declaredArgs ?? q.args,
      home: q.home,
      raftGroup: q.raftGroup ?? null,
    })),
    bindings: this.bindings.map((b) => ({ ...b, routing_key: b.routingKey })),
    exchangeBindings: this.e2e.map((b) => ({ ...b, routing_key: b.routingKey })),
    policies: [...this.policies.map((p) => ({ ...policyToWire(p), operator: false })), ...this.operatorPolicies.map((p) => ({ ...policyToWire(p), operator: true }))],
    consumed: this.consumed,
    userLimits: this.listUserLimits(),
    vhostLimits: this.listVhostLimits(),
    topicPermissions: this.listTopicPerms(),
    parameters: this.listRuntimeParams().filter((p) => p.component !== "vhost-limits" && p.component !== "shovel"),
    globalParameters: this.listGlobalParams().filter((p) => this.store.listParameters("global").some((row) => row.name === p.name)),
  };
}

/**
 * Import topology this process does not already have.
 *
 * @param snap Snapshot from `snapshot`, or null. Null returns immediately.
 * @returns Nothing. An existing user, exchange, or queue is left as it is. Bindings are appended even when the same binding is already present. Consumed ids are dropped locally.
 */
export function applySnapshot(this: Broker, snap: ReturnType<Broker["snapshot"]> | null | undefined) {
  if (!snap) return;
  const names = (snap.vhosts ?? []).map((row: string | { name: string }) => typeof row === "string" ? row : row.name);
  if (names.some((name) => typeof name !== "string")) throw new Error("Invalid snapshot vhost");
  for (const row of snap.users ?? []) if (!this.users.has(row.name)) {
    const wire = row as typeof row & { password_hash?: string };
    const hash = wire.hash ?? wire.password_hash;
    if (typeof hash !== "string") throw new Error("Invalid snapshot user hash");
    const user = { name: row.name, hash, tags: row.tags };
    this.users.set(user.name, { hash: user.hash, tags: user.tags });
    this.store.putUser(user);
  }
  for (const name of names) this.ensureBuiltins(name);
  for (const p of snap.permissions ?? []) {
    if (!this.perms.some((x) => x.user === p.user && x.vhost === p.vhost)) {
      this.perms.push(p);
      this.store.putPerm(p);
    }
  }
  for (const e of snap.exchanges ?? []) if (!this.exchanges.has(this.key(e.vhost, e.name))) {
    this.exchanges.set(this.key(e.vhost, e.name), e);
    if (e.durable) this.store.putExchange(e);
  }
  for (const q of snap.queues ?? []) {
    const row = queueFromWire(q as unknown as Record<string, unknown>);
    if (!this.queues.has(this.key(row.vhost, row.name))) {
      this.queues.set(this.key(row.vhost, row.name), this.makeQueue(row, !this.isLocalHome(row.home)));
      if (row.durable) this.store.putQueue(row);
    }
  }
  this.applyConsumed(snap.consumed);
  // Limits a member already has are kept; only missing rows are taken.
  for (const row of snap.userLimits ?? []) {
    if (!this.userConnLimit.has(row.user) && !this.userChanLimit.has(row.user)) this.applyLimitsRow("user_limits", row);
  }
  for (const row of snap.vhostLimits ?? []) {
    if (!this.vhostConnLimit.has(row.vhost) && !this.vhostQueueLimit.has(row.vhost)) this.applyLimitsRow("vhost_limits", row);
  }
  for (const b of snap.bindings ?? []) {
    this.bindings.push(b);
    this.store.putBinding(b);
  }
  // Topic permissions and parameters this member does not have.
  const topics = this.listTopicPerms();
  for (const p of snap.topicPermissions ?? []) {
    if (topics.some((t) => t.user === p.user && t.vhost === p.vhost && t.exchange === p.exchange)) continue;
    try {
      this.putTopicPerm(p);
    } catch {
      /* the sender validated the patterns */
    }
  }
  const params = this.listRuntimeParams();
  for (const p of snap.parameters ?? []) {
    if (params.some((r) => r.component === p.component && r.vhost === p.vhost && r.name === p.name)) continue;
    try {
      this.putRuntimeParam(p);
    } catch {
      /* the sender validated the value */
    }
  }
  const globals = new Set(this.store.listParameters("global").map((row) => row.name));
  for (const p of snap.globalParameters ?? []) if (!globals.has(p.name)) this.putGlobalParam(p.name, p.value);
}

/** Install authoritative Raft metadata; legacy peer hello snapshots remain additive. */
export function installMetaSnapshot(this: Broker, state: unknown): void {
  const object = (value: unknown): Record<string, unknown> => {
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid metadata snapshot row");
    return value as Record<string, unknown>;
  };
  const snap = object(state);
  const list = (key: string, required = false): unknown[] => {
    if (snap[key] === undefined && !required) return [];
    if (!Array.isArray(snap[key])) throw new Error(`Invalid metadata snapshot ${key}`);
    return snap[key] as unknown[];
  };
  const text = (value: unknown, empty = false): string => {
    if (typeof value !== "string" || (!empty && value.length === 0) || value.includes("\0")) throw new Error("Invalid metadata snapshot string");
    return value;
  };
  const bool = (value: unknown, fallback: boolean): boolean => {
    if (value === undefined) return fallback;
    if (typeof value !== "boolean") throw new Error("Invalid metadata snapshot boolean");
    return value;
  };
  const regex = (value: unknown): string => { const pattern = text(value, true); new RegExp(pattern); return pattern; };
  const hosts = new Set(list("vhosts", true).map((v) => text(typeof v === "string" ? v : object(v).name)));
  const host = (value: unknown): string => { const vhost = text(value); if (!hosts.has(vhost)) throw new Error("Unknown metadata snapshot vhost"); return vhost; };
  const users = list("users", true).map((value) => {
    const row = object(value), name = text(row.name), hash = text(row.hash ?? row.password_hash);
    if (!Array.isArray(row.tags) || row.tags.some((tag) => typeof tag !== "string")) throw new Error("Invalid metadata snapshot tags");
    return { name, hash, tags: row.tags as string[] };
  });
  const userNames = new Set(users.map((u) => u.name));
  const user = (value: unknown): string => { const name = text(value); if (!userNames.has(name)) throw new Error("Unknown metadata snapshot user"); return name; };
  const permissions = list("permissions", true).map((value) => { const r = object(value); return { user: user(r.user), vhost: host(r.vhost), configure: regex(r.configure), write: regex(r.write), read: regex(r.read) }; });
  const exchanges: ExRow[] = list("exchanges", true).map((value) => {
    const r = object(value), kind = text(r.kind ?? r.type);
    if (!["default", "direct", "fanout", "topic", "headers", "x-consistent-hash", "x-delayed-message", "x-local-random"].includes(kind)) throw new Error("Invalid snapshot exchange kind");
    const delayedType = r.delayedType ?? r.delayed_type ?? null;
    if (delayedType !== null && !["direct", "fanout", "topic", "headers"].includes(text(delayedType))) throw new Error("Invalid delayed exchange kind");
    return { vhost: host(r.vhost), name: text(r.name, true), kind, durable: bool(r.durable, true), autoDelete: bool(r.autoDelete ?? r.auto_delete, false), internal: bool(r.internal, false), alternate: r.alternate == null ? null : text(r.alternate, true), delayedType: delayedType as string | null };
  });
  const queueRows = list("queues", true).map((value) => {
    const r = object(value); host(r.vhost); text(r.name ?? r.queue); bool(r.durable, true); bool(r.exclusive, false); bool(r.autoDelete ?? r.auto_delete, false);
    if (r.home != null) text(r.home, true); if (r.raftGroup != null) text(r.raftGroup);
    if (r.args != null) {
      const raw = object(r.args);
      for (const key of ["x-message-ttl", "message_ttl_ms", "x-expires", "expires_ms", "x-max-length", "max_length", "x-max-length-bytes", "max_length_bytes", "x-max-priority", "max_priority", "x-delivery-limit", "delivery_limit", "max_age_ms"]) {
        const value = raw[key];
        if (value != null && (typeof value !== "number" || !Number.isSafeInteger(value) || value < (key === "x-delivery-limit" || key === "delivery_limit" ? -1 : 0))) throw new Error("Invalid snapshot queue number");
      }
      for (const [key, value] of Object.entries(raw)) if (key.startsWith("x-") && typeof value !== "string" && typeof value !== "number" && !(key === "x-single-active-consumer" && typeof value === "boolean")) throw new Error("Invalid snapshot queue argument");
    }
    const row = queueFromWire(r); const type = (r.args as Record<string, unknown> | undefined)?.["x-queue-type"] ?? (r.args as Record<string, unknown> | undefined)?.queue_type;
    if (type != null && !["classic", "quorum", "stream"].includes(String(type))) throw new Error("Invalid snapshot queue type");
    for (const key of ["x-message-ttl", "x-expires", "x-max-length", "x-max-length-bytes", "x-max-priority", "x-delivery-limit"]) {
      const value = row.args[key];
      if (value != null && (typeof value !== "number" || !Number.isSafeInteger(value) || value < (key === "x-delivery-limit" ? -1 : 0))) throw new Error("Invalid snapshot queue number");
    }
    if (row.args["x-overflow"] != null && !["drop-head", "reject-publish", "reject-publish-dlx"].includes(String(row.args["x-overflow"]))) throw new Error("Invalid snapshot queue overflow");
    if (row.args["x-dead-letter-strategy"] != null && !["at-most-once", "at-least-once"].includes(String(row.args["x-dead-letter-strategy"]))) throw new Error("Invalid snapshot queue dead-letter strategy");
    parseArgs(row.args); return row;
  });
  const queueKeys = new Set(queueRows.map((q) => this.key(q.vhost, q.name)));
  const exchangeKeys = new Set(exchanges.map((e) => this.key(e.vhost, e.name)));
  const bindings: BindRow[] = [], e2e: Broker["e2e"] = [];
  for (const value of [...list("bindings", true), ...list("exchangeBindings"), ...list("exchange_bindings"), ...list("e2e")]) {
    const r = object(value), vhost = host(r.vhost), source = text(r.exchange ?? r.source), routingKey = text(r.routingKey ?? r.routing_key ?? r.key ?? "", true);
    if (!exchangeKeys.has(this.key(vhost, source))) throw new Error("Unknown snapshot binding exchange");
    if ((r.destinationType ?? r.destination_type) === "exchange" || (r.source !== undefined && r.queue === undefined)) {
      const destination = text(r.destination); if (!exchangeKeys.has(this.key(vhost, destination))) throw new Error("Unknown snapshot binding destination");
      e2e.push({ vhost, source, destination, routingKey });
    } else {
      const queue = text(r.queue ?? r.destination); if (!queueKeys.has(this.key(vhost, queue))) throw new Error("Unknown snapshot binding queue");
      const args = r.args ?? []; if (!Array.isArray(args) || args.some((pair) => !Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string")) throw new Error("Invalid snapshot binding arguments");
      bindings.push({ vhost, exchange: source, queue, routingKey, args: args as BindRow["args"] });
    }
  }
  const unique = <T>(rows: T[], key: (row: T) => string): T[] => { const seen = new Set<string>(); for (const row of rows) { const id = key(row); if (seen.has(id)) throw new Error("Duplicate metadata snapshot row"); seen.add(id); } return rows; };
  unique(users, (u) => u.name); unique(permissions, (p) => `${p.user}\0${p.vhost}`); unique(exchanges, (e) => this.key(e.vhost, e.name)); unique(queueRows, (q) => this.key(q.vhost, q.name));
  unique(bindings, (b) => JSON.stringify([b.vhost, b.exchange, b.queue, b.routingKey, b.args])); unique(e2e, (b) => JSON.stringify([b.vhost, b.source, b.destination, b.routingKey]));
  const policies = [...list("policies"), ...list("operatorPolicies").map((value) => ({ ...object(value), operator: true }))].map((value) => {
    const r = object(value); host(r.vhost); text(r.name); regex(r.pattern);
    if (r.definition !== undefined) object(r.definition);
    const apply = r.applyTo ?? r.apply_to ?? r["apply-to"] ?? "all"; if (!["all", "queues", "exchanges"].includes(String(apply))) throw new Error("Invalid snapshot policy target");
    if (r.priority !== undefined && (typeof r.priority !== "number" || !Number.isFinite(r.priority))) throw new Error("Invalid snapshot policy priority");
    const p = policyFromWire({ ...r, apply_to: apply });
    for (const key of ["messageTtl", "expiresMs", "maxLength", "maxLengthBytes", "deliveryLimit"] as const) if (p[key] !== null && (!Number.isFinite(p[key]) || p[key]! < 0)) throw new Error("Invalid snapshot policy number");
    if (p.overflow !== null && !["drop-head", "reject-publish", "reject-publish-dlx"].includes(p.overflow)) throw new Error("Invalid snapshot policy overflow");
    if (p.dlxStrategy !== null && !["at-most-once", "at-least-once"].includes(p.dlxStrategy)) throw new Error("Invalid snapshot dead-letter strategy");
    return { ...p, operator: bool(r.operator, false) };
  });
  unique(policies, (p) => `${p.operator}\0${p.vhost}\0${p.name}`);
  const topics = list("topicPermissions").map((value) => { const r = object(value); return { user: user(r.user), vhost: host(r.vhost), exchange: text(r.exchange, true), write: regex(r.write), read: regex(r.read) }; });
  const limitRows = (key: string, id: "user" | "vhost") => list(key).map((value) => {
    const r = object(value); (id === "user" ? user : host)(r[id]);
    for (const name of ["max-connections", id === "user" ? "max-channels" : "max-queues"]) if (r[name] != null && (typeof r[name] !== "number" || !Number.isInteger(r[name]) || (r[name] as number) < -1)) throw new Error("Invalid snapshot limit");
    return r;
  });
  const userLimits = limitRows("userLimits", "user"), vhostLimits = limitRows("vhostLimits", "vhost");
  const parameters = list("parameters").map((value) => { const r = object(value); const component = text(r.component); if (component === "shovel" || component === "vhost-limits") throw new Error("Node-local or duplicated snapshot parameter"); if (!("value" in r)) throw new Error("Missing snapshot parameter value"); return { component, vhost: host(r.vhost), name: text(r.name), value: r.value }; });
  const globals = list("globalParameters").map((value) => { const r = object(value); if (!("value" in r)) throw new Error("Missing snapshot global value"); return { name: text(r.name), value: r.value }; });
  // Check serialization before deleting a single store row (also catches cyclic values).
  JSON.stringify({ users, exchanges, queueRows, bindings, e2e, policies, topics, userLimits, vhostLimits, parameters, globals });
  const survivingExclusive = [...this.queues.values()].filter((q) => q.exclusive && hosts.has(q.vhost) && !queueKeys.has(this.key(q.vhost, q.name)));
  const preservedKeys = new Set(survivingExclusive.map((q) => this.key(q.vhost, q.name)));
  const localBindings = this.bindings.filter((b) => preservedKeys.has(this.key(b.vhost, b.queue)) && exchangeKeys.has(this.key(b.vhost, b.exchange)));
  const removed = [...this.queues.values()].filter((q) => !queueKeys.has(this.key(q.vhost, q.name)) && !preservedKeys.has(this.key(q.vhost, q.name)));
  const normalPolicies = policies.filter((p) => !p.operator), operatorPolicies = policies.filter((p) => p.operator);
  const prepared = queueRows.map((row) => {
    const effective = capOperatorArgs(fillPolicyArgs(row.args, matchOne(normalPolicies, row.vhost, row.name, "queues")), matchOne(operatorPolicies, row.vhost, row.name, "queues"));
    const parsed = parseArgs(effective);
    return { row, effective, parsed, existing: this.queues.get(this.key(row.vhost, row.name)), fresh: this.makeQueue(row, !this.isLocalHome(row.home)) };
  });
  this.store.db.transaction(() => {
    // Stored exclusive queues can be absent from memory after restart too.
    const removedStored = this.store.listQueues().filter((q) => !queueKeys.has(this.key(q.vhost, q.name)) && !preservedKeys.has(this.key(q.vhost, q.name)));
    for (const q of [...removed, ...removedStored]) { this.store.deleteQueue(q.vhost, q.name); this.store.deleteStreamLog(q.vhost, q.name); }
    this.store.db.exec("DELETE FROM users; DELETE FROM permissions; DELETE FROM vhosts; DELETE FROM exchanges; DELETE FROM bindings; DELETE FROM exchange_bindings; DELETE FROM policies;");
    this.store.db.exec("DELETE FROM parameters WHERE component IN ('user-limits','vhost-limits','topic-permissions','global','operator-policies') OR component LIKE 'rt:%'");
    for (const row of this.store.db.query("SELECT DISTINCT vhost FROM parameters WHERE vhost <> ''").all() as Array<{ vhost: string }>) if (!hosts.has(row.vhost)) this.store.db.query("DELETE FROM parameters WHERE vhost=?").run(row.vhost);
    for (const vhost of hosts) this.store.ensureVhost(vhost);
    for (const u of users) this.store.putUser(u);
    for (const p of permissions) this.store.putPerm(p);
    for (const e of exchanges) if (e.durable) this.store.putExchange(e);
    for (const { row } of prepared) { if (row.durable) this.store.putQueue(row); else this.store.db.query("DELETE FROM queues WHERE vhost=? AND name=?").run(row.vhost, row.name); }
    for (const b of [...bindings, ...localBindings]) this.store.putBinding(b);
    for (const b of e2e) this.store.putExchangeBinding(b);
    for (const p of normalPolicies) this.store.putPolicy(p);
    for (const p of operatorPolicies) this.store.putParameter("operator-policies", p.vhost, p.name, JSON.stringify(p));
    for (const r of userLimits) this.store.putParameter("user-limits", "", r.user as string, JSON.stringify(r));
    for (const r of vhostLimits) this.store.putParameter("vhost-limits", r.vhost as string, "limits", JSON.stringify(r));
    for (const p of topics) this.store.putParameter("topic-permissions", p.vhost, `${p.user}\0${p.exchange}`, JSON.stringify(p));
    for (const p of parameters) this.store.putParameter("rt:" + p.component, p.vhost, p.name, JSON.stringify(p.value ?? null));
    for (const p of globals) this.store.putParameter("global", "", p.name, JSON.stringify(p.value ?? null));
  })();
  // Commit succeeded. Update existing objects so delivery callbacks retain their queue/body references.
  this.users = new Map(users.map((u) => [u.name, { hash: u.hash, tags: u.tags }])); this.vhosts = hosts; this.perms = permissions;
  this.exchanges = new Map(exchanges.map((e) => [this.key(e.vhost, e.name), e]));
  for (const q of removed) { this.queues.delete(this.key(q.vhost, q.name)); this.streams.delete(this.key(q.vhost, q.name)); if (q.raftGroup) this.cluster?.consensus?.node?.dropGroup(q.raftGroup); }
  for (const { row, effective, parsed, existing, fresh } of prepared) { const q = existing ?? fresh; Object.assign(q, row, { declaredArgs: { ...row.args }, args: effective, argsParsed: parsed }); this.queues.set(this.key(row.vhost, row.name), q); if (row.raftGroup) this.startQueueGroup(row.raftGroup, false); }
  this.bindings = [...bindings, ...localBindings]; this.e2e = e2e; this.policies = normalPolicies; this.operatorPolicies = operatorPolicies; this.topicPerms = topics;
  this.userConnLimit.clear(); this.userChanLimit.clear(); this.vhostConnLimit.clear(); this.vhostQueueLimit.clear();
  const setLimit = (map: Map<string, number>, key: string, value: unknown) => { if (typeof value === "number" && value >= 0) map.set(key, value); };
  for (const r of userLimits) { setLimit(this.userConnLimit, r.user as string, r["max-connections"]); setLimit(this.userChanLimit, r.user as string, r["max-channels"]); }
  for (const r of vhostLimits) { setLimit(this.vhostConnLimit, r.vhost as string, r["max-connections"]); setLimit(this.vhostQueueLimit, r.vhost as string, r["max-queues"]); }
}

Broker.prototype.applyRemote = applyRemote;
Broker.prototype.snapshot = snapshot;
Broker.prototype.applySnapshot = applySnapshot;
Broker.prototype.installMetaSnapshot = installMetaSnapshot;

declare module "./class.ts" {
  interface Broker {
    applyRemote: typeof applyRemote;
    snapshot: typeof snapshot;
    applySnapshot: typeof applySnapshot;
    installMetaSnapshot: typeof installMetaSnapshot;
  }
}

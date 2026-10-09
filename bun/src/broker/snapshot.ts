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
import { matchOne, policyItem, policyFromBody, fillPolicyArgs } from "./policy-data.ts";
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
  const qtype = String(argsIn["x-queue-type"] ?? argsIn.queue_type ?? "").toLowerCase();
  if (qtype === "quorum" || qtype === "classic" || qtype === "stream") args["x-queue-type"] = qtype;
  // Rust's names for the rest of its arguments.
  if (typeof argsIn.max_age_ms === "number" && args["x-max-age"] == null) args["x-max-age"] = `${Math.ceil(argsIn.max_age_ms / 1000)}s`;
  if (typeof argsIn.leader_locator === "string") args["x-queue-leader-locator"] = argsIn.leader_locator;
  if (argsIn.single_active === true) args["x-single-active-consumer"] = "true";
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
export function policyFromWire(p: Record<string, unknown>): Policy {
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
  };
}

/**
 * The wire form of a policy: Rust's `Policy` field names, which Rust parses
 * and `policyFromWire` reads back.
 */
export function policyToWire(p: Policy): Record<string, unknown> {
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
    users: [...this.users.entries()].map(([name, u]) => ({ name, hash: u.hash, tags: u.tags })),
    vhosts: [...this.vhosts],
    permissions: this.perms,
    exchanges: [...this.exchanges.values()],
    queues: [...this.queues.values()].map((q) => ({
      vhost: q.vhost,
      name: q.name,
      durable: q.durable,
      exclusive: q.exclusive,
      autoDelete: q.autoDelete,
      args: q.args,
      home: q.home,
      raftGroup: q.raftGroup ?? null,
    })),
    bindings: this.bindings,
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
  for (const u of snap.users ?? []) if (!this.users.has(u.name)) {
    this.users.set(u.name, { hash: u.hash, tags: u.tags });
    this.store.putUser(u);
  }
  for (const name of snap.vhosts ?? []) this.ensureBuiltins(name);
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

Broker.prototype.applyRemote = applyRemote;
Broker.prototype.snapshot = snapshot;
Broker.prototype.applySnapshot = applySnapshot;

declare module "./class.ts" {
  interface Broker {
    applyRemote: typeof applyRemote;
    snapshot: typeof snapshot;
    applySnapshot: typeof applySnapshot;
  }
}

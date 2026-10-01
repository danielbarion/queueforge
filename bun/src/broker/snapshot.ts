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
import { BUILTIN, emptyProm, type Consumer, type LiveMsg, type MgmtChannel, type MgmtConnection, type MgmtConsumer, type Policy, type Prom, type QArgs, type QueueLive, type TopicPerm } from "./model.ts";


/**
 * Apply one replicated topology record.
 *
 * @param kind Record kind, such as `exchange`, `queue`, `binding`, `user`, or `vhost`. An unknown kind is ignored.
 * @param payload Fields for that kind. A queue or exchange that is already present is not replaced, except a durable row is still stored.
 * @returns Nothing. The caller must not apply a message body with this method.
 */
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
    const row = payload as QueueRow;
    if (!this.queues.has(this.key(row.vhost, row.name))) {
      this.queues.set(this.key(row.vhost, row.name), this.makeQueue(row, !this.isLocalHome(row.home)));
    }
    if (row.durable) this.store.putQueue(row);
  } else if (kind === "delete_queue") {
    const vhost = String(payload.vhost);
    const name = String(payload.name ?? payload.queue);
    this.queues.delete(this.key(vhost, name));
    this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.queue === name));
    this.store.deleteQueue(vhost, name);
  } else if (kind === "binding") {
    const b = payload as BindRow;
    this.bindings.push(b);
    this.store.putBinding(b);
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
    })),
    bindings: this.bindings,
    consumed: this.consumed,
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
  for (const q of snap.queues ?? []) if (!this.queues.has(this.key(q.vhost, q.name))) {
    this.queues.set(this.key(q.vhost, q.name), this.makeQueue(q, !this.isLocalHome(q.home)));
    if (q.durable) this.store.putQueue(q);
  }
  this.applyConsumed(snap.consumed);
  for (const b of snap.bindings ?? []) {
    this.bindings.push(b);
    this.store.putBinding(b);
  }
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

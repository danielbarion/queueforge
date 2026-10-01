/**
 * Exchange, queue, and binding changes.
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


/** Broker.declareExchange. The parameters and return value are unchanged from the previous class method. */
export async function declareExchange(this: Broker, vhost: string, name: string, kind: string, durable: boolean, autoDelete: boolean, internal: boolean, alternate: string | null) {
  if (name.startsWith("amq.")) throw new ChanError(403, `ACCESS_REFUSED - exchange ${vhost}/${name}`);
  const row: ExRow = { vhost, name, kind, durable, autoDelete, internal, alternate };
  this.exchanges.set(this.key(vhost, name), row);
  if (durable) this.store.putExchange(row);
  await this.cluster?.replicate("exchange", row);
}

/** Broker.deleteExchange. The parameters and return value are unchanged from the previous class method. */
export async function deleteExchange(this: Broker, vhost: string, name: string) {
  if (name === "" || name.startsWith("amq.")) throw new ChanError(403, `ACCESS_REFUSED - exchange ${vhost}/${name}`);
  if (!this.exchanges.has(this.key(vhost, name))) throw new ChanError(404, `NOT_FOUND - exchange ${vhost}/${name}`);
  this.exchanges.delete(this.key(vhost, name));
  this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.exchange === name));
  this.store.deleteExchange(vhost, name);
  await this.cluster?.replicate("delete_exchange", { vhost, name });
}

/** Broker.declareQueue. The parameters and return value are unchanged from the previous class method. */
export async function declareQueue(this: Broker, opts: {
  vhost: string;
  name: string;
  durable: boolean;
  exclusive: boolean;
  autoDelete: boolean;
  passive: boolean;
  args: Record<string, string | number>;
}): Promise<{ name: string; messages: number; consumers: number }> {
  let name = opts.name;
  if (!name) name = `amq.gen-${crypto.randomUUID()}`;
  const home = this.homeOf(opts.vhost, name, opts.exclusive);
  const existing = this.queues.get(this.key(opts.vhost, name));
  if (opts.passive) {
    if (!existing) throw new ChanError(404, `NOT_FOUND - queue ${opts.vhost}/${name}`);
    this.prom.queuesDeclared++;
    return { name, messages: existing.ready.length, consumers: existing.consumers.length };
  }
  if (existing) {
    if (existing.durable !== opts.durable) throw new ChanError(406, `PRECONDITION_FAILED - inequivalent arg durable for queue ${name}`);
    this.prom.queuesDeclared++;
    return { name, messages: existing.ready.length, consumers: existing.consumers.length };
  }
  const qtype = String(opts.args["x-queue-type"] ?? "");
  if (!opts.durable && !opts.exclusive && qtype !== "quorum" && !this.transientNonexcl) {
    throw new ChanError(541, "INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated. By default, this feature is not permitted anymore.");
  }
  const here = [...this.queues.values()].filter((q) => q.vhost === opts.vhost).length;
  if (!this.queueAllowed(opts.vhost, here)) throw new ChanError(403, "ACCESS_REFUSED - queue limit");
  const requested = String(opts.args["x-queue-type"] ?? "");
  if (requested && requested !== "classic" && requested !== "quorum") {
    throw new ChanError(406, `PRECONDITION_FAILED - unsupported x-queue-type '${requested}'`);
  }
  let args = this.argsWithPolicy(opts.vhost, name, opts.args);
  let queueType = requested || (this.cfg.defaultQueueType === "quorum" && opts.durable && !opts.exclusive ? "quorum" : "classic");
  if (queueType === "quorum" && (!opts.durable || opts.exclusive)) {
    throw new ChanError(406, "PRECONDITION_FAILED - quorum queue must be durable and non-exclusive");
  }
  if (queueType === "quorum") args = { ...args, "x-queue-type": "quorum" };
  if (queueType === "quorum" && args["x-delivery-limit"] == null) args["x-delivery-limit"] = 20;
  const row: QueueRow = {
    vhost: opts.vhost,
    name,
    durable: opts.durable,
    exclusive: opts.exclusive,
    autoDelete: opts.autoDelete,
    args,
    home,
  };
  if (!this.isLocalHome(home)) {
    try {
      await this.cluster!.call(home!, "declare_queue", row);
    } catch (err) {
      const text = err instanceof Error ? err.message : String(err);
      if (!text.includes("exists")) throw new ChanError(541, `INTERNAL_ERROR - declare ${home}: ${text}`);
    }
  }
  const live = this.makeQueue(row, !this.isLocalHome(home));
  live.declaredArgs = { ...opts.args };
  this.queues.set(this.key(opts.vhost, name), live);
  if (opts.durable) this.store.putQueue(row);
  await this.cluster?.replicate("queue", row);
  this.prom.queuesDeclared++;
  this.prom.queuesCreated++;
  return { name, messages: 0, consumers: 0 };
}

/** Broker.deleteQueue. The parameters and return value are unchanged from the previous class method. */
export async function deleteQueue(this: Broker, vhost: string, name: string): Promise<number> {
  const q = this.queues.get(this.key(vhost, name));
  if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${name}`);
  if (!this.isLocalHome(q.home)) await this.cluster!.call(q.home!, "delete_queue", { vhost, name });
  const n = q.ready.length + q.unacked.size;
  this.prom.consumers = Math.max(0, this.prom.consumers - q.consumers.length);
  this.prom.queuesDeleted++;
  for (const c of q.consumers) c.onCancel?.();
  q.consumers = [];
  this.queues.delete(this.key(vhost, name));
  this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.queue === name));
  this.store.deleteQueue(vhost, name);
  await this.cluster?.replicate("delete_queue", { vhost, name });
  return n;
}

/** Broker.bind. The parameters and return value are unchanged from the previous class method. */
export async function bind(this: Broker, vhost: string, exchange: string, queue: string, routingKey: string, args: Array<[string, Field]>) {
  if (!this.exchanges.has(this.key(vhost, exchange))) throw new ChanError(404, `NOT_FOUND - exchange ${vhost}/${exchange}`);
  if (!this.queues.has(this.key(vhost, queue))) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${queue}`);
  const row: BindRow = { vhost, exchange, queue, routingKey, args };
  const argsJson = JSON.stringify(args);
  this.bindings = this.bindings.filter(
    (b) => !(b.vhost === vhost && b.exchange === exchange && b.queue === queue && b.routingKey === routingKey && JSON.stringify(b.args) === argsJson),
  );
  this.bindings.push(row);
  const q = this.queues.get(this.key(vhost, queue))!;
  if (q.durable || this.exchanges.get(this.key(vhost, exchange))?.durable) this.store.putBinding(row);
  await this.cluster?.replicate("binding", row);
}

/** Broker.unbind. The parameters and return value are unchanged from the previous class method. */
export async function unbind(this: Broker, vhost: string, exchange: string, queue: string, routingKey: string, args: Array<[string, Field]> | null) {
  const argsJson = args ? JSON.stringify(args) : null;
  this.bindings = this.bindings.filter((b) => {
    if (b.vhost !== vhost || b.exchange !== exchange || b.queue !== queue || b.routingKey !== routingKey) return true;
    if (argsJson == null) return false;
    return JSON.stringify(b.args) !== argsJson;
  });
  this.store.deleteBinding(vhost, exchange, queue, routingKey, argsJson);
  await this.cluster?.replicate("unbind", { vhost, exchange, queue, routingKey, args });
}

/** Broker.route. The parameters and return value are unchanged from the previous class method. */
export function route(this: Broker, vhost: string, exchange: string, routingKey: string, headers: Array<[string, Field]>): string[] {
  const seen = new Set<string>();
  const pending = [exchange];
  const out: string[] = [];
  while (pending.length) {
    const current = pending.pop()!;
    if (seen.has(current)) continue;
    seen.add(current);
    const dest = this.routeOne(vhost, current, routingKey, headers);
    out.push(...dest);
    const ex = this.exchanges.get(this.key(vhost, current));
    for (const edge of this.e2e) {
      if (edge.vhost !== vhost || edge.source !== current) continue;
      const ok = ex?.kind === "fanout"
        || (ex?.kind === "topic" ? topicMatches(edge.routingKey, routingKey) : edge.routingKey === routingKey);
      if (ok) pending.push(edge.destination);
    }
    if (!dest.length) {
      const userPol = this.matchPolicy(vhost, current, "exchanges");
      const opPol = this.matchOperatorPolicy(vhost, current, "exchanges");
      const alt = ex?.alternate || opPol?.alternate || userPol?.alternate || null;
      if (alt) pending.push(alt);
    }
  }
  return [...new Set(out)];
}

/** Broker.bindExchange. The parameters and return value are unchanged from the previous class method. */
export async function bindExchange(this: Broker, vhost: string, source: string, destination: string, routingKey: string) {
  if (source === "" || destination === "") throw new ChanError(406, "PRECONDITION_FAILED - cannot bind the default exchange");
  if (!this.exchanges.has(this.key(vhost, source)) || !this.exchanges.has(this.key(vhost, destination))) {
    throw new ChanError(404, `NOT_FOUND - no exchange '${source}' or '${destination}' in vhost '${vhost}'`);
  }
  this.e2e = this.e2e.filter((e) => !(e.vhost === vhost && e.source === source && e.destination === destination && e.routingKey === routingKey));
  this.e2e.push({ vhost, source, destination, routingKey });
}

/** Broker.unbindExchange. The parameters and return value are unchanged from the previous class method. */
export async function unbindExchange(this: Broker, vhost: string, source: string, destination: string, routingKey: string) {
  this.e2e = this.e2e.filter((e) => !(e.vhost === vhost && e.source === source && e.destination === destination && e.routingKey === routingKey));
}

/** Broker.routeOne. The parameters and return value are unchanged from the previous class method. */
export function routeOne(this: Broker, vhost: string, exchange: string, routingKey: string, headers: Array<[string, Field]>): string[] {
  if (exchange === "") {
    return this.queues.has(this.key(vhost, routingKey)) ? [routingKey] : [];
  }
  const ex = this.exchanges.get(this.key(vhost, exchange));
  if (!ex) throw new ChanError(404, `NOT_FOUND - exchange ${vhost}/${exchange}`);
  if (ex.internal && exchange !== "") throw new ChanError(403, `ACCESS_REFUSED - internal exchange ${exchange}`);
  const rows = this.bindings.filter((b) => b.vhost === vhost && b.exchange === exchange);
  if (ex.kind === "fanout") return [...new Set(rows.map((b) => b.queue))];
  if (ex.kind === "topic") return [...new Set(rows.filter((b) => topicMatches(b.routingKey, routingKey)).map((b) => b.queue))];
  if (ex.kind === "headers") return [...new Set(rows.filter((b) => headersMatch(b.args, headers)).map((b) => b.queue))];
  return [...new Set(rows.filter((b) => b.routingKey === routingKey).map((b) => b.queue))];
}

Broker.prototype.declareExchange = declareExchange;
Broker.prototype.deleteExchange = deleteExchange;
Broker.prototype.declareQueue = declareQueue;
Broker.prototype.deleteQueue = deleteQueue;
Broker.prototype.bind = bind;
Broker.prototype.unbind = unbind;
Broker.prototype.route = route;
Broker.prototype.bindExchange = bindExchange;
Broker.prototype.unbindExchange = unbindExchange;
Broker.prototype.routeOne = routeOne;

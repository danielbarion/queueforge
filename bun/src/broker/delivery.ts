/**
 * Consumer credit, ack, nack, get, expiry, and dead letter.
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


/** Broker.expire. The parameters and return value are unchanged from the previous class method. */
export function expire(this: Broker, q: QueueLive) {
  const now = Date.now();
  const keep: LiveMsg[] = [];
  for (const m of q.ready) {
    if (m.expiresAt != null && m.expiresAt <= now) {
      if (m.rowId != null) this.store.deleteMessage(m.rowId);
      const accepted = this.deadLetter(q, m, 0, "expired");
      if (!accepted && q.argsParsed.dlxStrategy === "at-least-once") {
        m.expiresAt = null;
        keep.push(m);
      } else {
        this.prom.dlxExpired++;
      }
    } else keep.push(m);
  }
  q.ready = keep;
}

/** Broker.deadLetter. The parameters and return value are unchanged from the previous class method. */
export function deadLetter(this: Broker, q: QueueLive, msg: LiveMsg, depth: number, reason: "expired" | "rejected" | "maxlen"): boolean {
  if (!q.argsParsed.dlx || depth > 8) return true;
  const headers = deathHeaders(q.name, reason, msg.exchange, msg.routingKey, msg.headers);
  const rk = q.argsParsed.dlxKey ?? msg.routingKey;
  let dests: string[] = [];
  try {
    dests = this.route(q.vhost, q.argsParsed.dlx, rk, headers);
  } catch {
    return q.argsParsed.dlxStrategy !== "at-least-once";
  }
  if (!dests.length) return q.argsParsed.dlxStrategy !== "at-least-once";
  let accepted = false;
  for (const name of dests) {
    const dest = this.queues.get(this.key(q.vhost, name));
    if (!dest || dest === q) continue;
    if (!this.isLocalHome(dest.home)) {
      accepted = true;
      void this.enqueue(q.vhost, name, {
        body: msg.body,
        exchange: q.argsParsed.dlx,
        routingKey: rk,
        headers,
        propRaw: propsWithDeath(headers),
        persistent: msg.persistent,
        priority: msg.priority,
        expiration: "",
      }).catch(() => {});
    } else if (
      this.enqueueLocal(
        dest,
        {
          body: msg.body,
          exchange: q.argsParsed.dlx,
          routingKey: rk,
          headers,
          propRaw: propsWithDeath(headers),
          persistent: msg.persistent,
          priority: msg.priority,
          expiration: "",
        },
        depth + 1,
      )
    ) {
      accepted = true;
    }
  }
  return accepted || q.argsParsed.dlxStrategy !== "at-least-once";
}

/** Broker.pump. The parameters and return value are unchanged from the previous class method. */
export function pump(this: Broker, q: QueueLive) {
  this.expire(q);
  let guard = 0;
  while (q.ready.length && guard++ < 100000) {
    const chosen = pickConsumer(q);
    if (!chosen) return;
    const msg = q.ready.shift()!;
    if (!chosen.noAck) q.unacked.set(msg.id, msg);
    else if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
    if (q.argsParsed.queueType === "quorum") {
      const target = chosen;
      void this.claimThenDeliver(q, target, msg);
      return;
    }
    this.noteDeliver(chosen.noAck, msg.redelivered);
    chosen.deliver(msg);
  }
}

/** Broker.noteDeliver. The parameters and return value are unchanged from the previous class method. */
export function noteDeliver(this: Broker, autoAck: boolean, redelivered: boolean) {
  this.prom.delivered++;
  if (autoAck) this.prom.deliveredConsumeAuto++;
  else this.prom.deliveredConsumeManual++;
  if (redelivered) this.prom.redelivered++;
}

/** Broker.claimThenDeliver. The parameters and return value are unchanged from the previous class method. */
export async function claimThenDeliver(this: Broker, q: QueueLive, chosen: Consumer, msg: LiveMsg) {
  await this.quorumDrop(q, msg.id);
  this.noteDeliver(chosen.noAck, msg.redelivered);
  chosen.deliver(msg);
  this.pump(q);
}

/** Broker.nextSession. The parameters and return value are unchanged from the previous class method. */
export function nextSession(this: Broker): number {
  const n = this.sessionsNext++;
  const slot = this.slot();
  if (!slot) return n;
  return slot * 0x100000000 + (n & 0xffffffff);
}

/** Broker.slot. The parameters and return value are unchanged from the previous class method. */
export function slot(this: Broker): number {
  if (!this.cfg.members.length || !this.cfg.nodeId) return 0;
  const sorted = [...this.cfg.members].sort((a, b) => (a.id < b.id ? -1 : 1));
  const i = sorted.findIndex((m) => m.id === this.cfg.nodeId);
  return i < 0 ? 1 : i + 1;
}

/** Broker.consume. The parameters and return value are unchanged from the previous class method. */
export async function consume(this: Broker, vhost: string, queue: string, consumer: Consumer): Promise<void> {
  const q = this.queues.get(this.key(vhost, queue));
  if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${queue}`);
  if (q.consumers.some((c) => c.exclusive) || (consumer.exclusive && q.consumers.length)) {
    throw new ChanError(403, `ACCESS_REFUSED - exclusive consumer on ${queue}`);
  }
  q.consumers.push(consumer);
  this.prom.consumers++;
  q.lastUsed = Date.now();
  this.sessions.set(consumer.session, { vhost, queue });
}

/** Broker.kick. The parameters and return value are unchanged from the previous class method. */
export async function kick(this: Broker, vhost: string, queue: string, session: number) {
  const q = this.queues.get(this.key(vhost, queue));
  if (!q) return;
  const consumer = q.consumers.find((c) => c.session === session);
  if (q.argsParsed.queueType === "quorum" && !this.isQuorumLeader()) {
    if (!consumer) return;
    q.consumers = q.consumers.filter((c) => c.session !== session);
    this.remoteQuorum.set(`${vhost}\0${queue}\0${consumer.tag}`, session);
    await this.cluster!.subscribe(
      this.quorumLeader(),
      { vhost, queue, session, noAck: consumer.noAck, exclusive: consumer.exclusive },
      (msg) => consumer.deliver(msg),
    );
    return;
  }
  if (q.argsParsed.queueType !== "quorum" && !this.isLocalHome(q.home)) {
    if (!consumer) return;
    await this.cluster!.subscribe(
      q.home!,
      { vhost, queue, session, noAck: consumer.noAck, exclusive: consumer.exclusive },
      (msg) => consumer.deliver(msg),
    );
    return;
  }
  this.pump(q);
}

/** Broker.cancel. The parameters and return value are unchanged from the previous class method. */
export async function cancel(this: Broker, vhost: string, queue: string, tag: string) {
  const q = this.queues.get(this.key(vhost, queue));
  if (!q) return;
  const consumer = q.consumers.find((c) => c.tag === tag);
  if (consumer) this.prom.consumers = Math.max(0, this.prom.consumers - 1);
  q.consumers = q.consumers.filter((c) => c.tag !== tag);
  const session = consumer?.session ?? this.remoteQuorum.get(`${vhost}\0${queue}\0${tag}`);
  this.remoteQuorum.delete(`${vhost}\0${queue}\0${tag}`);
  if (session != null && q.argsParsed.queueType === "quorum" && !this.isQuorumLeader()) {
    await this.cluster!.call(this.quorumLeader(), "unsub", { vhost, queue, session });
  }
}

/** Broker.ack. The parameters and return value are unchanged from the previous class method. */
export async function ack(this: Broker, vhost: string, queue: string, id: string) {
  const q = this.queues.get(this.key(vhost, queue));
  if (!q) return;
  if (q.argsParsed.queueType !== "quorum" && !this.isLocalHome(q.home)) {
    await this.cluster!.call(q.home!, "ack", { vhost, queue, id });
    return;
  }
  if (q.argsParsed.queueType === "quorum" && !q.unacked.has(id) && !this.isQuorumLeader()) {
    await this.cluster!.call(this.quorumLeader(), "ack", { vhost, queue, id });
    return;
  }
  const msg = q.unacked.get(id);
  if (!msg) return;
  q.unacked.delete(id);
  if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
  this.prom.acknowledged++;
  if (q.argsParsed.queueType === "quorum") {
    this.noteConsumed(q.vhost, q.name, id);
    await this.quorumDrop(q, id);
  }
  this.pump(q);
}

/** Broker.nack. The parameters and return value are unchanged from the previous class method. */
export async function nack(this: Broker, vhost: string, queue: string, id: string, requeue: boolean) {
  const q = this.queues.get(this.key(vhost, queue));
  if (!q) return;
  if (q.argsParsed.queueType !== "quorum" && !this.isLocalHome(q.home)) {
    await this.cluster!.call(q.home!, "nack", { vhost, queue, id, requeue });
    return;
  }
  if (q.argsParsed.queueType === "quorum" && !q.unacked.has(id) && !this.isQuorumLeader()) {
    await this.cluster!.call(this.quorumLeader(), "nack", { vhost, queue, id, requeue });
    return;
  }
  const msg = q.unacked.get(id);
  if (!msg) return;
  q.unacked.delete(id);
  if (requeue) {
    msg.deliveries = (msg.deliveries ?? 0) + 1;
    if (q.argsParsed.deliveryLimit != null && msg.deliveries >= q.argsParsed.deliveryLimit) {
      const accepted = this.deadLetter(q, msg, 0, "rejected");
      if (!accepted && q.argsParsed.dlxStrategy === "at-least-once") {
        q.unacked.set(id, msg);
        return;
      }
      if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
      this.prom.dlxDeliveryLimit++;
      if (q.argsParsed.queueType === "quorum") await this.quorumDrop(q, id);
    } else {
      msg.redelivered = true;
      q.ready.unshift(msg);
      this.pump(q);
    }
  } else {
    const accepted = this.deadLetter(q, msg, 0, "rejected");
    if (!accepted && q.argsParsed.dlxStrategy === "at-least-once") {
      msg.redelivered = true;
      q.ready.unshift(msg);
      this.pump(q);
      return;
    }
    if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
    this.prom.dlxRejected++;
    if (q.argsParsed.queueType === "quorum") await this.quorumDrop(q, id);
  }
}

/** Broker.get. The parameters and return value are unchanged from the previous class method. */
export async function get(this: Broker, vhost: string, queue: string, noAck: boolean): Promise<LiveMsg | null> {
  const q = this.queues.get(this.key(vhost, queue));
  if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${queue}`);
  if (q.argsParsed.queueType === "quorum" && !this.isQuorumLeader()) {
    try {
      const raw = (await this.cluster!.call(this.quorumLeader(), "get", { vhost, queue, noAck, no_ack: noAck })) as {
        empty?: boolean;
        msg?: { id?: string; body?: string; propRaw?: string; exchange?: string; routingKey?: string; persistent?: boolean; priority?: number; redelivered?: boolean };
        message?: { message_id?: string; body_b64?: string; exchange?: string; routing_key?: string; persistent?: boolean; redelivered?: boolean };
      } | null;
      if (!raw || raw.empty) return null;
      const bunMsg = raw.msg;
      const rustMsg = raw.message;
      const bodyB64 = bunMsg?.body ?? rustMsg?.body_b64;
      if (!bodyB64) return null;
      const id = String(bunMsg?.id ?? rustMsg?.message_id ?? "");
      this.dropLocal(vhost, queue, id);
      const leader = this.quorumLeader();
      const peers = this.cluster?.peerIds().filter((peer) => peer !== this.cfg.nodeId && peer !== leader) ?? [];
      await Promise.all(peers.map((peer) => this.cluster!.call(peer, "quorum_drop", { vhost, queue, id }).catch(() => null)));
      const propRawB64 = bunMsg?.propRaw ?? "";
      return {
        id,
        rowId: null,
        body: new Uint8Array(Buffer.from(bodyB64, "base64")),
        exchange: String(bunMsg?.exchange ?? rustMsg?.exchange ?? ""),
        routingKey: String(bunMsg?.routingKey ?? rustMsg?.routing_key ?? ""),
        headers: [],
        propRaw: propRawB64 ? new Uint8Array(Buffer.from(propRawB64, "base64")) : new Uint8Array(),
        persistent: (bunMsg?.persistent ?? rustMsg?.persistent) !== false,
        priority: Number(bunMsg?.priority ?? 0),
        expiresAt: null,
        redelivered: !!(bunMsg?.redelivered ?? rustMsg?.redelivered),
      };
    } catch {
      throw new ChanError(541, "INTERNAL_ERROR - queue home is unavailable");
    }
  }
  if (q.argsParsed.queueType !== "quorum" && !this.isLocalHome(q.home)) {
    try {
      const raw = (await this.cluster!.call(q.home!, "get", { vhost, queue, noAck })) as {
        empty?: boolean;
        msg?: LiveMsg & { body: string; propRaw: string };
      };
      if (!raw || raw.empty || !raw.msg) return null;
      const m = raw.msg;
      return {
        ...m,
        body: new Uint8Array(Buffer.from(m.body, "base64")),
        propRaw: new Uint8Array(Buffer.from(m.propRaw, "base64")),
      };
    } catch {
      throw new ChanError(541, "INTERNAL_ERROR - queue home is unavailable");
    }
  }
  this.expire(q);
  const msg = q.ready.shift();
  if (!msg) {
    this.prom.getEmpty++;
    return null;
  }
  this.prom.delivered++;
  if (noAck) this.prom.deliveredGetAuto++;
  else this.prom.deliveredGetManual++;
  if (msg.redelivered) this.prom.redelivered++;
  if (!noAck) q.unacked.set(msg.id, msg);
  else if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
  if (q.argsParsed.queueType === "quorum") await this.quorumDrop(q, msg.id);
  return msg;
}

/** Broker.purge. The parameters and return value are unchanged from the previous class method. */
export async function purge(this: Broker, vhost: string, queue: string): Promise<number> {
  const q = this.queues.get(this.key(vhost, queue));
  if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${queue}`);
  if (!this.isLocalHome(q.home)) {
    const n = (await this.cluster!.call(q.home!, "purge", { vhost, queue })) as number;
    return Number(n) || 0;
  }
  const n = q.ready.length;
  for (const m of q.ready) if (m.rowId != null) this.store.deleteMessage(m.rowId);
  q.ready = [];
  return n;
}

Broker.prototype.expire = expire;
Broker.prototype.deadLetter = deadLetter;
Broker.prototype.pump = pump;
Broker.prototype.noteDeliver = noteDeliver;
Broker.prototype.claimThenDeliver = claimThenDeliver;
Broker.prototype.nextSession = nextSession;
Broker.prototype.slot = slot;
Broker.prototype.consume = consume;
Broker.prototype.kick = kick;
Broker.prototype.cancel = cancel;
Broker.prototype.ack = ack;
Broker.prototype.nack = nack;
Broker.prototype.get = get;
Broker.prototype.purge = purge;

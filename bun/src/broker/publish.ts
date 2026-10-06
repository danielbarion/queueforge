/**
 * Publish and the classic enqueue path.
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
import { fedLinks } from "./federation.ts";
import { parseArgs, deathHeaders, propsWithDeath, argsFromFields, takeTransientSeq } from "./args.ts";
import { topicMatches, headersMatch, fnv1a, headerList, overflowOf, liveFrom, pickConsumer, queueHome } from "./routing.ts";
import { matchOne, policyItem, policyFromBody, fillPolicyArgs } from "./policy-data.ts";
import { BUILTIN, EMPTY_BODY, EMPTY_HEADERS, emptyProm, type Consumer, type LiveMsg, type MgmtChannel, type MgmtConnection, type MgmtConsumer, type Policy, type Prom, type QArgs, type QueueLive, type TopicPerm } from "./model.ts";


/**
 * Route one publish and enqueue a copy on each destination.
 *
 * @param input Vhost, exchange, routing key, body, headers, raw properties, persistence, priority, and expiration. `confirm` counts a publisher confirm. `mandatory` counts an unroutable publish as returned instead of dropped.
 * @returns `ack` when every destination accepted the body, `nack` when one rejected it, or `return` when nothing was routed. A missing default-exchange queue throws 404. BCC is stripped from the stored headers.
 */
const SLOW = Symbol("slow");

/**
 * Local classic publish with no fan-out. Returns the confirm outcome, or
 * `SLOW` when quorum, a remote home, or extra destinations need the full path.
 * The durable wait is the only promise. A resolved async wrapper per message
 * was a pipeline tax.
 */
function localClassicFast(
  this: Broker,
  input: {
    vhost: string;
    exchange: string;
    routingKey: string;
    body: Uint8Array;
    headers: Array<[string, Field]>;
    propRaw: Uint8Array;
    persistent: boolean;
    priority: number;
    expiration: string;
    confirm?: boolean;
    mandatory?: boolean;
  },
): "ack" | "nack" | Promise<"ack"> | typeof SLOW {
  if (input.exchange !== "") return SLOW;
  if (input.headers.some(([key]) => key === "CC" || key === "BCC")) return SLOW;
  if (fedLinks.some((link) => link.upstream === input.vhost)) return SLOW;
  const q = this.queues.get(this.key(input.vhost, input.routingKey));
  if (!q || q.argsParsed.queueType === "quorum" || !this.isLocalHome(q.home)) return SLOW;
  this.prom.received++;
  if (input.confirm) this.prom.receivedConfirm++;
  this.prom.routed++;
  const ok = this.enqueueLocal(q, {
    body: input.body,
    exchange: input.exchange,
    routingKey: input.routingKey,
    headers: input.headers,
    propRaw: input.propRaw,
    persistent: input.persistent,
    priority: input.priority,
    expiration: input.expiration,
  }, 0);
  if (!ok) return "nack";
  if (!input.persistent) {
    if (input.confirm) this.prom.confirmed++;
    return "ack";
  }
  // The covering fsync already ran. Ack in this turn, ahead of the microtask
  // that a cold lone confirm still uses to gather a burst. The counter moves
  // only when that flush left the append staged. A later row must not.
  if (this.store.flushIfLone()) {
    this.store.noteConfirmReleased();
    if (input.confirm) this.prom.confirmed++;
    return "ack";
  }
  return this.store.whenDurable().then(() => {
    if (input.confirm) this.prom.confirmed++;
    return "ack" as const;
  });
}

export function publish(this: Broker, input: {
  vhost: string;
  exchange: string;
  routingKey: string;
  body: Uint8Array;
  headers: Array<[string, Field]>;
  propRaw: Uint8Array;
  persistent: boolean;
  priority: number;
  expiration: string;
  confirm?: boolean;
  mandatory?: boolean;
}): Promise<"ack" | "nack" | "return"> | "ack" | "nack" | "return" {
  const fast = localClassicFast.call(this, input);
  if (fast !== SLOW) return fast;
  return publishSlow.call(this, input);
}

async function publishSlow(this: Broker, input: {
  vhost: string;
  exchange: string;
  routingKey: string;
  body: Uint8Array;
  headers: Array<[string, Field]>;
  propRaw: Uint8Array;
  persistent: boolean;
  priority: number;
  expiration: string;
  confirm?: boolean;
  mandatory?: boolean;
}): Promise<"ack" | "nack" | "return"> {
  this.prom.received++;
  if (input.confirm) this.prom.receivedConfirm++;
  if (input.exchange === "") {
    if (!this.queues.has(this.key(input.vhost, input.routingKey))) {
      throw new ChanError(404, `NOT_FOUND - no queue '${input.routingKey}' in vhost '${input.vhost}'`);
    }
  }
  const dests = this.route(input.vhost, input.exchange, input.routingKey, input.headers);
  for (const key of [...headerList(input.headers, "CC"), ...headerList(input.headers, "BCC")]) {
    if (key === input.routingKey) continue;
    for (const name of this.route(input.vhost, input.exchange, key, input.headers)) {
      if (!dests.includes(name)) dests.push(name);
    }
  }
  if (input.headers.some(([key]) => key === "BCC")) {
    input.headers = input.headers.filter(([key]) => key !== "BCC");
    if (input.propRaw.length) input.propRaw = replaceHeaderTable(input.propRaw, input.headers);
  }
  const copies = fedLinks.filter((link) => link.upstream === input.vhost && new RegExp(link.pattern).test(input.exchange));
  if (!dests.length && copies.length === 0) {
    if (input.mandatory) this.prom.unroutableReturned++;
    else this.prom.unroutableDropped++;
    return "return";
  }
  this.prom.routed += dests.length;
  let rejected = false;
  // Quorum and a forwarded classic home already waited for the covering fsync.
  // A second wait joins a flush that started after that one.
  let needsLocalDurable = false;
  const noteLocalDurable = (vhost: string, name: string) => {
    if (!input.persistent) return;
    const q = this.queues.get(this.key(vhost, name));
    if (!q || q.argsParsed.queueType === "quorum" || !this.isLocalHome(q.home)) return;
    needsLocalDurable = true;
  };
  for (const name of dests) {
    noteLocalDurable(input.vhost, name);
    const ok = await this.enqueue(input.vhost, name, {
      body: input.body,
      exchange: input.exchange,
      routingKey: input.routingKey,
      headers: input.headers,
      propRaw: input.propRaw,
      persistent: input.persistent,
      priority: input.priority,
      expiration: input.expiration,
    });
    if (!ok) rejected = true;
  }
  for (const link of copies) {
    for (const name of this.route(link.downstream, input.exchange, input.routingKey, input.headers)) {
      noteLocalDurable(link.downstream, name);
      const ok = await this.enqueue(link.downstream, name, {
        body: input.body,
        exchange: input.exchange,
        routingKey: input.routingKey,
        headers: input.headers,
        propRaw: input.propRaw,
        persistent: input.persistent,
        priority: input.priority,
        expiration: input.expiration,
      });
      if (!ok) rejected = true;
    }
  }
  // every_n_ms stages a local classic row and fsyncs on the interval. The confirm waits for that fsync.
  // Quorum and a forwarded home already waited on their own copy. Counting here
  // would blame this confirm for a newer row that is still staged.
  if (!rejected && needsLocalDurable) await this.store.whenDurable();
  if (!rejected && input.confirm) this.prom.confirmed++;
  return rejected ? "nack" : "ack";
}

/**
 * Enqueue one message on a named queue.
 *
 * @param vhost Vhost that owns the queue.
 * @param name Queue name. A missing queue throws 404.
 * @param src Body and routing fields. `expiration` is milliseconds as text.
 * @returns True when the queue accepted the message. A quorum queue uses `enqueueQuorum`. A classic queue whose home is another node is forwarded, and a failed forward throws 541.
 */
export async function enqueue(this: Broker,
  vhost: string,
  name: string,
  src: {
    body: Uint8Array;
    exchange: string;
    routingKey: string;
    headers: Array<[string, Field]>;
    propRaw: Uint8Array;
    persistent: boolean;
    priority: number;
    expiration: string;
  },
): Promise<boolean> {
  const q = this.queues.get(this.key(vhost, name));
  if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${name}`);
  if (q.argsParsed.queueType === "quorum") return this.enqueueQuorum(q, src);
  if (!this.isLocalHome(q.home)) {
    try {
      await this.cluster!.call(q.home!, "enqueue", {
        vhost,
        queue: name,
        body: Buffer.from(src.body).toString("base64"),
        exchange: src.exchange,
        routingKey: src.routingKey,
        headers: src.headers,
        propRaw: Buffer.from(src.propRaw).toString("base64"),
        persistent: src.persistent,
        priority: src.priority,
        expiration: src.expiration,
      });
      return true;
    } catch (err) {
      throw new ChanError(541, `INTERNAL_ERROR - queue home is unavailable`);
    }
  }
  return this.enqueueLocal(q, src, 0);
}

function needsJsonEscape(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 32 || c === 34 || c === 92) return true;
  }
  return false;
}

/** Last simple meta string. The benchmark repeats the same properties on every publish. */
let simpleMeta = "";
let simpleExchange = "";
let simpleKey = "";
let simplePersistent = false;
let simplePriority = -1;
let simpleRedelivered = false;
let simpleRaw: Uint8Array | null = null;

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/** Stored meta for a durable message. The plain shape matches JSON.parse on recovery. */
function durableMeta(
  src: {
    id?: string;
    exchange: string;
    routingKey: string;
    headers: Array<[string, Field]>;
    propRaw: Uint8Array;
    persistent: boolean;
    priority: number;
    redelivered?: boolean;
  },
  expiresAt: number | null,
): string {
  if (
    !src.id &&
    src.headers.length === 0 &&
    expiresAt == null &&
    !needsJsonEscape(src.exchange) &&
    !needsJsonEscape(src.routingKey)
  ) {
    const redelivered = !!src.redelivered;
    if (
      simpleMeta.length > 0 &&
      simpleExchange === src.exchange &&
      simpleKey === src.routingKey &&
      simplePersistent === src.persistent &&
      simplePriority === src.priority &&
      simpleRedelivered === redelivered &&
      simpleRaw != null &&
      sameBytes(simpleRaw, src.propRaw)
    ) {
      return simpleMeta;
    }
    const prop = Buffer.from(src.propRaw).toString("base64");
    simpleExchange = src.exchange;
    simpleKey = src.routingKey;
    simplePersistent = src.persistent;
    simplePriority = src.priority;
    simpleRedelivered = redelivered;
    simpleRaw = src.propRaw.slice();
    simpleMeta = `{"exchange":"${src.exchange}","routingKey":"${src.routingKey}","headers":[],"propRaw":"${prop}","persistent":${src.persistent},"priority":${src.priority},"expiresAt":null,"redelivered":${redelivered ? "true" : "false"}}`;
    return simpleMeta;
  }
  return JSON.stringify({
    id: src.id,
    exchange: src.exchange,
    routingKey: src.routingKey,
    headers: src.headers,
    propRaw: Buffer.from(src.propRaw).toString("base64"),
    persistent: src.persistent,
    priority: src.priority,
    expiresAt,
    redelivered: !!src.redelivered,
  });
}

/**
 * Place one message on a queue owned by this process.
 *
 * @param q Live queue. Ready messages and the store are updated.
 * @param src Body and routing fields. `id` is kept when the caller already chose one.
 * @param depth Dead-letter nesting. Callers start at 0.
 * @returns False when overflow rejects the publish. A durable persistent message is inserted before it becomes ready. A follower quorum copy stays in `replicas`.
 */
export function enqueueLocal(this: Broker,
  q: QueueLive,
  src: {
    body: Uint8Array;
    exchange: string;
    routingKey: string;
    headers: Array<[string, Field]>;
    propRaw: Uint8Array;
    persistent: boolean;
    priority: number;
    expiration: string;
    redelivered?: boolean;
    id?: string;
    /** Set while a quorum confirm is still waiting on the peer fsync. */
    confirmGate?: { hold: boolean };
  },
  depth: number,
): boolean {
  this.expire(q);
  const bytes = q.ready.bytes;
  const overCount = q.argsParsed.maxLength != null && q.ready.length >= q.argsParsed.maxLength;
  const overBytes = q.argsParsed.maxLengthBytes != null && bytes + src.body.length > q.argsParsed.maxLengthBytes;
  if (overCount || overBytes) {
    if (q.argsParsed.overflow === "reject-publish" || q.argsParsed.overflow === "reject-publish-dlx") {
      if (q.argsParsed.overflow === "reject-publish-dlx") {
        this.deadLetter(q, liveFrom(q, src), depth, "maxlen");
        this.prom.dlxMaxlen++;
      }
      return false;
    }
    while (
      q.ready.length &&
      ((q.argsParsed.maxLength != null && q.ready.length >= q.argsParsed.maxLength) ||
        (q.argsParsed.maxLengthBytes != null && q.ready.bytes + src.body.length > q.argsParsed.maxLengthBytes))
    ) {
      const dropped = q.ready.shift()!;
      if (dropped.rowId != null) this.store.deleteMessage(dropped.rowId);
      const accepted = this.deadLetter(q, dropped, depth, "maxlen");
      if (!accepted && q.argsParsed.dlxStrategy === "at-least-once") {
        q.ready.unshift(dropped);
        return false;
      }
      this.prom.dlxMaxlen++;
    }
  }
  let expiresAt: number | null = null;
  if (q.argsParsed.messageTtl != null) expiresAt = Date.now() + q.argsParsed.messageTtl;
  if (src.expiration) {
    const ms = Number(src.expiration);
    if (Number.isFinite(ms)) {
      const at = Date.now() + ms;
      expiresAt = expiresAt == null ? at : Math.min(expiresAt, at);
    }
  }
  let rowId: number | null = null;
  if (q.durable && src.persistent) {
    rowId = this.store.insertMessage(q.vhost, q.name, src.body, durableMeta(src, expiresAt));
  }
  const msg: LiveMsg = {
    id: src.id ?? (rowId != null ? `d-${rowId}` : `t-${takeTransientSeq()}`),
    rowId,
    body: src.body,
    exchange: src.exchange,
    routingKey: src.routingKey,
    headers: src.headers,
    propRaw: src.propRaw,
    persistent: src.persistent,
    priority: src.priority,
    expiresAt,
    redelivered: !!src.redelivered,
    confirmGate: src.confirmGate,
  };
  // The row owns the bytes and the property block. A backlog of full records
  // made a later confirm pause under the memory cap.
  if (rowId != null && q.argsParsed.queueType === "quorum") {
    msg.bodyBytes = src.body.length;
    msg.body = EMPTY_BODY;
    msg.propRaw = EMPTY_BODY;
    msg.headers = EMPTY_HEADERS;
    msg.exchange = "";
    msg.routingKey = "";
    msg.slim = true;
  }
  if (q.argsParsed.queueType === "quorum" && !this.isQuorumLeader()) {
    q.replicas.set(msg.id, msg);
    return true;
  }
  if (q.argsParsed.maxPriority != null) {
    const p = Math.min(src.priority, q.argsParsed.maxPriority);
    msg.priority = p;
    let i = 0;
    while (i < q.ready.length && q.ready.at(i)!.priority >= p) i++;
    q.ready.insertAt(i, msg);
  } else {
    q.ready.push(msg);
  }
  // A quorum confirm releases `confirmGate` and pumps after the peer fsync.
  if (!src.confirmGate && (q.argsParsed.queueType !== "quorum" || this.isQuorumLeader())) this.pump(q);
  return true;
}

Broker.prototype.publish = publish;
Broker.prototype.enqueue = enqueue;
Broker.prototype.enqueueLocal = enqueueLocal;

declare module "./class.ts" {
  interface Broker {
    publish: typeof publish;
    enqueue: typeof enqueue;
    enqueueLocal: typeof enqueueLocal;
  }
}

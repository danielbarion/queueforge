/**
 * Publish and the classic enqueue path.
 *
 * These functions are the Broker methods. Loading this file installs them.
 */
import { Broker, type QuorumAcks } from "./class.ts";
import { STORE_ONLY_PREFIX } from "../raft/node.ts";
import { fieldEq, fieldStr, replaceHeaderTable, writeTable, type Field } from "../codec.ts";
import { readProps } from "../amqp10/map.ts";
import type { Config } from "../config.ts";
import { ChanError } from "../errors.ts";
import { routeEpoch } from "./topology.ts";
import { Store, type BindRow, type ExRow, type QueueRow } from "../store.ts";
import { encodeQuorumAppend } from "../wire.ts";
import { durableMajority, type MemberCopy } from "../quorum-confirm.ts";
import { rabbitPasswordHashMatches } from "./auth.ts";
import { parseArgs, deathHeaders, propsWithDeath, argsFromFields, takeTransientSeq } from "./args.ts";
import { topicMatches, headersMatch, fnv1a, headerList, overflowOf, liveFrom, pickConsumer, queueHome } from "./routing.ts";
import { matchOne, policyItem, policyFromBody, fillPolicyArgs } from "./policy-data.ts";
import { BUILTIN, EMPTY_BODY, EMPTY_HEADERS, emptyProm, type Consumer, type LiveMsg, type MgmtChannel, type MgmtConnection, type MgmtConsumer, type Policy, type Prom, type QArgs, type QueueLive, type TopicPerm } from "./model.ts";
import { notePublish, noteEnqueue, POOL_MIN, taintBody } from "../body-pool.ts";


/**
 * Route one publish and enqueue a copy on each destination.
 *
 * @param input Vhost, exchange, routing key, body, headers, raw properties, persistence, priority, and expiration. `confirm` counts a publisher confirm. `mandatory` counts an unroutable publish as returned instead of dropped.
 * @returns `ack` when every destination accepted the body, `nack` when one rejected it, or `return` when nothing was routed. A default-exchange publish to a missing queue is unroutable. BCC is stripped from the stored headers.
 */
const SLOW = Symbol("slow");

/** Ready depth past which a new durable classic message keeps only its row id. */
const PAGE_AFTER_READY = 2048;
/** The same, in body bytes: 1 MiB bodies page after 64, not after 2048. */
const PAGE_AFTER_BYTES = 64 * 1024 * 1024;

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
): "ack" | "nack" | Promise<"ack" | "nack"> | typeof SLOW {
  if (input.headers.length && input.headers.some(([key]) => key === "CC" || key === "BCC")) return SLOW;
  // A publish burst names one exchange and key: the queue name it reached
  // last time is reused while the topology is unchanged (`routeEpoch`). The
  // queue itself is looked up again, so a deleted one is never used.
  const epoch = routeEpoch(this);
  const memo = fastRoute;
  let name: string | undefined;
  if (memo !== null && memo.epoch === epoch && memo.broker === this && memo.routingKey === input.routingKey && memo.exchange === input.exchange && memo.vhost === input.vhost) {
    name = memo.queue;
  } else {
    name = fastQueue.call(this, input);
    if (name === undefined) return SLOW;
    fastRoute = { broker: this, epoch, vhost: input.vhost, exchange: input.exchange, routingKey: input.routingKey, queue: name };
  }
  const q = this.queues.get(this.key(input.vhost, name));
  if (q === undefined) return SLOW;
  return publishToQueue.call(this, q, input);
}

/** Last fast-path route: see `localClassicFast`. */
let fastRoute: { broker: Broker; epoch: object; vhost: string; exchange: string; routingKey: string; queue: string } | null = null;

/** The name of the one queue a fast-path publish reaches, or undefined for the slow path. */
function fastQueue(this: Broker, input: { vhost: string; exchange: string; routingKey: string; headers: Array<[string, Field]>; propRaw: Uint8Array }): string | undefined {
  let name = input.routingKey;
  if (input.exchange !== "") {
    // A named exchange that routes to one queue is the common publish
    // (PerfTest uses amq.direct). The async multi-destination path was a
    // third of the work-queue cell. Routes are cached in `route`.
    const ex = this.exchanges.get(this.key(input.vhost, input.exchange));
    if (!ex || ex.kind === "x-delayed-message" || ex.kind === "headers" || ex.hashOn) return undefined;
    // Only kinds whose route depends on the key alone may be memoized.
    if (ex.kind !== "direct" && ex.kind !== "topic" && ex.kind !== "fanout") return undefined;
    const dests = this.route(input.vhost, input.exchange, input.routingKey, input.headers, input.propRaw);
    if (dests.length !== 1) return undefined;
    name = dests[0]!;
  }
  return name;
}

function publishToQueue(
  this: Broker,
  q: QueueLive,
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
): "ack" | "nack" | Promise<"ack" | "nack"> | typeof SLOW {
  if (q && q.argsParsed.queueType === "quorum") return localQuorumFast.call(this, q, input);
  if (!q || q.argsParsed.queueType === "quorum" || q.raftGroup || !this.isLocalHome(q.home)) return SLOW;
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
    pooled: true,
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
  // One ack promise per group commit: every confirm in it waits on the same
  // store promise, so a `then` per publish only repeated the same step.
  const durable = this.store.whenDurable();
  let acks = this.durableAcks;
  if (!acks || acks.of !== durable) {
    const next = { of: durable, confirms: 0, ack: null as unknown as Promise<"ack"> };
    next.ack = durable.then(() => {
      this.prom.confirmed += next.confirms;
      return "ack" as const;
    });
    acks = this.durableAcks = next;
  }
  if (input.confirm) acks.confirms++;
  return acks.ack;
}

/**
 * A unique part of quorum message ids for this process: its start time and a
 * random tag, then a counter. A `Date.now()` and a random string per publish
 * were a visible share of a 1 CPU quorum publish.
 */
const QUORUM_ID_BASE = `${Date.now()}-${Math.random().toString(16).slice(2, 10)}-`;
let quorumIdSeq = 0;
/** The fixed part of this broker's quorum ids, with and without Raft. */
const quorumIdPrefixes = new WeakMap<Broker, [string, string]>();
function quorumIdPrefix(broker: Broker, raft: boolean): string {
  let p = quorumIdPrefixes.get(broker);
  if (!p) {
    p = [`q-${broker.cfg.nodeId}-${QUORUM_ID_BASE}`, `${STORE_ONLY_PREFIX}${broker.cfg.nodeId}-${QUORUM_ID_BASE}`];
    quorumIdPrefixes.set(broker, p);
  }
  return p[raft ? 1 : 0];
}

/**
 * Persistent publish to a quorum queue whose whole durable majority is this
 * process: a lone broker (`quorumAlone`), or the only voter of its Raft group
 * (`soloLocal`, which skips the log). The body is stored here and held from
 * delivery until the covering fsync, as `enqueueQuorum` does, but without an
 * async chain per message: every publish in one group commit shares one ack,
 * which opens their gates and pumps each queue once.
 */
function localQuorumFast(
  this: Broker,
  q: QueueLive,
  input: { body: Uint8Array; exchange: string; routingKey: string; headers: Array<[string, Field]>; propRaw: Uint8Array; persistent: boolean; priority: number; expiration: string; confirm?: boolean },
): "nack" | Promise<"ack" | "nack"> | typeof SLOW {
  if (!input.persistent || !q.durable) return SLOW;
  const raft = !!this.cluster?.consensus?.node;
  const solo = raft ? this.soloLocal(q) : this.quorumAlone();
  if (!solo || !this.isQuorumLeader(q)) return SLOW;
  this.prom.received++;
  if (input.confirm) this.prom.receivedConfirm++;
  this.prom.routed++;
  const id = quorumIdPrefix(this, raft) + ++quorumIdSeq;
  const gate = { hold: true };
  const ok = this.enqueueLocal(q, {
    body: input.body,
    exchange: input.exchange,
    routingKey: input.routingKey,
    headers: input.headers,
    propRaw: input.propRaw,
    persistent: true,
    priority: input.priority,
    expiration: input.expiration,
    id,
    confirmGate: gate,
    // A Raft group may add a member that copies ready bodies from memory.
    pooled: !raft,
  }, 0);
  if (!ok) return "nack";
  // After the insert: the group commit that covers this row.
  const durable = this.store.whenDurable();
  let batch = this.quorumAcks;
  if (!batch || batch.of !== durable) {
    const next: QuorumAcks = { of: durable, confirms: 0, gates: [], queues: [], ids: [], ack: null as unknown as Promise<"ack" | "nack"> };
    next.ack = durable.then(
      () => {
        for (const g of next.gates) g.hold = false;
        for (const held of new Set(next.queues)) this.pump(held);
        this.prom.confirmed += next.confirms;
        return "ack" as const;
      },
      () => {
        for (let i = 0; i < next.ids.length; i++) this.dropLocal(next.queues[i]!.vhost, next.queues[i]!.name, next.ids[i]!);
        for (const held of new Set(next.queues)) this.pump(held);
        return "nack" as const;
      },
    );
    batch = this.quorumAcks = next;
  }
  batch.gates.push(gate);
  batch.queues.push(q);
  batch.ids.push(id);
  if (input.confirm) batch.confirms++;
  return batch.ack;
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
  // A pooled body routed a second time (shovel, federation, a re-publish)
  // is shared from here on and leaves the body pool.
  if (input.body.length >= POOL_MIN) notePublish(input.body);
  if (this.tracedVhosts.size !== 0 && this.tracedVhosts.has(input.vhost)) this.tracePublish(input);
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
  // Fan-out, a forward, CC/BCC, a delayed hold: not the single-queue path.
  if (input.body.length >= POOL_MIN) taintBody(input.body);
  // x-delayed-message: hold the message for its x-delay, then route it.
  // The publish is confirmed now, as the RabbitMQ plugin does. A waiting
  // message is stored, so it is still delivered after a restart.
  if (input.exchange !== "" && !(input as { delayed?: boolean }).delayed) {
    const ex = this.exchanges.get(this.key(input.vhost, input.exchange));
    if (ex?.kind === "x-delayed-message") {
      const raw = input.headers.find(([k]) => k === "x-delay")?.[1];
      const delay = raw && "v" in raw ? Number(raw.v) : 0;
      if (Number.isFinite(delay) && delay > 0) {
        this.prom.received++;
        this.holdDelayed(input, Date.now() + delay);
        return "ack";
      }
    }
  }
  this.prom.received++;
  if (input.confirm) this.prom.receivedConfirm++;
  // A default-exchange publish to a missing queue is unroutable, as in
  // RabbitMQ: returned when mandatory, else dropped. The channel stays open.
  const dests = this.route(input.vhost, input.exchange, input.routingKey, input.headers, input.propRaw);
  for (const key of [...headerList(input.headers, "CC"), ...headerList(input.headers, "BCC")]) {
    if (key === input.routingKey) continue;
    for (const name of this.route(input.vhost, input.exchange, key, input.headers, input.propRaw)) {
      if (!dests.includes(name)) dests.push(name);
    }
  }
  if (input.headers.some(([key]) => key === "BCC")) {
    input.headers = input.headers.filter(([key]) => key !== "BCC");
    if (input.propRaw.length) input.propRaw = replaceHeaderTable(input.propRaw, input.headers);
  }
  if (!dests.length) {
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
  // Start every destination before waiting on any. A local enqueue happens in
  // the call and a forward is written in the call, so each queue still sees
  // this channel's publishes in order. Awaiting one forward after another made
  // a fanout to queues on 3 other processes cost 15 round trips per publish.
  const waits: Array<Promise<boolean>> = [];
  const src = {
    body: input.body,
    exchange: input.exchange,
    routingKey: input.routingKey,
    headers: input.headers,
    propRaw: input.propRaw,
    persistent: input.persistent,
    priority: input.priority,
    expiration: input.expiration,
  };
  // Classic queues homed on one other process share one forward when that
  // process takes a `queues` list (enqueue_many).
  const byHome = new Map<string, string[]>();
  const many = this.cluster?.consensus?.manyPeers;
  for (const name of dests) {
    noteLocalDurable(input.vhost, name);
    const q = this.queues.get(this.key(input.vhost, name));
    const classic = q && q.argsParsed.queueType !== "quorum" && !(q.argsParsed.queueType === "stream" && q.raftGroup);
    if (classic && q.home && !this.isLocalHome(q.home) && many?.has(q.home)) {
      const list = byHome.get(q.home);
      if (list) list.push(name);
      else byHome.set(q.home, [name]);
      continue;
    }
    waits.push(this.enqueue(input.vhost, name, src));
  }
  for (const [home, names] of byHome) {
    if (names.length === 1) {
      waits.push(this.enqueue(input.vhost, names[0]!, src));
      continue;
    }
    waits.push(
      this.cluster!.callBytes(home, "enqueue", {
        vhost: input.vhost,
        queue: names[0],
        queues: names,
        exchange: src.exchange,
        routingKey: src.routingKey,
        headers: src.headers,
        persistent: src.persistent,
        priority: src.priority,
        expiration: src.expiration,
      }, src.body, src.propRaw).then(
        () => true,
        () => {
          throw new ChanError(541, "INTERNAL_ERROR - queue home is unavailable");
        },
      ),
    );
  }
  for (const ok of await Promise.all(waits)) if (!ok) rejected = true;
  // every_n_ms stages a local classic row and fsyncs on the interval. The confirm waits for that fsync.
  // Quorum and a forwarded home already waited on their own copy. Counting here
  // would blame this confirm for a newer row that is still staged.
  if (!rejected && needsLocalDurable) await this.store.whenDurable();
  if (!rejected && input.confirm) this.prom.confirmed++;
  return rejected ? "nack" : "ack";
}

/** Store key of the messages an x-delayed-message exchange is holding. */
const DELAYED = "delayed-message";
let delayedSeq = 0;

type DelayedRow = {
  exchange: string;
  routingKey: string;
  body: string;
  propRaw: string;
  persistent: boolean;
  priority: number;
  expiration: string;
  due: number;
};

/** Store key of delayed messages a cores-mode worker has not yet handed to the delay owner. */
const DELAYED_FORWARD = "delayed-message-forward";
/** Delayed rows this process has armed, so a repeated hand-over arms once. */
const armedDelayed = new Set<string>();
/** Recently routed delayed rows, so a hand-over retried after its reply was lost is not routed twice. */
const routedDelayed = new Set<string>();

/**
 * The process that holds and routes every delayed message: worker n0 when
 * one broker runs one worker per core, so pending messages do not depend on
 * which worker took the publish, and n0 exists for any worker count. Null
 * outside cores mode, where each process holds its own.
 */
export function delayOwner(this: Broker): string | null {
  if (!this.workerPool()) return null;
  return this.cfg.members.some((m) => m.id === "n0") ? "n0" : null;
}

/**
 * Hold one message for an x-delayed-message exchange until `due` (epoch ms).
 * The row is stored first, and dropped once the message is routed. In cores
 * mode a worker other than the delay owner stores the row as pending, hands
 * it to the owner, and drops it once the owner has stored it.
 */
export function holdDelayed(this: Broker, input: { vhost: string; exchange: string; routingKey: string; body: Uint8Array; propRaw: Uint8Array; persistent: boolean; priority: number; expiration: string }, due: number) {
  const name = `${due}-${this.cfg.nodeId || "solo"}-${QUORUM_ID_BASE}${++delayedSeq}`;
  const row: DelayedRow = {
    exchange: input.exchange,
    routingKey: input.routingKey,
    body: Buffer.from(input.body).toString("base64"),
    propRaw: Buffer.from(input.propRaw).toString("base64"),
    persistent: input.persistent,
    priority: input.priority,
    expiration: input.expiration,
    due,
  };
  const owner = this.delayOwner();
  if (owner && owner !== this.cfg.nodeId) {
    this.store.putParameter(DELAYED_FORWARD, input.vhost, name, JSON.stringify(row));
    forwardDelayed.call(this, owner, input.vhost, name, row, 0);
    return;
  }
  this.store.putParameter(DELAYED, input.vhost, name, JSON.stringify(row));
  scheduleDelayed.call(this, input.vhost, name, row);
}

/** Hand a pending delayed row to the owner, retrying until it is stored there. */
function forwardDelayed(this: Broker, owner: string, vhost: string, name: string, row: DelayedRow, attempt: number) {
  const retry = () => {
    const t = setTimeout(() => forwardDelayed.call(this, owner, vhost, name, row, attempt + 1), Math.min(5000, 100 * 2 ** attempt));
    t.unref?.();
  };
  if (!this.cluster?.peers.has(owner)) return retry();
  this.cluster.call(owner, "delay_hold", { vhost, name, row }).then(
    () => this.store.deleteParameter(DELAYED_FORWARD, vhost, name),
    retry,
  );
}

/**
 * Take a delayed row handed over by another worker: store it and arm it.
 * A row seen before (a retried hand-over) is not stored or armed again.
 */
export function acceptDelayed(this: Broker, vhost: string, name: string, row: DelayedRow) {
  if (armedDelayed.has(`${vhost}\0${name}`) || routedDelayed.has(`${vhost}\0${name}`)) return;
  this.store.putParameter(DELAYED, vhost, name, JSON.stringify(row));
  scheduleDelayed.call(this, vhost, name, row);
}

function scheduleDelayed(this: Broker, vhost: string, name: string, row: DelayedRow) {
  const id = `${vhost}\0${name}`;
  if (armedDelayed.has(id)) return;
  armedDelayed.add(id);
  const timer = setTimeout(() => {
    armedDelayed.delete(id);
    routedDelayed.add(id);
    if (routedDelayed.size > 10_000) routedDelayed.delete(routedDelayed.values().next().value!);
    const propRaw = new Uint8Array(Buffer.from(row.propRaw, "base64"));
    const send = {
      vhost,
      exchange: row.exchange,
      routingKey: row.routingKey,
      body: new Uint8Array(Buffer.from(row.body, "base64")),
      headers: readProps(propRaw).headers,
      propRaw,
      persistent: row.persistent,
      priority: row.priority,
      expiration: row.expiration,
      confirm: false,
      mandatory: false,
      delayed: true,
    };
    // The row goes once the copy is routed (or the exchange is gone).
    void Promise.resolve(this.publish(send as Parameters<Broker["publish"]>[0]))
      .catch(() => {})
      .finally(() => this.store.deleteParameter(DELAYED, vhost, name));
  }, Math.max(0, Math.min(row.due - Date.now(), 2 ** 31 - 1)));
  timer.unref?.();
}

/**
 * Re-arm the delayed messages stored before a restart. Late ones are routed
 * now. Rows a cores-mode worker had not yet handed to the owner are handed
 * over again, or armed here when this process is now the owner (or there is
 * none, as after going back to one process).
 */
export function resumeDelayed(this: Broker) {
  for (const r of this.store.listParameters(DELAYED)) {
    try {
      scheduleDelayed.call(this, r.vhost, r.name, JSON.parse(r.value) as DelayedRow);
    } catch {
      /* a corrupt row is skipped */
    }
  }
  const owner = this.delayOwner();
  for (const r of this.store.listParameters(DELAYED_FORWARD)) {
    let row: DelayedRow;
    try {
      row = JSON.parse(r.value) as DelayedRow;
    } catch {
      continue;
    }
    if (owner && owner !== this.cfg.nodeId) {
      forwardDelayed.call(this, owner, r.vhost, r.name, row, 0);
      continue;
    }
    this.store.putParameter(DELAYED, r.vhost, r.name, r.value);
    this.store.deleteParameter(DELAYED_FORWARD, r.vhost, r.name);
    scheduleDelayed.call(this, r.vhost, r.name, row);
  }
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
  // Quorum appends, stream logs and forwards keep or queue the body elsewhere.
  if (src.body.length >= POOL_MIN) taintBody(src.body);
  const q = this.queues.get(this.key(vhost, name));
  if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${name}`);
  if (q.argsParsed.queueType === "quorum") return this.enqueueQuorum(q, src);
  if (q.argsParsed.queueType === "stream" && q.raftGroup) return this.enqueueStreamRaft(q, src);
  if (!this.isLocalHome(q.home)) {
    try {
      await this.cluster!.callBytes(q.home!, "enqueue", {
        vhost,
        queue: name,
        exchange: src.exchange,
        routingKey: src.routingKey,
        headers: src.headers,
        persistent: src.persistent,
        priority: src.priority,
        expiration: src.expiration,
      }, src.body, src.propRaw);
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

/** True when `id` needs no JSON escaping (the broker's ids are digits, letters, dashes and dots). */
function plainId(id: string): boolean {
  for (let i = 0; i < id.length; i++) {
    const c = id.charCodeAt(i);
    if (c < 32 || c === 34 || c === 92 || c > 126) return false;
  }
  return true;
}

/**
 * The stored meta of a message without an id, headers or expiry, as the
 * JSON.stringify shape. The last one is kept: a publish burst repeats it.
 * Null when the exchange or key would need escaping.
 */
function simpleMetaFor(src: { exchange: string; routingKey: string; propRaw: Uint8Array; persistent: boolean; priority: number; redelivered?: boolean }): string | null {
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
  if (needsJsonEscape(src.exchange) || needsJsonEscape(src.routingKey)) return null;
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

/** Stored meta for a durable message. The plain shape matches JSON.parse on recovery. */
export function durableMeta(
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
  // A quorum message carries a fresh id each time, so the plain string is
  // cached without it and the id goes in front: `{"id":...,` then the rest,
  // the key order JSON.stringify gives below. An id needs no escape when it
  // is the broker's own (digits, letters, dashes).
  const id = src.id;
  if (src.headers.length === 0 && expiresAt == null && (!id || plainId(id))) {
    const meta = simpleMetaFor(src);
    if (meta !== null) return id ? `{"id":"${id}",${meta.slice(1)}` : meta;
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
    /** The single-destination publish path: a pooled body may be reused after its ack (see `body-pool.ts`). */
    pooled?: boolean;
  },
  depth: number,
): boolean {
  const big = src.body.length >= POOL_MIN;
  if (q.argsParsed.queueType === "stream") {
    if (big) taintBody(src.body);
    // A replicated stream takes appends only through its log (a dead letter or shovel lands here).
    if (q.raftGroup) void this.enqueueStreamRaft(q, src);
    else this.appendStream(q, src);
    return true;
  }
  this.expire(q);
  const bytes = q.ready.bytes;
  const overCount = q.argsParsed.maxLength != null && q.ready.length >= q.argsParsed.maxLength;
  const overBytes = q.argsParsed.maxLengthBytes != null && bytes + src.body.length > q.argsParsed.maxLengthBytes;
  if (overCount || overBytes) {
    if (big) taintBody(src.body);
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
    // Nothing older is left to drop, so the new message is the head and goes:
    // x-max-length 0 keeps nothing, as in RabbitMQ.
    if (
      (q.argsParsed.maxLength != null && q.ready.length >= q.argsParsed.maxLength) ||
      (q.argsParsed.maxLengthBytes != null && q.ready.bytes + src.body.length > q.argsParsed.maxLengthBytes)
    ) {
      this.deadLetter(q, liveFrom(q, src), depth, "maxlen");
      this.prom.dlxMaxlen++;
      return true;
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
    // `||`: a forward without `message_id` decodes as "". Shared by every
    // message in the queue, it overwrote `unacked` entries and kept their rows.
    id: src.id || (rowId != null ? `d-${rowId}` : `t-${takeTransientSeq()}`),
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
  // made a later confirm pause under the memory cap. A durable message behind
  // a deep backlog is paged: delivery reloads it from the store, as RabbitMQ
  // classic queues page bodies to disk. 300,000 queued 1 KiB bodies otherwise
  // outgrew a 1 GiB container. A follower's quorum copy is only read after a
  // failover, so it is paged at once. A leader's shallow queue keeps its
  // messages whole: reloading each one cost the 4-CPU quorum cell a store
  // read, a JSON parse and a base64 decode per delivery.
  const deep = q.ready.length >= PAGE_AFTER_READY || q.ready.bytes >= PAGE_AFTER_BYTES;
  const replica = q.argsParsed.queueType === "quorum" && !this.isQuorumLeader(q);
  if (rowId != null && (replica || deep)) {
    msg.bodyBytes = src.body.length;
    msg.body = EMPTY_BODY;
    msg.propRaw = EMPTY_BODY;
    msg.headers = EMPTY_HEADERS;
    msg.exchange = "";
    msg.routingKey = "";
    msg.slim = true;
  }
  // Reusable only while the queue holds the one live copy, in memory.
  if (big) noteEnqueue(src.body, src.pooled === true && !replica && !msg.slim, this.store, rowId);
  if (replica) {
    q.replicas.set(msg.id, msg);
    return true;
  }
  if (q.argsParsed.maxPriority != null) {
    const p = Math.min(src.priority, q.argsParsed.maxPriority);
    msg.priority = p;
    // A burst at one priority appends; walking the queue per publish (and
    // rebuilding it) made a priority queue publish O(depth).
    q.ready.pushByPriority(msg);
  } else {
    q.ready.push(msg);
  }
  // A quorum confirm releases `confirmGate` and pumps after the peer fsync.
  if (!src.confirmGate && (q.argsParsed.queueType !== "quorum" || this.isQuorumLeader(q))) this.pump(q);
  return true;
}

Broker.prototype.publish = publish;
Broker.prototype.enqueue = enqueue;
Broker.prototype.enqueueLocal = enqueueLocal;
Broker.prototype.holdDelayed = holdDelayed;
Broker.prototype.resumeDelayed = resumeDelayed;
Broker.prototype.delayOwner = delayOwner;
Broker.prototype.acceptDelayed = acceptDelayed;

declare module "./class.ts" {
  interface Broker {
    holdDelayed: typeof holdDelayed;
    resumeDelayed: typeof resumeDelayed;
    delayOwner: typeof delayOwner;
    acceptDelayed: typeof acceptDelayed;
    publish: typeof publish;
    enqueue: typeof enqueue;
    enqueueLocal: typeof enqueueLocal;
  }
}

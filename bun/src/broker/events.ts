/**
 * Event exchange (`amq.rabbitmq.event`) and firehose tracing
 * (`amq.rabbitmq.trace`), as RabbitMQ's plugin and `rabbitmqctl trace_on` do.
 *
 * Both exchanges are internal topic exchanges. The broker publishes into them
 * directly, so a client can bind to them but not publish. Each producer does
 * nothing until a queue is bound, so the publish path pays one size check.
 */
import { W, writeTable, type Field } from "../codec.ts";
import { Broker } from "./class.ts";

export const EVENT_EXCHANGE = "amq.rabbitmq.event";
export const TRACE_EXCHANGE = "amq.rabbitmq.trace";
/** RabbitMQ publishes every vhost's events into this one. */
const EVENT_VHOST = "/";

/** Properties with headers and a timestamp, the shape both plugins send. */
function eventProps(headers: Array<[string, Field]>): Uint8Array {
  const w = new W();
  // headers (bit 13) and timestamp (bit 6).
  w.u16(0x2000 | 0x0040);
  writeTable(w, headers);
  w.u64(Math.floor(Date.now() / 1000));
  return w.concat();
}

/**
 * Publish one broker-made message to an internal exchange on this node.
 * Returns the number of queues it reached. Errors are swallowed: an event or
 * a trace copy must never fail the operation that caused it.
 */
export function publishInternal(
  this: Broker,
  vhost: string,
  exchange: string,
  routingKey: string,
  headers: Array<[string, Field]>,
  body: Uint8Array,
  propRaw?: Uint8Array,
): number {
  if (!this.bindings.some((b) => b.vhost === vhost && b.exchange === exchange)) return 0;
  let reached = 0;
  try {
    for (const name of this.routeOne(vhost, exchange, routingKey, headers)) {
      const q = this.queues.get(this.key(vhost, name));
      if (!q || !this.isLocalHome(q.home)) continue;
      const ok = this.enqueueLocal(
        q,
        {
          body,
          exchange,
          routingKey,
          headers,
          propRaw: propRaw ?? eventProps(headers),
          persistent: false,
          priority: 0,
          expiration: "",
        },
        0,
      );
      if (ok) reached++;
      this.pump(q);
    }
  } catch {
    /* an event is best effort */
  }
  return reached;
}

/**
 * Emit `amq.rabbitmq.event` with routing key `key`, such as `queue.created`.
 *
 * @param fields Event fields. `vhost` is added. Strings and booleans become headers.
 */
export function emitEvent(this: Broker, key: string, vhost: string, fields: Record<string, string | number | boolean | null>): void {
  if (!this.bindings.some((b) => b.vhost === EVENT_VHOST && b.exchange === EVENT_EXCHANGE)) return;
  const headers: Array<[string, Field]> = [["vhost", { t: "S", v: vhost }]];
  for (const [k, v] of Object.entries(fields)) {
    if (v == null) continue;
    if (typeof v === "boolean") headers.push([k, { t: "t", v }]);
    else if (typeof v === "number") headers.push([k, { t: "l", v }]);
    else headers.push([k, { t: "S", v }]);
  }
  headers.push(["timestamp_in_ms", { t: "l", v: Date.now() }]);
  this.publishInternal(EVENT_VHOST, EVENT_EXCHANGE, key, headers, new Uint8Array(0));
}

/** True when tracing is on for `vhost`. */
export function tracing(this: Broker, vhost: string): boolean {
  return this.tracedVhosts.size !== 0 && this.tracedVhosts.has(vhost);
}

/**
 * Copy one publish to `amq.rabbitmq.trace` as `publish.<exchange>`, with the
 * headers RabbitMQ's firehose adds. The body is the original body.
 */
export function tracePublish(
  this: Broker,
  input: { vhost: string; exchange: string; routingKey: string; body: Uint8Array; headers: Array<[string, Field]> },
  user = "",
): void {
  const headers: Array<[string, Field]> = [
    ["exchange_name", { t: "S", v: input.exchange }],
    ["routing_keys", { t: "A", v: [{ t: "S", v: input.routingKey }] }],
    ["properties", { t: "F", v: [["headers", { t: "F", v: input.headers }]] }],
    ["node", { t: "S", v: this.cfg.nodeId || "queueforge" }],
    ["vhost", { t: "S", v: input.vhost }],
    ["user", { t: "S", v: user }],
  ];
  this.publishInternal(input.vhost, TRACE_EXCHANGE, `publish.${input.exchange}`, headers, input.body);
}

/** Copy one delivery to `amq.rabbitmq.trace` as `deliver.<queue>`. */
export function traceDeliver(
  this: Broker,
  vhost: string,
  queue: string,
  msg: { exchange: string; routingKey: string; body: Uint8Array; headers: Array<[string, Field]>; redelivered: boolean },
): void {
  const headers: Array<[string, Field]> = [
    ["exchange_name", { t: "S", v: msg.exchange }],
    ["routing_keys", { t: "A", v: [{ t: "S", v: msg.routingKey }] }],
    ["properties", { t: "F", v: [["headers", { t: "F", v: msg.headers }]] }],
    ["node", { t: "S", v: this.cfg.nodeId || "queueforge" }],
    ["redelivered", { t: "t", v: msg.redelivered }],
    ["vhost", { t: "S", v: vhost }],
  ];
  this.publishInternal(vhost, TRACE_EXCHANGE, `deliver.${queue}`, headers, msg.body);
}

Broker.prototype.publishInternal = publishInternal;
Broker.prototype.emitEvent = emitEvent;
Broker.prototype.tracing = tracing;
Broker.prototype.tracePublish = tracePublish;
Broker.prototype.traceDeliver = traceDeliver;

declare module "./class.ts" {
  interface Broker {
    publishInternal: typeof publishInternal;
    emitEvent: typeof emitEvent;
    tracing: typeof tracing;
    tracePublish: typeof tracePublish;
    traceDeliver: typeof traceDeliver;
  }
}

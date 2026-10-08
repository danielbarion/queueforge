/**
 * Stream queues (`x-queue-type: stream`), as RabbitMQ 4 serves them over AMQP.
 *
 * A stream is an append-only log. Publishing appends; consuming never removes.
 * Each consumer reads from its own offset, set by `x-stream-offset`: `first`,
 * `last`, `next` (the default), an offset number, or a timestamp. Acks return
 * credit only. Retention drops the oldest entries past `x-max-length-bytes`
 * or `x-max-age`. Entries are stored in sqlite, so a stream survives a
 * restart; every delivery carries its `x-stream-offset` header.
 */
import { W, writeTable, type Field } from "../codec.ts";
import { ChanError } from "../errors.ts";
import { Broker } from "./class.ts";
import type { Consumer, LiveMsg, QueueLive } from "./model.ts";

export type StreamEntry = {
  offset: number;
  ts: number;
  body: Uint8Array;
  exchange: string;
  routingKey: string;
  headers: Array<[string, Field]>;
  propRaw: Uint8Array;
};

export type StreamState = {
  log: StreamEntry[];
  /** Offset of log[0]. */
  first: number;
  /** Offset the next append takes. */
  next: number;
  bytes: number;
  readers: Array<{ consumer: Consumer; cursor: number }>;
};

/** `x-max-age` as RabbitMQ writes it: a number and a unit, Y M D h m s. */
export function parseMaxAge(text: string | null): number | null {
  if (!text) return null;
  const m = /^(\d+)([YMDhms])$/.exec(text.trim());
  if (!m) return null;
  const n = Number(m[1]);
  const unit: Record<string, number> = { Y: 365 * 86400e3, M: 30 * 86400e3, D: 86400e3, h: 3600e3, m: 60e3, s: 1e3 };
  return n * unit[m[2]!]!;
}

/** The stream state of `q`, loading stored entries on first use. */
export function streamOf(this: Broker, q: QueueLive): StreamState {
  let s = this.streams.get(this.key(q.vhost, q.name));
  if (s) return s;
  const log = this.store.listStreamEntries(q.vhost, q.name);
  const first = log[0]?.offset ?? this.store.streamNextOffset(q.vhost, q.name);
  const next = log.length ? log[log.length - 1]!.offset + 1 : first;
  s = { log, first, next, bytes: log.reduce((n, e) => n + e.body.length, 0), readers: [] };
  this.streams.set(this.key(q.vhost, q.name), s);
  return s;
}

/** Append one message. Returns its offset. */
export function appendStream(
  this: Broker,
  q: QueueLive,
  src: { body: Uint8Array; exchange: string; routingKey: string; headers: Array<[string, Field]>; propRaw: Uint8Array },
): number {
  const s = this.streamOf(q);
  const entry: StreamEntry = { offset: s.next++, ts: Date.now(), body: src.body, exchange: src.exchange, routingKey: src.routingKey, headers: src.headers, propRaw: src.propRaw };
  s.log.push(entry);
  s.bytes += entry.body.length;
  this.store.appendStreamEntry(q.vhost, q.name, entry);
  this.trimStream(q);
  this.pumpStream(q);
  return entry.offset;
}

/** Apply `x-max-length-bytes` and `x-max-age`. The newest entry always stays. */
export function trimStream(this: Broker, q: QueueLive): void {
  const s = this.streamOf(q);
  const maxBytes = q.argsParsed.maxLengthBytes;
  const maxAge = parseMaxAge(q.args["x-max-age"] == null ? null : String(q.args["x-max-age"]));
  const now = Date.now();
  let drop = 0;
  let bytes = s.bytes;
  while (drop < s.log.length - 1) {
    const e = s.log[drop]!;
    const tooBig = maxBytes != null && bytes > maxBytes;
    const tooOld = maxAge != null && now - e.ts > maxAge;
    if (!tooBig && !tooOld) break;
    bytes -= e.body.length;
    drop++;
  }
  if (!drop) return;
  const until = s.log[drop]!.offset;
  s.log.splice(0, drop);
  s.bytes = bytes;
  s.first = until;
  this.store.deleteStreamEntriesBefore(q.vhost, q.name, until);
}

/** Where a new reader starts, from its `x-stream-offset` argument. */
function startOffset(s: StreamState, spec: Field | undefined): number {
  if (!spec) return s.next;
  if (spec.t === "S" || spec.t === "s") {
    if (spec.v === "first") return s.first;
    if (spec.v === "last") return s.log.length ? s.log[s.log.length - 1]!.offset : s.next;
    if (spec.v === "next") return s.next;
    const age = parseMaxAge(spec.v);
    if (age != null) return firstAtOrAfter(s, Date.now() - age);
    throw new ChanError(406, `PRECONDITION_FAILED - invalid x-stream-offset '${spec.v}'`);
  }
  if (spec.t === "T") return firstAtOrAfter(s, spec.v * 1000);
  if (spec.t === "I" || spec.t === "l") return Math.max(s.first, Math.min(spec.v, s.next));
  throw new ChanError(406, "PRECONDITION_FAILED - invalid x-stream-offset");
}

function firstAtOrAfter(s: StreamState, ts: number): number {
  for (const e of s.log) if (e.ts >= ts) return e.offset;
  return s.next;
}

/** Register a reader. Called by {@link Broker.consume} for a stream. */
export function addStreamReader(this: Broker, q: QueueLive, consumer: Consumer, spec: Field | undefined): void {
  const s = this.streamOf(q);
  s.readers.push({ consumer, cursor: startOffset(s, spec) });
}

/** Drop a reader by consumer tag. Returns true when one was removed. */
export function removeStreamReader(this: Broker, q: QueueLive, tag: string): boolean {
  const s = this.streams.get(this.key(q.vhost, q.name));
  if (!s) return false;
  const before = s.readers.length;
  s.readers = s.readers.filter((r) => r.consumer.tag !== tag);
  return s.readers.length !== before;
}

/** Copy properties with `x-stream-offset` added to the headers table. */
function withOffset(propRaw: Uint8Array, headers: Array<[string, Field]>, offset: number): Uint8Array {
  const flags = propRaw.length >= 2 ? (propRaw[0]! << 8) | propRaw[1]! : 0;
  const table: Array<[string, Field]> = [...headers.filter(([k]) => k !== "x-stream-offset"), ["x-stream-offset", { t: "l", v: offset }]];
  const w = new W();
  w.u16(flags | 0x2000);
  let at = 2;
  const shortstr = () => {
    const n = propRaw[at]!;
    w.bytes(propRaw.subarray(at, at + 1 + n));
    at += 1 + n;
  };
  if (flags & 0x8000) shortstr();
  if (flags & 0x4000) shortstr();
  if (flags & 0x2000) {
    const n = ((propRaw[at]! << 24) | (propRaw[at + 1]! << 16) | (propRaw[at + 2]! << 8) | propRaw[at + 3]!) >>> 0;
    at += 4 + n;
  }
  writeTable(w, table);
  w.bytes(propRaw.subarray(at));
  return w.concat();
}

/** Deliver to every reader that has credit, from its cursor. */
export function pumpStream(this: Broker, q: QueueLive): void {
  const s = this.streams.get(this.key(q.vhost, q.name));
  if (!s) return;
  for (const reader of s.readers) {
    if (reader.cursor < s.first) reader.cursor = s.first;
    while (reader.cursor < s.next && reader.consumer.want()) {
      const e = s.log[reader.cursor - s.first];
      if (!e) break;
      reader.cursor++;
      const msg: LiveMsg = {
        id: `s-${e.offset}`,
        rowId: null,
        body: e.body,
        exchange: e.exchange,
        routingKey: e.routingKey,
        headers: [...e.headers, ["x-stream-offset", { t: "l", v: e.offset }]],
        propRaw: withOffset(e.propRaw, e.headers, e.offset),
        persistent: true,
        priority: 0,
        expiresAt: null,
        redelivered: false,
      };
      this.prom.delivered++;
      reader.consumer.deliver(msg);
    }
  }
}

Broker.prototype.streamOf = streamOf;
Broker.prototype.appendStream = appendStream;
Broker.prototype.trimStream = trimStream;
Broker.prototype.addStreamReader = addStreamReader;
Broker.prototype.removeStreamReader = removeStreamReader;
Broker.prototype.pumpStream = pumpStream;

declare module "./class.ts" {
  interface Broker {
    streamOf: typeof streamOf;
    appendStream: typeof appendStream;
    trimStream: typeof trimStream;
    addStreamReader: typeof addStreamReader;
    removeStreamReader: typeof removeStreamReader;
    pumpStream: typeof pumpStream;
  }
}

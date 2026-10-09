/**
 * Stream queues (`x-queue-type: stream`), as RabbitMQ 4 serves them over AMQP.
 *
 * A stream is an append-only log. Publishing appends; consuming never removes.
 * Each consumer reads from its own offset, set by `x-stream-offset`: `first`,
 * `last`, `next` (the default), an offset number, or a timestamp. Acks return
 * credit only. Retention drops the oldest entries past `x-max-length-bytes`
 * or `x-max-age`. Entries are stored in sqlite, so a stream survives a
 * restart; every delivery carries its `x-stream-offset` header.
 *
 * When the cluster runs queue groups, a stream is replicated as RabbitMQ's
 * are: it has its own Raft group, a publish is confirmed once its `sappend`
 * commits on a majority, and every member applies the same appends in the
 * same order, so offsets agree and a consumer reads on whichever member it
 * is connected to. The group's leader is the writer; losing it elects
 * another.
 */
import { W, writeTable, type Field } from "../codec.ts";
import { ChanError } from "../errors.ts";
import { Broker } from "./class.ts";
import type { Consumer, LiveMsg, QueueLive } from "./model.ts";

export type StreamEntry = {
  offset: number;
  ts: number;
  /** Body bytes, also when the body is paged out. */
  size: number;
  /** Null once paged out: the row in `stream_messages` has it. */
  body: Uint8Array | null;
  exchange: string;
  routingKey: string;
  headers: Array<[string, Field]>;
  propRaw: Uint8Array;
  /** Raft index of the `sappend` that wrote it, on a replicated stream. */
  raftIndex?: number;
};

export type StreamState = {
  log: StreamEntry[];
  /** Offset of log[0]. */
  first: number;
  /** Offset the next append takes. */
  next: number;
  bytes: number;
  /** Body bytes still held in `log`. Older bodies are read back from the store. */
  resident: number;
  /** Index in `log` of the oldest entry that may still hold its body. */
  hot: number;
  readers: Array<{ consumer: Consumer; cursor: number }>;
  /** Highest Raft index applied, so a replay after a restart is skipped. */
  raftIndex: number;
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

/**
 * Body bytes one stream keeps in memory. Readers near the tail are served from
 * memory; older entries are read back from sqlite, as RabbitMQ reads stream
 * segments from disk. Without this a stream with no retention grew until the
 * process was killed.
 */
const STREAM_RESIDENT_BYTES = 16 * 1024 * 1024;
const NO_HEADERS: Array<[string, Field]> = [];
const NO_BYTES = new Uint8Array(0);

/** The stream state of `q`, loading the stored index on first use. Bodies stay in the store. */
export function streamOf(this: Broker, q: QueueLive): StreamState {
  let s = this.streams.get(this.key(q.vhost, q.name));
  if (s) return s;
  const index = this.store.listStreamIndex(q.vhost, q.name);
  const log: StreamEntry[] = index.rows.map((r) => ({
    offset: r.offset, ts: r.ts, size: r.size, body: null, exchange: "", routingKey: "", headers: NO_HEADERS, propRaw: NO_BYTES,
  }));
  const first = log[0]?.offset ?? this.store.streamNextOffset(q.vhost, q.name);
  const next = log.length ? log[log.length - 1]!.offset + 1 : first;
  const applied = Number(this.store.listParameters("stream-raft").find((p) => p.vhost === q.vhost && p.name === q.name)?.value ?? 0);
  s = {
    log, first, next, bytes: log.reduce((n, e) => n + e.size, 0), resident: 0, hot: log.length,
    readers: [], raftIndex: Math.max(applied, index.raftIndex),
  };
  this.streams.set(this.key(q.vhost, q.name), s);
  return s;
}

/** Add an entry that holds its body, then page the oldest bodies past the budget. */
function pushEntry(s: StreamState, entry: StreamEntry) {
  s.log.push(entry);
  s.bytes += entry.size;
  if (entry.body) s.resident += entry.size;
  while (s.resident > STREAM_RESIDENT_BYTES && s.hot < s.log.length - 1) {
    const old = s.log[s.hot]!;
    if (old.body) {
      s.resident -= old.size;
      old.body = null;
      old.exchange = "";
      old.routingKey = "";
      old.headers = NO_HEADERS;
      old.propRaw = NO_BYTES;
    }
    s.hot++;
  }
}

/** The entry with its body, from memory or from the store. */
function fullEntry(this: Broker, q: QueueLive, e: StreamEntry): StreamEntry | null {
  if (e.body) return e;
  const row = this.store.readStreamEntry(q.vhost, q.name, e.offset);
  if (!row) return null;
  return { ...row, size: row.body.length };
}

/** Append one message. Returns its offset. */
export function appendStream(
  this: Broker,
  q: QueueLive,
  src: { body: Uint8Array; exchange: string; routingKey: string; headers: Array<[string, Field]>; propRaw: Uint8Array },
): number {
  const s = this.streamOf(q);
  const entry: StreamEntry = { offset: s.next++, ts: Date.now(), size: src.body.length, body: src.body, exchange: src.exchange, routingKey: src.routingKey, headers: src.headers, propRaw: src.propRaw };
  this.store.appendStreamEntry(q.vhost, q.name, { ...entry, body: src.body });
  pushEntry(s, entry);
  this.trimStream(q);
  this.pumpStream(q);
  return entry.offset;
}

/**
 * Publish to a replicated stream: commit an `sappend` in its Raft group.
 *
 * @returns True once committed and applied on the leader; false otherwise.
 */
export async function enqueueStreamRaft(
  this: Broker,
  q: QueueLive,
  src: { body: Uint8Array; exchange: string; routingKey: string; headers: Array<[string, Field]>; propRaw: Uint8Array },
): Promise<boolean> {
  const node = this.cluster?.consensus?.node;
  if (!node || !q.raftGroup) return false;
  try {
    await node.propose(q.raftGroup, "sappend", {
      vhost: q.vhost,
      queue: q.name,
      ts: Date.now(),
      body_b64: Buffer.from(src.body).toString("base64"),
      exchange: src.exchange,
      routing_key: src.routingKey,
      headers: src.headers,
      propRaw: Buffer.from(src.propRaw).toString("base64"),
    });
    return true;
  } catch {
    return false;
  }
}

/**
 * Apply one committed `sappend` here. Every member assigns the same offset,
 * as it applies the same entries in the same order.
 *
 * @param index The entry's Raft index; one at or below the last applied is a replay.
 */
export function applyStreamAppend(this: Broker, q: QueueLive, index: number, data: Record<string, unknown>) {
  const s = this.streamOf(q);
  if (index <= s.raftIndex) return;
  s.raftIndex = index;
  const body = new Uint8Array(Buffer.from(String(data.body_b64 ?? ""), "base64"));
  const entry: StreamEntry = {
    offset: s.next++,
    ts: Number(data.ts ?? Date.now()),
    size: body.length,
    body,
    exchange: String(data.exchange ?? ""),
    routingKey: String(data.routing_key ?? ""),
    headers: (data.headers as StreamEntry["headers"]) ?? [],
    propRaw: new Uint8Array(Buffer.from(String(data.propRaw ?? ""), "base64")),
    raftIndex: index,
  };
  this.store.appendStreamEntry(q.vhost, q.name, { ...entry, body });
  pushEntry(s, entry);
  this.trimStream(q);
  this.pumpStream(q);
}

/** A replicated stream as its Raft snapshot: the retained entries and where it stands. */
export function streamSnapshot(this: Broker, q: QueueLive) {
  const s = this.streamOf(q);
  return {
    stream: {
      vhost: q.vhost,
      queue: q.name,
      first: s.first,
      next: s.next,
      raftIndex: s.raftIndex,
      entries: s.log.flatMap((paged) => {
        const e = fullEntry.call(this, q, paged);
        if (!e) return [];
        return [{
          offset: e.offset,
          ts: e.ts,
          body_b64: Buffer.from(e.body!).toString("base64"),
          exchange: e.exchange,
          routing_key: e.routingKey,
          headers: e.headers,
          propRaw: Buffer.from(e.propRaw).toString("base64"),
        }];
      }),
    },
  };
}

/** Replace this member's copy of a replicated stream with a snapshot from its leader. */
export function installStreamSnapshot(this: Broker, q: QueueLive, state: unknown) {
  const snap = (state as { stream?: { first?: number; next?: number; raftIndex?: number; entries?: Array<Record<string, unknown>> } } | null)?.stream;
  if (!snap) return;
  const s = this.streamOf(q);
  this.store.deleteStreamLog(q.vhost, q.name);
  s.log = [];
  s.bytes = 0;
  s.resident = 0;
  s.hot = 0;
  for (const e of snap.entries ?? []) {
    const body = new Uint8Array(Buffer.from(String(e.body_b64 ?? ""), "base64"));
    const entry: StreamEntry = {
      offset: Number(e.offset),
      ts: Number(e.ts),
      size: body.length,
      body,
      exchange: String(e.exchange ?? ""),
      routingKey: String(e.routing_key ?? ""),
      headers: (e.headers as StreamEntry["headers"]) ?? [],
      propRaw: new Uint8Array(Buffer.from(String(e.propRaw ?? ""), "base64")),
    };
    this.store.appendStreamEntry(q.vhost, q.name, { ...entry, body });
    pushEntry(s, entry);
  }
  s.first = Number(snap.first ?? s.log[0]?.offset ?? 0);
  s.next = Number(snap.next ?? s.first);
  s.raftIndex = Number(snap.raftIndex ?? 0);
  this.store.deleteStreamEntriesBefore(q.vhost, q.name, s.first);
  this.store.putParameter("stream-raft", q.vhost, q.name, String(s.raftIndex));
  this.pumpStream(q);
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
    bytes -= e.size;
    if (e.body) s.resident -= e.size;
    drop++;
  }
  if (!drop) return;
  const until = s.log[drop]!.offset;
  s.log.splice(0, drop);
  s.hot = Math.max(0, s.hot - drop);
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
      const at = s.log[reader.cursor - s.first];
      if (!at) break;
      const e = fullEntry.call(this, q, at);
      if (!e) break;
      reader.cursor++;
      const msg: LiveMsg = {
        id: `s-${e.offset}`,
        rowId: null,
        body: e.body!,
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
Broker.prototype.enqueueStreamRaft = enqueueStreamRaft;
Broker.prototype.applyStreamAppend = applyStreamAppend;
Broker.prototype.streamSnapshot = streamSnapshot;
Broker.prototype.installStreamSnapshot = installStreamSnapshot;

declare module "./class.ts" {
  interface Broker {
    streamOf: typeof streamOf;
    appendStream: typeof appendStream;
    trimStream: typeof trimStream;
    addStreamReader: typeof addStreamReader;
    removeStreamReader: typeof removeStreamReader;
    pumpStream: typeof pumpStream;
    enqueueStreamRaft: typeof enqueueStreamRaft;
    applyStreamAppend: typeof applyStreamAppend;
    streamSnapshot: typeof streamSnapshot;
    installStreamSnapshot: typeof installStreamSnapshot;
  }
}

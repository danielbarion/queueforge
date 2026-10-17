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
import { readProps } from "../amqp10/map.ts";
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
  /** Delivery headers and properties with `x-stream-offset`, built on first delivery. */
  wireHeaders?: Array<[string, Field]>;
  wireProps?: Uint8Array;
  /** `x-stream-filter-value` header, read on first use by a filtering reader. Null when absent. */
  filterValue?: string | null;
};

/** Whether a filtering reader takes this entry (RabbitMQ's x-stream-filter, applied per message). */
function filterMatch(e: StreamEntry, filter: Set<string>, matchUnfiltered: boolean): boolean {
  if (e.filterValue === undefined) {
    const f = e.headers.find(([k]) => k === "x-stream-filter-value")?.[1];
    e.filterValue = f && (f.t === "S" || f.t === "s") ? f.v : f?.t === "x" ? new TextDecoder().decode(f.v) : null;
  }
  return e.filterValue === null ? matchUnfiltered : filter.has(e.filterValue);
}

/**
 * One stream consumer. `ahead` holds entries read back from the store past the
 * resident window, `ahead[aheadAt]` being the one at `cursor`.
 */
type StreamReader = { consumer: Consumer; cursor: number; ahead?: StreamEntry[]; aheadAt?: number };

/**
 * Read-ahead for a reader behind the resident window. One range query replaces
 * a query, a JSON parse and a copy per delivery; a reader from `first` on a
 * long stream ran at a quarter of the tail rate. Bounded in bytes and count.
 */
const READ_AHEAD_BYTES = 4 * 1024 * 1024;
const READ_AHEAD_ENTRIES = 512;

export type StreamState = {
  /**
   * Entries whose body is only in the store, oldest first: offsets `first` on.
   * Timestamps and sizes only. A full object each held ~350 bytes for every
   * message ever written, so a busy stream outgrew its container.
   */
  coldTs: number[];
  coldSize: number[];
  /** Index of the oldest live slot in `coldTs` / `coldSize`. */
  coldHead: number;
  /** Entries that hold their body, after the cold ones; `log[logHead]` is the oldest. */
  log: StreamEntry[];
  logHead: number;
  /** Offset of the oldest entry. */
  first: number;
  /** Offset the next append takes. */
  next: number;
  bytes: number;
  /** Body bytes held in `log`. */
  resident: number;
  readers: Array<StreamReader>;
  /** Highest Raft index applied, so a replay after a restart is skipped. */
  raftIndex: number;
};

function coldCount(s: StreamState): number {
  return s.coldTs.length - s.coldHead;
}

/** The resident entry at `offset`, or undefined when it is cold or out of range. */
function residentAt(s: StreamState, offset: number): StreamEntry | undefined {
  const i = offset - s.first - coldCount(s);
  return i >= 0 ? s.log[s.logHead + i] : undefined;
}

function sizeAt(s: StreamState, offset: number): number {
  const i = offset - s.first;
  return i < coldCount(s) ? s.coldSize[s.coldHead + i]! : residentAt(s, offset)!.size;
}

function tsAt(s: StreamState, offset: number): number {
  const i = offset - s.first;
  return i < coldCount(s) ? s.coldTs[s.coldHead + i]! : residentAt(s, offset)!.ts;
}

/** Drop the dead front of both arrays once it is most of them. */
function compact(s: StreamState) {
  if (s.coldHead > 4096 && s.coldHead * 2 > s.coldTs.length) {
    s.coldTs = s.coldTs.slice(s.coldHead);
    s.coldSize = s.coldSize.slice(s.coldHead);
    s.coldHead = 0;
  }
  if (s.logHead > 4096 && s.logHead * 2 > s.log.length) {
    s.log = s.log.slice(s.logHead);
    s.logHead = 0;
  }
}

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

/** The stream state of `q`, loading the stored index on first use. Bodies stay in the store. */
export function streamOf(this: Broker, q: QueueLive): StreamState {
  let s = this.streams.get(this.key(q.vhost, q.name));
  if (s) return s;
  const index = this.store.listStreamIndex(q.vhost, q.name);
  const first = index.rows[0]?.offset ?? this.store.streamNextOffset(q.vhost, q.name);
  const applied = Number(this.store.listParameters("stream-raft").find((p) => p.vhost === q.vhost && p.name === q.name)?.value ?? 0);
  s = {
    coldTs: index.rows.map((r) => r.ts), coldSize: index.rows.map((r) => r.size), coldHead: 0,
    log: [], logHead: 0, first, next: first + index.rows.length,
    bytes: index.rows.reduce((n, r) => n + r.size, 0), resident: 0,
    readers: [], raftIndex: Math.max(applied, index.raftIndex),
  };
  this.streams.set(this.key(q.vhost, q.name), s);
  return s;
}

/** Add an entry that holds its body, then move the oldest past the budget to the cold index. */
function pushEntry(s: StreamState, entry: StreamEntry) {
  s.log.push(entry);
  s.bytes += entry.size;
  s.resident += entry.size;
  while (s.resident > STREAM_RESIDENT_BYTES && s.log.length - s.logHead > 1) {
    const old = s.log[s.logHead]!;
    s.log[s.logHead] = undefined as unknown as StreamEntry;
    s.logHead++;
    s.resident -= old.size;
    s.coldTs.push(old.ts);
    s.coldSize.push(old.size);
  }
  compact(s);
}

/** The entry at `offset` with its body, from memory or from the store. */
function fullEntry(this: Broker, q: QueueLive, s: StreamState, offset: number): StreamEntry | null {
  const resident = residentAt(s, offset);
  if (resident) return resident;
  const row = this.store.readStreamEntry(q.vhost, q.name, offset);
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
  const propRaw = new Uint8Array(Buffer.from(String(data.propRaw ?? ""), "base64"));
  const entry: StreamEntry = {
    offset: s.next++,
    ts: Number(data.ts ?? Date.now()),
    size: body.length,
    body,
    exchange: String(data.exchange ?? ""),
    routingKey: String(data.routing_key ?? ""),
    headers: streamHeaders(data.headers, propRaw),
    propRaw,
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
      entries: Array.from({ length: s.next - s.first }, (_, i) => fullEntry.call(this, q, s, s.first + i)).flatMap((e) => {
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
  s.coldTs = [];
  s.coldSize = [];
  s.coldHead = 0;
  s.log = [];
  s.logHead = 0;
  s.bytes = 0;
  s.resident = 0;
  let firstSeen: number | undefined;
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
    firstSeen ??= entry.offset;
    pushEntry(s, entry);
  }
  // Positions in the index are offsets from `first`, so it is the first entry installed.
  s.first = firstSeen ?? Number(snap.first ?? 0);
  s.next = firstSeen != null ? s.first + coldCount(s) + s.log.length - s.logHead : Number(snap.next ?? s.first);
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
  const total = s.next - s.first;
  while (drop < total - 1) {
    const offset = s.first + drop;
    const tooBig = maxBytes != null && bytes > maxBytes;
    const tooOld = maxAge != null && now - tsAt(s, offset) > maxAge;
    if (!tooBig && !tooOld) break;
    bytes -= sizeAt(s, offset);
    drop++;
  }
  if (!drop) return;
  const fromCold = Math.min(drop, coldCount(s));
  s.coldHead += fromCold;
  for (let i = fromCold; i < drop; i++) {
    s.resident -= s.log[s.logHead]!.size;
    s.log[s.logHead] = undefined as unknown as StreamEntry;
    s.logHead++;
  }
  const until = s.first + drop;
  s.bytes = bytes;
  s.first = until;
  compact(s);
  this.store.deleteStreamEntriesBefore(q.vhost, q.name, until);
}

/** Where a new reader starts, from its `x-stream-offset` argument. */
function startOffset(s: StreamState, spec: Field | undefined): number {
  if (!spec) return s.next;
  if (spec.t === "S" || spec.t === "s") {
    if (spec.v === "first") return s.first;
    if (spec.v === "last") return s.next > s.first ? s.next - 1 : s.next;
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
  for (let offset = s.first; offset < s.next; offset++) if (tsAt(s, offset) >= ts) return offset;
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

/**
 * An `sappend`'s header table. Bun and Rust send it typed (`[name, {t, v}]`);
 * PHP sends untyped pairs, so that table is read from the properties instead.
 */
function streamHeaders(headers: unknown, propRaw: Uint8Array): Array<[string, Field]> {
  if (!Array.isArray(headers) || headers.length === 0) return [];
  const typed = headers.every((h) => Array.isArray(h) && typeof h[1] === "object" && h[1] !== null && "t" in h[1]);
  if (typed) return headers as Array<[string, Field]>;
  return propRaw.length >= 2 ? readProps(propRaw).headers : [];
}

/**
 * The last property block {@link withOffset} encoded, for its inputs, and where
 * its offset value sits. `x-stream-offset` is the table's last entry, so its
 * eight bytes end right before the rest of the properties. Entries of one
 * publisher share their properties, and re-encoding the table per entry was a
 * tenth of a stream cell.
 */
let offsetProps: Uint8Array | null = null;
let offsetHeaders: Array<[string, Field]> | null = null;
let offsetTemplate: Uint8Array = new Uint8Array(0);
let offsetAt = 0;

function sameHeaderList(a: Array<[string, Field]> | null, b: Array<[string, Field]>): boolean {
  return a === b || (a !== null && a.length === 0 && b.length === 0);
}

/** Copy properties with `x-stream-offset` added to the headers table. */
export function withOffset(propRaw: Uint8Array, headers: Array<[string, Field]>, offset: number): Uint8Array {
  if (offsetProps !== propRaw || !sameHeaderList(offsetHeaders, headers)) {
    const template = encodeWithOffset(propRaw, headers, 0);
    offsetTemplate = template.encoded;
    offsetAt = template.valueAt;
    offsetProps = propRaw;
    offsetHeaders = headers;
  }
  const out = offsetTemplate.slice();
  const hi = Math.floor(offset / 0x100000000);
  const lo = offset >>> 0;
  out[offsetAt] = hi >>> 24;
  out[offsetAt + 1] = (hi >>> 16) & 0xff;
  out[offsetAt + 2] = (hi >>> 8) & 0xff;
  out[offsetAt + 3] = hi & 0xff;
  out[offsetAt + 4] = lo >>> 24;
  out[offsetAt + 5] = (lo >>> 16) & 0xff;
  out[offsetAt + 6] = (lo >>> 8) & 0xff;
  out[offsetAt + 7] = lo & 0xff;
  return out;
}

/** {@link withOffset} without the cache, and where the offset's eight bytes are. */
export function encodeWithOffset(propRaw: Uint8Array, headers: Array<[string, Field]>, offset: number): { encoded: Uint8Array; valueAt: number } {
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
  const rest = propRaw.subarray(at);
  w.bytes(rest);
  const encoded = w.concat();
  return { encoded, valueAt: encoded.length - rest.length - 8 };
}

/** The entry at `offset` with its body: from memory, the read-ahead, or one range read. */
function readerEntry(this: Broker, q: QueueLive, s: StreamState, reader: StreamReader, offset: number): StreamEntry | null {
  const resident = residentAt(s, offset);
  if (resident) return resident;
  const ahead = reader.ahead;
  if (ahead && reader.aheadAt! < ahead.length && ahead[reader.aheadAt!]!.offset === offset) return ahead[reader.aheadAt!++]!;
  // Count what fits the budget from the cold index, which holds every size.
  const cold = s.first + coldCount(s);
  let n = 0;
  let bytes = 0;
  for (let at = offset; at < cold && n < READ_AHEAD_ENTRIES; at++) {
    const size = sizeAt(s, at);
    if (n > 0 && bytes + size > READ_AHEAD_BYTES) break;
    bytes += size;
    n++;
  }
  const rows = this.store.readStreamRange(q.vhost, q.name, offset, Math.max(1, n));
  if (!rows.length || rows[0]!.offset !== offset) {
    reader.ahead = undefined;
    return null;
  }
  reader.ahead = rows.map((row) => ({ ...row, size: row.body.length }));
  reader.aheadAt = 1;
  return reader.ahead[0]!;
}

/** Deliver to every reader that has credit, from its cursor. */
export function pumpStream(this: Broker, q: QueueLive): void {
  const s = this.streams.get(this.key(q.vhost, q.name));
  if (!s) return;
  for (const reader of s.readers) {
    if (reader.cursor < s.first) reader.cursor = s.first;
    const filter = reader.consumer.streamFilter;
    while (reader.cursor < s.next && reader.consumer.want()) {
      const e = readerEntry.call(this, q, s, reader, reader.cursor);
      if (!e) break;
      reader.cursor++;
      if (filter !== undefined && !filterMatch(e, filter, reader.consumer.matchUnfiltered === true)) continue;
      const msg: LiveMsg = {
        id: `s-${e.offset}`,
        rowId: null,
        body: e.body!,
        exchange: e.exchange,
        routingKey: e.routingKey,
        headers: e.wireHeaders ??= [...e.headers, ["x-stream-offset", { t: "l", v: e.offset }]],
        // Encoded once per entry, not once per reader and delivery.
        propRaw: e.wireProps ??= withOffset(e.propRaw, e.headers, e.offset),
        persistent: true,
        priority: 0,
        expiresAt: null,
        redelivered: false,
      };
      this.prom.delivered++;
      reader.consumer.deliver(msg);
    }
    // Caught up with the resident tail: the read-ahead is no longer needed.
    if (reader.ahead && reader.aheadAt! >= reader.ahead.length) reader.ahead = undefined;
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

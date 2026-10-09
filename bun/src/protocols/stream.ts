/**
 * The RabbitMQ stream protocol (port 5552), on the broker's stream queues.
 *
 * A stream here is the same `x-queue-type: stream` queue AMQP 0-9-1 and 1.0
 * clients use, so every protocol reads every other's messages. Messages on
 * the wire are AMQP 1.0 encoded, as RabbitMQ's are; {@link inbound} and
 * {@link outbound} map them to and from the stored 0-9-1 form.
 *
 * Supported: SASL PLAIN, tune, open, heartbeats, create and delete stream,
 * metadata, declare and delete publisher, publish (v1 and v2) with confirms
 * and deduplication by publisher reference, query publisher sequence,
 * subscribe from first, last, next, an offset or a timestamp, credit,
 * unsubscribe, store and query offset, stream stats, command versions, and
 * super streams: create, delete, partitions and route. Each message is
 * delivered as a chunk of one, so credit counts messages.
 *
 * Stored consumer offsets and publisher sequences are kept in the
 * parameter store, so they survive a restart.
 */
import type { Socket } from "bun";
import { inbound, outbound, writeProps } from "../amqp10/map.ts";
import type { Broker, LiveMsg } from "../broker/index.ts";
import type { Field } from "../codec.ts";

const OK = 1;
const STREAM_DOES_NOT_EXIST = 2;
const SUBSCRIPTION_ID_ALREADY_EXISTS = 3;
const SUBSCRIPTION_ID_DOES_NOT_EXIST = 4;
const STREAM_ALREADY_EXISTS = 5;
const SASL_MECHANISM_NOT_SUPPORTED = 7;
const AUTHENTICATION_FAILURE = 8;
const VIRTUAL_HOST_ACCESS_FAILURE = 12;
const INTERNAL_ERROR = 15;
const ACCESS_REFUSED = 16;
const PRECONDITION_FAILED = 17;
const PUBLISHER_DOES_NOT_EXIST = 18;
const NO_OFFSET = 19;

const K = {
  declarePublisher: 1,
  publish: 2,
  publishConfirm: 3,
  publishError: 4,
  queryPublisherSequence: 5,
  deletePublisher: 6,
  subscribe: 7,
  deliver: 8,
  credit: 9,
  storeOffset: 10,
  queryOffset: 11,
  unsubscribe: 12,
  create: 13,
  delete: 14,
  metadata: 15,
  metadataUpdate: 16,
  peerProperties: 17,
  saslHandshake: 18,
  saslAuthenticate: 19,
  tune: 20,
  open: 21,
  close: 22,
  heartbeat: 23,
  route: 24,
  partitions: 25,
  consumerUpdate: 26,
  commandVersions: 27,
  streamStats: 28,
  createSuperStream: 29,
  deleteSuperStream: 30,
} as const;

/** Commands this server answers, with the versions it speaks. */
const VERSIONS: Array<[number, number, number]> = [
  [K.declarePublisher, 1, 1],
  [K.publish, 1, 2],
  [K.publishConfirm, 1, 1],
  [K.publishError, 1, 1],
  [K.queryPublisherSequence, 1, 1],
  [K.deletePublisher, 1, 1],
  [K.subscribe, 1, 1],
  [K.deliver, 1, 1],
  [K.credit, 1, 1],
  [K.storeOffset, 1, 1],
  [K.queryOffset, 1, 1],
  [K.unsubscribe, 1, 1],
  [K.create, 1, 1],
  [K.delete, 1, 1],
  [K.metadata, 1, 1],
  [K.metadataUpdate, 1, 1],
  [K.peerProperties, 1, 1],
  [K.saslHandshake, 1, 1],
  [K.saslAuthenticate, 1, 1],
  [K.tune, 1, 1],
  [K.open, 1, 1],
  [K.close, 1, 1],
  [K.heartbeat, 1, 1],
  [K.route, 1, 1],
  [K.partitions, 1, 1],
  [K.commandVersions, 1, 1],
  [K.streamStats, 1, 1],
  [K.createSuperStream, 1, 1],
  [K.deleteSuperStream, 1, 1],
];

const FRAME_MAX = 1024 * 1024;
const HEARTBEAT_S = 60;
const OFFSETS = "stream-offsets";
const SEQUENCES = "stream-publishers";
const PARTITION_ORDER = "x-stream-partition-order";
/** Headers the stream store adds that a stream client does not see. */
const STREAM_ONLY = new Set(["x-stream-offset"]);

const enc = new TextEncoder();
const dec = new TextDecoder();

/** Big-endian reader over one frame. Strings are int16-prefixed; -1 is null. */
class Rd {
  at = 0;
  private v: DataView;
  constructor(readonly b: Uint8Array) {
    this.v = new DataView(b.buffer, b.byteOffset, b.byteLength);
  }
  left() {
    return this.b.length - this.at;
  }
  private need(n: number) {
    if (this.at + n > this.b.length) throw new Error("stream: short frame");
  }
  u8() {
    this.need(1);
    return this.b[this.at++]!;
  }
  u16() {
    this.need(2);
    const n = this.v.getUint16(this.at);
    this.at += 2;
    return n;
  }
  i16() {
    this.need(2);
    const n = this.v.getInt16(this.at);
    this.at += 2;
    return n;
  }
  u32() {
    this.need(4);
    const n = this.v.getUint32(this.at);
    this.at += 4;
    return n;
  }
  i32() {
    this.need(4);
    const n = this.v.getInt32(this.at);
    this.at += 4;
    return n;
  }
  u64() {
    this.need(8);
    const n = this.v.getBigUint64(this.at);
    this.at += 8;
    return n;
  }
  i64() {
    this.need(8);
    const n = this.v.getBigInt64(this.at);
    this.at += 8;
    return n;
  }
  str(): string | null {
    const n = this.i16();
    if (n < 0) return null;
    this.need(n);
    const s = dec.decode(this.b.subarray(this.at, this.at + n));
    this.at += n;
    return s;
  }
  bytes(): Uint8Array {
    const n = this.i32();
    if (n < 0) return new Uint8Array(0);
    this.need(n);
    const out = this.b.subarray(this.at, this.at + n);
    this.at += n;
    return out;
  }
  strings(): string[] {
    const n = this.i32();
    const out: string[] = [];
    for (let i = 0; i < n; i++) out.push(this.str() ?? "");
    return out;
  }
  map(): Record<string, string> {
    const n = this.i32();
    const out: Record<string, string> = {};
    for (let i = 0; i < n; i++) {
      const k = this.str() ?? "";
      out[k] = this.str() ?? "";
    }
    return out;
  }
}

/** Big-endian writer for one frame body. */
class Wr {
  private buf = new Uint8Array(64);
  private v = new DataView(this.buf.buffer);
  at = 0;
  private room(n: number) {
    if (this.at + n <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < this.at + n) size *= 2;
    const next = new Uint8Array(size);
    next.set(this.buf.subarray(0, this.at));
    this.buf = next;
    this.v = new DataView(next.buffer);
  }
  u8(n: number) {
    this.room(1);
    this.buf[this.at++] = n;
    return this;
  }
  u16(n: number) {
    this.room(2);
    this.v.setUint16(this.at, n);
    this.at += 2;
    return this;
  }
  u32(n: number) {
    this.room(4);
    this.v.setUint32(this.at, n >>> 0);
    this.at += 4;
    return this;
  }
  i32(n: number) {
    this.room(4);
    this.v.setInt32(this.at, n);
    this.at += 4;
    return this;
  }
  u64(n: bigint | number) {
    this.room(8);
    this.v.setBigUint64(this.at, BigInt(n));
    this.at += 8;
    return this;
  }
  i64(n: bigint | number) {
    this.room(8);
    this.v.setBigInt64(this.at, BigInt(n));
    this.at += 8;
    return this;
  }
  bytes(b: Uint8Array) {
    this.room(b.length);
    this.buf.set(b, this.at);
    this.at += b.length;
    return this;
  }
  str(s: string) {
    const b = enc.encode(s);
    this.u16(b.length);
    return this.bytes(b);
  }
  strings(items: string[]) {
    this.i32(items.length);
    for (const s of items) this.str(s);
    return this;
  }
  map(m: Record<string, string>) {
    const entries = Object.entries(m);
    this.i32(entries.length);
    for (const [k, v] of entries) this.str(k).str(v);
    return this;
  }
  done() {
    return this.buf.slice(0, this.at);
  }
}

/** An offset specification: type 1 first, 2 last, 3 next, 4 an offset, 5 a timestamp in ms. */
function readOffset(r: Rd): Field | null {
  const type = r.u16();
  if (type === 1) return { t: "S", v: "first" };
  if (type === 2) return { t: "S", v: "last" };
  if (type === 3) return { t: "S", v: "next" };
  if (type === 4) return { t: "l", v: Number(r.u64()) };
  if (type === 5) return { t: "T", v: Math.floor(Number(r.i64()) / 1000) };
  return null;
}

/** One frame: size, key, version, body. */
function frame(key: number, version: number, body: (w: Wr) => void): Uint8Array {
  const w = new Wr();
  w.u32(0).u16(key).u16(version);
  body(w);
  const out = w.done();
  new DataView(out.buffer).setUint32(0, out.length - 4);
  return out;
}

function response(key: number, corr: number, code: number, rest?: (w: Wr) => void): Uint8Array {
  return frame(key | 0x8000, 1, (w) => {
    w.u32(corr).u16(code);
    rest?.(w);
  });
}

type Publisher = { stream: string; ref: string | null };
type Subscription = {
  stream: string;
  tag: string;
  session: number;
  credit: number;
  /** Where to start when there is no single-active-consumer offset. */
  spec: Field;
  /** True while a broker consumer feeds this subscription. */
  consuming: boolean;
  /** Single-active-consumer group key, or null. */
  group: string | null;
};

type Member = { session: StreamSession; id: number; sub: Subscription };
type Group = { superStream: string | null; members: Member[]; active: Member | null };

/**
 * Single-active-consumer groups, shared by every connection: one per vhost,
 * stream and consumer name. On a plain stream the oldest member is active.
 * On a super stream partition `i`, member `i % n` is, so the partitions
 * spread over the group, as RabbitMQ assigns them.
 */
const groups = new Map<string, Group>();

function rebalance(key: string, partitionIndex: (superStream: string, stream: string) => number) {
  const g = groups.get(key);
  if (!g) return;
  if (!g.members.length) {
    groups.delete(key);
    return;
  }
  const stream = g.members[0]!.sub.stream;
  const index = g.superStream ? Math.max(0, partitionIndex(g.superStream, stream)) : 0;
  const want = g.members[index % g.members.length]!;
  if (g.active === want) return;
  if (g.active) g.active.session.deactivate(g.active.id, g.active.sub);
  g.active = want;
  want.session.activate(want.id, want.sub);
}

/** Stream-create arguments as queue arguments: `max-age` becomes `x-max-age`. */
function queueArgs(args: Record<string, string>): Record<string, string | number> {
  const out: Record<string, string | number> = { "x-queue-type": "stream" };
  for (const [k, v] of Object.entries(args)) {
    const key = k.startsWith("x-") ? k : `x-${k}`;
    out[key] = /^\d+$/.test(v) ? Number(v) : v;
  }
  return out;
}

export class StreamSession {
  private buf = new Uint8Array(0);
  private user = "";
  private vhost = "/";
  private authed = false;
  private opened = false;
  private closed = false;
  private chain: Promise<unknown> = Promise.resolve();
  private publishers = new Map<number, Publisher>();
  private subs = new Map<number, Subscription>();
  private heartbeat: Timer | null = null;
  /** Server-sent requests (consumer updates) waiting for the client's answer. */
  private pending = new Map<number, (r: Rd) => Promise<void> | void>();
  private nextCorr = 1;

  constructor(
    private readonly broker: Broker,
    private readonly write: (bytes: Uint8Array) => void,
    private readonly end: () => void,
    private readonly advertised: { host: string; port: number },
  ) {}

  feed(data: Uint8Array): void {
    this.chain = this.chain.then(() => this.drain(data)).catch(() => this.stop());
  }

  /** The transport closed: drop subscriptions. */
  closedByPeer(): void {
    if (this.closed) return;
    this.closed = true;
    void this.chain.then(() => this.cleanup());
  }

  private async drain(data: Uint8Array) {
    if (this.closed) return;
    if (this.buf.length) {
      const joined = new Uint8Array(this.buf.length + data.length);
      joined.set(this.buf);
      joined.set(data, this.buf.length);
      data = joined;
    }
    let at = 0;
    while (!this.closed && data.length - at >= 4) {
      const size = new DataView(data.buffer, data.byteOffset + at, 4).getUint32(0);
      if (size > FRAME_MAX) return this.stop();
      if (data.length - at - 4 < size) break;
      const body = data.subarray(at + 4, at + 4 + size);
      at += 4 + size;
      await this.onFrame(new Rd(body));
    }
    this.buf = at < data.length ? data.slice(at) : new Uint8Array(0);
  }

  private async onFrame(r: Rd) {
    const key = r.u16();
    const version = r.u16();
    // Before open only the handshake commands are allowed.
    const handshake = key === K.peerProperties || key === K.saslHandshake || key === K.saslAuthenticate || key === K.tune || key === K.open;
    if (!this.opened && !handshake && key !== K.heartbeat && key !== K.close) return this.stop();
    switch (key) {
      case K.peerProperties:
        return this.onPeerProperties(r);
      case K.saslHandshake: {
        const corr = r.u32();
        return this.write(response(key, corr, OK, (w) => w.strings(["PLAIN"])));
      }
      case K.saslAuthenticate:
        return this.onAuthenticate(r);
      case K.tune:
        // The client's answer to our tune. Heartbeats start once it is in.
        r.u32();
        return this.startHeartbeat(r.u32());
      case K.open:
        return this.onOpen(r);
      case K.close: {
        const corr = r.u32();
        this.write(response(key, corr, OK));
        await this.cleanup();
        return this.stop();
      }
      case K.heartbeat:
        return;
      case K.commandVersions: {
        const corr = r.u32();
        return this.write(
          response(key, corr, OK, (w) => {
            w.i32(VERSIONS.length);
            for (const [k, min, max] of VERSIONS) w.u16(k).u16(min).u16(max);
          }),
        );
      }
      case K.create:
        return this.onCreate(r);
      case K.delete:
        return this.onDelete(r);
      case K.metadata:
        return this.onMetadata(r);
      case K.declarePublisher:
        return this.onDeclarePublisher(r);
      case K.deletePublisher: {
        const corr = r.u32();
        const id = r.u8();
        const known = this.publishers.delete(id);
        return this.write(response(key, corr, known ? OK : PUBLISHER_DOES_NOT_EXIST));
      }
      case K.publish:
        return this.onPublish(r, version);
      case K.queryPublisherSequence: {
        const corr = r.u32();
        const ref = r.str() ?? "";
        const stream = r.str() ?? "";
        const seq = this.sequence(stream, ref);
        return this.write(response(key, corr, OK, (w) => w.u64(seq)));
      }
      case K.subscribe:
        return this.onSubscribe(r);
      case K.credit:
        return this.onCredit(r);
      case K.unsubscribe: {
        const corr = r.u32();
        const id = r.u8();
        const sub = this.subs.get(id);
        if (!sub) return this.write(response(key, corr, SUBSCRIPTION_ID_DOES_NOT_EXIST));
        await this.dropSub(id, sub);
        return this.write(response(key, corr, OK));
      }
      case K.storeOffset: {
        const ref = r.str() ?? "";
        const stream = r.str() ?? "";
        const offset = r.u64();
        if (this.queue(stream) && this.broker.can(this.user, this.vhost, "read", stream)) {
          this.broker.store.putParameter(OFFSETS, this.vhost, `${stream}\0${ref}`, offset.toString());
        }
        return;
      }
      case K.queryOffset: {
        const corr = r.u32();
        const ref = r.str() ?? "";
        const stream = r.str() ?? "";
        if (!this.queue(stream)) return this.write(response(key, corr, STREAM_DOES_NOT_EXIST, (w) => w.u64(0)));
        const stored = this.param(OFFSETS, `${stream}\0${ref}`);
        if (stored == null) return this.write(response(key, corr, NO_OFFSET, (w) => w.u64(0)));
        return this.write(response(key, corr, OK, (w) => w.u64(BigInt(stored))));
      }
      case K.streamStats:
        return this.onStats(r);
      case K.route:
        return this.onRoute(r);
      case K.partitions: {
        const corr = r.u32();
        const superStream = r.str() ?? "";
        if (!this.broker.exchanges.has(this.broker.key(this.vhost, superStream))) {
          return this.write(response(key, corr, STREAM_DOES_NOT_EXIST, (w) => w.strings([])));
        }
        return this.write(response(key, corr, OK, (w) => w.strings(this.partitionsOf(superStream))));
      }
      case K.consumerUpdate | 0x8000: {
        const corr = r.u32();
        r.u16();
        const handler = this.pending.get(corr);
        this.pending.delete(corr);
        return handler?.(r);
      }
      case K.createSuperStream:
        return this.onCreateSuperStream(r);
      case K.deleteSuperStream:
        return this.onDeleteSuperStream(r);
      default:
        // An unknown request with a correlation id gets an answer; anything else is dropped.
        if (r.left() >= 4) {
          const corr = r.u32();
          this.write(response(key, corr, PRECONDITION_FAILED));
        }
    }
  }

  private onPeerProperties(r: Rd) {
    const corr = r.u32();
    r.map();
    // `version` is the RabbitMQ stream protocol level answered; clients gate features on it.
    const props = { product: "QueueForge", platform: "Bun", version: "4.3.0", copyright: "QueueForge", information: "RabbitMQ stream protocol" };
    this.write(response(K.peerProperties, corr, OK, (w) => w.map(props)));
  }

  private async onAuthenticate(r: Rd) {
    const corr = r.u32();
    const mechanism = r.str();
    const data = r.bytes();
    if (mechanism !== "PLAIN") return this.write(response(K.saslAuthenticate, corr, SASL_MECHANISM_NOT_SUPPORTED));
    const [, user = "", pass = ""] = dec.decode(data).split("\0");
    if (!user || !(await this.broker.verify(user, pass))) {
      this.write(response(K.saslAuthenticate, corr, AUTHENTICATION_FAILURE));
      return this.stop();
    }
    this.user = user;
    this.authed = true;
    this.write(response(K.saslAuthenticate, corr, OK));
    this.write(frame(K.tune, 1, (w) => w.u32(FRAME_MAX).u32(HEARTBEAT_S)));
  }

  private startHeartbeat(seconds: number) {
    if (this.heartbeat || seconds <= 0) return;
    this.heartbeat = setInterval(() => this.write(frame(K.heartbeat, 1, () => {})), seconds * 1000);
  }

  private onOpen(r: Rd) {
    const corr = r.u32();
    const vhost = r.str() ?? "/";
    if (!this.authed || !this.broker.hasVhostAccess(this.user, vhost)) {
      this.write(response(K.open, corr, VIRTUAL_HOST_ACCESS_FAILURE));
      return this.stop();
    }
    this.vhost = vhost;
    this.opened = true;
    const props = { advertised_host: this.advertised.host, advertised_port: String(this.advertised.port) };
    this.write(response(K.open, corr, OK, (w) => w.map(props)));
  }

  private queue(stream: string) {
    const q = this.broker.queues.get(this.broker.key(this.vhost, stream));
    return q && q.argsParsed.queueType === "stream" ? q : null;
  }

  private param(component: string, name: string): string | null {
    for (const p of this.broker.store.listParameters(component)) if (p.vhost === this.vhost && p.name === name) return p.value;
    return null;
  }

  private sequence(stream: string, ref: string): bigint {
    const v = this.param(SEQUENCES, `${stream}\0${ref}`);
    return v == null ? 0n : BigInt(v);
  }

  private async onCreate(r: Rd) {
    const corr = r.u32();
    const stream = r.str() ?? "";
    const args = r.map();
    this.write(response(K.create, corr, await this.createStream(stream, args)));
  }

  private async createStream(stream: string, args: Record<string, string>): Promise<number> {
    if (!stream || stream.startsWith("amq.")) return PRECONDITION_FAILED;
    if (!this.broker.can(this.user, this.vhost, "configure", stream)) return ACCESS_REFUSED;
    if (this.broker.queues.has(this.broker.key(this.vhost, stream))) return STREAM_ALREADY_EXISTS;
    try {
      await this.broker.declareQueue({ vhost: this.vhost, name: stream, durable: true, exclusive: false, autoDelete: false, passive: false, args: queueArgs(args) });
      return OK;
    } catch {
      return PRECONDITION_FAILED;
    }
  }

  private async onDelete(r: Rd) {
    const corr = r.u32();
    const stream = r.str() ?? "";
    this.write(response(K.delete, corr, await this.deleteStream(stream)));
  }

  private async deleteStream(stream: string): Promise<number> {
    if (!this.queue(stream)) return STREAM_DOES_NOT_EXIST;
    if (!this.broker.can(this.user, this.vhost, "configure", stream)) return ACCESS_REFUSED;
    await this.broker.deleteQueue(this.vhost, stream).catch(() => 0);
    for (const [id, sub] of this.subs) {
      if (sub.stream === stream) {
        this.subs.delete(id);
        this.write(frame(K.metadataUpdate, 1, (w) => w.u16(STREAM_DOES_NOT_EXIST).str(stream)));
      }
    }
    return OK;
  }

  private onMetadata(r: Rd) {
    const corr = r.u32();
    const streams = r.strings();
    this.write(
      frame(K.metadata | 0x8000, 1, (w) => {
        w.u32(corr);
        // One broker: this node, as reference 0.
        w.i32(1).u16(0).str(this.advertised.host).u32(this.advertised.port);
        w.i32(streams.length);
        for (const s of streams) {
          const exists = !!this.queue(s);
          w.str(s).u16(exists ? OK : STREAM_DOES_NOT_EXIST).u16(exists ? 0 : 0xffff).i32(0);
        }
      }),
    );
  }

  private onDeclarePublisher(r: Rd) {
    const corr = r.u32();
    const id = r.u8();
    const ref = r.str();
    const stream = r.str() ?? "";
    if (this.publishers.has(id)) return this.write(response(K.declarePublisher, corr, PRECONDITION_FAILED));
    if (!this.queue(stream)) return this.write(response(K.declarePublisher, corr, STREAM_DOES_NOT_EXIST));
    if (!this.broker.can(this.user, this.vhost, "write", stream)) return this.write(response(K.declarePublisher, corr, ACCESS_REFUSED));
    this.publishers.set(id, { stream, ref: ref || null });
    this.write(response(K.declarePublisher, corr, OK));
  }

  private onPublish(r: Rd, version: number) {
    const id = r.u8();
    const count = r.u32();
    const pub = this.publishers.get(id);
    const q = pub ? this.queue(pub.stream) : null;
    const confirmed: bigint[] = [];
    const failed: Array<[bigint, number]> = [];
    const commits: Promise<void>[] = [];
    let last = pub?.ref ? this.sequence(pub.stream, pub.ref) : -1n;
    const lastBefore = last;
    for (let i = 0; i < count; i++) {
      const publishingId = r.u64();
      if (version >= 2) r.str();
      const payload = r.bytes();
      if (!pub) {
        failed.push([publishingId, PUBLISHER_DOES_NOT_EXIST]);
        continue;
      }
      if (!q) {
        failed.push([publishingId, STREAM_DOES_NOT_EXIST]);
        continue;
      }
      // A named publisher's ids rise; one at or below the last stored is a duplicate.
      if (pub.ref && publishingId <= last) {
        confirmed.push(publishingId);
        continue;
      }
      try {
        const msg = inbound(payload);
        const src = {
          body: msg.body,
          exchange: "",
          routingKey: pub.stream,
          headers: msg.props.headers,
          propRaw: writeProps(msg.props),
        };
        if (pub.ref) last = publishingId;
        if (q.raftGroup) {
          // A replicated stream confirms once the append committed on a majority.
          commits.push(this.broker.enqueueStreamRaft(q, src).then((ok) => {
            if (ok) confirmed.push(publishingId);
            else failed.push([publishingId, INTERNAL_ERROR]);
          }));
          continue;
        }
        this.broker.appendStream(q, src);
        confirmed.push(publishingId);
      } catch {
        failed.push([publishingId, INTERNAL_ERROR]);
      }
    }
    if (commits.length) {
      void Promise.all(commits).then(() => this.finishPublish(id, pub, last, lastBefore, confirmed, failed));
      return;
    }
    this.finishPublish(id, pub, last, lastBefore, confirmed, failed);
  }

  private finishPublish(id: number, pub: { stream: string; ref: string | null } | undefined, last: bigint, lastBefore: bigint, confirmed: bigint[], failed: Array<[bigint, number]>) {
    if (pub?.ref && last !== lastBefore) this.broker.store.putParameter(SEQUENCES, this.vhost, `${pub.stream}\0${pub.ref}`, last.toString());
    if (confirmed.length) {
      this.write(
        frame(K.publishConfirm, 1, (w) => {
          w.u8(id).u32(confirmed.length);
          for (const c of confirmed) w.u64(c);
        }),
      );
    }
    if (failed.length) {
      this.write(
        frame(K.publishError, 1, (w) => {
          w.u8(id).u32(failed.length);
          for (const [c, code] of failed) w.u64(c).u16(code);
        }),
      );
    }
  }

  private async onSubscribe(r: Rd) {
    const corr = r.u32();
    const id = r.u8();
    const stream = r.str() ?? "";
    const spec = readOffset(r);
    if (!spec) return this.write(response(K.subscribe, corr, PRECONDITION_FAILED));
    const credit = r.u16();
    const props = r.left() >= 4 ? r.map() : {};
    if (this.subs.has(id)) return this.write(response(K.subscribe, corr, SUBSCRIPTION_ID_ALREADY_EXISTS));
    if (!this.queue(stream)) return this.write(response(K.subscribe, corr, STREAM_DOES_NOT_EXIST));
    if (!this.broker.can(this.user, this.vhost, "read", stream)) return this.write(response(K.subscribe, corr, ACCESS_REFUSED));
    const single = props["single-active-consumer"] === "true";
    if (single && !props.name) return this.write(response(K.subscribe, corr, PRECONDITION_FAILED));
    const group = single ? `${this.vhost}\0${stream}\0${props.name}` : null;
    const sub: Subscription = { stream, tag: `stream-${crypto.randomUUID()}`, session: this.broker.nextSession(), credit, spec, consuming: false, group };
    this.subs.set(id, sub);
    this.write(response(K.subscribe, corr, OK));
    if (!group) return this.startConsuming(id, sub, spec);
    const g = groups.get(group) ?? { superStream: props["super-stream"] || null, members: [], active: null };
    groups.set(group, g);
    g.members.push({ session: this, id, sub });
    rebalance(group, (superStream) => this.partitionsOf(superStream).indexOf(stream));
  }

  private async startConsuming(id: number, sub: Subscription, spec: Field) {
    if (sub.consuming || this.subs.get(id) !== sub) return;
    sub.consuming = true;
    await this.broker.consume(this.vhost, sub.stream, {
      tag: sub.tag,
      session: sub.session,
      noAck: true,
      exclusive: false,
      streamOffset: spec,
      want: () => !this.closed && sub.consuming && this.subs.get(id) === sub && sub.credit > 0,
      deliver: (msg: LiveMsg) => this.deliver(id, sub, msg),
    } as Parameters<Broker["consume"]>[2]);
    const q = this.queue(sub.stream);
    if (q) this.broker.pumpStream(q);
  }

  /** Make this subscription its group's active consumer. It starts where the client's answer says. */
  activate(id: number, sub: Subscription) {
    const corr = this.nextCorr++;
    this.pending.set(corr, (r) => this.startConsuming(id, sub, readOffset(r) ?? sub.spec));
    this.write(frame(K.consumerUpdate, 1, (w) => w.u32(corr).u8(id).u8(1)));
  }

  /** Stop feeding a subscription that is no longer its group's active consumer. */
  deactivate(id: number, sub: Subscription) {
    if (sub.consuming) {
      sub.consuming = false;
      void this.broker.cancel(this.vhost, sub.stream, sub.tag).catch(() => {});
    }
    if (this.closed || this.subs.get(id) !== sub) return;
    const corr = this.nextCorr++;
    this.pending.set(corr, () => {});
    this.write(frame(K.consumerUpdate, 1, (w) => w.u32(corr).u8(id).u8(0)));
  }

  /** Send one message as a chunk of one entry. */
  private deliver(id: number, sub: Subscription, msg: LiveMsg) {
    sub.credit--;
    const offsetField = msg.headers.find(([k]) => k === "x-stream-offset")?.[1];
    const offset = offsetField && "v" in offsetField ? Number(offsetField.v) : 0;
    // The offset travels in the chunk header, not as an annotation.
    const entry = outbound(msg, STREAM_ONLY);
    const data = new Uint8Array(4 + entry.length);
    new DataView(data.buffer).setUint32(0, entry.length);
    data.set(entry, 4);
    this.write(
      frame(K.deliver, 1, (w) => {
        w.u8(id);
        // Osiris chunk header: magic and version, user chunk, 1 entry, 1 record.
        w.u8(0x50).u8(0).u16(1).u32(1);
        w.i64(Date.now()).u64(0).u64(offset);
        w.i32(Bun.hash.crc32(data) | 0).u32(data.length).u32(0).u32(0);
        w.bytes(data);
      }),
    );
  }

  private onCredit(r: Rd) {
    const id = r.u8();
    const credit = r.u16();
    const sub = this.subs.get(id);
    if (!sub) {
      this.write(frame(K.credit | 0x8000, 1, (w) => w.u16(SUBSCRIPTION_ID_DOES_NOT_EXIST).u8(id)));
      return;
    }
    sub.credit += credit;
    const q = this.queue(sub.stream);
    if (q) this.broker.pumpStream(q);
  }

  private onStats(r: Rd) {
    const corr = r.u32();
    const stream = r.str() ?? "";
    const q = this.queue(stream);
    if (!q) return this.write(response(K.streamStats, corr, STREAM_DOES_NOT_EXIST, (w) => w.i32(0)));
    const s = this.broker.streamOf(q);
    const last = s.next - 1;
    this.write(
      response(K.streamStats, corr, OK, (w) => {
        // A chunk holds one message here, so the chunk ids are message offsets.
        w.i32(4);
        w.str("first_chunk_id").i64(s.first);
        w.str("last_chunk_id").i64(last);
        w.str("committed_chunk_id").i64(last);
        w.str("committed_offset").i64(last);
      }),
    );
  }

  /** Partition streams of a super stream, in partition order. */
  private partitionsOf(superStream: string): string[] {
    const order = (args: Array<[string, Field]>) => {
      const f = args.find(([k]) => k === PARTITION_ORDER)?.[1];
      return f && "v" in f ? Number(f.v) : 0;
    };
    return this.broker.bindings
      .filter((b) => b.vhost === this.vhost && b.exchange === superStream)
      .sort((a, b) => order(a.args) - order(b.args))
      .map((b) => b.queue);
  }

  private onRoute(r: Rd) {
    const corr = r.u32();
    const routingKey = r.str() ?? "";
    const superStream = r.str() ?? "";
    if (!this.broker.exchanges.has(this.broker.key(this.vhost, superStream))) {
      return this.write(response(K.route, corr, STREAM_DOES_NOT_EXIST, (w) => w.strings([])));
    }
    const streams = this.broker.bindings.filter((b) => b.vhost === this.vhost && b.exchange === superStream && b.routingKey === routingKey).map((b) => b.queue);
    this.write(response(K.route, corr, OK, (w) => w.strings(streams)));
  }

  private async onCreateSuperStream(r: Rd) {
    const corr = r.u32();
    const name = r.str() ?? "";
    const partitions = r.strings();
    const keys = r.strings();
    const args = r.map();
    const reply = (code: number) => this.write(response(K.createSuperStream, corr, code));
    if (!name || partitions.length === 0 || partitions.length !== keys.length) return reply(PRECONDITION_FAILED);
    if (!this.broker.can(this.user, this.vhost, "configure", name)) return reply(ACCESS_REFUSED);
    if (this.broker.exchanges.has(this.broker.key(this.vhost, name))) return reply(STREAM_ALREADY_EXISTS);
    for (const p of partitions) if (this.broker.queues.has(this.broker.key(this.vhost, p))) return reply(STREAM_ALREADY_EXISTS);
    try {
      // A super stream is a direct exchange with one stream bound per partition, as in RabbitMQ.
      await this.broker.declareExchange(this.vhost, name, "direct", true, false, false, null);
      for (let i = 0; i < partitions.length; i++) {
        const code = await this.createStream(partitions[i]!, args);
        if (code !== OK) return reply(code);
        await this.broker.bind(this.vhost, name, partitions[i]!, keys[i]!, [[PARTITION_ORDER, { t: "l", v: i }]]);
      }
      return reply(OK);
    } catch {
      return reply(PRECONDITION_FAILED);
    }
  }

  private async onDeleteSuperStream(r: Rd) {
    const corr = r.u32();
    const name = r.str() ?? "";
    if (!this.broker.exchanges.has(this.broker.key(this.vhost, name))) return this.write(response(K.deleteSuperStream, corr, STREAM_DOES_NOT_EXIST));
    if (!this.broker.can(this.user, this.vhost, "configure", name)) return this.write(response(K.deleteSuperStream, corr, ACCESS_REFUSED));
    const partitions = this.partitionsOf(name);
    await this.broker.deleteExchange(this.vhost, name).catch(() => {});
    for (const p of partitions) await this.deleteStream(p);
    this.write(response(K.deleteSuperStream, corr, OK));
  }

  private async dropSub(id: number, sub: Subscription) {
    this.subs.delete(id);
    sub.consuming = false;
    await this.broker.cancel(this.vhost, sub.stream, sub.tag).catch(() => {});
    if (!sub.group) return;
    const g = groups.get(sub.group);
    if (!g) return;
    g.members = g.members.filter((m) => m.sub !== sub);
    if (g.active?.sub === sub) g.active = null;
    rebalance(sub.group, (superStream) => this.partitionsOf(superStream).indexOf(sub.stream));
  }

  private async cleanup() {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
    for (const [id, sub] of [...this.subs]) await this.dropSub(id, sub);
    this.publishers.clear();
  }

  private stop() {
    if (!this.closed) {
      this.closed = true;
      void this.cleanup();
    }
    try {
      this.end();
    } catch {
      /* the transport is already gone */
    }
  }
}

type Conn = { session: StreamSession; out: Uint8Array[] };

/** Write what the socket takes; queue the rest until it drains. */
function writeOut(socket: Socket<Conn>, conn: Conn, bytes: Uint8Array) {
  if (conn.out.length) {
    conn.out.push(bytes);
    return;
  }
  const n = socket.write(bytes);
  if (n < bytes.length) conn.out.push(bytes.subarray(Math.max(0, n)));
}

function flushOut(socket: Socket<Conn>, conn: Conn) {
  while (conn.out.length) {
    const head = conn.out[0]!;
    const n = socket.write(head);
    if (n < head.length) {
      conn.out[0] = head.subarray(Math.max(0, n));
      return;
    }
    conn.out.shift();
  }
}

/**
 * Bind the stream port.
 *
 * @param host Listen address. Clients are told to reconnect to it, so a
 * wildcard address is advertised as `localhost`.
 */
export function startStream(host: string, port: number, broker: Broker, tls: { cert: string; key: string } | null = null) {
  const advertised = { host: host === "0.0.0.0" || host === "::" ? "localhost" : host, port };
  Bun.listen<Conn>({
    hostname: host,
    port,
    ...(tls ? { tls: { cert: Bun.file(tls.cert), key: Bun.file(tls.key) } } : {}),
    socket: {
      open(socket) {
        socket.setNoDelay(true);
        const conn: Conn = { out: [], session: null as unknown as StreamSession };
        conn.session = new StreamSession(
          broker,
          (bytes) => {
            try {
              writeOut(socket, conn, bytes);
            } catch {
              /* closed */
            }
          },
          () => socket.end(),
          advertised,
        );
        socket.data = conn;
      },
      data(socket, data) {
        socket.data?.session.feed(new Uint8Array(data));
      },
      drain(socket) {
        if (socket.data) flushOut(socket, socket.data);
      },
      close(socket) {
        socket.data?.session.closedByPeer();
      },
      error(socket) {
        socket.data?.session.closedByPeer();
      },
    },
  });
}

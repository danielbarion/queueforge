/**
 * MQTT 3.1, 3.1.1 and 5.0, as RabbitMQ's MQTT plugin maps them onto AMQP.
 *
 * A topic is a routing key on `amq.topic`: `/` becomes `.`, `+` becomes `*`.
 * Each client gets one queue, `mqtt-subscription-<client id>qos1`, bound once
 * per subscription filter. A clean session's queue goes when the client does;
 * a persistent session's queue is durable and keeps QoS 1 messages offline.
 * Logins check the user store; publishes and subscriptions check permissions.
 * Retained messages are stored per vhost and topic. QoS 2 is served as QoS 1,
 * as RabbitMQ 4 does.
 *
 * {@link MqttSession} does not own a socket, so TCP and WebSocket share it.
 */
import type { Broker, LiveMsg } from "../broker/index.ts";
import { ChanError } from "../broker/index.ts";
import { W, writeTable, type Field } from "../codec.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();
const TOPIC_EXCHANGE = "amq.topic";
const EMPTY = new Uint8Array(0);
/** Unacked QoS 1 deliveries per client before the queue waits. */
const INFLIGHT = 128;

/** MQTT topic to AMQP routing key. */
export function topicToKey(topic: string): string {
  return topic.replaceAll("/", ".");
}

/** AMQP routing key back to an MQTT topic. */
export function keyToTopic(key: string): string {
  return key.replaceAll(".", "/");
}

/** MQTT subscription filter to an AMQP binding key. */
function filterToKey(filter: string): string {
  return filter
    .split("/")
    .map((part) => (part === "+" ? "*" : part))
    .join(".");
}

/** Whether an MQTT filter matches a topic. */
export function mqttMatch(filter: string, topic: string): boolean {
  const f = filter.split("/");
  const t = topic.split("/");
  for (let i = 0; i < f.length; i++) {
    if (f[i] === "#") return true;
    if (i >= t.length) return false;
    if (f[i] !== "+" && f[i] !== t[i]) return false;
  }
  return f.length === t.length;
}

function varint(n: number): number[] {
  const out: number[] = [];
  do {
    let byte = n % 128;
    n = Math.floor(n / 128);
    if (n > 0) byte |= 0x80;
    out.push(byte);
  } while (n > 0);
  return out;
}

function packet(first: number, body: Uint8Array | number[]): Uint8Array {
  const b = body instanceof Uint8Array ? body : Uint8Array.from(body);
  const len = varint(b.length);
  const out = new Uint8Array(1 + len.length + b.length);
  out[0] = first;
  out.set(len, 1);
  out.set(b, 1 + len.length);
  return out;
}

function str(text: string): Uint8Array {
  const b = enc.encode(text);
  const out = new Uint8Array(2 + b.length);
  out[0] = b.length >> 8;
  out[1] = b.length & 0xff;
  out.set(b, 2);
  return out;
}

function join(parts: Array<Uint8Array | number[]>): Uint8Array {
  const arrays = parts.map((p) => (p instanceof Uint8Array ? p : Uint8Array.from(p)));
  const out = new Uint8Array(arrays.reduce((n, a) => n + a.length, 0));
  let at = 0;
  for (const a of arrays) {
    out.set(a, at);
    at += a.length;
  }
  return out;
}

/** A cursor over one packet body. Reads past the end throw. */
class Reader {
  at = 0;
  constructor(readonly b: Uint8Array) {}
  left() {
    return this.b.length - this.at;
  }
  u8() {
    if (this.at >= this.b.length) throw new Error("short packet");
    return this.b[this.at++]!;
  }
  u16() {
    return (this.u8() << 8) | this.u8();
  }
  bytes(n: number) {
    if (this.at + n > this.b.length) throw new Error("short packet");
    const out = this.b.subarray(this.at, this.at + n);
    this.at += n;
    return out;
  }
  binary() {
    return this.bytes(this.u16());
  }
  str() {
    return dec.decode(this.binary());
  }
  varint() {
    let value = 0;
    let mult = 1;
    for (let i = 0; i < 4; i++) {
      const byte = this.u8();
      value += (byte & 0x7f) * mult;
      if ((byte & 0x80) === 0) return value;
      mult *= 128;
    }
    throw new Error("bad varint");
  }
  /** An MQTT 5 property block, kept raw. */
  props() {
    return this.bytes(this.varint());
  }
}

type Retained = { payload: Uint8Array; qos: number; props: Uint8Array; expiresAt: number | null };
/**
 * One subscription and its MQTT 5 options. The options are kept on the
 * binding's arguments, so a persistent session gets them back.
 */
type Sub = { filter: string; qos: number; key: string; noLocal: boolean; rap: boolean; id: number };

/** MQTT 5 property ids used here. */
const PROP = {
  payloadFormat: 0x01,
  messageExpiry: 0x02,
  subscriptionId: 0x0b,
  sessionExpiry: 0x11,
  assignedClientId: 0x12,
  willDelay: 0x18,
  topicAliasMax: 0x22,
  topicAlias: 0x23,
} as const;
/** Topic aliases a client may set on its publishes. */
const TOPIC_ALIAS_MAX = 16;

/** Byte width of each fixed-size property; strings, binaries, varints and pairs are handled apart. */
function propWidth(id: number): number {
  switch (id) {
    case 0x01: case 0x17: case 0x19: case 0x24: case 0x25: case 0x28: case 0x29: case 0x2a:
      return 1;
    case 0x13: case 0x21: case 0x22: case 0x23:
      return 2;
    case 0x02: case 0x11: case 0x18: case 0x27:
      return 4;
    default:
      return 0;
  }
}

/**
 * Walk an MQTT 5 property block. `fn` gets each property's id and the byte
 * range of the whole property (id included) and of its value.
 */
export function eachProp(raw: Uint8Array, fn: (id: number, start: number, valueAt: number, end: number) => void): void {
  let at = 0;
  while (at < raw.length) {
    const start = at;
    const id = raw[at++]!;
    const valueAt = at;
    const w = propWidth(id);
    if (w) at += w;
    else if (id === 0x0b) {
      while (at < raw.length && raw[at]! & 0x80) at++;
      at++;
    } else if (id === 0x26) {
      at += 2 + ((raw[at]! << 8) | raw[at + 1]!);
      at += 2 + ((raw[at]! << 8) | raw[at + 1]!);
    } else at += 2 + ((raw[at]! << 8) | raw[at + 1]!);
    if (at > raw.length) return;
    fn(id, start, valueAt, at);
  }
}

function u32At(raw: Uint8Array, at: number): number {
  return ((raw[at]! << 24) | (raw[at + 1]! << 16) | (raw[at + 2]! << 8) | raw[at + 3]!) >>> 0;
}

/** A 4-byte property's value, or null when absent. */
function propU32(raw: Uint8Array, id: number): number | null {
  let out: number | null = null;
  eachProp(raw, (pid, _s, v) => {
    if (pid === id) out = u32At(raw, v);
  });
  return out;
}

/** The properties without the ids in `drop`. Returns `raw` itself when nothing is dropped. */
function withoutProps(raw: Uint8Array, drop: readonly number[]): Uint8Array {
  const keep: Array<[number, number]> = [];
  let dropped = false;
  eachProp(raw, (id, start, _v, end) => {
    if (drop.includes(id)) dropped = true;
    else keep.push([start, end]);
  });
  if (!dropped) return raw;
  return join(keep.map(([s, e]) => raw.subarray(s, e)));
}

function u32Prop(id: number, n: number): number[] {
  return [id, (n >>> 24) & 0xff, (n >>> 16) & 0xff, (n >>> 8) & 0xff, n & 0xff];
}

/** Live sessions by vhost and client id, for takeover. */
const live = new Map<string, MqttSession>();
/** Retained messages by vhost and topic. Loaded from the store on first use. */
const retained = new Map<string, Map<string, Retained>>();
/**
 * Disconnected sessions waiting for their expiry interval or will delay, by
 * vhost and client id. A reconnect of the same client id cancels both.
 */
const pending = new Map<string, { expire: Timer | null; will: Timer | null }>();
let ownerSeq = 0;

function retainedFor(broker: Broker, vhost: string): Map<string, Retained> {
  let map = retained.get(vhost);
  if (map) return map;
  map = new Map();
  for (const row of broker.store.listParameters("mqtt-retained")) {
    if (row.vhost !== vhost) continue;
    try {
      const v = JSON.parse(row.value) as { p: string; q: number; r?: string; e?: number | null };
      map.set(row.name, { payload: Buffer.from(v.p, "base64"), qos: v.q, props: Buffer.from(v.r ?? "", "base64"), expiresAt: v.e ?? null });
    } catch {
      /* a corrupt row is skipped */
    }
  }
  retained.set(vhost, map);
  return map;
}

function cancelPending(key: string) {
  const p = pending.get(key);
  if (!p) return;
  if (p.expire) clearTimeout(p.expire);
  if (p.will) clearTimeout(p.will);
  pending.delete(key);
}

function subArgs(s: Sub): Array<[string, Field]> {
  return [
    ["x-mqtt-qos", { t: "I", v: s.qos }],
    ["x-mqtt-no-local", { t: "t", v: s.noLocal }],
    ["x-mqtt-rap", { t: "t", v: s.rap }],
    ["x-mqtt-subscription-id", { t: "I", v: s.id }],
  ];
}

/** One MQTT client connection. */
export class MqttSession {
  private buf: Uint8Array = new Uint8Array(0);
  private version = 4;
  private vhost = "/";
  private user = "";
  private clientId = "";
  /** Clean start (v5) or clean session (v3): drop what a previous session left. */
  private clean = true;
  /** The session outlives the connection: clean=false (v3), or a session expiry interval (v5). */
  private persistent = false;
  /** v5 session expiry interval in seconds; 0xFFFFFFFF never expires. */
  private sessionExpiry = 0;
  private queue = "";
  private subs: Sub[] = [];
  /** Some subscription has no-local set: this session's publishes carry its id. */
  private noLocal = false;
  private will: { topic: string; payload: Uint8Array; qos: number; retain: boolean; props: Uint8Array; delay: number } | null = null;
  /** Topic aliases the client set on its publishes. */
  private aliases: Map<number, string> | null = null;
  private connected = false;
  private closed = false;
  private keepalive = 0;
  private lastSeen = Date.now();
  private timer: Timer | null = null;
  private nextPacketId = 1;
  /** QoS 1 deliveries waiting for PUBACK: packet id to queue message id. */
  private inflight = new Map<number, string>();
  private consumerTag = "";
  private chain: Promise<void> = Promise.resolve();
  /** Common name of a verified client certificate (mqtts listener). */
  peerCN: string | null = null;

  constructor(
    private readonly broker: Broker,
    private readonly write: (frame: Uint8Array) => void,
    private readonly end: () => void,
  ) {}

  /** Bytes from the transport. Packets are handled one at a time, in order. */
  feed(data: Uint8Array): void {
    this.lastSeen = Date.now();
    this.chain = this.chain.then(() => this.drain(data)).catch(() => this.drop());
  }

  /** The transport closed. A missing DISCONNECT publishes the will. */
  closedByPeer(): void {
    this.chain = this.chain.then(() => this.teardown(true)).catch(() => {});
  }

  private async drain(data: Uint8Array) {
    const merged = new Uint8Array(this.buf.length + data.length);
    merged.set(this.buf);
    merged.set(data, this.buf.length);
    this.buf = merged;
    while (!this.closed && this.buf.length >= 2) {
      let value = 0;
      let mult = 1;
      let i = 1;
      let done = false;
      while (i < this.buf.length && i <= 4) {
        const byte = this.buf[i]!;
        value += (byte & 0x7f) * mult;
        i++;
        if ((byte & 0x80) === 0) {
          done = true;
          break;
        }
        mult *= 128;
      }
      if (!done || this.buf.length < i + value) return;
      const first = this.buf[0]!;
      const body = this.buf.slice(i, i + value);
      this.buf = this.buf.subarray(i + value);
      await this.handle(first, body);
    }
  }

  private async handle(first: number, body: Uint8Array) {
    const type = first >> 4;
    if (!this.connected && type !== 1) return this.drop();
    switch (type) {
      case 1:
        return this.onConnect(body);
      case 3:
        return this.onPublish(first, body);
      case 4:
        return this.onPuback(body);
      case 5: // PUBREC: QoS 2 is served as QoS 1, so complete the flow.
        return this.write(packet(0x62, [body[0] ?? 0, body[1] ?? 0]));
      case 6: // PUBREL
        return this.write(packet(0x70, [body[0] ?? 0, body[1] ?? 0]));
      case 8:
        return this.onSubscribe(body);
      case 10:
        return this.onUnsubscribe(body);
      case 12:
        return this.write(Uint8Array.of(0xd0, 0x00));
      case 14:
        this.will = null;
        if (this.version === 5 && body.length > 1) {
          // DISCONNECT may lower the session expiry interval.
          const r = new Reader(body);
          r.u8();
          const expiry = propU32(r.props(), PROP.sessionExpiry);
          if (expiry != null) {
            this.sessionExpiry = expiry;
            this.persistent = expiry > 0;
          }
        }
        return this.teardown(false);
      default:
        return this.drop();
    }
  }

  private connack(code: number, present = false, assigned = "") {
    if (this.version === 5) {
      const props: Array<Uint8Array | number[]> = [[PROP.topicAliasMax, 0, TOPIC_ALIAS_MAX]];
      if (assigned) props.push([PROP.assignedClientId], str(assigned));
      const block = join(props);
      this.write(packet(0x20, join([[present ? 1 : 0, code], varint(block.length), block])));
    } else this.write(packet(0x20, [present ? 1 : 0, code]));
  }

  private async onConnect(body: Uint8Array) {
    const r = new Reader(body);
    const name = r.str();
    this.version = r.u8();
    if ((name !== "MQTT" && name !== "MQIsdp") || ![3, 4, 5].includes(this.version)) {
      this.version = 4;
      this.connack(1);
      return this.drop();
    }
    const flags = r.u8();
    this.keepalive = r.u16();
    if (this.version === 5) this.sessionExpiry = propU32(r.props(), PROP.sessionExpiry) ?? 0;
    this.clientId = r.str();
    this.clean = (flags & 0x02) !== 0;
    this.persistent = this.version === 5 ? this.sessionExpiry > 0 : !this.clean;
    if (flags & 0x04) {
      const props = this.version === 5 ? r.props().slice() : new Uint8Array(0);
      const topic = r.str();
      const payload = r.binary().slice();
      const delay = this.version === 5 ? propU32(props, PROP.willDelay) ?? 0 : 0;
      this.will = {
        topic,
        payload,
        qos: Math.min(1, (flags >> 3) & 3),
        retain: (flags & 0x20) !== 0,
        props: withoutProps(props, [PROP.willDelay]),
        delay,
      };
    }
    const username = flags & 0x80 ? r.str() : "";
    const password = flags & 0x40 ? dec.decode(r.binary()) : "";
    // RabbitMQ reads `vhost:user` as a vhost-qualified login.
    let user = username;
    if (username.includes(":")) {
      const at = username.indexOf(":");
      this.vhost = username.slice(0, at) || "/";
      user = username.slice(at + 1);
    }
    const badLogin = this.version === 5 ? 0x86 : 4;
    const notAuthorized = this.version === 5 ? 0x87 : 5;
    // RabbitMQ's mqtt.ssl_cert_login: no user name and a verified client
    // certificate log in as its common name, which must exist. No password.
    if (!username && this.peerCN) {
      if (!this.broker.users.has(this.peerCN)) {
        this.connack(badLogin);
        return this.drop();
      }
      user = this.peerCN;
    } else if (!user || !(await this.broker.verify(user, password))) {
      this.connack(badLogin);
      return this.drop();
    }
    if (!this.broker.hasVhostAccess(user, this.vhost)) {
      this.connack(notAuthorized);
      return this.drop();
    }
    let assigned = "";
    if (!this.clientId) {
      if (!this.clean && this.version !== 5) {
        this.connack(2);
        return this.drop();
      }
      this.clientId = `mqtt-${crypto.randomUUID()}`;
      assigned = this.clientId;
    }
    this.user = user;
    // A second connection with the same client id takes the session over.
    const key = `${this.vhost}\0${this.clientId}`;
    const previous = live.get(key);
    if (previous && previous !== this) await previous.kick();
    // A reconnect before the will delay or the session expiry ends both.
    cancelPending(key);
    live.set(key, this);
    this.queue = `mqtt-subscription-${this.clientId}qos1`;
    const exists = this.broker.queues.has(this.broker.key(this.vhost, this.queue));
    if (this.clean && exists) await this.broker.deleteQueue(this.vhost, this.queue).catch(() => 0);
    const present = !this.clean && exists;
    if (present) this.subs = this.restoreSubs();
    this.noLocal = this.subs.some((s) => s.noLocal);
    this.connected = true;
    this.connack(0, present, assigned);
    if (this.keepalive > 0) {
      this.timer = setInterval(() => {
        if (Date.now() - this.lastSeen > this.keepalive * 1500) void this.teardown(true);
      }, Math.max(1000, this.keepalive * 500));
    }
    if (present) await this.startConsuming();
  }

  /** A persistent session's filters come back from its queue's bindings. */
  private restoreSubs(): Sub[] {
    const out: Sub[] = [];
    for (const b of this.broker.bindings) {
      if (b.vhost !== this.vhost || b.queue !== this.queue || b.exchange !== TOPIC_EXCHANGE) continue;
      const filter = b.routingKey.split(".").map((p) => (p === "*" ? "+" : p)).join("/");
      const arg = (k: string) => b.args?.find(([name]) => name === k)?.[1];
      const num = (k: string, d: number) => {
        const f = arg(k);
        return f && (f.t === "I" || f.t === "l") ? Number(f.v) : d;
      };
      const flag = (k: string) => {
        const f = arg(k);
        return f?.t === "t" && f.v;
      };
      out.push({ filter, qos: num("x-mqtt-qos", 1), key: b.routingKey, noLocal: flag("x-mqtt-no-local"), rap: flag("x-mqtt-rap"), id: num("x-mqtt-subscription-id", 0) });
    }
    return out;
  }

  private async ensureQueue() {
    if (this.broker.queues.has(this.broker.key(this.vhost, this.queue))) return;
    await this.broker.declareQueue({
      vhost: this.vhost,
      name: this.queue,
      durable: this.persistent,
      exclusive: !this.persistent,
      autoDelete: false,
      passive: false,
      args: {},
      owner: this.persistent ? undefined : -(++ownerSeq),
    });
  }

  private async startConsuming() {
    if (this.consumerTag || this.closed) return;
    await this.ensureQueue();
    const tag = `mqtt-${this.clientId}`;
    this.consumerTag = tag;
    await this.broker.consume(this.vhost, this.queue, {
      tag,
      session: this.broker.nextSession(),
      noAck: false,
      exclusive: false,
      want: () => !this.closed && this.inflight.size < INFLIGHT,
      deliver: (msg) => this.deliver(msg),
    });
    const consumer = this.broker.queues.get(this.broker.key(this.vhost, this.queue))?.consumers.find((c) => c.tag === tag);
    if (consumer) await this.broker.kick(this.vhost, this.queue, consumer.session);
  }

  /** Send one queued message to the client at the QoS its subscriptions allow. */
  private deliver(msg: LiveMsg) {
    if (this.closed) return;
    const topic = keyToTopic(msg.routingKey);
    let msgQos = msg.persistent ? 1 : 0;
    let raw: Uint8Array = EMPTY;
    let retainFlag = false;
    let publisher = "";
    for (const [k, v] of msg.headers) {
      if (k === "x-mqtt-publish-qos" && "v" in v) msgQos = Number(v.v);
      else if (k === "x-mqtt-props" && v.t === "x") raw = v.v;
      else if (k === "x-mqtt-retain" && v.t === "t") retainFlag = v.v;
      else if (k === "x-mqtt-publisher" && (v.t === "S" || v.t === "s")) publisher = v.v;
    }
    let subQos = -1;
    let rap = false;
    let local = true;
    let ids: number[] | null = null;
    for (const s of this.subs) {
      if (!mqttMatch(s.filter, topic)) continue;
      subQos = Math.max(subQos, s.qos);
      if (s.rap) rap = true;
      if (!s.noLocal) local = false;
      if (s.id) (ids ??= []).push(s.id);
    }
    // No-local: every matching subscription refuses this session's own publishes.
    if (publisher && publisher === this.clientId && local && subQos >= 0) {
      void Promise.resolve(this.broker.ack(this.vhost, this.queue, msg.id)).catch(() => {});
      return;
    }
    const qos = Math.max(0, Math.min(subQos < 0 ? 0 : subQos, msgQos));
    if (this.version === 5) {
      // The remaining expiry interval travels with the delivery, plus every matching subscription id.
      if (msg.expiresAt != null) {
        const left = Math.max(1, Math.ceil((msg.expiresAt - Date.now()) / 1000));
        raw = join([withoutProps(raw, [PROP.messageExpiry]), u32Prop(PROP.messageExpiry, left)]);
      }
      if (ids) raw = join([raw, ...ids.map((id) => [PROP.subscriptionId, ...varint(id)])]);
    }
    let id = 0;
    if (qos === 1) {
      id = this.packetId();
      this.inflight.set(id, msg.id);
    }
    // The retain flag is forwarded only to retain-as-published subscriptions.
    this.write(this.publishPacket(topic, msg.body, qos, retainFlag && rap, msg.redelivered && qos === 1, id, raw));
    if (qos === 0) void Promise.resolve(this.broker.ack(this.vhost, this.queue, msg.id)).catch(() => {});
  }

  private packetId(): number {
    for (let i = 0; i < 65535; i++) {
      const id = this.nextPacketId;
      this.nextPacketId = id >= 65535 ? 1 : id + 1;
      if (!this.inflight.has(id)) return id;
    }
    return this.nextPacketId;
  }

  private publishPacket(topic: string, payload: Uint8Array, qos: number, retain: boolean, dup: boolean, id: number, props: Uint8Array) {
    const first = 0x30 | (dup ? 0x08 : 0) | (qos << 1) | (retain ? 1 : 0);
    const parts: Array<Uint8Array | number[]> = [str(topic)];
    if (qos > 0) parts.push([id >> 8, id & 0xff]);
    if (this.version === 5) parts.push(varint(props.length), props);
    parts.push(payload);
    return packet(first, join(parts));
  }

  private onPuback(body: Uint8Array) {
    const id = ((body[0] ?? 0) << 8) | (body[1] ?? 0);
    const msgId = this.inflight.get(id);
    if (msgId == null) return;
    this.inflight.delete(id);
    void Promise.resolve(this.broker.ack(this.vhost, this.queue, msgId)).catch(() => {});
    const q = this.broker.queues.get(this.broker.key(this.vhost, this.queue));
    if (q) this.broker.pump(q);
  }

  private async onPublish(first: number, body: Uint8Array) {
    const qos = Math.min(1, (first >> 1) & 3);
    const realQos = (first >> 1) & 3;
    const retain = (first & 1) !== 0;
    const r = new Reader(body);
    let topic = r.str();
    const id = realQos > 0 ? r.u16() : 0;
    let props: Uint8Array = this.version === 5 ? r.props().slice() : EMPTY;
    const payload = r.bytes(r.left()).slice();
    if (props.length) {
      // A topic alias names this topic for later publishes; an empty topic uses it.
      let alias = -1;
      eachProp(props, (pid, _s, v) => {
        if (pid === PROP.topicAlias) alias = (props[v]! << 8) | props[v + 1]!;
      });
      if (alias >= 0) {
        if (alias === 0 || alias > TOPIC_ALIAS_MAX) return this.disconnect(0x94);
        this.aliases ??= new Map();
        if (topic) this.aliases.set(alias, topic);
        else topic = this.aliases.get(alias) ?? "";
        if (!topic) return this.disconnect(0x82);
        props = withoutProps(props, [PROP.topicAlias]);
      }
    }
    if (!topic || topic.includes("+") || topic.includes("#")) return this.drop();
    const ok = await this.publish(topic, payload, qos, retain, props);
    if (!ok) {
      if (this.version === 5 && realQos > 0) this.write(packet(0x40, [id >> 8, id & 0xff, 0x87, 0]));
      else return this.drop();
      return;
    }
    if (realQos === 1) this.write(packet(0x40, [id >> 8, id & 0xff]));
    else if (realQos === 2) this.write(packet(0x50, [id >> 8, id & 0xff]));
  }

  /** v5 DISCONNECT with a reason code, then close. */
  private disconnect(reason: number) {
    if (this.version === 5) this.write(packet(0xe0, [reason, 0]));
    this.drop();
  }

  /** Publish to amq.topic and keep a retained copy. Returns false when refused. */
  private async publish(topic: string, payload: Uint8Array, qos: number, retain: boolean, props: Uint8Array): Promise<boolean> {
    const key = topicToKey(topic);
    if (!this.broker.can(this.user, this.vhost, "write", TOPIC_EXCHANGE)) return false;
    if (!this.broker.topicWriteAllowed(this.user, this.vhost, TOPIC_EXCHANGE, key)) return false;
    const expiry = props.length ? propU32(props, PROP.messageExpiry) : null;
    if (retain) {
      const map = retainedFor(this.broker, this.vhost);
      if (payload.length === 0) {
        map.delete(topic);
        this.broker.store.deleteParameter("mqtt-retained", this.vhost, topic);
      } else {
        const expiresAt = expiry != null ? Date.now() + expiry * 1000 : null;
        map.set(topic, { payload, qos, props, expiresAt });
        const value = JSON.stringify({ p: Buffer.from(payload).toString("base64"), q: qos, r: Buffer.from(props).toString("base64"), e: expiresAt });
        this.broker.store.putParameter("mqtt-retained", this.vhost, topic, value);
      }
      if (payload.length === 0) return true;
    }
    const headers: Array<[string, Field]> = [["x-mqtt-publish-qos", { t: "I", v: qos }]];
    if (props.length) headers.push(["x-mqtt-props", { t: "x", v: props }]);
    if (retain) headers.push(["x-mqtt-retain", { t: "t", v: true }]);
    if (this.noLocal) headers.push(["x-mqtt-publisher", { t: "S", v: this.clientId }]);
    // The message expiry interval is the AMQP expiration, so queues drop it in time.
    const expiration = expiry != null ? String(expiry * 1000) : "";
    const w = new W();
    // headers (bit 13), delivery-mode (bit 12), expiration (bit 8)
    w.u16(0x2000 | 0x1000 | (expiration ? 0x0100 : 0));
    writeTable(w, headers);
    w.u8(qos === 1 ? 2 : 1);
    if (expiration) w.shortstr(expiration);
    try {
      const result = await this.broker.publish({
        vhost: this.vhost,
        exchange: TOPIC_EXCHANGE,
        routingKey: key,
        body: payload,
        headers,
        propRaw: w.concat(),
        persistent: qos === 1,
        priority: 0,
        expiration,
        confirm: qos === 1,
      });
      return result !== "nack";
    } catch (err) {
      if (err instanceof ChanError) return false;
      throw err;
    }
  }

  private async onSubscribe(body: Uint8Array) {
    const r = new Reader(body);
    const id = r.u16();
    let subId = 0;
    if (this.version === 5) {
      const props = r.props();
      eachProp(props, (pid, _s, v) => {
        if (pid !== PROP.subscriptionId) return;
        const vr = new Reader(props);
        vr.at = v;
        subId = vr.varint();
      });
    }
    const codes: number[] = [];
    const sendRetained: Sub[] = [];
    while (r.left() > 0) {
      const filter = r.str();
      const options = r.u8();
      const qos = Math.min(1, options & 3);
      const noLocal = this.version === 5 && (options & 0x04) !== 0;
      const rap = this.version === 5 && (options & 0x08) !== 0;
      const handling = this.version === 5 ? (options >> 4) & 3 : 0;
      const key = filterToKey(filter);
      if (filter.startsWith("$share/")) {
        // RabbitMQ 4.3 does not support shared subscriptions either.
        codes.push(this.version === 5 ? 0x9e : 0x80);
        continue;
      }
      const allowed =
        this.broker.can(this.user, this.vhost, "read", TOPIC_EXCHANGE) &&
        this.broker.can(this.user, this.vhost, "configure", this.queue) &&
        this.broker.topicReadAllowed(this.user, this.vhost, TOPIC_EXCHANGE, key);
      if (!allowed) {
        codes.push(this.version === 5 ? 0x87 : 0x80);
        continue;
      }
      try {
        await this.ensureQueue();
        const existed = this.subs.some((s) => s.filter === filter);
        const sub: Sub = { filter, qos, key, noLocal, rap, id: subId };
        // One binding per filter: its arguments carry the current options.
        if (existed) await this.broker.unbind(this.vhost, TOPIC_EXCHANGE, this.queue, key, null).catch(() => {});
        await this.broker.bind(this.vhost, TOPIC_EXCHANGE, this.queue, key, subArgs(sub));
        this.subs = this.subs.filter((s) => s.filter !== filter);
        this.subs.push(sub);
        // Retain handling: 0 always sends retained messages, 1 only for a new subscription, 2 never.
        if (handling === 0 || (handling === 1 && !existed)) sendRetained.push(sub);
        codes.push(qos);
      } catch {
        codes.push(0x80);
      }
    }
    this.noLocal = this.subs.some((s) => s.noLocal);
    this.write(packet(0x90, this.version === 5 ? [id >> 8, id & 0xff, 0, ...codes] : [id >> 8, id & 0xff, ...codes]));
    await this.startConsuming();
    // Retained messages that match a new filter, flagged as retained.
    const map = retainedFor(this.broker, this.vhost);
    const now = Date.now();
    for (const sub of sendRetained) {
      for (const [topic, msg] of map) {
        if (!mqttMatch(sub.filter, topic)) continue;
        if (msg.expiresAt != null && msg.expiresAt <= now) {
          map.delete(topic);
          this.broker.store.deleteParameter("mqtt-retained", this.vhost, topic);
          continue;
        }
        let props = msg.props;
        if (this.version === 5) {
          if (msg.expiresAt != null) props = join([withoutProps(props, [PROP.messageExpiry]), u32Prop(PROP.messageExpiry, Math.max(1, Math.ceil((msg.expiresAt - now) / 1000)))]);
          if (sub.id) props = join([props, [PROP.subscriptionId, ...varint(sub.id)]]);
        }
        // A retained QoS 1 copy is sent at QoS 0 here: it is not on the queue to ack.
        this.write(this.publishPacket(topic, msg.payload, 0, true, false, 0, props));
      }
    }
  }

  private async onUnsubscribe(body: Uint8Array) {
    const r = new Reader(body);
    const id = r.u16();
    if (this.version === 5) r.props();
    const codes: number[] = [];
    while (r.left() > 0) {
      const filter = r.str();
      const sub = this.subs.find((s) => s.filter === filter);
      if (sub) {
        await this.broker.unbind(this.vhost, TOPIC_EXCHANGE, this.queue, sub.key, null).catch(() => {});
        this.subs = this.subs.filter((s) => s !== sub);
        codes.push(0);
      } else codes.push(0x11);
    }
    this.noLocal = this.subs.some((s) => s.noLocal);
    this.write(packet(0xb0, this.version === 5 ? [id >> 8, id & 0xff, 0, ...codes] : [id >> 8, id & 0xff]));
  }

  /** Close this session because another connection took its client id. */
  async kick() {
    this.will = null;
    await this.teardown(false);
  }

  private drop() {
    void this.teardown(true);
  }

  private async teardown(abnormal: boolean) {
    if (this.closed) return;
    this.closed = true;
    if (this.timer) clearInterval(this.timer);
    const key = `${this.vhost}\0${this.clientId}`;
    const current = live.get(key) === this;
    if (current) live.delete(key);
    try {
      this.end();
    } catch {
      /* transport already gone */
    }
    if (!this.connected) return;
    if (this.consumerTag) await this.broker.cancel(this.vhost, this.queue, this.consumerTag).catch(() => {});
    // Unacked QoS 1 deliveries go back to the queue for the next session.
    for (const msgId of this.inflight.values()) {
      await Promise.resolve(this.broker.nack(this.vhost, this.queue, msgId, true)).catch(() => {});
    }
    this.inflight.clear();
    if (!this.persistent) await this.broker.deleteQueue(this.vhost, this.queue).catch(() => 0);
    // A v5 session expires its interval after the disconnect, unless the
    // client comes back first. 0xFFFFFFFF never expires.
    const entry: { expire: Timer | null; will: Timer | null } = { expire: null, will: null };
    if (current && this.persistent && this.version === 5 && this.sessionExpiry !== 0xffffffff) {
      const vhost = this.vhost;
      const queue = this.queue;
      entry.expire = setTimeout(() => {
        if (pending.get(key) === entry) pending.delete(key);
        if (!live.has(key)) void this.broker.deleteQueue(vhost, queue).catch(() => 0);
      }, Math.min(this.sessionExpiry * 1000, 2 ** 31 - 1));
      entry.expire.unref?.();
    }
    if (abnormal && this.will) {
      const w = this.will;
      this.will = null;
      // Will delay: the will waits, but never past the end of the session.
      const sessionLeft = this.persistent ? (this.sessionExpiry === 0xffffffff || this.version !== 5 ? Infinity : this.sessionExpiry) : 0;
      const delay = Math.min(w.delay, sessionLeft);
      if (delay > 0 && current) {
        entry.will = setTimeout(() => {
          entry.will = null;
          if (pending.get(key) === entry && !entry.expire) pending.delete(key);
          void this.publish(w.topic, w.payload, w.qos, w.retain, w.props).catch(() => false);
        }, Math.min(delay * 1000, 2 ** 31 - 1));
        entry.will.unref?.();
      } else await this.publish(w.topic, w.payload, w.qos, w.retain, w.props).catch(() => false);
    }
    if (entry.expire || entry.will) pending.set(key, entry);
  }
}

/**
 * Listen for MQTT on `host`:`port`.
 *
 * @param host Bind address passed to Bun.listen.
 * @param port TCP port from config.
 * @param broker Broker that routes and stores the messages.
 */
export function startMqtt(host: string, port: number, broker: Broker, tls: { cert: string; key: string; ca?: string } | null = null) {
  Bun.listen<MqttSession | undefined>({
    hostname: host,
    port,
    // With a CA, a client certificate is asked for but optional, as RabbitMQ's
    // verify_peer with fail_if_no_peer_cert=false.
    ...(tls
      ? {
          tls: {
            cert: Bun.file(tls.cert),
            key: Bun.file(tls.key),
            ...(tls.ca ? { ca: Bun.file(tls.ca), requestCert: true, rejectUnauthorized: false } : {}),
          },
        }
      : {}),
    socket: {
      handshake(socket, _success, _error) {
        if (!tls?.ca || !socket.data) return;
        const s = socket as unknown as { authorized?: boolean; getPeerCertificate?: () => { subject?: { CN?: string } } | null };
        const cn = s.getPeerCertificate?.()?.subject?.CN;
        if (cn && s.authorized) socket.data.peerCN = cn;
      },
      open(socket) {
        socket.data = new MqttSession(
          broker,
          (frame) => {
            try {
              socket.write(frame);
            } catch {
              /* closed */
            }
          },
          () => socket.end(),
        );
      },
      data(socket, data) {
        socket.data?.feed(new Uint8Array(data));
      },
      close(socket) {
        socket.data?.closedByPeer();
      },
      error(socket) {
        socket.data?.closedByPeer();
      },
    },
  });
}

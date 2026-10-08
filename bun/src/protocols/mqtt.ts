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

type Retained = { payload: Uint8Array; qos: number; props: Uint8Array };
type Sub = { filter: string; qos: number; key: string };

/** Live sessions by vhost and client id, for takeover. */
const live = new Map<string, MqttSession>();
/** Retained messages by vhost and topic. Loaded from the store on first use. */
const retained = new Map<string, Map<string, Retained>>();
let ownerSeq = 0;

function retainedFor(broker: Broker, vhost: string): Map<string, Retained> {
  let map = retained.get(vhost);
  if (map) return map;
  map = new Map();
  for (const row of broker.store.listParameters("mqtt-retained")) {
    if (row.vhost !== vhost) continue;
    try {
      const v = JSON.parse(row.value) as { p: string; q: number; r?: string };
      map.set(row.name, { payload: Buffer.from(v.p, "base64"), qos: v.q, props: Buffer.from(v.r ?? "", "base64") });
    } catch {
      /* a corrupt row is skipped */
    }
  }
  retained.set(vhost, map);
  return map;
}

/** One MQTT client connection. */
export class MqttSession {
  private buf: Uint8Array = new Uint8Array(0);
  private version = 4;
  private vhost = "/";
  private user = "";
  private clientId = "";
  private clean = true;
  private queue = "";
  private subs: Sub[] = [];
  private will: { topic: string; payload: Uint8Array; qos: number; retain: boolean; props: Uint8Array } | null = null;
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
        return this.teardown(false);
      default:
        return this.drop();
    }
  }

  private connack(code: number, present = false) {
    if (this.version === 5) this.write(packet(0x20, [present ? 1 : 0, code, 0]));
    else this.write(packet(0x20, [present ? 1 : 0, code]));
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
    if (this.version === 5) r.props();
    this.clientId = r.str();
    this.clean = (flags & 0x02) !== 0;
    if (flags & 0x04) {
      const props = this.version === 5 ? r.props() : new Uint8Array(0);
      const topic = r.str();
      const payload = r.binary().slice();
      this.will = { topic, payload, qos: Math.min(1, (flags >> 3) & 3), retain: (flags & 0x20) !== 0, props };
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
    if (!user || !(await this.broker.verify(user, password))) {
      this.connack(badLogin);
      return this.drop();
    }
    if (!this.broker.hasVhostAccess(user, this.vhost)) {
      this.connack(notAuthorized);
      return this.drop();
    }
    if (!this.clientId) {
      if (!this.clean && this.version !== 5) {
        this.connack(2);
        return this.drop();
      }
      this.clientId = `mqtt-${crypto.randomUUID()}`;
    }
    this.user = user;
    // A second connection with the same client id takes the session over.
    const key = `${this.vhost}\0${this.clientId}`;
    const previous = live.get(key);
    if (previous && previous !== this) await previous.kick();
    live.set(key, this);
    this.queue = `mqtt-subscription-${this.clientId}qos1`;
    const exists = this.broker.queues.has(this.broker.key(this.vhost, this.queue));
    if (this.clean && exists) await this.broker.deleteQueue(this.vhost, this.queue).catch(() => 0);
    const present = !this.clean && exists;
    if (present) this.subs = this.restoreSubs();
    this.connected = true;
    this.connack(0, present);
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
      out.push({ filter, qos: 1, key: b.routingKey });
    }
    return out;
  }

  private async ensureQueue() {
    if (this.broker.queues.has(this.broker.key(this.vhost, this.queue))) return;
    await this.broker.declareQueue({
      vhost: this.vhost,
      name: this.queue,
      durable: !this.clean,
      exclusive: this.clean,
      autoDelete: false,
      passive: false,
      args: {},
      owner: this.clean ? -(++ownerSeq) : undefined,
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
    let subQos = -1;
    for (const s of this.subs) if (mqttMatch(s.filter, topic)) subQos = Math.max(subQos, s.qos);
    const header = msg.headers.find(([k]) => k === "x-mqtt-publish-qos")?.[1];
    const msgQos = header && "v" in header ? Number(header.v) : msg.persistent ? 1 : 0;
    const qos = Math.max(0, Math.min(subQos < 0 ? 0 : subQos, msgQos));
    const props = msg.headers.find(([k]) => k === "x-mqtt-props")?.[1];
    const raw = props?.t === "x" ? props.v : new Uint8Array(0);
    let id = 0;
    if (qos === 1) {
      id = this.packetId();
      this.inflight.set(id, msg.id);
    }
    this.write(this.publishPacket(topic, msg.body, qos, false, msg.redelivered && qos === 1, id, raw));
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
    const topic = r.str();
    const id = realQos > 0 ? r.u16() : 0;
    const props = this.version === 5 ? r.props().slice() : new Uint8Array(0);
    const payload = r.bytes(r.left()).slice();
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

  /** Publish to amq.topic and keep a retained copy. Returns false when refused. */
  private async publish(topic: string, payload: Uint8Array, qos: number, retain: boolean, props: Uint8Array): Promise<boolean> {
    const key = topicToKey(topic);
    if (!this.broker.can(this.user, this.vhost, "write", TOPIC_EXCHANGE)) return false;
    if (!this.broker.topicWriteAllowed(this.user, this.vhost, TOPIC_EXCHANGE, key)) return false;
    if (retain) {
      const map = retainedFor(this.broker, this.vhost);
      if (payload.length === 0) {
        map.delete(topic);
        this.broker.store.deleteParameter("mqtt-retained", this.vhost, topic);
      } else {
        map.set(topic, { payload, qos, props });
        const value = JSON.stringify({ p: Buffer.from(payload).toString("base64"), q: qos, r: Buffer.from(props).toString("base64") });
        this.broker.store.putParameter("mqtt-retained", this.vhost, topic, value);
      }
      if (payload.length === 0) return true;
    }
    const headers: Array<[string, Field]> = [["x-mqtt-publish-qos", { t: "I", v: qos }]];
    if (props.length) headers.push(["x-mqtt-props", { t: "x", v: props }]);
    const w = new W();
    // headers (bit 13) and delivery-mode (bit 12)
    w.u16(0x2000 | 0x1000);
    writeTable(w, headers);
    w.u8(qos === 1 ? 2 : 1);
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
        expiration: "",
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
    if (this.version === 5) r.props();
    const codes: number[] = [];
    const added: Sub[] = [];
    while (r.left() > 0) {
      const filter = r.str();
      const options = r.u8();
      const qos = Math.min(1, options & 3);
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
        await this.broker.bind(this.vhost, TOPIC_EXCHANGE, this.queue, key, []);
        this.subs = this.subs.filter((s) => s.filter !== filter);
        const sub = { filter, qos, key };
        this.subs.push(sub);
        added.push(sub);
        codes.push(qos);
      } catch {
        codes.push(0x80);
      }
    }
    this.write(packet(0x90, this.version === 5 ? [id >> 8, id & 0xff, 0, ...codes] : [id >> 8, id & 0xff, ...codes]));
    await this.startConsuming();
    // Retained messages that match a new filter, flagged as retained.
    const map = retainedFor(this.broker, this.vhost);
    for (const sub of added) {
      for (const [topic, msg] of map) {
        if (!mqttMatch(sub.filter, topic)) continue;
        const qos = Math.min(sub.qos, msg.qos);
        // A retained QoS 1 copy is sent at QoS 0 here: it is not on the queue to ack.
        this.write(this.publishPacket(topic, msg.payload, 0, true, false, 0, msg.props));
        void qos;
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
    if (live.get(key) === this) live.delete(key);
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
    if (this.clean) await this.broker.deleteQueue(this.vhost, this.queue).catch(() => 0);
    if (abnormal && this.will) {
      const w = this.will;
      this.will = null;
      await this.publish(w.topic, w.payload, w.qos, w.retain, w.props).catch(() => false);
    }
  }
}

/**
 * Listen for MQTT on `host`:`port`.
 *
 * @param host Bind address passed to Bun.listen.
 * @param port TCP port from config.
 * @param broker Broker that routes and stores the messages.
 */
export function startMqtt(host: string, port: number, broker: Broker) {
  Bun.listen<MqttSession | undefined>({
    hostname: host,
    port,
    socket: {
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

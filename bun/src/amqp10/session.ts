/**
 * One AMQP 1.0 connection on the AMQP port, as RabbitMQ 4 serves it.
 *
 * SASL PLAIN logs in. The open hostname `vhost:<name>` picks the vhost.
 * Links use v2 addresses: a client sender targets `/exchanges/:x/:key`,
 * `/exchanges/:x` (the subject is the key), `/queues/:q`, or a null target
 * with the address in each message's `to`. A client receiver reads from
 * `/queues/:q` and gets at most its link credit, with drain honoured.
 * Dispositions map to the broker's ack (accepted), requeue (released,
 * modified) and dead-letter (rejected, modified undeliverable-here).
 * Messages are mapped to the broker's 0-9-1 form by {@link inbound} and
 * back by {@link outbound}, so 1.0 and 0-9-1 clients share queues.
 *
 * {@link Amqp10Session} does not own a socket; the 0-9-1 listener hands it
 * the bytes after it sees a 1.0 protocol header.
 */
import type { Broker, LiveMsg } from "../broker/index.ts";
import { ChanError } from "../broker/index.ts";
import { inbound, outbound, writeProps } from "./map.ts";
import { AMap, array, Decoder, described, Described, encode, field, sym, Sym, ubyte, uint, ushort } from "./types.ts";

const HEADER_AMQP = Uint8Array.of(65, 77, 81, 80, 0, 1, 0, 0);
const HEADER_SASL = Uint8Array.of(65, 77, 81, 80, 3, 1, 0, 0);
/** The largest frame this broker accepts, and sends unless the peer wants smaller. */
const MAX_FRAME = 128 * 1024;
/** Credit granted to a client sender, topped up when half is used. */
const LINK_CREDIT = 256;
const WINDOW = 0x7fffffff;

const P = { open: 0x10, begin: 0x11, attach: 0x12, flow: 0x13, transfer: 0x14, disposition: 0x15, detach: 0x16, end: 0x17, close: 0x18 };
const SASL = { mechanisms: 0x40, init: 0x41, outcome: 0x44 };
const OUTCOME = { accepted: 0x24, rejected: 0x25, released: 0x26, modified: 0x27 };

type Target = { exchange: string; key: string | null } | null;

type InLink = {
  dir: "in";
  handle: number;
  name: string;
  /** null for a link whose messages carry their own `to`. */
  target: Target;
  credit: number;
  deliveryCount: number;
  /** Frames of a transfer whose last frame has not arrived. */
  partial: { chunks: Uint8Array[]; id: number; settled: boolean } | null;
};

type OutLink = {
  dir: "out";
  handle: number;
  name: string;
  queue: string;
  tag: string;
  session: number;
  presettled: boolean;
  credit: number;
  deliveryCount: number;
  drain: boolean;
  detached: boolean;
};

type Link = InLink | OutLink;

type Unsettled = { link: OutLink; msgId: string };

type Session = {
  channel: number;
  links: Map<number, Link>;
  nextOutgoingId: number;
  nextIncomingId: number;
  /** The peer's window: we may send transfers while nextOutgoingId is below this. */
  remoteLimit: number;
  nextDeliveryId: number;
  unsettled: Map<number, Unsettled>;
  /** Transfer frames held while the peer's incoming window is closed. */
  held: Uint8Array[];
};

const dec = new TextDecoder();

class Refuse extends Error {
  constructor(
    readonly condition: string,
    message: string,
  ) {
    super(message);
  }
}

function errorValue(condition: string, description: string) {
  return described(0x1d, [sym(condition), description]);
}

/** One frame: size, data offset 2, type, channel, then the body. */
function frame(type: number, channel: number, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(8 + body.length);
  const view = new DataView(out.buffer);
  view.setUint32(0, out.length);
  out[4] = 2;
  out[5] = type;
  view.setUint16(6, channel);
  out.set(body, 8);
  return out;
}

function performative(code: number, fields: unknown[]): Uint8Array {
  while (fields.length && (fields[fields.length - 1] === null || fields[fields.length - 1] === undefined)) fields.pop();
  return encode(described(code, fields));
}

/** Percent-decode one address segment. A bad escape keeps the raw text. */
function segment(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

/**
 * Parse a v2 address, plus the v1 forms RabbitMQ 3.x clients still send.
 *
 * @returns exchange and key, where a null key means "use the subject", or
 * `queue` for a /queues address.
 */
export function parseAddress(address: string): { exchange: string; key: string | null } | { queue: string } | null {
  const parts = address.split("/");
  if (parts[0] !== "") return null;
  if (parts[1] === "queues" && parts.length === 3 && parts[2]) return { queue: segment(parts[2]) };
  if (parts[1] === "exchanges" && (parts.length === 3 || parts.length === 4) && parts[2] !== undefined) {
    const exchange = segment(parts[2]);
    return { exchange: exchange === "amq.default" ? "" : exchange, key: parts.length === 4 ? segment(parts[3]!) : null };
  }
  if (parts[1] === "queue" && parts.length === 3) return { queue: segment(parts[2]!) };
  if (parts[1] === "amq" && parts[2] === "queue" && parts.length === 4) return { queue: segment(parts[3]!) };
  if (parts[1] === "exchange" && parts.length >= 3) return { exchange: segment(parts[2]!), key: parts.length >= 4 ? segment(parts.slice(3).join("/")) : "" };
  if (parts[1] === "topic" && parts.length >= 3) return { exchange: "amq.topic", key: segment(parts.slice(2).join("/")) };
  return null;
}

export class Amqp10Session {
  private buf: Uint8Array = new Uint8Array(0);
  private stage: "header" | "sasl" | "header2" | "open" | "closed" = "header";
  private user = "";
  private vhost = "/";
  private opened = false;
  private remoteMaxFrame = MAX_FRAME;
  private sessions = new Map<number, Session>();
  private chain: Promise<unknown> = Promise.resolve();
  private heartbeat: Timer | null = null;
  private static seq = 0;
  private readonly id = ++Amqp10Session.seq;

  constructor(
    private readonly broker: Broker,
    private readonly write: (bytes: Uint8Array) => void,
    private readonly end: () => void,
  ) {}

  /** Bytes from the transport, starting with the protocol header. */
  feed(data: Uint8Array): void {
    this.chain = this.chain
      .then(() => this.drain(data))
      .catch((err) => this.fail("amqp:internal-error", String((err as Error)?.message ?? err)));
  }

  /** The transport closed: requeue unsettled deliveries and cancel consumers. */
  closed(): void {
    if (this.stage === "closed") return;
    this.stage = "closed";
    this.chain = this.chain.then(() => this.teardown());
  }

  private async drain(data: Uint8Array) {
    if (this.stage === "closed") return;
    if (this.buf.length) {
      const joined = new Uint8Array(this.buf.length + data.length);
      joined.set(this.buf);
      joined.set(data, this.buf.length);
      data = joined;
    }
    let at = 0;
    while ((this.stage as string) !== "closed") {
      if (this.stage === "header" || this.stage === "header2") {
        if (data.length - at < 8) break;
        const h = data.subarray(at, at + 8);
        at += 8;
        if (!this.header(h)) return;
        continue;
      }
      if (data.length - at < 8) break;
      const size = new DataView(data.buffer, data.byteOffset + at, 4).getUint32(0);
      if (size < 8 || size > MAX_FRAME) return this.fail("amqp:connection:framing-error", `frame size ${size}`);
      if (data.length - at < size) break;
      const doff = data[at + 4]! * 4;
      const type = data[at + 5]!;
      const channel = (data[at + 6]! << 8) | data[at + 7]!;
      const body = data.subarray(at + doff, at + size);
      at += size;
      if (body.length === 0) continue;
      await this.onFrame(type, channel, body);
    }
    this.buf = at < data.length ? data.slice(at) : new Uint8Array(0);
  }

  /** Answer a protocol header. Returns false when the connection is done. */
  private header(h: Uint8Array): boolean {
    const isSasl = h.every((b, i) => b === HEADER_SASL[i]);
    const isAmqp = h.every((b, i) => b === HEADER_AMQP[i]);
    if (this.stage === "header" && isSasl) {
      this.write(HEADER_SASL);
      this.send(1, 0, performative(SASL.mechanisms, [array([sym("PLAIN")])]));
      this.stage = "sasl";
      return true;
    }
    if (this.stage === "header2" && isAmqp) {
      this.write(HEADER_AMQP);
      this.stage = "open";
      return true;
    }
    // Plain AMQP without SASL has no identity; offer SASL and stop, as RabbitMQ does.
    this.write(HEADER_SASL);
    this.stop();
    return false;
  }

  private send(type: number, channel: number, body: Uint8Array) {
    if (this.stage === "closed") return;
    this.write(frame(type, channel, body));
  }

  private async onFrame(type: number, channel: number, body: Uint8Array) {
    const d = new Decoder(body);
    const perf = d.value();
    if (!(perf instanceof Described)) return this.fail("amqp:decode-error", "frame body is not a performative");
    const code = typeof perf.descriptor === "number" ? perf.descriptor : -1;
    const f = Array.isArray(perf.value) ? perf.value : [];
    if (this.stage === "sasl") {
      if (type !== 1 || code !== SASL.init) return this.fail("amqp:not-allowed", "expected sasl-init");
      return this.onSaslInit(f);
    }
    if (type !== 0) return this.fail("amqp:not-allowed", "unexpected SASL frame");
    if (!this.opened && code !== P.open) return this.fail("amqp:not-allowed", "expected open");
    switch (code) {
      case P.open:
        return this.onOpen(f);
      case P.begin:
        return this.onBegin(channel, f);
      case P.attach:
        return this.onAttach(channel, f);
      case P.flow:
        return this.onFlow(channel, f);
      case P.transfer:
        return this.onTransfer(channel, f, body.subarray(d.at));
      case P.disposition:
        return this.onDisposition(channel, f);
      case P.detach:
        return this.onDetach(channel, f);
      case P.end:
        return this.onEnd(channel);
      case P.close:
        return this.onClose();
      default:
        return this.fail("amqp:not-implemented", `performative 0x${code.toString(16)}`);
    }
  }

  private async onSaslInit(f: unknown[]) {
    const mechanism = field(f, 0);
    const response = field(f, 1);
    let ok = false;
    if (mechanism instanceof Sym && mechanism.s === "PLAIN" && response instanceof Uint8Array) {
      const [, user = "", pass = ""] = dec.decode(response).split("\0");
      ok = !!user && (await this.broker.verify(user, pass));
      if (ok) this.user = user;
    }
    this.send(1, 0, performative(SASL.outcome, [ubyte(ok ? 0 : 1)]));
    if (!ok) return this.stop();
    this.stage = "header2";
  }

  private onOpen(f: unknown[]) {
    const hostname = field(f, 1);
    const maxFrame = field(f, 2);
    const idle = field(f, 4);
    if (typeof maxFrame === "number" && maxFrame >= 512) this.remoteMaxFrame = Math.min(maxFrame, MAX_FRAME);
    if (typeof hostname === "string" && hostname.startsWith("vhost:")) this.vhost = hostname.slice(6);
    this.opened = true;
    this.send(
      0,
      0,
      performative(P.open, [
        `queueforge-${this.id}`,
        null,
        uint(MAX_FRAME),
        ushort(65535),
        null,
        null,
        null,
        null,
        null,
        new AMap([
          [sym("product"), "QueueForge"],
          [sym("platform"), "Bun"],
        ]),
      ]),
    );
    if (!this.broker.hasVhostAccess(this.user, this.vhost)) {
      return this.fail("amqp:not-allowed", `access to vhost '${this.vhost}' refused for user '${this.user}'`);
    }
    // Empty frames at half the peer's idle timeout keep the connection alive.
    if (typeof idle === "number" && idle > 0) {
      this.heartbeat = setInterval(() => this.write(frame(0, 0, new Uint8Array(0))), Math.max(500, idle / 2));
    }
  }

  private onBegin(channel: number, f: unknown[]) {
    const nextOutgoing = field(f, 1);
    const incomingWindow = field(f, 2);
    const s: Session = {
      channel,
      links: new Map(),
      nextOutgoingId: 0,
      nextIncomingId: typeof nextOutgoing === "number" ? nextOutgoing : 0,
      remoteLimit: typeof incomingWindow === "number" ? incomingWindow : WINDOW,
      nextDeliveryId: 0,
      unsettled: new Map(),
      held: [],
    };
    this.sessions.set(channel, s);
    this.send(0, channel, performative(P.begin, [ushort(channel), uint(0), uint(WINDOW), uint(WINDOW), uint(255)]));
  }

  private session(channel: number): Session {
    const s = this.sessions.get(channel);
    if (!s) throw new Refuse("amqp:not-found", `no session on channel ${channel}`);
    return s;
  }

  private async onAttach(channel: number, f: unknown[]) {
    const s = this.session(channel);
    const name = String(field(f, 0) ?? "");
    const handle = Number(field(f, 1) ?? 0);
    const clientIsReceiver = field(f, 2) === true;
    const sndSettle = Number(field(f, 3) ?? 2);
    const rcvSettle = Number(field(f, 4) ?? 0);
    const source = field(f, 5);
    const target = field(f, 6);
    const reply = (src: unknown, tgt: unknown, extra: unknown[] = []) =>
      this.send(0, channel, performative(P.attach, [name, uint(handle), !clientIsReceiver, ubyte(sndSettle), ubyte(rcvSettle), src, tgt, null, null, ...extra]));
    try {
      if (clientIsReceiver) {
        const address = source instanceof Described ? field(source.value, 0) : null;
        const parsed = typeof address === "string" ? parseAddress(address) : null;
        if (!parsed || !("queue" in parsed)) throw new Refuse("amqp:invalid-field", `source address '${String(address)}' is not a queue`);
        const q = this.broker.queues.get(this.broker.key(this.vhost, parsed.queue));
        if (!q) throw new Refuse("amqp:not-found", `no queue '${parsed.queue}' in vhost '${this.vhost}'`);
        if (!this.broker.can(this.user, this.vhost, "read", parsed.queue)) {
          throw new Refuse("amqp:unauthorized-access", `read access to queue '${parsed.queue}' refused for user '${this.user}'`);
        }
        const link: OutLink = {
          dir: "out",
          handle,
          name,
          queue: parsed.queue,
          tag: `amq.ctag-1.0-${this.id}-${channel}-${handle}`,
          session: this.broker.nextSession(),
          presettled: sndSettle === 1,
          credit: 0,
          deliveryCount: 0,
          drain: false,
          detached: false,
        };
        reply(source, target, [uint(0)]);
        s.links.set(handle, link);
        try {
          await this.broker.consume(this.vhost, parsed.queue, {
            tag: link.tag,
            session: link.session,
            noAck: link.presettled,
            exclusive: false,
            want: () => !link.detached && link.credit > 0,
            deliver: (msg: LiveMsg) => this.deliver(s, link, msg),
          } as Parameters<Broker["consume"]>[2]);
        } catch (err) {
          s.links.delete(handle);
          throw err instanceof ChanError ? new Refuse(err.code === 403 ? "amqp:resource-locked" : "amqp:not-found", err.message) : err;
        }
        return;
      }
      let resolved: Target;
      const address = target instanceof Described ? field(target.value, 0) : null;
      if (address === null || address === undefined) resolved = null;
      else {
        const parsed = typeof address === "string" ? parseAddress(address) : null;
        if (!parsed) throw new Refuse("amqp:invalid-field", `target address '${String(address)}' is not valid`);
        resolved = "queue" in parsed ? { exchange: "", key: parsed.queue } : parsed;
        this.checkTarget(resolved, "queue" in parsed ? parsed.queue : null);
      }
      const initial = Number(field(f, 9) ?? 0);
      const link: InLink = { dir: "in", handle, name, target: resolved, credit: LINK_CREDIT, deliveryCount: initial, partial: null };
      s.links.set(handle, link);
      reply(source, target);
      this.linkFlow(s, link);
    } catch (err) {
      if (!(err instanceof Refuse)) throw err;
      // A refused attach is an attach with no terminus, then a detach with the error.
      if (clientIsReceiver) reply(null, target, [uint(0)]);
      else reply(source, null);
      this.send(0, channel, performative(P.detach, [uint(handle), true, errorValue(err.condition, err.message)]));
    }
  }

  /** Refuse a target whose exchange or queue is missing, or that the user cannot write. */
  private checkTarget(t: { exchange: string; key: string | null }, queue: string | null) {
    if (queue !== null && !this.broker.queues.has(this.broker.key(this.vhost, queue))) {
      throw new Refuse("amqp:not-found", `no queue '${queue}' in vhost '${this.vhost}'`);
    }
    if (t.exchange !== "") {
      const ex = this.broker.exchanges.get(this.broker.key(this.vhost, t.exchange));
      if (!ex) throw new Refuse("amqp:not-found", `no exchange '${t.exchange}' in vhost '${this.vhost}'`);
      if (ex.internal) throw new Refuse("amqp:unauthorized-access", `exchange '${t.exchange}' is internal`);
    }
    const resource = t.exchange || "amq.default";
    if (!this.broker.can(this.user, this.vhost, "write", resource)) {
      throw new Refuse("amqp:unauthorized-access", `write access to exchange '${resource}' refused for user '${this.user}'`);
    }
  }

  /** Tell the peer about a link's credit and delivery count, with the session window. */
  private linkFlow(s: Session, link: Link, echo = false) {
    this.send(
      0,
      s.channel,
      performative(P.flow, [
        uint(s.nextIncomingId),
        uint(WINDOW),
        uint(s.nextOutgoingId),
        uint(WINDOW),
        uint(link.handle),
        uint(link.deliveryCount),
        uint(link.credit),
        null,
        link.dir === "out" ? link.drain : null,
        echo || null,
      ]),
    );
  }

  private onFlow(channel: number, f: unknown[]) {
    const s = this.session(channel);
    const nextIncoming = field(f, 0);
    const window = Number(field(f, 1) ?? WINDOW);
    s.remoteLimit = (typeof nextIncoming === "number" ? nextIncoming : 0) + window;
    this.release(s);
    const handle = field(f, 4);
    if (typeof handle !== "number") return;
    const link = s.links.get(handle);
    if (!link) return;
    const echo = field(f, 9) === true;
    if (link.dir === "in") {
      if (echo) this.linkFlow(s, link);
      return;
    }
    const count = field(f, 5);
    const credit = Number(field(f, 6) ?? 0);
    // The receiver's credit is relative to the delivery count it has seen.
    const seen = typeof count === "number" ? count : 0;
    link.credit = Math.max(0, seen + credit - link.deliveryCount);
    link.drain = field(f, 8) === true;
    this.pump(link);
    if (link.drain || echo) {
      // Give the queue a turn to fill the credit; whatever is left is drained.
      setTimeout(() => {
        if (link.detached) return;
        if (link.drain && link.credit > 0) {
          link.deliveryCount += link.credit;
          link.credit = 0;
        }
        this.linkFlow(s, link);
      }, 0);
    }
  }

  private pump(link: OutLink) {
    const q = this.broker.queues.get(this.broker.key(this.vhost, link.queue));
    if (q) void Promise.resolve(this.broker.kick(this.vhost, link.queue, link.session)).catch(() => {});
  }

  /** Send one queued message on an outgoing link, split to the peer's frame size. */
  private deliver(s: Session, link: OutLink, msg: LiveMsg) {
    if (link.detached || this.stage === "closed") {
      if (!link.presettled) void Promise.resolve(this.broker.nack(this.vhost, link.queue, msg.id, true)).catch(() => {});
      return;
    }
    link.credit--;
    link.deliveryCount++;
    const id = s.nextDeliveryId++;
    if (!link.presettled) s.unsettled.set(id, { link, msgId: msg.id });
    const payload = outbound(msg);
    const tag = new Uint8Array(4);
    new DataView(tag.buffer).setUint32(0, id);
    const first = performative(P.transfer, [uint(link.handle), uint(id), tag, uint(0), link.presettled, false]);
    // Room left for payload once a continuation performative is in the frame.
    const room = this.remoteMaxFrame - 8 - first.length - 8;
    if (payload.length <= room) {
      return this.transfer(s, concat(first, payload));
    }
    let at = 0;
    let head = true;
    while (at < payload.length) {
      const chunk = payload.subarray(at, at + room);
      at += chunk.length;
      const more = at < payload.length;
      const perf = head
        ? performative(P.transfer, [uint(link.handle), uint(id), tag, uint(0), link.presettled, more])
        : performative(P.transfer, [uint(link.handle), null, null, null, link.presettled, more]);
      head = false;
      this.transfer(s, concat(perf, chunk));
    }
  }

  /** Write one transfer frame, or hold it until the peer's window opens. */
  private transfer(s: Session, body: Uint8Array) {
    const f = frame(0, s.channel, body);
    if (s.held.length || s.nextOutgoingId >= s.remoteLimit) {
      s.held.push(f);
      return;
    }
    s.nextOutgoingId++;
    this.write(f);
  }

  private release(s: Session) {
    while (s.held.length && s.nextOutgoingId < s.remoteLimit) {
      s.nextOutgoingId++;
      this.write(s.held.shift()!);
    }
  }

  private async onTransfer(channel: number, f: unknown[], payload: Uint8Array) {
    const s = this.session(channel);
    s.nextIncomingId++;
    const link = s.links.get(Number(field(f, 0)));
    if (!link || link.dir !== "in") return this.fail("amqp:session:unattached-handle", "transfer on an unknown link");
    const more = field(f, 5) === true;
    if (!link.partial) {
      link.partial = { chunks: [], id: Number(field(f, 1) ?? 0), settled: field(f, 4) === true };
    }
    if (field(f, 9) === true) {
      link.partial = null;
      return;
    }
    link.partial.chunks.push(payload.slice());
    if (more) return;
    const { chunks, id, settled } = link.partial;
    link.partial = null;
    link.credit--;
    link.deliveryCount++;
    if (link.credit < LINK_CREDIT / 2) {
      link.credit = LINK_CREDIT;
      this.linkFlow(s, link);
    }
    const outcome = await this.publish(link, chunks.length === 1 ? chunks[0]! : concat(...chunks));
    if (!settled) this.send(0, channel, performative(P.disposition, [true, uint(id), null, true, outcome]));
  }

  /** Route one inbound message. Returns the outcome to settle it with. */
  private async publish(link: InLink, payload: Uint8Array): Promise<unknown> {
    let msg;
    try {
      msg = inbound(payload);
    } catch (err) {
      return described(OUTCOME.rejected, [errorValue("amqp:decode-error", String((err as Error).message))]);
    }
    let target = link.target;
    if (target === null) {
      const parsed = msg.to ? parseAddress(msg.to) : null;
      if (!parsed) return described(OUTCOME.rejected, [errorValue("amqp:invalid-field", `'to' address '${msg.to ?? ""}' is not valid`)]);
      target = "queue" in parsed ? { exchange: "", key: parsed.queue } : parsed;
      try {
        this.checkTarget(target, null);
      } catch (err) {
        if (err instanceof Refuse) return described(OUTCOME.rejected, [errorValue(err.condition, err.message)]);
        throw err;
      }
    }
    const routingKey = target.key ?? msg.subject ?? "";
    if (!this.broker.topicWriteAllowed(this.user, this.vhost, target.exchange, routingKey)) {
      return described(OUTCOME.rejected, [errorValue("amqp:unauthorized-access", "write access to topic refused")]);
    }
    if (this.broker.blocked) await this.broker.whenUnblocked();
    try {
      const result = await this.broker.publish({
        vhost: this.vhost,
        exchange: target.exchange,
        routingKey,
        body: msg.body,
        headers: msg.props.headers,
        propRaw: writeProps(msg.props),
        persistent: msg.persistent,
        priority: msg.priority,
        expiration: msg.props.expiration ?? "",
        confirm: true,
        mandatory: true,
      });
      if (result === "return") return described(OUTCOME.released, []);
      if (result === "nack") return described(OUTCOME.rejected, [errorValue("amqp:internal-error", "the queue refused the message")]);
      return described(OUTCOME.accepted, []);
    } catch (err) {
      if (err instanceof ChanError) return described(OUTCOME.rejected, [errorValue(err.code === 403 ? "amqp:unauthorized-access" : "amqp:not-found", err.message)]);
      throw err;
    }
  }

  private async onDisposition(channel: number, f: unknown[]) {
    const s = this.session(channel);
    if (field(f, 0) !== true) return;
    const first = Number(field(f, 1) ?? 0);
    const last = Number(field(f, 2) ?? first);
    const settled = field(f, 3) === true;
    const state = field(f, 4);
    const code = state instanceof Described && typeof state.descriptor === "number" ? state.descriptor : -1;
    const touched = new Set<OutLink>();
    for (let id = first; id <= last; id++) {
      const u = s.unsettled.get(id);
      if (!u) continue;
      s.unsettled.delete(id);
      touched.add(u.link);
      const q = u.link.queue;
      if (code === OUTCOME.accepted) await Promise.resolve(this.broker.ack(this.vhost, q, u.msgId)).catch(() => {});
      else if (code === OUTCOME.rejected) await Promise.resolve(this.broker.nack(this.vhost, q, u.msgId, false)).catch(() => {});
      else if (code === OUTCOME.modified && field((state as Described).value, 1) === true) {
        await Promise.resolve(this.broker.nack(this.vhost, q, u.msgId, false)).catch(() => {});
      } else await Promise.resolve(this.broker.nack(this.vhost, q, u.msgId, true)).catch(() => {});
    }
    if (!settled) this.send(0, channel, performative(P.disposition, [false, uint(first), uint(last), true, state ?? null]));
    for (const link of touched) this.pump(link);
  }

  private async onDetach(channel: number, f: unknown[]) {
    const s = this.session(channel);
    const handle = Number(field(f, 0) ?? 0);
    const link = s.links.get(handle);
    if (link) await this.dropLink(s, link);
    this.send(0, channel, performative(P.detach, [uint(handle), true]));
  }

  /** Cancel a link's consumer and requeue what it holds unsettled. */
  private async dropLink(s: Session, link: Link) {
    s.links.delete(link.handle);
    if (link.dir !== "out") return;
    link.detached = true;
    await this.broker.cancel(this.vhost, link.queue, link.tag).catch(() => {});
    for (const [id, u] of [...s.unsettled]) {
      if (u.link !== link) continue;
      s.unsettled.delete(id);
      await Promise.resolve(this.broker.nack(this.vhost, link.queue, u.msgId, true)).catch(() => {});
    }
  }

  private async onEnd(channel: number) {
    const s = this.sessions.get(channel);
    if (s) {
      for (const link of [...s.links.values()]) await this.dropLink(s, link);
      this.sessions.delete(channel);
    }
    this.send(0, channel, performative(P.end, []));
  }

  private async onClose() {
    await this.teardown();
    this.send(0, 0, performative(P.close, []));
    this.stop();
  }

  private async teardown() {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = null;
    for (const s of this.sessions.values()) for (const link of [...s.links.values()]) await this.dropLink(s, link);
    this.sessions.clear();
  }

  /** Close the connection with an error, as RabbitMQ does for a protocol fault. */
  private async fail(condition: string, description: string) {
    if (this.stage === "closed") return;
    if (this.stage === "open" && this.opened) {
      this.send(0, 0, performative(P.close, [errorValue(condition, description)]));
    }
    await this.teardown();
    this.stop();
  }

  private stop() {
    this.stage = "closed";
    try {
      this.end();
    } catch {
      /* the transport is already gone */
    }
  }
}

function concat(...parts: Uint8Array[]): Uint8Array {
  let n = 0;
  for (const p of parts) n += p.length;
  const out = new Uint8Array(n);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

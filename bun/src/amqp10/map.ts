/**
 * AMQP 1.0 messages to and from the broker's 0-9-1 form.
 *
 * The mapping follows RabbitMQ 4: a single data section is the 0-9-1 body;
 * properties map field for field; application-properties become headers
 * and headers come back as application-properties, except `x-` headers,
 * which travel as message annotations. A body that is not one data section
 * (amqp-value, amqp-sequence, several data sections) is kept as its encoded
 * sections and marked with {@link SECTIONS_TYPE}, so a 1.0 consumer gets it
 * back unchanged.
 */
import { R, readTable, W, writeTable, type Field } from "../codec.ts";
import type { LiveMsg } from "../broker/index.ts";
import { AMap, Decoder, described, Described, Encoder, field, long, Sym, sym, Timestamp, ubyte, uint, Uuid } from "./types.ts";

/** content-type of a 0-9-1 body that holds encoded AMQP 1.0 body sections. */
export const SECTIONS_TYPE = "message/vnd.rabbitmq.amqp";

/** Every 0-9-1 basic property, decoded. */
export type BasicProps = {
  contentType?: string;
  contentEncoding?: string;
  headers: Array<[string, Field]>;
  deliveryMode?: number;
  priority?: number;
  correlationId?: string;
  replyTo?: string;
  expiration?: string;
  messageId?: string;
  timestamp?: number;
  type?: string;
  userId?: string;
  appId?: string;
};

export function readProps(raw: Uint8Array): BasicProps {
  const out: BasicProps = { headers: [] };
  if (raw.length < 2) return out;
  const r = new R(raw);
  const flags = r.u16();
  const take = (bit: number) => (flags & (1 << (15 - bit))) !== 0;
  if (take(0)) out.contentType = r.shortstr();
  if (take(1)) out.contentEncoding = r.shortstr();
  if (take(2)) out.headers = readTable(r);
  if (take(3)) out.deliveryMode = r.u8();
  if (take(4)) out.priority = r.u8();
  if (take(5)) out.correlationId = r.shortstr();
  if (take(6)) out.replyTo = r.shortstr();
  if (take(7)) out.expiration = r.shortstr();
  if (take(8)) out.messageId = r.shortstr();
  if (take(9)) out.timestamp = r.u64();
  if (take(10)) out.type = r.shortstr();
  if (take(11)) out.userId = r.shortstr();
  if (take(12)) out.appId = r.shortstr();
  return out;
}

export function writeProps(p: BasicProps): Uint8Array {
  let flags = 0;
  const set = (bit: number, on: unknown) => {
    if (on !== undefined) flags |= 1 << (15 - bit);
  };
  set(0, p.contentType);
  set(1, p.contentEncoding);
  set(2, p.headers.length ? p.headers : undefined);
  set(3, p.deliveryMode);
  set(4, p.priority);
  set(5, p.correlationId);
  set(6, p.replyTo);
  set(7, p.expiration);
  set(8, p.messageId);
  set(9, p.timestamp);
  set(10, p.type);
  set(11, p.userId);
  set(12, p.appId);
  const w = new W();
  w.u16(flags);
  if (p.contentType !== undefined) w.shortstr(p.contentType);
  if (p.contentEncoding !== undefined) w.shortstr(p.contentEncoding);
  if (p.headers.length) writeTable(w, p.headers);
  if (p.deliveryMode !== undefined) w.u8(p.deliveryMode);
  if (p.priority !== undefined) w.u8(p.priority);
  if (p.correlationId !== undefined) w.shortstr(p.correlationId);
  if (p.replyTo !== undefined) w.shortstr(p.replyTo);
  if (p.expiration !== undefined) w.shortstr(p.expiration);
  if (p.messageId !== undefined) w.shortstr(p.messageId);
  if (p.timestamp !== undefined) w.u64(p.timestamp);
  if (p.type !== undefined) w.shortstr(p.type);
  if (p.userId !== undefined) w.shortstr(p.userId);
  if (p.appId !== undefined) w.shortstr(p.appId);
  return w.concat();
}

/** A 1.0 message, as the sections the broker cares about. */
export type Inbound = {
  body: Uint8Array;
  props: BasicProps;
  persistent: boolean;
  priority: number;
  /** properties.to, for links with a null target. */
  to?: string;
  /** properties.subject, the routing key for an /exchanges/:x address. */
  subject?: string;
};

const dec = new TextDecoder();

/** An id field (message-id, correlation-id) as a 0-9-1 short string. */
function idText(v: unknown): string | undefined {
  if (v === null || v === undefined) return undefined;
  if (typeof v === "string") return v;
  if (typeof v === "number") return String(v);
  if (v instanceof Uint8Array) return dec.decode(v);
  if (v instanceof Uuid) {
    const h = Buffer.from(v.bytes).toString("hex");
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
  }
  return String(v);
}

function text(v: unknown): string | undefined {
  if (v === null || v === undefined) return undefined;
  if (v instanceof Sym) return v.s;
  if (v instanceof Uint8Array) return dec.decode(v);
  return String(v);
}

/** One application property or annotation as a 0-9-1 field. */
function toField(v: unknown): Field {
  if (typeof v === "string") return { t: "S", v };
  if (typeof v === "boolean") return { t: "t", v };
  if (typeof v === "number") return Number.isInteger(v) ? { t: "l", v } : { t: "S", v: String(v) };
  if (v instanceof Uint8Array) return { t: "x", v };
  if (v instanceof Sym) return { t: "S", v: v.s };
  if (v instanceof Timestamp) return { t: "T", v: Math.floor(v.ms / 1000) };
  if (v === null || v === undefined) return { t: "V" };
  return { t: "S", v: String(v) };
}

/** A 0-9-1 field as an AMQP 1.0 simple value. Tables and arrays have no 1.0 application-property form and are dropped. */
function fromField(f: Field): unknown | undefined {
  switch (f.t) {
    case "S":
    case "s":
      return f.v;
    case "I":
    case "l":
      return long(f.v);
    case "t":
      return f.v;
    case "x":
      return f.v;
    case "T":
      return new Timestamp(f.v * 1000);
    case "V":
      return null;
    default:
      return undefined;
  }
}

/**
 * Parse a complete 1.0 transfer payload.
 *
 * @param payload Every section of one message, frames already joined.
 */
export function inbound(payload: Uint8Array): Inbound {
  const d = new Decoder(payload);
  const props: BasicProps = { headers: [] };
  let persistent = false;
  let priority = 0;
  let to: string | undefined;
  let subject: string | undefined;
  const dataSections: Uint8Array[] = [];
  let otherBody = false;
  let bodyStart = -1;
  let bodyEnd = payload.length;
  while (d.more()) {
    const start = d.at;
    const section = d.value();
    if (!(section instanceof Described)) continue;
    const c = typeof section.descriptor === "number" ? section.descriptor : -1;
    const v = section.value;
    if (c >= 0x75 && c <= 0x77) {
      if (bodyStart < 0) bodyStart = start;
      bodyEnd = d.at;
      if (c === 0x75 && v instanceof Uint8Array) dataSections.push(v);
      else otherBody = true;
      continue;
    }
    if (c === 0x70) {
      persistent = field(v, 0) === true;
      const p = field(v, 1);
      if (typeof p === "number") priority = p;
      const ttl = field(v, 2);
      if (typeof ttl === "number") props.expiration = String(ttl);
    } else if (c === 0x72 && v instanceof AMap) {
      for (const [k, val] of v.entries) {
        const key = text(k);
        // RabbitMQ carries x- annotations as headers; routing hints stay out.
        if (key && key.startsWith("x-") && key !== "x-exchange" && key !== "x-routing-key") props.headers.push([key, toField(val)]);
      }
    } else if (c === 0x73) {
      props.messageId = idText(field(v, 0));
      const user = field(v, 1);
      if (user instanceof Uint8Array) props.userId = dec.decode(user);
      to = text(field(v, 2));
      subject = text(field(v, 3));
      props.replyTo = text(field(v, 4));
      props.correlationId = idText(field(v, 5));
      props.contentType = text(field(v, 6));
      props.contentEncoding = text(field(v, 7));
      const expiry = field(v, 8);
      const created = field(v, 9);
      if (created instanceof Timestamp) props.timestamp = Math.floor(created.ms / 1000);
      if (expiry instanceof Timestamp && props.expiration === undefined) {
        props.expiration = String(Math.max(0, Math.round(expiry.ms - Date.now())));
      }
    } else if (c === 0x74 && v instanceof AMap) {
      for (const [k, val] of v.entries) {
        const key = text(k);
        if (key) props.headers.push([key, toField(val)]);
      }
    }
  }
  let body: Uint8Array;
  if (!otherBody && dataSections.length === 1) body = dataSections[0]!;
  else if (!otherBody && dataSections.length === 0) body = new Uint8Array(0);
  else {
    body = payload.slice(bodyStart, bodyEnd);
    props.contentType = SECTIONS_TYPE;
  }
  props.deliveryMode = persistent ? 2 : 1;
  if (priority) props.priority = priority;
  return { body, props, persistent, priority, to, subject };
}

/**
 * Encode a broker message as a 1.0 transfer payload.
 *
 * @param msg The queued message, with its 0-9-1 property block.
 * @param skip Header names to leave out.
 */
export function outbound(msg: LiveMsg, skip?: Set<string>): Uint8Array {
  const p = readProps(msg.propRaw);
  const e = new Encoder();
  // header: durable, priority, ttl, first-acquirer, delivery-count
  e.value(
    described(0x70, [
      msg.persistent || p.deliveryMode === 2,
      ubyte(p.priority ?? 4),
      p.expiration ? uint(Number(p.expiration)) : null,
      !msg.redelivered,
      // delivery-count counts failed attempts (modified with delivery-failed); requeues do not add to it.
      uint(0),
    ]),
  );
  const annotations = new AMap([
    [sym("x-exchange"), msg.exchange],
    [sym("x-routing-key"), msg.routingKey],
  ]);
  const app = new AMap();
  for (const [k, f] of p.headers) {
    const v = fromField(f);
    if (v === undefined || skip?.has(k)) continue;
    if (k.startsWith("x-")) annotations.entries.push([sym(k), v]);
    else app.entries.push([k, v]);
  }
  e.value(described(0x72, annotations));
  const props = [
    p.messageId ?? null,
    p.userId !== undefined ? new TextEncoder().encode(p.userId) : null,
    null,
    null,
    p.replyTo ?? null,
    p.correlationId ?? null,
    p.contentType !== undefined && p.contentType !== SECTIONS_TYPE ? sym(p.contentType) : null,
    p.contentEncoding !== undefined ? sym(p.contentEncoding) : null,
    null,
    p.timestamp !== undefined ? new Timestamp(p.timestamp * 1000) : null,
  ];
  while (props.length && props[props.length - 1] === null) props.pop();
  if (props.length) e.value(described(0x73, props));
  if (app.entries.length) e.value(described(0x74, app));
  if (p.contentType === SECTIONS_TYPE) e.bytes(msg.body);
  else e.value(described(0x75, msg.body));
  return e.done();
}

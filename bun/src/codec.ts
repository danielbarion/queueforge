const text = new TextDecoder();
const enc = new TextEncoder();

export class R {
  o = 0;
  constructor(public b: Uint8Array) {}
  left() {
    return this.b.length - this.o;
  }
  u8() {
    const v = this.b[this.o] ?? 0;
    this.o += 1;
    return v;
  }
  u16() {
    const v = ((this.b[this.o] ?? 0) << 8) | (this.b[this.o + 1] ?? 0);
    this.o += 2;
    return v;
  }
  u32() {
    const v =
      ((this.b[this.o] ?? 0) * 0x1000000 +
        ((this.b[this.o + 1] ?? 0) << 16) +
        ((this.b[this.o + 2] ?? 0) << 8) +
        (this.b[this.o + 3] ?? 0)) >>>
      0;
    this.o += 4;
    return v;
  }
  u64() {
    const hi = this.u32();
    const lo = this.u32();
    return hi * 0x100000000 + lo;
  }
  i8() {
    const v = this.u8();
    return v > 127 ? v - 256 : v;
  }
  i16() {
    const v = this.u16();
    return v > 32767 ? v - 65536 : v;
  }
  i32() {
    const v = this.u32();
    return v > 0x7fffffff ? v - 0x100000000 : v;
  }
  i64() {
    return this.u64();
  }
  shortstr() {
    const n = this.u8();
    const s = text.decode(this.b.subarray(this.o, this.o + n));
    this.o += n;
    return s;
  }
  longstr() {
    const n = this.u32();
    const s = this.b.subarray(this.o, this.o + n);
    this.o += n;
    return s;
  }
  skip(n: number) {
    this.o += n;
  }
}

export class W {
  parts: Uint8Array[] = [];
  u8(v: number) {
    this.parts.push(Uint8Array.of(v & 0xff));
  }
  u16(v: number) {
    this.parts.push(Uint8Array.of((v >> 8) & 0xff, v & 0xff));
  }
  u32(v: number) {
    this.parts.push(Uint8Array.of((v >>> 24) & 0xff, (v >>> 16) & 0xff, (v >>> 8) & 0xff, v & 0xff));
  }
  u64(v: number) {
    const hi = Math.floor(v / 0x100000000);
    const lo = v >>> 0;
    this.u32(hi);
    this.u32(lo);
  }
  bytes(b: Uint8Array) {
    this.parts.push(b);
  }
  shortstr(s: string) {
    const b = enc.encode(s);
    this.u8(b.length);
    this.bytes(b);
  }
  longstr(s: string | Uint8Array) {
    const b = typeof s === "string" ? enc.encode(s) : s;
    this.u32(b.length);
    this.bytes(b);
  }
  bits(flags: boolean[]) {
    let byte = 0;
    let n = 0;
    for (const f of flags) {
      if (f) byte |= 1 << n;
      n++;
      if (n === 8) {
        this.u8(byte);
        byte = 0;
        n = 0;
      }
    }
    if (n) this.u8(byte);
  }
  concat() {
    const n = this.parts.reduce((a, p) => a + p.length, 0);
    const out = new Uint8Array(n);
    let o = 0;
    for (const p of this.parts) {
      out.set(p, o);
      o += p.length;
    }
    return out;
  }
}

export function methodFrame(channel: number, payload: Uint8Array): Uint8Array {
  const w = new W();
  w.u8(1);
  w.u16(channel);
  w.u32(payload.length);
  w.bytes(payload);
  w.u8(0xce);
  return w.concat();
}

export function heartbeatFrame(): Uint8Array {
  return Uint8Array.of(8, 0, 0, 0, 0, 0, 0, 0xce);
}

export function method(classId: number, methodId: number, args: (w: W) => void): Uint8Array {
  const w = new W();
  w.u16(classId);
  w.u16(methodId);
  args(w);
  return w.concat();
}

export type Field =
  | { t: "S"; v: string }
  | { t: "s"; v: string }
  | { t: "I"; v: number }
  | { t: "l"; v: number }
  | { t: "t"; v: boolean }
  | { t: "x"; v: Uint8Array }
  | { t: "V" }
  | { t: "F"; v: Array<[string, Field]> }
  | { t: "A"; v: Field[] }
  | { t: "T"; v: number }
  | { t: "other"; raw: string };

export function fieldEq(a: Field, b: Field): boolean {
  if (a.t === "V" || b.t === "V") return a.t === b.t;
  if ((a.t === "S" || a.t === "s") && (b.t === "S" || b.t === "s")) return a.v === b.v;
  if ((a.t === "I" || a.t === "l") && (b.t === "I" || b.t === "l")) return a.v === b.v;
  if (a.t === "t" && b.t === "t") return a.v === b.v;
  return false;
}

export function fieldStr(f: Field | undefined): string {
  if (!f) return "";
  if (f.t === "S" || f.t === "s") return f.v;
  if (f.t === "I" || f.t === "l") return String(f.v);
  if (f.t === "t") return f.v ? "true" : "false";
  return "";
}

/**
 * Read one field value after its type byte.
 *
 * Type letters follow RabbitMQ, not the 0-9-1 spec table: `s` is a signed
 * 16-bit integer (amqplib sends small numbers that way) and `u` unsigned.
 * Floats and decimals are skipped and kept as `other`.
 */
function readValue(r: R, kind: string): Field {
  switch (kind) {
    case "S":
      return { t: "S", v: text.decode(r.longstr()) };
    case "s":
    case "U":
      return { t: "I", v: r.i16() };
    case "u":
      return { t: "I", v: r.u16() };
    case "I":
      return { t: "I", v: r.i32() };
    case "i":
      return { t: "I", v: r.u32() };
    case "l":
    case "L":
      return { t: "l", v: Number(r.u64()) };
    case "t":
      return { t: "t", v: r.u8() !== 0 };
    case "b":
      return { t: "I", v: r.i8() };
    case "B":
      return { t: "I", v: r.u8() };
    case "x": {
      const n = r.u32();
      const v = r.b.subarray(r.o, r.o + n);
      r.o += n;
      return { t: "x", v };
    }
    case "V":
      return { t: "V" };
    case "F":
      return { t: "F", v: readTable(r) };
    case "T":
      return { t: "T", v: r.u64() };
    case "A": {
      const n = r.u32();
      const endA = r.o + n;
      const items: Field[] = [];
      while (r.o < endA) items.push(readValue(r, String.fromCharCode(r.u8())));
      r.o = endA;
      return { t: "A", v: items };
    }
    case "D":
      r.skip(5);
      return { t: "other", raw: "D" };
    case "f":
      r.skip(4);
      return { t: "other", raw: "f" };
    case "d":
      r.skip(8);
      return { t: "other", raw: "d" };
    default:
      return { t: "other", raw: kind };
  }
}

export function readTable(r: R): Array<[string, Field]> {
  const size = r.u32();
  const end = r.o + size;
  const out: Array<[string, Field]> = [];
  while (r.o < end) {
    const name = r.shortstr();
    out.push([name, readValue(r, String.fromCharCode(r.u8()))]);
  }
  r.o = end;
  return out;
}

function writeFieldValue(w: W, field: Field) {
  if (field.t === "S") {
    w.u8("S".charCodeAt(0));
    w.longstr(field.v);
  } else if (field.t === "s") {
    // RabbitMQ reads `s` as a 16-bit integer, so text goes out as `S`.
    w.u8("S".charCodeAt(0));
    w.longstr(field.v);
  } else if (field.t === "x") {
    w.u8("x".charCodeAt(0));
    w.u32(field.v.length);
    w.bytes(field.v);
  } else if (field.t === "I") {
    w.u8("I".charCodeAt(0));
    w.u32(field.v >>> 0);
  } else if (field.t === "t") {
    w.u8("t".charCodeAt(0));
    w.u8(field.v ? 1 : 0);
  } else if (field.t === "l") {
    w.u8("l".charCodeAt(0));
    w.u64(field.v);
  } else if (field.t === "F") {
    w.u8("F".charCodeAt(0));
    writeTable(w, field.v);
  } else if (field.t === "A") {
    w.u8("A".charCodeAt(0));
    const arr = new W();
    for (const item of field.v) writeFieldValue(arr, item);
    const body = arr.concat();
    w.u32(body.length);
    w.bytes(body);
  } else if (field.t === "T") {
    w.u8("T".charCodeAt(0));
    w.u64(field.v);
  } else {
    w.u8("V".charCodeAt(0));
  }
}

export function writeTable(w: W, fields: Array<[string, Field]>) {
  const inner = new W();
  for (const [name, field] of fields) {
    inner.shortstr(name);
    if (field.t === "S") {
      inner.u8("S".charCodeAt(0));
      inner.longstr(field.v);
    } else if (field.t === "s") {
      inner.u8("S".charCodeAt(0));
      inner.longstr(field.v);
    } else if (field.t === "x") {
      inner.u8("x".charCodeAt(0));
      inner.u32(field.v.length);
      inner.bytes(field.v);
    } else if (field.t === "I") {
      inner.u8("I".charCodeAt(0));
      inner.u32(field.v >>> 0);
    } else if (field.t === "t") {
      inner.u8("t".charCodeAt(0));
      inner.u8(field.v ? 1 : 0);
    } else if (field.t === "l") {
      inner.u8("l".charCodeAt(0));
      inner.u64(field.v);
    } else if (field.t === "F") {
      inner.u8("F".charCodeAt(0));
      writeTable(inner, field.v);
    } else if (field.t === "A") {
      inner.u8("A".charCodeAt(0));
      const arr = new W();
      for (const item of field.v) writeFieldValue(arr, item);
      const body = arr.concat();
      inner.u32(body.length);
      inner.bytes(body);
    } else if (field.t === "T") {
      inner.u8("T".charCodeAt(0));
      inner.u64(field.v);
    } else {
      inner.u8("V".charCodeAt(0));
    }
  }
  const body = inner.concat();
  w.u32(body.length);
  w.bytes(body);
}

export function tableGet(fields: Array<[string, Field]>, key: string): Field | undefined {
  return fields.find((f) => f[0] === key)?.[1];
}

export type ContentProps = {
  raw: Uint8Array;
  headers: Array<[string, Field]>;
  deliveryMode: number;
  priority: number;
  expiration: string;
  /** The user-id property, or empty. RabbitMQ refuses one that is not the login. */
  userId: string;
  /** The reply-to property, or empty. */
  replyTo: string;
};

/**
 * Copy a property block with reply-to replaced.
 *
 * @param propRaw Flags and property values, as stored for redelivery.
 * @param replyTo New reply-to. The flag must already be set.
 */
export function replaceReplyTo(propRaw: Uint8Array, replyTo: string): Uint8Array {
  const r = new R(propRaw);
  const flags = r.u16();
  const take = (bit: number) => (flags & (1 << (15 - bit))) !== 0;
  if (take(0)) r.shortstr();
  if (take(1)) r.shortstr();
  if (take(2)) r.o += 4 + (new R(propRaw.subarray(r.o)).u32());
  if (take(3)) r.u8();
  if (take(4)) r.u8();
  if (take(5)) r.shortstr();
  const start = r.o;
  r.shortstr();
  const end = r.o;
  const w = new W();
  w.bytes(propRaw.subarray(0, start));
  w.shortstr(replyTo);
  w.bytes(propRaw.subarray(end));
  return w.concat();
}

export function readContentHeader(payload: Uint8Array): { bodySize: number; props: ContentProps } {
  const r = new R(payload);
  r.u16();
  r.u16();
  const bodySize = r.u64();
  const flagStart = r.o;
  let flags = r.u16();
  const headers: Array<[string, Field]> = [];
  let deliveryMode = 1;
  let priority = 0;
  let expiration = "";
  const take = (bit: number) => (flags & (1 << (15 - bit))) !== 0;
  if (take(0)) r.shortstr();
  if (take(1)) r.shortstr();
  if (take(2)) headers.push(...readTable(r));
  if (take(3)) deliveryMode = r.u8();
  if (take(4)) priority = r.u8();
  if (take(5)) r.shortstr();
  let replyTo = "";
  if (take(6)) replyTo = r.shortstr();
  if (take(7)) expiration = r.shortstr();
  let userId = "";
  if (take(11)) {
    if (take(8)) r.shortstr();
    if (take(9)) r.u64();
    if (take(10)) r.shortstr();
    userId = r.shortstr();
  }
  while (flags & 1) {
    flags = r.u16();
  }
  return {
    bodySize,
    props: { raw: payload.subarray(flagStart), headers, deliveryMode, priority, expiration, userId, replyTo },
  };
}

export function contentHeaderFrame(channel: number, bodySize: number, propRaw: Uint8Array): Uint8Array {
  const w = new W();
  w.u16(60);
  w.u16(0);
  w.u64(bodySize);
  w.bytes(propRaw);
  const payload = w.concat();
  const f = new W();
  f.u8(2);
  f.u16(channel);
  f.u32(payload.length);
  f.bytes(payload);
  f.u8(0xce);
  return f.concat();
}

/** One body frame. An empty body gets no frame, as the spec requires. */
/** The frame-max this broker proposes in connection.tune. */
export const FRAME_MAX = 131072;

/**
 * Body frames for one message, split so no frame exceeds `frameMax`.
 *
 * A frame is 7 header bytes, the payload, and the end byte. A client that
 * negotiated 131072 closes the connection on a larger frame, so a 1 MiB body
 * goes out as nine frames, as RabbitMQ sends it.
 */
export function bodyFrame(channel: number, body: Uint8Array, frameMax = FRAME_MAX): Uint8Array {
  if (body.length === 0) return new Uint8Array(0);
  const chunk = Math.max(1, (frameMax > 0 ? frameMax : FRAME_MAX) - 8);
  const frames = Math.ceil(body.length / chunk);
  const out = new Uint8Array(body.length + frames * 8);
  let o = 0;
  for (let at = 0; at < body.length; at += chunk) {
    const n = Math.min(chunk, body.length - at);
    out[o] = 3;
    out[o + 1] = channel >> 8;
    out[o + 2] = channel & 0xff;
    out[o + 3] = (n >>> 24) & 0xff;
    out[o + 4] = (n >>> 16) & 0xff;
    out[o + 5] = (n >>> 8) & 0xff;
    out[o + 6] = n & 0xff;
    out.set(body.subarray(at, at + n), o + 7);
    out[o + 7 + n] = 0xce;
    o += n + 8;
  }
  return out;
}

export function replaceHeaderTable(propRaw: Uint8Array, headers: Array<[string, Field]>): Uint8Array {
  const r = new R(propRaw);
  const flags = r.u16();
  const take = (bit: number) => (flags & (1 << (15 - bit))) !== 0;
  const before = new W();
  before.u16(flags);
  if (take(0)) before.shortstr(r.shortstr());
  if (take(1)) before.shortstr(r.shortstr());
  if (take(2)) readTable(r);
  const out = new W();
  out.bytes(before.concat());
  if (take(2)) writeTable(out, headers);
  out.bytes(propRaw.subarray(r.o));
  return out.concat();
}

export function emptyProps(): Uint8Array {
  const w = new W();
  w.u16(0);
  return w.concat();
}

const EMPTY_PROPS = emptyProps();

function putU16(b: Uint8Array, o: number, v: number) {
  b[o] = (v >>> 8) & 0xff;
  b[o + 1] = v & 0xff;
}

function putU32(b: Uint8Array, o: number, v: number) {
  b[o] = (v >>> 24) & 0xff;
  b[o + 1] = (v >>> 16) & 0xff;
  b[o + 2] = (v >>> 8) & 0xff;
  b[o + 3] = v & 0xff;
}

function putU64(b: Uint8Array, o: number, v: number) {
  putU32(b, o, Math.floor(v / 0x100000000));
  putU32(b, o + 4, v >>> 0);
}

/** basic.ack or basic.nack with multiple and requeue clear. Same bytes as the generic frame builder. */
export function encodeSettle(channel: number, deliveryTag: number, nack: boolean): Uint8Array {
  const b = new Uint8Array(21);
  b[0] = 1;
  putU16(b, 1, channel);
  putU32(b, 3, 13);
  putU16(b, 7, 60);
  putU16(b, 9, nack ? 120 : 80);
  putU64(b, 11, deliveryTag);
  b[20] = 0xce;
  return b;
}

/**
 * basic.deliver, content header, and body as one buffer.
 * Returns null when a shortstr would not fit or the body needs more than one
 * frame, so the caller uses the generic builder.
 */
export function encodeDeliver(
  channel: number,
  tag: string,
  deliveryTag: number,
  msg: { exchange: string; routingKey: string; redelivered: boolean; propRaw: Uint8Array; body: Uint8Array },
  frameMax = FRAME_MAX,
): Uint8Array | null {
  const tagB = enc.encode(tag);
  const exB = enc.encode(msg.exchange);
  const rkB = enc.encode(msg.routingKey);
  if (tagB.length > 255 || exB.length > 255 || rkB.length > 255) return null;
  // A body past one frame goes through `bodyFrame`, which splits it at frame-max.
  if (msg.body.length + 8 > frameMax) return null;
  const prop = msg.propRaw.length ? msg.propRaw : EMPTY_PROPS;
  const methodLen = 16 + tagB.length + exB.length + rkB.length;
  const headerLen = 12 + prop.length;
  // An empty body has no body frame: amqplib refuses one after a size-0 header.
  const bodyLen = msg.body.length ? 8 + msg.body.length : 0;
  const b = new Uint8Array(8 + methodLen + 8 + headerLen + bodyLen);
  let o = 0;
  b[o++] = 1;
  putU16(b, o, channel);
  o += 2;
  putU32(b, o, methodLen);
  o += 4;
  putU16(b, o, 60);
  o += 2;
  putU16(b, o, 60);
  o += 2;
  b[o++] = tagB.length;
  b.set(tagB, o);
  o += tagB.length;
  putU64(b, o, deliveryTag);
  o += 8;
  b[o++] = msg.redelivered ? 1 : 0;
  b[o++] = exB.length;
  b.set(exB, o);
  o += exB.length;
  b[o++] = rkB.length;
  b.set(rkB, o);
  o += rkB.length;
  b[o++] = 0xce;

  b[o++] = 2;
  putU16(b, o, channel);
  o += 2;
  putU32(b, o, headerLen);
  o += 4;
  putU16(b, o, 60);
  o += 2;
  putU16(b, o, 0);
  o += 2;
  putU64(b, o, msg.body.length);
  o += 8;
  b.set(prop, o);
  o += prop.length;
  b[o++] = 0xce;
  if (!bodyLen) return b;

  b[o++] = 3;
  putU16(b, o, channel);
  o += 2;
  putU32(b, o, msg.body.length);
  o += 4;
  b.set(msg.body, o);
  o += msg.body.length;
  b[o++] = 0xce;
  return b;
}

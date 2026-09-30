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

export function readTable(r: R): Array<[string, Field]> {
  const size = r.u32();
  const end = r.o + size;
  const out: Array<[string, Field]> = [];
  while (r.o < end) {
    const name = r.shortstr();
    const kind = String.fromCharCode(r.u8());
    let field: Field;
    if (kind === "S") field = { t: "S", v: text.decode(r.longstr()) };
    else if (kind === "s") field = { t: "s", v: r.shortstr() };
    else if (kind === "I") field = { t: "I", v: r.i32() };
    else if (kind === "i") field = { t: "I", v: r.u32() };
    else if (kind === "l" || kind === "L") field = { t: "l", v: Number(r.u64()) };
    else if (kind === "t") field = { t: "t", v: r.u8() !== 0 };
    else if (kind === "b" || kind === "B") field = { t: "I", v: kind === "b" ? r.i8() : r.u8() };
    else if (kind === "U") field = { t: "I", v: r.i16() };
    else if (kind === "u") field = { t: "I", v: r.u16() };
    else if (kind === "x") {
      const n = r.u32();
      const v = r.b.subarray(r.o, r.o + n);
      r.o += n;
      field = { t: "x", v };
    } else if (kind === "V") field = { t: "V" };
    else if (kind === "F") {
      r.u32();
      field = { t: "other", raw: "F" };
    } else if (kind === "T") {
      r.u64();
      field = { t: "other", raw: "T" };
    } else if (kind === "A") {
      const n = r.u32();
      const endA = r.o + n;
      const items: Field[] = [];
      while (r.o < endA) {
        const itemKind = String.fromCharCode(r.u8());
        if (itemKind === "S") items.push({ t: "S", v: text.decode(r.longstr()) });
        else if (itemKind === "s") items.push({ t: "s", v: r.shortstr() });
        else break;
      }
      r.o = endA;
      field = { t: "A", v: items };
    } else if (kind === "D") {
      r.skip(5);
      field = { t: "other", raw: "D" };
    } else if (kind === "f") {
      r.skip(4);
      field = { t: "other", raw: "f" };
    } else if (kind === "d") {
      r.skip(8);
      field = { t: "other", raw: "d" };
    } else {
      field = { t: "other", raw: kind };
    }
    out.push([name, field]);
  }
  r.o = end;
  return out;
}

function writeFieldValue(w: W, field: Field) {
  if (field.t === "S") {
    w.u8("S".charCodeAt(0));
    w.longstr(field.v);
  } else if (field.t === "s") {
    w.u8("s".charCodeAt(0));
    w.shortstr(field.v);
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
      inner.u8("s".charCodeAt(0));
      inner.shortstr(field.v);
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
};

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
  if (take(6)) r.shortstr();
  if (take(7)) expiration = r.shortstr();
  while (flags & 1) {
    flags = r.u16();
  }
  return {
    bodySize,
    props: { raw: payload.subarray(flagStart), headers, deliveryMode, priority, expiration },
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

export function bodyFrame(channel: number, body: Uint8Array): Uint8Array {
  const f = new W();
  f.u8(3);
  f.u16(channel);
  f.u32(body.length);
  f.bytes(body);
  f.u8(0xce);
  return f.concat();
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

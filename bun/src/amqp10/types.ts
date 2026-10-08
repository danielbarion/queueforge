/**
 * The AMQP 1.0 type system: decode to plain values, encode from tagged ones.
 *
 * Decoding gives JS values: numbers, strings, booleans, null, `Uint8Array`
 * for binary, {@link Sym} for symbols, arrays for lists and arrays,
 * {@link AMap} for maps and {@link Described} for described types. 64-bit
 * integers become numbers, which is exact up to 2^53.
 *
 * Encoding takes the same shapes. A JS number with no tag is a uint when it
 * is a non-negative integer below 2^32, else a long or a double. Use the
 * tag helpers for any other wire type.
 */

export class Sym {
  constructor(readonly s: string) {}
}

export class AMap {
  constructor(readonly entries: Array<[unknown, unknown]> = []) {}
  get(key: string): unknown {
    for (const [k, v] of this.entries) if (k === key || (k instanceof Sym && k.s === key)) return v;
    return undefined;
  }
}

export class Described {
  constructor(
    readonly descriptor: number | string,
    readonly value: unknown,
  ) {}
}

/** A value with an explicit wire type. */
export class Typed {
  constructor(
    readonly type: "ubyte" | "ushort" | "uint" | "ulong" | "byte" | "short" | "int" | "long" | "double" | "timestamp" | "uuid" | "array" | "char",
    readonly v: unknown,
  ) {}
}

export class Timestamp {
  constructor(readonly ms: number) {}
}

export class Uuid {
  constructor(readonly bytes: Uint8Array) {}
}

export const sym = (s: string) => new Sym(s);
export const ubyte = (n: number) => new Typed("ubyte", n);
export const ushort = (n: number) => new Typed("ushort", n);
export const uint = (n: number) => new Typed("uint", n);
export const ulong = (n: number) => new Typed("ulong", n);
export const long = (n: number) => new Typed("long", n);
export const int = (n: number) => new Typed("int", n);
/** An AMQP array; every element must encode with the same constructor. */
export const array = (items: unknown[]) => new Typed("array", items);
export const described = (code: number, value: unknown) => new Described(code, value);

const enc = new TextEncoder();
const dec = new TextDecoder();

export class Decoder {
  at = 0;
  private view: DataView;
  constructor(readonly b: Uint8Array) {
    this.view = new DataView(b.buffer, b.byteOffset, b.byteLength);
  }

  more(): boolean {
    return this.at < this.b.length;
  }

  private need(n: number) {
    if (this.at + n > this.b.length) throw new Error("amqp10: short value");
  }

  private u8() {
    this.need(1);
    return this.b[this.at++]!;
  }

  private u32() {
    this.need(4);
    const v = this.view.getUint32(this.at);
    this.at += 4;
    return v;
  }

  private take(n: number): Uint8Array {
    this.need(n);
    const out = this.b.subarray(this.at, this.at + n);
    this.at += n;
    return out;
  }

  /** One value, with any descriptor applied. */
  value(): unknown {
    const code = this.u8();
    if (code === 0x00) {
      const descriptor = this.value();
      const value = this.value();
      const d = typeof descriptor === "number" ? descriptor : descriptor instanceof Sym ? descriptor.s : String(descriptor);
      return new Described(d, value);
    }
    return this.body(code);
  }

  private body(code: number): unknown {
    const v = this.view;
    switch (code) {
      case 0x40:
        return null;
      case 0x41:
        return true;
      case 0x42:
        return false;
      case 0x56:
        return this.u8() !== 0;
      case 0x43:
      case 0x44:
        return 0;
      case 0x50:
        return this.u8();
      case 0x51:
        this.need(1);
        return v.getInt8(this.at++);
      case 0x52:
      case 0x53:
        return this.u8();
      case 0x54:
        this.need(1);
        return v.getInt8(this.at++);
      case 0x55:
        this.need(1);
        return v.getInt8(this.at++);
      case 0x60: {
        this.need(2);
        const n = v.getUint16(this.at);
        this.at += 2;
        return n;
      }
      case 0x61: {
        this.need(2);
        const n = v.getInt16(this.at);
        this.at += 2;
        return n;
      }
      case 0x70:
        return this.u32();
      case 0x71:
      case 0x73: {
        this.need(4);
        const n = code === 0x71 ? v.getInt32(this.at) : v.getUint32(this.at);
        this.at += 4;
        return code === 0x73 ? String.fromCodePoint(n) : n;
      }
      case 0x72: {
        this.need(4);
        const n = v.getFloat32(this.at);
        this.at += 4;
        return n;
      }
      case 0x80:
      case 0x81:
      case 0x83: {
        this.need(8);
        const n = code === 0x80 ? Number(v.getBigUint64(this.at)) : Number(v.getBigInt64(this.at));
        this.at += 8;
        return code === 0x83 ? new Timestamp(n) : n;
      }
      case 0x82: {
        this.need(8);
        const n = v.getFloat64(this.at);
        this.at += 8;
        return n;
      }
      case 0x94:
        return this.take(4).slice();
      case 0x84:
        return this.take(8).slice();
      case 0x98:
        return new Uuid(this.take(16).slice());
      case 0xa0:
        return this.take(this.u8()).slice();
      case 0xb0:
        return this.take(this.u32()).slice();
      case 0xa1:
        return dec.decode(this.take(this.u8()));
      case 0xb1:
        return dec.decode(this.take(this.u32()));
      case 0xa3:
        return new Sym(dec.decode(this.take(this.u8())));
      case 0xb3:
        return new Sym(dec.decode(this.take(this.u32())));
      case 0x45:
        return [];
      case 0xc0:
      case 0xd0: {
        const wide = code === 0xd0;
        if (wide) this.u32();
        else this.u8();
        const count = wide ? this.u32() : this.u8();
        const out: unknown[] = [];
        for (let i = 0; i < count; i++) out.push(this.value());
        return out;
      }
      case 0xc1:
      case 0xd1: {
        const wide = code === 0xd1;
        if (wide) this.u32();
        else this.u8();
        const count = wide ? this.u32() : this.u8();
        const m = new AMap();
        for (let i = 0; i + 1 < count; i += 2) m.entries.push([this.value(), this.value()]);
        return m;
      }
      case 0xe0:
      case 0xf0: {
        const wide = code === 0xf0;
        if (wide) this.u32();
        else this.u8();
        const count = wide ? this.u32() : this.u8();
        let elem = this.u8();
        let descriptor: unknown = undefined;
        if (elem === 0x00) {
          descriptor = this.value();
          elem = this.u8();
        }
        const out: unknown[] = [];
        for (let i = 0; i < count; i++) {
          const item = this.body(elem);
          out.push(descriptor === undefined ? item : new Described(descriptor as number, item));
        }
        return out;
      }
      default:
        throw new Error(`amqp10: unknown type 0x${code.toString(16)}`);
    }
  }
}

/** Decode every value in `b`, in order. */
export function decodeAll(b: Uint8Array): unknown[] {
  const d = new Decoder(b);
  const out: unknown[] = [];
  while (d.more()) out.push(d.value());
  return out;
}

export class Encoder {
  private buf = new Uint8Array(256);
  private view = new DataView(this.buf.buffer);
  at = 0;

  private room(n: number) {
    if (this.at + n <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < this.at + n) size *= 2;
    const next = new Uint8Array(size);
    next.set(this.buf.subarray(0, this.at));
    this.buf = next;
    this.view = new DataView(next.buffer);
  }

  u8(n: number) {
    this.room(1);
    this.buf[this.at++] = n;
  }

  u32(n: number) {
    this.room(4);
    this.view.setUint32(this.at, n);
    this.at += 4;
  }

  bytes(b: Uint8Array) {
    this.room(b.length);
    this.buf.set(b, this.at);
    this.at += b.length;
  }

  done(): Uint8Array {
    return this.buf.slice(0, this.at);
  }

  value(v: unknown): void {
    if (v === null || v === undefined) return this.u8(0x40);
    if (v === true) return this.u8(0x41);
    if (v === false) return this.u8(0x42);
    if (typeof v === "string") return this.variable(enc.encode(v), 0xa1, 0xb1);
    if (v instanceof Sym) return this.variable(enc.encode(v.s), 0xa3, 0xb3);
    if (v instanceof Uint8Array) return this.variable(v, 0xa0, 0xb0);
    if (v instanceof Timestamp) {
      this.u8(0x83);
      this.room(8);
      this.view.setBigInt64(this.at, BigInt(Math.trunc(v.ms)));
      this.at += 8;
      return;
    }
    if (v instanceof Uuid) {
      this.u8(0x98);
      return this.bytes(v.bytes);
    }
    if (v instanceof Described) {
      this.u8(0x00);
      if (typeof v.descriptor === "number") this.value(ulong(v.descriptor));
      else this.value(new Sym(v.descriptor));
      return this.value(v.value);
    }
    if (v instanceof Typed) return this.typed(v);
    if (typeof v === "number") {
      if (Number.isInteger(v) && v >= 0 && v < 2 ** 32) return this.typed(uint(v));
      if (Number.isInteger(v)) return this.typed(long(v));
      return this.typed(new Typed("double", v));
    }
    if (typeof v === "bigint") return this.typed(long(Number(v)));
    if (Array.isArray(v)) return this.list(v);
    if (v instanceof AMap) return this.map(v);
    throw new Error(`amqp10: cannot encode ${typeof v}`);
  }

  private variable(b: Uint8Array, small: number, large: number) {
    if (b.length < 256) {
      this.u8(small);
      this.u8(b.length);
    } else {
      this.u8(large);
      this.u32(b.length);
    }
    this.bytes(b);
  }

  private typed(t: Typed) {
    const n = t.v as number;
    switch (t.type) {
      case "ubyte":
        this.u8(0x50);
        return this.u8(n);
      case "byte":
        this.u8(0x51);
        return this.u8(n & 0xff);
      case "ushort":
        this.u8(0x60);
        this.room(2);
        this.view.setUint16(this.at, n);
        this.at += 2;
        return;
      case "short":
        this.u8(0x61);
        this.room(2);
        this.view.setInt16(this.at, n);
        this.at += 2;
        return;
      case "uint":
        if (n === 0) return this.u8(0x43);
        if (n < 256) {
          this.u8(0x52);
          return this.u8(n);
        }
        this.u8(0x70);
        return this.u32(n);
      case "int":
        if (n >= -128 && n < 128) {
          this.u8(0x54);
          return this.u8(n & 0xff);
        }
        this.u8(0x71);
        this.room(4);
        this.view.setInt32(this.at, n);
        this.at += 4;
        return;
      case "ulong":
        if (n === 0) return this.u8(0x44);
        if (n < 256) {
          this.u8(0x53);
          return this.u8(n);
        }
        this.u8(0x80);
        this.room(8);
        this.view.setBigUint64(this.at, BigInt(n));
        this.at += 8;
        return;
      case "long":
        if (n >= -128 && n < 128) {
          this.u8(0x55);
          return this.u8(n & 0xff);
        }
        this.u8(0x81);
        this.room(8);
        this.view.setBigInt64(this.at, BigInt(n));
        this.at += 8;
        return;
      case "double":
        this.u8(0x82);
        this.room(8);
        this.view.setFloat64(this.at, n);
        this.at += 8;
        return;
      case "timestamp":
        return this.value(new Timestamp(n));
      case "uuid":
        return this.value(new Uuid(t.v as Uint8Array));
      case "char":
        this.u8(0x73);
        return this.u32(String(t.v).codePointAt(0) ?? 0);
      case "array":
        return this.array(t.v as unknown[]);
    }
  }

  private compound(items: unknown[], small: number, large: number, count: number) {
    const inner = new Encoder();
    for (const item of items) inner.value(item);
    const body = inner.done();
    if (body.length + 1 < 256 && count < 256) {
      this.u8(small);
      this.u8(body.length + 1);
      this.u8(count);
    } else {
      this.u8(large);
      this.u32(body.length + 4);
      this.u32(count);
    }
    this.bytes(body);
  }

  private list(items: unknown[]) {
    if (items.length === 0) return this.u8(0x45);
    this.compound(items, 0xc0, 0xd0, items.length);
  }

  private map(m: AMap) {
    const flat: unknown[] = [];
    for (const [k, v] of m.entries) flat.push(k, v);
    this.compound(flat, 0xc1, 0xd1, flat.length);
  }

  /** Arrays use the wide element constructor, so every element has the same size class. */
  private array(items: unknown[]) {
    const inner = new Encoder();
    let ctor = 0x40;
    for (const item of items) {
      if (item instanceof Sym) {
        ctor = 0xb3;
        const b = enc.encode(item.s);
        inner.u32(b.length);
        inner.bytes(b);
      } else if (typeof item === "string") {
        ctor = 0xb1;
        const b = enc.encode(item);
        inner.u32(b.length);
        inner.bytes(b);
      } else {
        throw new Error("amqp10: arrays hold strings or symbols here");
      }
    }
    const body = inner.done();
    this.u8(0xf0);
    this.u32(body.length + 5);
    this.u32(items.length);
    this.u8(ctor);
    this.bytes(body);
  }
}

export function encode(v: unknown): Uint8Array {
  const e = new Encoder();
  e.value(v);
  return e.done();
}

/**
 * Field `i` of a described list, or undefined when the list is shorter.
 * Performatives and sections are described lists with trailing nulls omitted.
 */
export function field(list: unknown, i: number): unknown {
  return Array.isArray(list) ? list[i] ?? undefined : undefined;
}

/** A described value's numeric descriptor, or -1. Symbolic descriptors are mapped to their codes. */
export function code(v: unknown): number {
  if (!(v instanceof Described)) return -1;
  if (typeof v.descriptor === "number") return v.descriptor;
  return SYMBOLIC[v.descriptor] ?? -1;
}

const SYMBOLIC: Record<string, number> = {
  "amqp:accepted:list": 0x24,
  "amqp:rejected:list": 0x25,
  "amqp:released:list": 0x26,
  "amqp:modified:list": 0x27,
  "amqp:source:list": 0x28,
  "amqp:target:list": 0x29,
  "amqp:data:binary": 0x75,
};

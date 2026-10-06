/**
 * Quorum ids this node has already delivered.
 *
 * Membership is a flat hash table. The id bytes sit in one buffer. A Set of
 * one string per delivery made a later confirm pause once a few hundred
 * thousand ids were still live.
 */
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function isAscii(s: string): boolean {
  for (let i = 0; i < s.length; i++) if (s.charCodeAt(i) > 127) return false;
  return true;
}

function hashKey(vhost: string, queue: string, id: string): [number, number] {
  let a = 0x811c9dc5;
  let b = 0x9e3779b9;
  const mix = (s: string) => {
    for (let i = 0; i < s.length; i++) {
      const c = s.charCodeAt(i);
      a = Math.imul(a ^ c, 0x01000193);
      b = Math.imul(b ^ c, 0x85ebca6b);
      b = (b << 13) | (b >>> 19);
    }
    a = Math.imul(a ^ 0x7f, 0x01000193);
  };
  mix(vhost);
  mix(queue);
  mix(id);
  a = a >>> 0;
  b = b >>> 0;
  if (a === 0 && b === 0) a = 1;
  return [a, b];
}

export class ConsumedSet {
  private cap = 1024;
  /** Empty slots are 0,0. A live hash is never 0,0. */
  private hash = new Uint32Array(this.cap * 2);
  private live = 0;
  private rec = 0;
  private off = new Uint32Array(256);
  private len = new Uint32Array(256);
  private ha = new Uint32Array(256);
  private hb = new Uint32Array(256);
  private bytes = new Uint8Array(4096);
  private byteLen = 0;

  add(vhost: string, queue: string, id: string): boolean {
    const [a, b] = hashKey(vhost, queue, id);
    if (this.locate(a, b) >= 0) return false;
    if ((this.live + 1) * 2 > this.cap) this.rehash(this.cap * 2);
    const placed = this.append(vhost, queue, id);
    if (this.rec === this.off.length) this.growRecs();
    this.off[this.rec] = placed.off;
    this.len[this.rec] = placed.len;
    this.ha[this.rec] = a;
    this.hb[this.rec] = b;
    this.rec++;
    this.insert(a, b);
    this.live++;
    return true;
  }

  list(): Array<{ vhost: string; queue: string; id: string }> {
    const out: Array<{ vhost: string; queue: string; id: string }> = new Array(this.rec);
    for (let r = 0; r < this.rec; r++) {
      let o = this.off[r]!;
      const vhostLen = readU32(this.bytes, o);
      o += 4;
      const vhost = decoder.decode(this.bytes.subarray(o, o + vhostLen));
      o += vhostLen;
      const queueLen = readU32(this.bytes, o);
      o += 4;
      const queue = decoder.decode(this.bytes.subarray(o, o + queueLen));
      o += queueLen;
      const idLen = readU32(this.bytes, o);
      o += 4;
      const id = decoder.decode(this.bytes.subarray(o, o + idLen));
      out[r] = { vhost, queue, id };
    }
    return out;
  }

  private locate(a: number, b: number): number {
    let i = ((a ^ b) >>> 0) % this.cap;
    for (let n = 0; n < this.cap; n++) {
      const o = i * 2;
      const ha = this.hash[o]!;
      const hb = this.hash[o + 1]!;
      if (ha === 0 && hb === 0) return -1;
      if (ha === a && hb === b) return i;
      i++;
      if (i === this.cap) i = 0;
    }
    return -1;
  }

  private insert(a: number, b: number) {
    let i = ((a ^ b) >>> 0) % this.cap;
    for (;;) {
      const o = i * 2;
      if (this.hash[o] === 0 && this.hash[o + 1] === 0) {
        this.hash[o] = a;
        this.hash[o + 1] = b;
        return;
      }
      i++;
      if (i === this.cap) i = 0;
    }
  }

  private rehash(cap: number) {
    this.cap = cap;
    this.hash = new Uint32Array(cap * 2);
    for (let r = 0; r < this.rec; r++) this.insert(this.ha[r]!, this.hb[r]!);
  }

  private growRecs() {
    const n = this.off.length * 2;
    const off = new Uint32Array(n);
    const len = new Uint32Array(n);
    const ha = new Uint32Array(n);
    const hb = new Uint32Array(n);
    off.set(this.off);
    len.set(this.len);
    ha.set(this.ha);
    hb.set(this.hb);
    this.off = off;
    this.len = len;
    this.ha = ha;
    this.hb = hb;
  }

  private ensure(extra: number) {
    if (this.byteLen + extra <= this.bytes.length) return;
    let n = this.bytes.length;
    while (n < this.byteLen + extra) n *= 2;
    const next = new Uint8Array(n);
    next.set(this.bytes.subarray(0, this.byteLen));
    this.bytes = next;
  }

  private append(vhost: string, queue: string, id: string): { off: number; len: number } {
    if (isAscii(vhost) && isAscii(queue) && isAscii(id)) {
      const len = 12 + vhost.length + queue.length + id.length;
      this.ensure(len);
      const off = this.byteLen;
      this.writeU32(vhost.length);
      this.writeAscii(vhost);
      this.writeU32(queue.length);
      this.writeAscii(queue);
      this.writeU32(id.length);
      this.writeAscii(id);
      return { off, len };
    }
    const vb = encoder.encode(vhost);
    const qb = encoder.encode(queue);
    const ib = encoder.encode(id);
    const len = 12 + vb.length + qb.length + ib.length;
    this.ensure(len);
    const off = this.byteLen;
    this.writeU32(vb.length);
    this.bytes.set(vb, this.byteLen);
    this.byteLen += vb.length;
    this.writeU32(qb.length);
    this.bytes.set(qb, this.byteLen);
    this.byteLen += qb.length;
    this.writeU32(ib.length);
    this.bytes.set(ib, this.byteLen);
    this.byteLen += ib.length;
    return { off, len };
  }

  private writeU32(n: number) {
    const o = this.byteLen;
    this.bytes[o] = n & 0xff;
    this.bytes[o + 1] = (n >>> 8) & 0xff;
    this.bytes[o + 2] = (n >>> 16) & 0xff;
    this.bytes[o + 3] = (n >>> 24) & 0xff;
    this.byteLen = o + 4;
  }

  private writeAscii(s: string) {
    for (let i = 0; i < s.length; i++) this.bytes[this.byteLen++] = s.charCodeAt(i);
  }
}

function readU32(buf: Uint8Array, o: number): number {
  return (buf[o]! | (buf[o + 1]! << 8) | (buf[o + 2]! << 16) | (buf[o + 3]! << 24)) >>> 0;
}

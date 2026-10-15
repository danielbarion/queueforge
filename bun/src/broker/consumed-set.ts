/**
 * Quorum ids this node has already delivered, until every member dropped them.
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
  /** Record index + 1 per slot; 0 is an empty slot. Linear probing. */
  private slots = new Uint32Array(this.cap);
  private live = 0;
  private rec = 0;
  private off = new Uint32Array(256);
  private len = new Uint32Array(256);
  /** A record's hash; 0,0 marks a deleted record (a live hash is never 0,0). */
  private ha = new Uint32Array(256);
  private hb = new Uint32Array(256);
  private bytes = new Uint8Array(4096);
  private byteLen = 0;

  /** Ids held. */
  get size(): number {
    return this.live;
  }

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
    this.insert(this.rec);
    this.rec++;
    this.live++;
    return true;
  }

  /** Whether the id is held. */
  has(vhost: string, queue: string, id: string): boolean {
    const [a, b] = hashKey(vhost, queue, id);
    return this.locate(a, b) >= 0;
  }

  /** Forget an id. The space is reclaimed once deleted records outnumber live ones. */
  delete(vhost: string, queue: string, id: string): boolean {
    const [a, b] = hashKey(vhost, queue, id);
    const slot = this.locate(a, b);
    if (slot < 0) return false;
    const r = this.slots[slot]! - 1;
    this.ha[r] = 0;
    this.hb[r] = 0;
    this.removeSlot(slot);
    this.live--;
    const dead = this.rec - this.live;
    if (dead >= 1024 && dead > this.live) this.compact();
    return true;
  }

  list(): Array<{ vhost: string; queue: string; id: string }> {
    const out: Array<{ vhost: string; queue: string; id: string }> = [];
    for (let r = 0; r < this.rec; r++) {
      if (this.ha[r] === 0 && this.hb[r] === 0) continue;
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
      out.push({ vhost, queue, id });
    }
    return out;
  }

  private home(a: number, b: number): number {
    return ((a ^ b) >>> 0) % this.cap;
  }

  private locate(a: number, b: number): number {
    let i = this.home(a, b);
    for (let n = 0; n < this.cap; n++) {
      const s = this.slots[i]!;
      if (s === 0) return -1;
      if (this.ha[s - 1] === a && this.hb[s - 1] === b) return i;
      i++;
      if (i === this.cap) i = 0;
    }
    return -1;
  }

  private insert(r: number) {
    let i = this.home(this.ha[r]!, this.hb[r]!);
    while (this.slots[i] !== 0) {
      i++;
      if (i === this.cap) i = 0;
    }
    this.slots[i] = r + 1;
  }

  /** Empty slot `i`, moving later entries of the same probe run back into the gap. */
  private removeSlot(i: number) {
    this.slots[i] = 0;
    let j = i;
    for (;;) {
      j++;
      if (j === this.cap) j = 0;
      const s = this.slots[j]!;
      if (s === 0) return;
      const k = this.home(this.ha[s - 1]!, this.hb[s - 1]!);
      // An entry whose home lies cyclically in (i, j] is still reachable.
      if (i <= j ? i < k && k <= j : i < k || k <= j) continue;
      this.slots[i] = s;
      this.slots[j] = 0;
      i = j;
    }
  }

  private rehash(cap: number) {
    this.cap = cap;
    this.slots = new Uint32Array(cap);
    for (let r = 0; r < this.rec; r++) if (this.ha[r] !== 0 || this.hb[r] !== 0) this.insert(r);
  }

  /** Rewrite the records and their bytes without the deleted ones. */
  private compact() {
    const n = Math.max(256, this.live);
    const off = new Uint32Array(n);
    const len = new Uint32Array(n);
    const ha = new Uint32Array(n);
    const hb = new Uint32Array(n);
    let bytesLen = 0;
    for (let r = 0; r < this.rec; r++) if (this.ha[r] !== 0 || this.hb[r] !== 0) bytesLen += this.len[r]!;
    const bytes = new Uint8Array(Math.max(4096, bytesLen));
    let w = 0;
    let at = 0;
    for (let r = 0; r < this.rec; r++) {
      if (this.ha[r] === 0 && this.hb[r] === 0) continue;
      const l = this.len[r]!;
      bytes.set(this.bytes.subarray(this.off[r]!, this.off[r]! + l), at);
      off[w] = at;
      len[w] = l;
      ha[w] = this.ha[r]!;
      hb[w] = this.hb[r]!;
      at += l;
      w++;
    }
    this.off = off;
    this.len = len;
    this.ha = ha;
    this.hb = hb;
    this.bytes = bytes;
    this.byteLen = at;
    this.rec = w;
    let cap = 1024;
    while (cap < this.live * 4) cap *= 2;
    this.rehash(cap);
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

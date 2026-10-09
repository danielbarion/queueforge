/**
 * Preallocated overwrite log for classic durable confirms.
 *
 * sqlite stays `synchronous=OFF` and is the read cache. The confirm waits on
 * an `fdatasync` of a chunk whose blocks were already written and synced, so
 * the flush does not allocate. A crash replays this log back into sqlite.
 *
 * Each chunk is its own file. The next chunk is materialized on another
 * thread, so that work does not dirty the file the confirm is syncing.
 *
 * A rotated chunk is removed once every insert in it has a synced delete,
 * oldest first. A delete record is always in the same or a later chunk than
 * its insert, so dropping a prefix never brings an acked body back.
 */
import {
  closeSync,
  constants,
  fdatasyncSync,
  fstatSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readSync,
  readdirSync,
  unlinkSync,
  writeSync,
} from "node:fs";

const HEADER = 16;
const CHUNK = 4 * 1024 * 1024;
/** Prepare the next file once the active chunk has less than this left. */
const KICK_REMAINING = 2 * 1024 * 1024;
/**
 * Chunks materialized before the first confirm. The one-confirm ladder writes
 * about 16 MiB, and a background zero-fill on this same disk stretches that
 * fsync tail. Later chunks are still built off to the side.
 */
const START_CHUNKS = 8;
/**
 * Rotated chunks kept behind the active one before the oldest chunk's
 * remaining live rows are written forward, so a few unacked messages do not
 * pin every later chunk.
 */
const COMPACT_AFTER = 16;
const MAX_NAME = 64 * 1024;
const MAX_META = 1024 * 1024;
const MAX_BODY = 64 * 1024 * 1024;

export type LogInsert = {
  id: number;
  vhost: string;
  queue: string;
  body: Uint8Array;
  meta: string;
};

export type LogOp =
  | { kind: "insert"; id: number; vhost: string; queue: string; meta: string; body: Uint8Array }
  | { kind: "delete"; id: number };

type ActiveChunk = { id: number; fd: number; len: number; size: number };

/** IEEE CRC-32, the same polynomial the table version used. Native, so replay and the group commit do not loop per byte in JS. */
function crc32(buf: Uint8Array): number {
  return Bun.hash.crc32(buf) >>> 0;
}

function chunkPath(dir: string, id: number): string {
  return `${dir}/${String(id).padStart(8, "0")}`;
}

function headerBuf(logical: number): Buffer {
  const buf = Buffer.alloc(HEADER);
  buf.write("QFHD", 0, 4, "ascii");
  buf.writeUInt32LE(1, 4);
  buf.writeBigUInt64LE(BigInt(logical), 8);
  return buf;
}

function writeAllAt(fd: number, buf: Uint8Array, at: number) {
  let off = 0;
  while (off < buf.length) {
    const n = writeSync(fd, buf, off, buf.length - off, at + off);
    if (n <= 0) throw new Error("durable log short write");
    off += n;
  }
}

function readHeader(fd: number): number | null {
  const buf = Buffer.alloc(HEADER);
  const n = readSync(fd, buf, 0, HEADER, 0);
  if (n < HEADER) return null;
  if (buf.toString("ascii", 0, 4) !== "QFHD") return null;
  const logical = Number(buf.readBigUInt64LE(8));
  if (!Number.isFinite(logical) || logical < HEADER) return null;
  return logical;
}

/** Create a fully written chunk and fsync it before any confirm uses it. */
export function createChunkFile(path: string, size: number): number {
  const fd = openSync(path, constants.O_RDWR | constants.O_CREAT, 0o644);
  const zeros = Buffer.alloc(64 * 1024);
  let at = 0;
  while (at < size) {
    const n = Math.min(zeros.length, size - at);
    const wrote = writeSync(fd, zeros, 0, n, at);
    if (wrote <= 0) throw new Error("durable log preallocation short write");
    at += wrote;
  }
  writeAllAt(fd, headerBuf(HEADER), 0);
  fdatasyncSync(fd);
  return fd;
}

function inspect(path: string): { logical: number; size: number } | null {
  let fd: number;
  try {
    fd = openSync(path, "r");
  } catch {
    return null;
  }
  try {
    const size = fstatSync(fd).size;
    const logical = readHeader(fd);
    if (logical == null) return null;
    return { logical: Math.min(logical, size), size };
  } finally {
    closeSync(fd);
  }
}

/** Magic, type, id, the three length fields, body length, and crc. */
const RECORD_FIXED = 4 + 1 + 8 + 2 + 2 + 4 + 4 + 4;
const MAGIC = 0x4c4d4651; // "QFML" read as a little-endian u32

/** Write one record at `o` and return the offset after it. Strings are encoded straight into `buf`. */
function writeRecord(
  buf: Buffer,
  o: number,
  type: number,
  id: number,
  vhost: string,
  vhostLen: number,
  queue: string,
  queueLen: number,
  meta: string,
  metaLen: number,
  body: Uint8Array,
): number {
  const start = o;
  buf.writeUInt32LE(MAGIC, o);
  buf[o + 4] = type;
  // Two halves instead of a BigInt per record. Ids stay below 2^53.
  buf.writeUInt32LE(id >>> 0, o + 5);
  buf.writeUInt32LE(Math.floor(id / 0x1_0000_0000), o + 9);
  o += 13;
  buf.writeUInt16LE(vhostLen, o);
  o += 2;
  if (vhostLen) o += buf.write(vhost, o, vhostLen, "utf8");
  buf.writeUInt16LE(queueLen, o);
  o += 2;
  if (queueLen) o += buf.write(queue, o, queueLen, "utf8");
  buf.writeUInt32LE(metaLen, o);
  o += 4;
  if (metaLen) o += buf.write(meta, o, metaLen, "utf8");
  buf.writeUInt32LE(body.length, o);
  o += 4;
  buf.set(body, o);
  o += body.length;
  buf.writeUInt32LE(crc32(buf.subarray(start, o)), o);
  return o + 4;
}

const EMPTY = new Uint8Array(0);

/**
 * One buffer for a whole group commit, sized first, then written in one pass.
 * Deletes come first: a replay applies them before this group's inserts.
 */
function encodeRows(deletes: number[], inserts: LogInsert[]): Buffer {
  let total = deletes.length * RECORD_FIXED;
  const lens = new Array<number>(inserts.length * 3);
  for (let i = 0; i < inserts.length; i++) {
    const row = inserts[i]!;
    const v = Buffer.byteLength(row.vhost, "utf8");
    const q = Buffer.byteLength(row.queue, "utf8");
    const m = Buffer.byteLength(row.meta, "utf8");
    lens[i * 3] = v;
    lens[i * 3 + 1] = q;
    lens[i * 3 + 2] = m;
    total += RECORD_FIXED + v + q + m + row.body.length;
  }
  const buf = Buffer.allocUnsafe(total);
  let o = 0;
  for (const id of deletes) o = writeRecord(buf, o, 2, id, "", 0, "", 0, "", 0, EMPTY);
  for (let i = 0; i < inserts.length; i++) {
    const row = inserts[i]!;
    o = writeRecord(buf, o, 1, row.id, row.vhost, lens[i * 3]!, row.queue, lens[i * 3 + 1]!, row.meta, lens[i * 3 + 2]!, row.body);
  }
  return buf;
}

function parseRecord(buf: Buffer, pos: number, limit: number): { next: number; op: LogOp } | null {
  if (pos + 4 > limit) return null;
  if (buf[pos] === 0 && buf[pos + 1] === 0 && buf[pos + 2] === 0 && buf[pos + 3] === 0) return null;
  if (buf.toString("ascii", pos, pos + 4) !== "QFML") return null;
  let o = pos + 4;
  const need = (n: number) => o + n <= limit;
  if (!need(1 + 8 + 2)) return null;
  const type = buf.readUInt8(o);
  o += 1;
  const id = Number(buf.readBigUInt64LE(o));
  o += 8;
  const vhostLen = buf.readUInt16LE(o);
  o += 2;
  if (vhostLen > MAX_NAME || !need(vhostLen)) return null;
  const vhost = buf.toString("utf8", o, o + vhostLen);
  o += vhostLen;
  if (!need(2)) return null;
  const queueLen = buf.readUInt16LE(o);
  o += 2;
  if (queueLen > MAX_NAME || !need(queueLen)) return null;
  const queue = buf.toString("utf8", o, o + queueLen);
  o += queueLen;
  if (!need(4)) return null;
  const metaLen = buf.readUInt32LE(o);
  o += 4;
  if (metaLen > MAX_META || !need(metaLen)) return null;
  const meta = buf.toString("utf8", o, o + metaLen);
  o += metaLen;
  if (!need(4)) return null;
  const bodyLen = buf.readUInt32LE(o);
  o += 4;
  if (bodyLen > MAX_BODY || !need(bodyLen + 4)) return null;
  const body = Buffer.from(buf.subarray(o, o + bodyLen));
  o += bodyLen;
  const crc = buf.readUInt32LE(o);
  o += 4;
  if (crc32(buf.subarray(pos, o - 4)) !== crc) return null;
  if (type !== 1 && type !== 2) return null;
  const op: LogOp =
    type === 2 ? { kind: "delete", id } : { kind: "insert", id, vhost, queue, meta, body };
  return { next: o, op };
}

function scanFile(fd: number, logical: number, apply: (op: LogOp) => void): number {
  if (logical <= HEADER) return HEADER;
  const buf = Buffer.alloc(logical);
  let got = 0;
  while (got < logical) {
    const n = readSync(fd, buf, got, logical - got, got);
    if (n <= 0) break;
    got += n;
  }
  const limit = Math.min(logical, got);
  let pos = HEADER;
  while (pos + 4 <= limit) {
    const rec = parseRecord(buf, pos, limit);
    if (!rec) return pos;
    apply(rec.op);
    if (rec.next <= pos) return pos;
    pos = rec.next;
  }
  return pos;
}

/** Make unlinks in `dir` durable. */
function syncDir(dir: string) {
  let fd = -1;
  try {
    fd = openSync(dir, "r");
    fsyncSync(fd);
  } catch {
    /* A lost unlink only replays a chunk whose rows are all deleted later. */
  } finally {
    if (fd >= 0) closeSync(fd);
  }
}

export class FastLog {
  private active: ActiveChunk;
  /** Lowest chunk id still on disk. */
  private oldest: number;
  /** Row id to the chunk holding its latest insert, for rows without a synced delete. */
  private readonly liveChunk = new Map<number, number>();
  /** Chunk id to the number of rows in `liveChunk` that point at it. */
  private readonly liveCount = new Map<number, number>();
  private retireQueued = false;
  /** The oldest chunk whose rows were last handed out to be written forward, and when. */
  private relocating = { chunk: 0, at: 0 };
  /** Chunks removed since open. */
  retiredChunks = 0;
  private worker: Worker | null = null;
  private preparingId = 0;
  private closed = false;
  /** Next chunk is already on disk. Avoids an open/stat on every confirm. */
  private nextReady = false;
  private readonly state = new SharedArrayBuffer(8);
  private readonly view = new BigInt64Array(this.state);

  private constructor(private readonly dir: string, active: ActiveChunk, oldest: number) {
    this.active = active;
    this.oldest = Math.min(oldest, active.id);
    Atomics.store(this.view, 0, 0n);
  }

  /** Open the log beside `sqlitePath`, or null when durability stays in sqlite. */
  static open(sqlitePath: string): FastLog | null {
    if (!sqlitePath || sqlitePath === ":memory:") return null;
    const dir = `${sqlitePath}.dlog`;
    mkdirSync(dir, { recursive: true });
    const ids = chunkIds(dir);
    while (ids.length) {
      const id = ids[ids.length - 1]!;
      const path = chunkPath(dir, id);
      const info = inspect(path);
      // An empty file that never reached a full chunk is a crashed extend.
      if (!info || (info.logical === HEADER && info.size < CHUNK)) {
        try {
          unlinkSync(path);
        } catch {
          /* already gone */
        }
        ids.pop();
        continue;
      }
      break;
    }
    if (ids.length === 0) {
      let fd = 0;
      for (let id = 1; id <= START_CHUNKS; id++) {
        const created = createChunkFile(chunkPath(dir, id), CHUNK);
        if (id === 1) fd = created;
        else closeSync(created);
      }
      const log = new FastLog(dir, { id: 1, fd, len: HEADER, size: CHUNK }, 1);
      log.nextReady = true;
      return log;
    }
    // The header length is a hint. Confirms fsync the record bytes and leave
    // the header page clean, so the real end is the last valid record.
    let chosen: { id: number; end: number; size: number } | null = null;
    for (const id of ids) {
      const path = chunkPath(dir, id);
      const fd = openSync(path, constants.O_RDWR);
      try {
        const size = fstatSync(fd).size;
        const end = scanFile(fd, size, () => {});
        if (!chosen || end > HEADER) chosen = { id, end, size };
      } finally {
        closeSync(fd);
      }
    }
    const pick = chosen ?? { id: ids[0]!, end: HEADER, size: CHUNK };
    const fd = openSync(chunkPath(dir, pick.id), constants.O_RDWR);
    return new FastLog(dir, { id: pick.id, fd, len: pick.end, size: pick.size }, ids[0]!);
  }

  /** Apply every complete record. A torn tail rewinds the active chunk. */
  replay(apply: (op: LogOp) => void) {
    for (const id of chunkIds(this.dir)) {
      const path = chunkPath(this.dir, id);
      const owned = id !== this.active.id;
      const fd = owned ? openSync(path, "r") : this.active.fd;
      try {
        const size = fstatSync(fd).size;
        const good = scanFile(fd, size, (op) => {
          if (op.kind === "insert") this.track(op.id, id);
          else this.untrack(op.id);
          apply(op);
        });
        if (id === this.active.id && good < this.active.len) {
          writeAllAt(fd, headerBuf(good), 0);
          fdatasyncSync(fd);
          this.active.len = good;
        }
      } finally {
        if (owned) closeSync(fd);
      }
    }
  }

  /**
   * Append these rows and fsync them. The length moves only after the sync,
   * so a failed sync retries the same region.
   */
  syncRows(inserts: LogInsert[], deletes: number[]) {
    if (inserts.length === 0 && deletes.length === 0) return;
    const buf = encodeRows(deletes, inserts);
    if (this.active.len + buf.length > this.active.size) {
      this.rotate(Math.max(CHUNK, HEADER + buf.length));
    }
    if (this.active.len + buf.length > this.active.size) {
      throw new Error("durable log chunk is too small");
    }
    const newLen = this.active.len + buf.length;
    // One sequential overwrite. Rewriting the header would dirty byte 0 and
    // make this fdatasync flush a second region.
    writeAllAt(this.active.fd, buf, this.active.len);
    fdatasyncSync(this.active.fd);
    this.active.len = newLen;
    for (const id of deletes) this.untrack(id);
    for (const row of inserts) this.track(row.id, this.active.id);
    this.kick();
    this.scheduleRetire();
  }

  /**
   * Live rows that still hold the oldest chunk, once enough chunks sit behind
   * it. The caller appends them again; that insert moves them to the active
   * chunk and the old one can go. At most one chunk's worth of bytes is
   * rewritten per call.
   */
  relocationCandidates(): number[] {
    if (this.active.id - this.oldest <= COMPACT_AFTER) return [];
    const live = this.liveCount.get(this.oldest) ?? 0;
    if (live === 0) return [];
    const { chunk, at } = this.relocating;
    if (chunk === this.oldest && this.active.id - at < COMPACT_AFTER) return [];
    this.relocating = { chunk: this.oldest, at: this.active.id };
    const ids: number[] = [];
    for (const [id, held] of this.liveChunk) {
      if (held === this.oldest) ids.push(id);
    }
    return ids;
  }

  /** Remove rotated chunks from the front while none of their rows are live. */
  retire() {
    let removed = 0;
    while (this.oldest < this.active.id && (this.liveCount.get(this.oldest) ?? 0) === 0) {
      try {
        unlinkSync(chunkPath(this.dir, this.oldest));
      } catch (err) {
        // A chunk left behind must keep every later delete, so stop here.
        if ((err as NodeJS.ErrnoException).code !== "ENOENT") break;
      }
      this.liveCount.delete(this.oldest);
      this.oldest++;
      removed++;
    }
    if (removed === 0) return;
    this.retiredChunks += removed;
    syncDir(this.dir);
  }

  /** Chunk files on disk from the oldest kept through the active one. */
  get keptChunks(): number {
    return this.active.id - this.oldest + 1;
  }

  private track(id: number, chunk: number) {
    this.untrack(id);
    this.liveChunk.set(id, chunk);
    this.liveCount.set(chunk, (this.liveCount.get(chunk) ?? 0) + 1);
  }

  private untrack(id: number) {
    const chunk = this.liveChunk.get(id);
    if (chunk === undefined) return;
    this.liveChunk.delete(id);
    const left = (this.liveCount.get(chunk) ?? 1) - 1;
    if (left > 0) this.liveCount.set(chunk, left);
    else this.liveCount.delete(chunk);
  }

  /** Unlink and the directory fsync run after the confirm callbacks, not inside them. */
  private scheduleRetire() {
    if (this.retireQueued || this.closed) return;
    if (this.oldest >= this.active.id || (this.liveCount.get(this.oldest) ?? 0) > 0) return;
    this.retireQueued = true;
    setImmediate(() => {
      this.retireQueued = false;
      if (!this.closed) this.retire();
    });
  }

  close() {
    this.closed = true;
    try {
      writeAllAt(this.active.fd, headerBuf(this.active.len), 0);
      fdatasyncSync(this.active.fd);
    } catch {
      /* the record scan is the source of truth */
    }
    const deadline = Date.now() + 5000;
    while (Atomics.load(this.view, 0) === 1n && Date.now() < deadline) {
      Atomics.wait(this.view, 0, 1n, 1000);
    }
    this.worker?.terminate();
    this.worker = null;
    try {
      closeSync(this.active.fd);
    } catch {
      /* already closed */
    }
  }

  private rotate(size: number) {
    const nextId = this.active.id + 1;
    const path = chunkPath(this.dir, nextId);
    if (this.preparingId === nextId) {
      const deadline = Date.now() + 5000;
      while (Atomics.load(this.view, 0) === 1n && Date.now() < deadline) {
        Atomics.wait(this.view, 0, 1n, 1000);
      }
    }
    const info = inspect(path);
    // A batch larger than one chunk (relocated rows can be) needs a file at
    // least that big. The preallocated next chunk is only CHUNK bytes.
    const ready = !!info && info.logical === HEADER && info.size >= size;
    const fd = ready ? openSync(path, constants.O_RDWR) : createChunkFile(path, size);
    const fileSize = ready && info ? info.size : size;
    closeSync(this.active.fd);
    this.active = { id: nextId, fd, len: HEADER, size: fileSize };
    this.preparingId = 0;
    this.nextReady = false;
    Atomics.store(this.view, 0, 0n);
  }

  /** Materialize the next chunk on its own file so this fsync stays an overwrite. */
  private kick() {
    if (this.closed) return;
    if (this.active.size - this.active.len >= KICK_REMAINING) return;
    if (this.nextReady) return;
    const state = Atomics.load(this.view, 0);
    if (state === 1n || state === 2n) return;
    const nextId = this.active.id + 1;
    const ready = inspect(chunkPath(this.dir, nextId));
    if (ready && ready.logical === HEADER && ready.size >= CHUNK) {
      this.nextReady = true;
      return;
    }
    this.preparingId = nextId;
    Atomics.store(this.view, 0, 1n);
    if (!this.worker) {
      this.worker = new Worker(new URL("./prealloc-worker.ts", import.meta.url));
      (this.worker as unknown as { unref?: () => void }).unref?.();
    }
    this.worker.postMessage({ path: chunkPath(this.dir, nextId), size: CHUNK, state: this.state });
  }
}

function chunkIds(dir: string): number[] {
  return readdirSync(dir)
    .map((name) => (/^\d+$/.test(name) ? Number(name) : 0))
    .filter((id) => id > 0)
    .sort((a, b) => a - b);
}

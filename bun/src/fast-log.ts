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
 *
 * A removed chunk is reused as a spare next chunk instead of unlinked, so
 * steady traffic does not zero-fill a new 4 MiB file every few milliseconds.
 * The old records stay in it, so every record's crc is salted with the id
 * of the chunk file it was written to (header version 2): renamed to a new
 * id, none of them reads back. Version 1 chunks carry unsalted crcs and are
 * never reused.
 */
import {
  closeSync,
  constants,
  existsSync,
  fdatasyncSync,
  fstatSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readSync,
  readdirSync,
  renameSync,
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
/** Retired chunks kept ahead of the active one for reuse. */
const SPARES = 2;
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

/** A type 3 record as read: the inserts of ids `first` .. `first + queues.length - 1`. */
type GroupOp = { kind: "group"; first: number; vhost: string; meta: string; body: Uint8Array; queues: string[] };

type ActiveChunk = { id: number; fd: number; len: number; size: number; salt: number };

/** Header version of chunks with salted record crcs. */
const SALTED = 2;

/**
 * The crc salt of records in chunk file `id`. A multiply by an odd constant
 * is one to one on 32 bits, so no two chunk ids share a salt, and only id 0
 * (never used) has none.
 */
function saltOf(id: number): number {
  return Math.imul(id, 0x9e3779b1) >>> 0;
}

/** IEEE CRC-32, the same polynomial the table version used. Native, so replay and the group commit do not loop per byte in JS. */
function crc32(buf: Uint8Array): number {
  return Bun.hash.crc32(buf) >>> 0;
}

function chunkPath(dir: string, id: number): string {
  return `${dir}/${String(id).padStart(8, "0")}`;
}

function headerBuf(logical: number, version = SALTED): Buffer {
  const buf = Buffer.alloc(HEADER);
  buf.write("QFHD", 0, 4, "ascii");
  buf.writeUInt32LE(version, 4);
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

function readHeader(fd: number): { logical: number; version: number } | null {
  const buf = Buffer.alloc(HEADER);
  const n = readSync(fd, buf, 0, HEADER, 0);
  if (n < HEADER) return null;
  if (buf.toString("ascii", 0, 4) !== "QFHD") return null;
  const logical = Number(buf.readBigUInt64LE(8));
  if (!Number.isFinite(logical) || logical < HEADER) return null;
  return { logical, version: buf.readUInt32LE(4) };
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

function inspect(path: string): { logical: number; size: number; version: number } | null {
  let fd: number;
  try {
    fd = openSync(path, "r");
  } catch {
    return null;
  }
  try {
    const size = fstatSync(fd).size;
    const header = readHeader(fd);
    if (header == null) return null;
    return { logical: Math.min(header.logical, size), size, version: header.version };
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
  salt: number,
  crcs?: number[],
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
  buf.writeUInt32LE((crc32(buf.subarray(start, o)) ^ salt) >>> 0, o);
  crcs?.push(o);
  return o + 4;
}

/** Magic, type, first id, vhost length, meta length, body length, member count, and crc. */
const GROUP_FIXED = 4 + 1 + 8 + 2 + 4 + 4 + 4 + 4;
/** Members in one group record. A fanout wider than this writes several. */
const MAX_GROUP = 4096;

/**
 * Rows from `i` that can share one record: the same body object, meta and
 * vhost, and consecutive ids. The copies of one fanout publish are such a run.
 */
function groupLength(inserts: LogInsert[], i: number): number {
  const head = inserts[i]!;
  let n = 1;
  while (i + n < inserts.length && n < MAX_GROUP) {
    const row = inserts[i + n]!;
    if (row.body !== head.body || row.meta !== head.meta || row.vhost !== head.vhost || row.id !== head.id + n) break;
    n++;
  }
  return n;
}

/**
 * Write a type 3 record for `n` rows from `from`: the body and meta once,
 * then one queue name per member. `lens` holds each row's vhost, queue and
 * meta byte lengths, three per row.
 */
function writeGroup(buf: Buffer, o: number, rows: LogInsert[], from: number, n: number, lens: number[], salt: number, crcs?: number[]): number {
  const start = o;
  const head = rows[from]!;
  const vhostLen = lens[from * 3]!;
  const metaLen = lens[from * 3 + 2]!;
  buf.writeUInt32LE(MAGIC, o);
  buf[o + 4] = 3;
  buf.writeUInt32LE(head.id >>> 0, o + 5);
  buf.writeUInt32LE(Math.floor(head.id / 0x1_0000_0000), o + 9);
  o += 13;
  buf.writeUInt16LE(vhostLen, o);
  o += 2;
  if (vhostLen) o += buf.write(head.vhost, o, vhostLen, "utf8");
  buf.writeUInt32LE(metaLen, o);
  o += 4;
  if (metaLen) o += buf.write(head.meta, o, metaLen, "utf8");
  buf.writeUInt32LE(head.body.length, o);
  o += 4;
  buf.set(head.body, o);
  o += head.body.length;
  buf.writeUInt32LE(n, o);
  o += 4;
  for (let k = 0; k < n; k++) {
    const queueLen = lens[(from + k) * 3 + 1]!;
    buf.writeUInt16LE(queueLen, o);
    o += 2;
    if (queueLen) o += buf.write(rows[from + k]!.queue, o, queueLen, "utf8");
  }
  buf.writeUInt32LE((crc32(buf.subarray(start, o)) ^ salt) >>> 0, o);
  crcs?.push(o);
  return o + 4;
}

const EMPTY = new Uint8Array(0);

/**
 * One buffer for a whole group commit, sized first, then written in one pass.
 * Deletes come first: a replay applies them before this group's inserts.
 * Rows that share a body (see {@link groupLength}) share one type 3 record,
 * so a fanout to 20 queues writes its body once instead of 20 times.
 *
 * @param starts Filled with each insert's record offset in the buffer.
 * @param ends Filled with each insert's record end. Members of one group share both.
 * @param salt The crc salt of the chunk the buffer is written to.
 * @param crcs Filled with each record's crc offset, for {@link resalt}.
 */
function encodeRows(deletes: number[], inserts: LogInsert[], starts?: number[], ends?: number[], salt = 0, crcs?: number[]): Buffer {
  let total = deletes.length * RECORD_FIXED;
  const lens = new Array<number>(inserts.length * 3);
  const runs: number[] = [];
  for (let i = 0; i < inserts.length;) {
    const n = groupLength(inserts, i);
    runs.push(n);
    const head = inserts[i]!;
    const v = Buffer.byteLength(head.vhost, "utf8");
    const m = Buffer.byteLength(head.meta, "utf8");
    lens[i * 3] = v;
    lens[i * 3 + 2] = m;
    if (n === 1) {
      const q = Buffer.byteLength(head.queue, "utf8");
      lens[i * 3 + 1] = q;
      total += RECORD_FIXED + v + q + m + head.body.length;
    } else {
      total += GROUP_FIXED + v + m + head.body.length;
      for (let k = 0; k < n; k++) {
        const q = Buffer.byteLength(inserts[i + k]!.queue, "utf8");
        lens[(i + k) * 3 + 1] = q;
        total += 2 + q;
      }
    }
    i += n;
  }
  const buf = Buffer.allocUnsafe(total);
  let o = 0;
  for (const id of deletes) o = writeRecord(buf, o, 2, id, "", 0, "", 0, "", 0, EMPTY, salt, crcs);
  let i = 0;
  for (const n of runs) {
    const at = o;
    if (n === 1) {
      const row = inserts[i]!;
      o = writeRecord(buf, o, 1, row.id, row.vhost, lens[i * 3]!, row.queue, lens[i * 3 + 1]!, row.meta, lens[i * 3 + 2]!, row.body, salt, crcs);
    } else {
      o = writeGroup(buf, o, inserts, i, n, lens, salt, crcs);
    }
    for (let k = 0; k < n; k++) {
      starts?.push(at);
      ends?.push(o);
    }
    i += n;
  }
  return buf;
}

/** Move the records of an encoded batch from one chunk's salt to another's. */
function resalt(buf: Buffer, crcs: number[], from: number, to: number) {
  const delta = (from ^ to) >>> 0;
  if (delta === 0) return;
  for (const at of crcs) buf.writeUInt32LE((buf.readUInt32LE(at) ^ delta) >>> 0, at);
}

/**
 * `copy` false keeps the body a view of `buf`, for a buffer that holds this one record.
 * A record reads with the chunk's `salt` or with none: a version 1 chunk's
 * records have none, and a reused file's old records carry another chunk's.
 */
function parseRecord(buf: Buffer, pos: number, limit: number, salt: number, copy = true): { next: number; op: LogOp | GroupOp } | null {
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
  if (type === 3) return parseGroup(buf, pos, o, limit, id, vhost, salt, copy);
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
  const body = copy ? Buffer.from(buf.subarray(o, o + bodyLen)) : buf.subarray(o, o + bodyLen);
  o += bodyLen;
  const crc = buf.readUInt32LE(o);
  o += 4;
  if (!crcMatches(buf.subarray(pos, o - 4), crc, salt)) return null;
  if (type !== 1 && type !== 2) return null;
  const op: LogOp =
    type === 2 ? { kind: "delete", id } : { kind: "insert", id, vhost, queue, meta, body };
  return { next: o, op };
}

/** The rest of a type 3 record from `o`, just past its vhost. See {@link writeGroup}. */
function parseGroup(buf: Buffer, pos: number, o: number, limit: number, first: number, vhost: string, salt: number, copy: boolean): { next: number; op: GroupOp } | null {
  const need = (n: number) => o + n <= limit;
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
  const body = copy ? Buffer.from(buf.subarray(o, o + bodyLen)) : buf.subarray(o, o + bodyLen);
  o += bodyLen;
  const count = buf.readUInt32LE(o);
  o += 4;
  if (count < 1 || count > MAX_GROUP) return null;
  const queues = new Array<string>(count);
  for (let k = 0; k < count; k++) {
    if (!need(2)) return null;
    const queueLen = buf.readUInt16LE(o);
    o += 2;
    if (queueLen > MAX_NAME || !need(queueLen)) return null;
    queues[k] = buf.toString("utf8", o, o + queueLen);
    o += queueLen;
  }
  if (!need(4)) return null;
  const crc = buf.readUInt32LE(o);
  o += 4;
  if (!crcMatches(buf.subarray(pos, o - 4), crc, salt)) return null;
  return { next: o, op: { kind: "group", first, vhost, meta, body, queues } };
}

function crcMatches(record: Uint8Array, crc: number, salt: number): boolean {
  const sum = crc32(record);
  return sum === crc || ((sum ^ salt) >>> 0) === crc;
}

/** Each record from the header on, with a type 3 record expanded into its inserts. */
function scanFile(fd: number, logical: number, salt: number, apply: (op: LogOp, at: number, end: number) => void): number {
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
    const rec = parseRecord(buf, pos, limit, salt);
    if (!rec) return pos;
    const op = rec.op;
    if (op.kind === "group") {
      for (let k = 0; k < op.queues.length; k++) {
        apply({ kind: "insert", id: op.first + k, vhost: op.vhost, queue: op.queues[k]!, meta: op.meta, body: op.body }, pos, rec.next);
      }
    } else {
      apply(op, pos, rec.next);
    }
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

/**
 * Row id to the chunk, offset and length of its latest insert record.
 *
 * One Map entry per row names a slot in three typed arrays. Three Maps, one
 * per field, were about 135 bytes per queued message.
 */
export class LiveIndex {
  private readonly slots = new Map<number, number>();
  private readonly free: number[] = [];
  private used = 0;
  chunk = new Uint32Array(1024);
  at = new Uint32Array(1024);
  len = new Uint32Array(1024);

  get size(): number {
    return this.slots.size;
  }

  has(id: number): boolean {
    return this.slots.has(id);
  }

  /** The slot of `id` in {@link chunk}, {@link at} and {@link len}. */
  slot(id: number): number | undefined {
    return this.slots.get(id);
  }

  set(id: number, chunk: number, at: number, len: number) {
    let slot = this.slots.get(id);
    if (slot === undefined) {
      slot = this.free.pop() ?? this.used++;
      if (slot >= this.chunk.length) this.grow();
      this.slots.set(id, slot);
    }
    this.chunk[slot] = chunk;
    this.at[slot] = at;
    this.len[slot] = len;
  }

  /** Forget `id`. Returns the chunk it pointed at, or undefined when it was not live. */
  delete(id: number): number | undefined {
    const slot = this.slots.get(id);
    if (slot === undefined) return undefined;
    this.slots.delete(id);
    const chunk = this.chunk[slot];
    // A drained backlog gives its arrays back.
    if (this.slots.size === 0) {
      this.free.length = 0;
      this.used = 0;
      if (this.chunk.length > 1024) {
        this.chunk = new Uint32Array(1024);
        this.at = new Uint32Array(1024);
        this.len = new Uint32Array(1024);
      }
    } else {
      this.free.push(slot);
    }
    return chunk;
  }

  ids(): IterableIterator<number> {
    return this.slots.keys();
  }

  entries(): IterableIterator<[number, number]> {
    return this.slots.entries();
  }

  private grow() {
    const size = this.chunk.length * 2;
    for (const field of ["chunk", "at", "len"] as const) {
      const next = new Uint32Array(size);
      next.set(this[field]);
      this[field] = next;
    }
  }
}

export class FastLog {
  private active: ActiveChunk;
  /** Lowest chunk id still on disk. */
  private oldest: number;
  /** Row id to its latest insert record, for rows without a synced delete. */
  private readonly live = new LiveIndex();
  /** Chunk id to the number of rows in `live` that point at it. */
  private readonly liveCount = new Map<number, number>();
  /** Inserts ever written to each chunk, live or not. Relocation compares against it. */
  private readonly chunkInserts = new Map<number, number>();
  /** Read descriptors of rotated chunks, for rows paged out of memory. */
  private readonly readFds = new Map<number, number>();
  private retireQueued = false;
  /** The oldest chunk whose rows were last handed out to be written forward, and when. */
  private relocating = { chunk: 0, at: 0 };
  /** Chunks removed since open. */
  retiredChunks = 0;
  /** Of those, chunks renamed ahead of the active one for reuse. */
  recycledChunks = 0;
  /** Highest chunk id on disk, spares included. */
  private ahead = 0;
  /** Rotations that left the old chunk's fsync to the sync worker. */
  handOffs = 0;
  /**
   * Waits on a worker past `stallWarnMs`. The wait goes on: giving up would
   * fail confirms the worker did sync, or write into a chunk it still fills.
   */
  workerStalls = 0;
  stallWarnMs = 5000;
  /** Tests only: each worker holds every sync or chunk this long. */
  workerDelayMs = 0;
  private worker: Worker | null = null;
  private preparingId = 0;
  private closed = false;
  /** Next chunk is already on disk. Avoids an open/stat on every confirm. */
  private nextReady = false;
  private readonly state = new SharedArrayBuffer(8);
  private readonly view = new BigInt64Array(this.state);
  /**
   * Batches written but not yet covered by an fdatasync, oldest first. Their
   * rows are tracked only once synced, so a chunk is never removed while the
   * relocated copy of one of its rows could still be lost.
   */
  private unsynced: Array<{ seq: bigint; chunk: number; at: number; inserts: number[]; starts: number[]; ends: number[]; deletes: number[] }> = [];
  private writtenSeq = 0n;
  private syncedSeq = 0n;
  private syncWorker: Worker | null = null;
  /** requested, done, fd, failed, previous chunk fd. See `sync-worker.ts`. */
  private readonly syncState = new BigInt64Array(new SharedArrayBuffer(40));
  /** Called once per completed sync with the highest batch it covered. */
  onSynced: ((seq: bigint, ok: boolean) => void) | null = null;

  private constructor(private readonly dir: string, active: ActiveChunk, oldest: number, private readonly unsalted = new Set<number>(), ahead = active.id) {
    this.active = active;
    this.oldest = Math.min(oldest, active.id);
    this.ahead = Math.max(ahead, active.id);
    Atomics.store(this.view, 0, 0n);
  }

  /** The header version chunk `id` was written with. A version 1 chunk is never reused. */
  private versionOf(id: number): number {
    return this.unsalted.has(id) ? 1 : SALTED;
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
    // A fresh log writes its chunks on first use (see `prepare`). A process
    // that never stores a message keeps 32 MiB of zeros out of its page cache.
    if (ids.length === 0) return new FastLog(dir, { id: 1, fd: -1, len: HEADER, size: CHUNK, salt: saltOf(1) }, 1);
    // The header length is a hint. Confirms fsync the record bytes and leave
    // the header page clean, so the real end is the last valid record.
    const unsalted = new Set<number>();
    let chosen: { id: number; end: number; size: number } | null = null;
    for (const id of ids) {
      const path = chunkPath(dir, id);
      const fd = openSync(path, constants.O_RDWR);
      try {
        const size = fstatSync(fd).size;
        if ((readHeader(fd)?.version ?? SALTED) < SALTED) unsalted.add(id);
        const end = scanFile(fd, size, saltOf(id), () => {});
        if (!chosen || end > HEADER) chosen = { id, end, size };
      } finally {
        closeSync(fd);
      }
    }
    const pick = chosen ?? { id: ids[0]!, end: HEADER, size: CHUNK };
    const fd = openSync(chunkPath(dir, pick.id), constants.O_RDWR);
    return new FastLog(dir, { id: pick.id, fd, len: pick.end, size: pick.size, salt: saltOf(pick.id) }, ids[0]!, unsalted, ids[ids.length - 1]!);
  }

  /** Apply every complete record. A torn tail rewinds the active chunk. */
  replay(apply: (op: LogOp) => void) {
    for (const id of chunkIds(this.dir)) {
      const path = chunkPath(this.dir, id);
      const owned = id !== this.active.id;
      const fd = owned ? openSync(path, "r") : this.active.fd;
      try {
        const size = fstatSync(fd).size;
        const good = scanFile(fd, size, saltOf(id), (op, at, end) => {
          if (op.kind === "insert") this.track(op.id, id, at, end - at);
          else this.untrack(op.id);
          apply(op);
        });
        if (id === this.active.id && good < this.active.len) {
          writeAllAt(fd, headerBuf(good, this.versionOf(id)), 0);
          fdatasyncSync(fd);
          this.active.len = good;
        }
      } finally {
        if (owned) closeSync(fd);
      }
    }
  }

  /**
   * Write the fresh log's first {@link START_CHUNKS} chunks. Called when this
   * process gets a durable queue, so the zero-fill happens at declare time,
   * and again before every append in case a row arrives without one.
   */
  prepare() {
    if (this.active.fd >= 0 || this.closed) return;
    let fd = -1;
    for (let id = 1; id <= START_CHUNKS; id++) {
      const created = createChunkFile(chunkPath(this.dir, id), CHUNK);
      if (id === 1) fd = created;
      else closeSync(created);
    }
    this.active.fd = fd;
    this.ahead = Math.max(this.ahead, START_CHUNKS);
    this.nextReady = true;
  }

  /** Recovery index: sqlite is a cache once this log is authoritative. */
  hasLiveMessage(id: number): boolean {
    return this.live.has(id);
  }

  /**
   * Read a synced row back from its insert record, for a row whose body was
   * dropped from memory. Null when the row is not live in a synced batch.
   */
  readRow(id: number): LogInsert | null {
    const slot = this.live.slot(id);
    if (slot === undefined) return null;
    const chunk = this.live.chunk[slot]!;
    const at = this.live.at[slot]!;
    const len = this.live.len[slot]!;
    let fd = chunk === this.active.id ? this.active.fd : this.readFds.get(chunk);
    if (fd === undefined) {
      fd = openSync(chunkPath(this.dir, chunk), "r");
      this.readFds.set(chunk, fd);
    }
    // Its own allocation, not the shared pool: the body below is a view of it.
    const buf = Buffer.allocUnsafeSlow(len);
    let got = 0;
    while (got < len) {
      const n = readSync(fd, buf, got, len - got, at + got);
      if (n <= 0) break;
      got += n;
    }
    const rec = got === len ? parseRecord(buf, 0, len, saltOf(chunk), false) : null;
    if (!rec) return null;
    const op = rec.op;
    // `body` is a view of this one-record buffer, so it pins nothing else.
    if (op.kind === "group") {
      const member = id - op.first;
      if (member < 0 || member >= op.queues.length) return null;
      return { id, vhost: op.vhost, queue: op.queues[member]!, meta: op.meta, body: op.body };
    }
    if (op.kind !== "insert" || op.id !== id) return null;
    return { id, vhost: op.vhost, queue: op.queue, meta: op.meta, body: op.body };
  }

  /** Used when migrating the current sqlite state back into this log. */
  liveMessageIds(): IterableIterator<number> {
    return this.live.ids();
  }

  /**
   * Append these rows and fsync them. The length moves only after the sync,
   * so a failed sync retries the same region.
   */
  syncRows(inserts: LogInsert[], deletes: number[]) {
    if (inserts.length === 0 && deletes.length === 0) return;
    this.prepare();
    const starts: number[] = [];
    const ends: number[] = [];
    const crcs: number[] = [];
    const salt = this.active.salt;
    const buf = encodeRows(deletes, inserts, starts, ends, salt, crcs);
    this.makeRoom(buf.length);
    resalt(buf, crcs, salt, this.active.salt);
    const at = this.active.len;
    // One sequential overwrite. Rewriting the header would dirty byte 0 and
    // make this fdatasync flush a second region.
    writeAllAt(this.active.fd, buf, at);
    fdatasyncSync(this.active.fd);
    this.active.len = at + buf.length;
    // This sync also covers every batch written before it.
    this.unsynced.push({ seq: ++this.writtenSeq, chunk: this.active.id, at, inserts: inserts.map((row) => row.id), starts, ends, deletes: [...deletes] });
    this.kick();
    this.completeThrough(this.writtenSeq, true);
  }

  /** False once a background sync failed. The caller then syncs inline. */
  get canAppend(): boolean {
    return !this.closed && Atomics.load(this.syncState, 3) === 0n;
  }

  /** Every batch is covered by a completed sync. */
  get settled(): boolean {
    return this.syncedSeq === this.writtenSeq;
  }

  /**
   * Append these rows and sync them on the sync worker.
   *
   * The event loop keeps reading and delivering during the fsync. Batches
   * written while one sync runs share the next. `onSynced` reports each
   * completion; nothing here is durable before it.
   *
   * @returns The batch sequence number that `onSynced` will cover.
   */
  appendRows(inserts: LogInsert[], deletes: number[]): bigint {
    if (inserts.length === 0 && deletes.length === 0) return this.writtenSeq;
    this.prepare();
    const starts: number[] = [];
    const ends: number[] = [];
    const crcs: number[] = [];
    const salt = this.active.salt;
    const buf = encodeRows(deletes, inserts, starts, ends, salt, crcs);
    this.makeRoom(buf.length, true);
    // A rotation moved the batch to a chunk with another salt.
    resalt(buf, crcs, salt, this.active.salt);
    const at = this.active.len;
    writeAllAt(this.active.fd, buf, at);
    this.active.len += buf.length;
    const seq = ++this.writtenSeq;
    this.unsynced.push({ seq, chunk: this.active.id, at, inserts: inserts.map((row) => row.id), starts, ends, deletes: [...deletes] });
    this.kick();
    if (!this.syncWorker) {
      this.syncWorker = new Worker(new URL("./sync-worker.ts", import.meta.url));
      (this.syncWorker as unknown as { unref?: () => void }).unref?.();
      this.syncWorker.onmessage = (event: MessageEvent<bigint>) => {
        this.completeThrough(event.data, Atomics.load(this.syncState, 3) === 0n);
      };
      this.syncWorker.postMessage({ state: this.syncState.buffer, delayMs: this.workerDelayMs });
    }
    // The fd before the sequence: the worker reads them in the other order.
    Atomics.store(this.syncState, 2, BigInt(this.active.fd));
    Atomics.store(this.syncState, 0, seq);
    Atomics.notify(this.syncState, 0);
    return seq;
  }

  /**
   * Rotate before a batch that does not fit.
   *
   * The inline path first lets the sync worker finish with the old fd. A
   * worker batch hands the old chunk over instead: the next request syncs it
   * before the new chunk, so its batches are covered in order without this
   * thread waiting on that fsync. Only one chunk is handed over at a time.
   */
  private makeRoom(bytes: number, aside = false) {
    if (this.active.len + bytes > this.active.size) {
      const handOff = aside && !!this.syncWorker && !this.settled && Atomics.load(this.syncState, 4) === 0n;
      if (!handOff) this.drainWorker();
      this.rotate(Math.max(CHUNK, HEADER + bytes), handOff);
    }
    if (this.active.len + bytes > this.active.size) {
      throw new Error("durable log chunk is too small");
    }
  }

  /** Block until the sync worker covered every request, then report it. */
  private drainWorker(deadlineMs = Infinity) {
    if (!this.syncWorker) return;
    const want = Atomics.load(this.syncState, 0);
    this.waitWorker(() => Atomics.load(this.syncState, 1) < want, this.syncState, 1, "sync", deadlineMs);
    const done = Atomics.load(this.syncState, 1);
    this.completeThrough(done, done >= want && Atomics.load(this.syncState, 3) === 0n);
  }

  /**
   * Wait while `busy` holds, waking on changes to `state[slot]`. A stall past
   * `stallWarnMs` is logged and counted; only `close` passes a deadline.
   */
  private waitWorker(busy: () => boolean, state: BigInt64Array, slot: number, what: string, deadlineMs: number) {
    const started = Date.now();
    let warnAt = started + this.stallWarnMs;
    let stalled = false;
    while (busy()) {
      const now = Date.now();
      if (now - started >= deadlineMs) return;
      if (now >= warnAt) {
        if (!stalled) this.workerStalls++;
        stalled = true;
        console.error(`queueforge-bun: durable log ${what} worker busy for ${now - started} ms`);
        warnAt = now + Math.max(this.stallWarnMs, 1000);
      }
      Atomics.wait(state, slot, Atomics.load(state, slot), Math.max(1, Math.min(1000, warnAt - now)));
    }
  }

  /** Track the rows of every batch through `seq`, in order, then report it once. */
  private completeThrough(seq: bigint, ok: boolean) {
    if (seq <= this.syncedSeq) return;
    this.syncedSeq = seq;
    while (this.unsynced.length && this.unsynced[0]!.seq <= seq) {
      const batch = this.unsynced.shift()!;
      // A failed sync still keeps its chunk: those rows remain live in memory.
      for (const id of batch.deletes) this.untrack(id);
      for (let i = 0; i < batch.inserts.length; i++) {
        const start = batch.starts[i]!;
        this.track(batch.inserts[i]!, batch.chunk, batch.at + start, batch.ends[i]! - start);
      }
    }
    if (ok) this.scheduleRetire();
    this.onSynced?.(seq, ok);
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
    // A mostly live chunk is a backlog, not a few stragglers. Writing it
    // forward frees nothing and rewrote the whole backlog every 16 chunks.
    if (live * 4 > (this.chunkInserts.get(this.oldest) ?? live)) return [];
    const { chunk, at } = this.relocating;
    if (chunk === this.oldest && this.active.id - at < COMPACT_AFTER) return [];
    this.relocating = { chunk: this.oldest, at: this.active.id };
    const ids: number[] = [];
    for (const [id, slot] of this.live.entries()) {
      if (this.live.chunk[slot] === this.oldest) ids.push(id);
    }
    return ids;
  }

  /**
   * Remove rotated chunks from the front while none of their rows are live.
   *
   * Rows count as live only once their batch is synced, so a chunk that still
   * holds an unsynced batch stays: after a hand-off the worker can cover an
   * earlier delete batch first, and unlinking then lost the insert written
   * after it, whose confirm was still to come.
   *
   * A removed version 2 chunk is renamed ahead of the active one while fewer
   * than {@link SPARES} wait there, instead of unlinked.
   */
  retire() {
    let removed = 0;
    const firstUnsynced = this.unsynced[0]?.chunk ?? Infinity;
    while (this.oldest < this.active.id && this.oldest < firstUnsynced && (this.liveCount.get(this.oldest) ?? 0) === 0) {
      const fd = this.readFds.get(this.oldest);
      if (fd !== undefined) {
        try {
          closeSync(fd);
        } catch {
          /* already closed */
        }
        this.readFds.delete(this.oldest);
      }
      if (!this.recycle(this.oldest)) {
        try {
          unlinkSync(chunkPath(this.dir, this.oldest));
        } catch (err) {
          // A chunk left behind must keep every later delete, so stop here.
          if ((err as NodeJS.ErrnoException).code !== "ENOENT") break;
        }
      }
      this.liveCount.delete(this.oldest);
      this.chunkInserts.delete(this.oldest);
      this.unsalted.delete(this.oldest);
      this.oldest++;
      removed++;
    }
    if (removed === 0) return;
    this.retiredChunks += removed;
    syncDir(this.dir);
  }

  /**
   * Rename removable chunk `id` to the next free id ahead of the active one.
   *
   * Its records stay in the file, salted with `id`, so none of them reads
   * back under the new name. A crash before the rename leaves it where it
   * was: rows whose deletes are all in later chunks, as an unlink that did
   * not reach the disk does. The header goes back to an empty chunk so
   * `rotate` takes the file as ready; if that write is lost, `rotate` writes
   * the file again.
   */
  private recycle(id: number): boolean {
    if (this.unsalted.has(id)) return false;
    const top = Math.max(this.ahead, this.preparingId, this.active.id);
    if (top - this.active.id >= SPARES) return false;
    const to = top + 1;
    const from = chunkPath(this.dir, id);
    const dest = chunkPath(this.dir, to);
    if (existsSync(dest)) return false;
    let fd = -1;
    try {
      fd = openSync(from, constants.O_RDWR);
      if (fstatSync(fd).size < CHUNK) return false;
      writeAllAt(fd, headerBuf(HEADER), 0);
    } catch {
      return false;
    } finally {
      if (fd >= 0) closeSync(fd);
    }
    try {
      renameSync(from, dest);
    } catch {
      return false;
    }
    this.ahead = to;
    this.recycledChunks++;
    return true;
  }

  /** Chunk files on disk from the oldest kept through the active one. */
  get keptChunks(): number {
    return this.active.id - this.oldest + 1;
  }

  private track(id: number, chunk: number, at: number, len: number) {
    this.untrack(id);
    this.live.set(id, chunk, at, len);
    this.liveCount.set(chunk, (this.liveCount.get(chunk) ?? 0) + 1);
    this.chunkInserts.set(chunk, (this.chunkInserts.get(chunk) ?? 0) + 1);
  }

  private untrack(id: number) {
    const chunk = this.live.delete(id);
    if (chunk === undefined) return;
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
    // A worker that never returns must not hold up the shutdown.
    this.drainWorker(5000);
    this.closed = true;
    for (const fd of this.readFds.values()) {
      try {
        closeSync(fd);
      } catch {
        /* already closed */
      }
    }
    this.readFds.clear();
    if (this.syncWorker) {
      Atomics.store(this.syncState, 0, -1n);
      Atomics.notify(this.syncState, 0);
      this.syncWorker.terminate();
      this.syncWorker = null;
    }
    if (this.active.fd < 0) return;
    try {
      writeAllAt(this.active.fd, headerBuf(this.active.len, this.versionOf(this.active.id)), 0);
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

  private rotate(size: number, handOff = false) {
    const nextId = this.active.id + 1;
    const path = chunkPath(this.dir, nextId);
    if (this.preparingId === nextId) {
      // Creating the file here while the worker still writes it would let its
      // zeros land over records already confirmed.
      this.waitWorker(() => Atomics.load(this.view, 0) === 1n, this.view, 0, "prealloc", Infinity);
    }
    const info = inspect(path);
    // A batch larger than one chunk (relocated rows can be) needs a file at
    // least that big. The preallocated next chunk is only CHUNK bytes. A
    // version 1 file is written again: its records carry no salt.
    const ready = !!info && info.logical === HEADER && info.size >= size && info.version >= SALTED;
    const fd = ready ? openSync(path, constants.O_RDWR) : createChunkFile(path, size);
    const fileSize = ready && info ? info.size : size;
    // The sync worker still owes this chunk an fsync. It syncs and closes it.
    // The new fd goes into FD first: a worker that takes the old one from
    // PREVIOUS and closes it then reads FD next. With the old fd still there
    // (appendRows stored the new one only after its write), its fdatasync
    // hit EBADF, FAILED latched, and every waiting confirm was refused.
    if (handOff) {
      Atomics.store(this.syncState, 2, BigInt(fd));
      Atomics.store(this.syncState, 4, BigInt(this.active.fd));
      this.handOffs++;
    }
    else closeSync(this.active.fd);
    this.unsalted.delete(nextId);
    this.active = { id: nextId, fd, len: HEADER, size: fileSize, salt: saltOf(nextId) };
    this.ahead = Math.max(this.ahead, nextId);
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
    if (ready && ready.logical === HEADER && ready.size >= CHUNK && ready.version >= SALTED) {
      this.nextReady = true;
      return;
    }
    this.preparingId = nextId;
    Atomics.store(this.view, 0, 1n);
    if (!this.worker) {
      this.worker = new Worker(new URL("./prealloc-worker.ts", import.meta.url));
      (this.worker as unknown as { unref?: () => void }).unref?.();
    }
    this.worker.postMessage({ path: chunkPath(this.dir, nextId), size: CHUNK, state: this.state, delayMs: this.workerDelayMs });
  }
}

function chunkIds(dir: string): number[] {
  return readdirSync(dir)
    .map((name) => (/^\d+$/.test(name) ? Number(name) : 0))
    .filter((id) => id > 0)
    .sort((a, b) => a - b);
}

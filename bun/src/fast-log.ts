/**
 * Preallocated overwrite log for classic durable confirms.
 *
 * sqlite stays `synchronous=OFF` and is the read cache. The confirm waits on
 * an `fdatasync` of a chunk whose blocks were already written and synced, so
 * the flush does not allocate. A crash replays this log back into sqlite.
 *
 * Each chunk is its own file. The next chunk is materialized on another
 * thread, so that work does not dirty the file the confirm is syncing.
 */
import {
  closeSync,
  constants,
  fdatasyncSync,
  fstatSync,
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

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

function crc32(buf: Uint8Array): number {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i++) c = (CRC_TABLE[(c ^ buf[i]!) & 0xff]! ^ (c >>> 8)) >>> 0;
  return (c ^ 0xffffffff) >>> 0;
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

function encodeRecord(
  type: number,
  id: number,
  vhost: string,
  queue: string,
  meta: string,
  body: Uint8Array,
): Buffer {
  const vhostBytes = Buffer.from(vhost);
  const queueBytes = Buffer.from(queue);
  const metaBytes = Buffer.from(meta);
  const bodyBytes = Buffer.from(body);
  const len =
    4 + 1 + 8 + 2 + vhostBytes.length + 2 + queueBytes.length + 4 + metaBytes.length + 4 + bodyBytes.length + 4;
  const buf = Buffer.allocUnsafe(len);
  let o = 0;
  buf.write("QFML", o, 4, "ascii");
  o += 4;
  buf.writeUInt8(type, o);
  o += 1;
  buf.writeBigUInt64LE(BigInt(id), o);
  o += 8;
  buf.writeUInt16LE(vhostBytes.length, o);
  o += 2;
  vhostBytes.copy(buf, o);
  o += vhostBytes.length;
  buf.writeUInt16LE(queueBytes.length, o);
  o += 2;
  queueBytes.copy(buf, o);
  o += queueBytes.length;
  buf.writeUInt32LE(metaBytes.length, o);
  o += 4;
  metaBytes.copy(buf, o);
  o += metaBytes.length;
  buf.writeUInt32LE(bodyBytes.length, o);
  o += 4;
  bodyBytes.copy(buf, o);
  o += bodyBytes.length;
  buf.writeUInt32LE(crc32(buf.subarray(0, o)), o);
  return buf;
}

function encodeRows(deletes: number[], inserts: LogInsert[]): Buffer {
  const parts: Buffer[] = [];
  for (const id of deletes) parts.push(encodeRecord(2, id, "", "", "", Buffer.alloc(0)));
  for (const row of inserts) parts.push(encodeRecord(1, row.id, row.vhost, row.queue, row.meta, row.body));
  return Buffer.concat(parts);
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

export class FastLog {
  private active: ActiveChunk;
  private worker: Worker | null = null;
  private preparingId = 0;
  private closed = false;
  /** Next chunk is already on disk. Avoids an open/stat on every confirm. */
  private nextReady = false;
  private readonly state = new SharedArrayBuffer(8);
  private readonly view = new BigInt64Array(this.state);

  private constructor(private readonly dir: string, active: ActiveChunk) {
    this.active = active;
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
      const log = new FastLog(dir, { id: 1, fd, len: HEADER, size: CHUNK });
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
    return new FastLog(dir, { id: pick.id, fd, len: pick.end, size: pick.size });
  }

  /** Apply every complete record. A torn tail rewinds the active chunk. */
  replay(apply: (op: LogOp) => void) {
    for (const id of chunkIds(this.dir)) {
      const path = chunkPath(this.dir, id);
      const owned = id !== this.active.id;
      const fd = owned ? openSync(path, "r") : this.active.fd;
      try {
        const size = fstatSync(fd).size;
        const good = scanFile(fd, size, apply);
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
    this.kick();
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
    const ready = !!info && info.logical === HEADER && info.size >= Math.min(size, CHUNK);
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

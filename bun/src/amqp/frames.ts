/**
 * AMQP frame read and write for one connection.
 *
 * Owns the outbound queue and the inbound byte buffer. Method bodies live
 * in the sibling files; this file only frames them.
 */
import { Amqp10Session } from "../amqp10/session.ts";
import { method, methodFrame, R, writeTable } from "../codec.ts";
import { ChanError } from "../broker/index.ts";
import { Conn, principalToWire } from "./listen.ts";
import { replyWorker } from "./reply.ts";

/**
 * The `version` server property: the RabbitMQ release whose behavior this
 * broker follows. Clients pick features from it. PerfTest declares transient
 * queues for anything below 4.3, which 4.3 itself refuses. Rust and PHP send
 * the same value; `queueforge_version` is this build.
 */
const SERVER_VERSION = "4.3.0";
const QUEUEFORGE_VERSION = "0.1.0";

/** Every AMQP frame ends with this octet. A mismatch closes the connection. */
const FRAME_END = 0xce;

/**
 * A body frame at least this large is streamed into its channel's body buffer
 * while it arrives; only a channel with one (`DIRECT_BODY` in publish.ts)
 * accepts it.
 */
const STREAM_FRAME = 16 * 1024;
const EMPTY_BODY = new Uint8Array(0);

/**
 * Queue one frame behind any write already in flight.
 *
 * @param frame A complete frame, including its end octet.
 * @returns The write chain. Later frames wait on this promise.
 */
export function send(this: Conn, frame: Uint8Array) {
  // One frame, nothing waiting on the socket: stage it without the array
  // `sendMany` takes. This is every delivery and every confirm.
  if (!this.closed && this.writesPending === 0 && this.outHead >= this.outbound.length) {
    this.staged.push(frame);
    this.stagedBytes += frame.length;
    if (this.stagedBytes >= COALESCE_LIMIT) flushCoalesced.call(this);
    else armCoalesce.call(this);
    return SENT;
  }
  return this.sendMany([frame]);
}

/** First arena of a connection; it doubles up to {@link ARENA_MAX} while full. */
const ARENA_MIN = 4 * 1024;
const ARENA_MAX = 64 * 1024;

/**
 * `n` writable bytes for one outbound frame, from the connection's arena
 * when it fits (see `Conn.arena`). The caller fills every byte.
 */
export function outFrame(conn: Conn, n: number): Uint8Array {
  if (n > ARENA_MAX >> 2) return new Uint8Array(n);
  let arena = conn.arena;
  if (arena === null || arena.length - conn.arenaPos < n) {
    const size = arena === null ? ARENA_MIN : Math.min(ARENA_MAX, arena.length * 2);
    // Every byte of a frame is written before it is sent, so no zero fill.
    arena = Buffer.allocUnsafeSlow(Math.max(size, n * 2));
    conn.arena = arena;
    conn.arenaPos = 0;
  }
  const at = conn.arenaPos;
  conn.arenaPos = at + n;
  return arena.subarray(at, at + n);
}

/** All of `parts` as one view when they sit back to back in one buffer, else null. */
function contiguous(parts: Uint8Array[], n: number): Uint8Array | null {
  const first = parts[0]!;
  const buffer = first.buffer;
  let end = first.byteOffset + first.length;
  for (let i = 1; i < parts.length; i++) {
    const part = parts[i]!;
    if (part.buffer !== buffer || part.byteOffset !== end) return null;
    end += part.length;
  }
  return new Uint8Array(buffer, first.byteOffset, n);
}

/** The arena may be written from its start again: no staged or queued frame can still point into it. */
function rewindArena(conn: Conn) {
  if (conn.reuseWrites && conn.arena !== null && conn.staged.length === 0 && conn.writesPending === 0 && conn.outHead >= conn.outbound.length) {
    conn.arenaPos = 0;
  }
}

/** Bytes gathered before one write. A larger burst still flushes in order. */
const COALESCE_LIMIT = 64 * 1024;

/** What a staged write returns: nothing to wait for, so one shared promise. */
export const SENT: Promise<void> = Promise.resolve();

/**
 * One gather buffer for the whole process. A write finishes synchronously, so
 * it is free again when `flushCoalesced` returns; a partial write copies its
 * tail out. A fresh zero-filled buffer per flush showed up in the profile.
 */
let gather = new Uint8Array(4 * COALESCE_LIMIT);
const GATHER_MAX = 1024 * 1024;

/** Connections with frames staged for the coalesce microtask. */
const staging = new Set<Conn>();

/**
 * Write every staged frame now, ahead of a blocking fsync.
 *
 * A lone durable publish fsyncs in the publisher's turn, before the coalesce
 * microtask runs, so the delivery it produced waited for the disk too. A
 * delivery may leave before the publisher's confirm; the confirm still waits.
 */
export function flushStagedWrites() {
  if (staging.size === 0) return;
  for (const conn of staging) {
    if (conn.closed || !conn.staged.length) continue;
    try {
      flushCoalesced.call(conn);
    } catch {
      /* the coalesce microtask closes the connection */
    }
  }
  staging.clear();
}

/**
 * Queue one microtask that writes every frame staged on this turn.
 *
 * The microtask runs before the caller's `await` continues, so a delivery is
 * on the socket before the next publish frame is parsed. A timer would add a
 * millisecond and stretch the group-commit cycle.
 */
function armCoalesce(this: Conn) {
  if (this.coalesceScheduled) return;
  this.coalesceScheduled = true;
  staging.add(this);
  queueMicrotask(() => {
    this.coalesceScheduled = false;
    staging.delete(this);
    if (this.closed) {
      this.staged = [];
      this.stagedBytes = 0;
      return;
    }
    try {
      flushCoalesced.call(this);
    } catch {
      this.closed = true;
      this.connClosed = true;
      try {
        this.socket.end();
      } catch {
        /* this connection is already gone */
      }
    }
  });
}

/**
 * Write staged frames as one buffer.
 *
 * A short write keeps the unsent tail on `outbound`, ahead of any later frame.
 * Bytes already sitting on `outbound` are older and go out first.
 */
function flushCoalesced(this: Conn) {
  if (!this.staged.length) return;
  if (this.outHead < this.outbound.length) {
    for (const frame of this.staged) { this.outbound.push(frame); this.outboundBytes += frame.length; }
    this.staged = [];
    this.stagedBytes = 0;
    flush.call(this);
    return;
  }
  const parts = this.staged;
  this.staged = [];
  this.stagedBytes = 0;
  let buf: Uint8Array = parts[0]!;
  let shared = false;
  let n = 0;
  for (let i = 0; i < parts.length; i++) n += parts[i]!.length;
  // Frames encoded back to back in the arena go out as one view, uncopied.
  const run = parts.length > 1 ? contiguous(parts, n) : null;
  if (run !== null) buf = run;
  else if (parts.length > 1) {
    if (this.reuseWrites && n > GATHER_MAX) {
      writeGathered(this, parts);
      return;
    }
    if (this.reuseWrites && n <= GATHER_MAX) {
      if (gather.length < n) gather = new Uint8Array(Math.min(GATHER_MAX, Math.max(n, gather.length * 2)));
      buf = gather.subarray(0, n);
      shared = true;
    } else {
      buf = new Uint8Array(n);
    }
    let o = 0;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i]!;
      const len = part.length;
      // `set` costs ~10 ns at any length; confirms (21 bytes) and other short
      // frames copy faster byte by byte.
      if (len <= 24) for (let j = 0; j < len; j++) buf[o + j] = part[j]!;
      else buf.set(part, o);
      o += len;
    }
  }
  const wrote = this.socket.write(buf);
  if (wrote === buf.length) {
    this.socket.flush?.();
    rewindArena(this);
    wakeOutput(this);
    return;
  }
  const tail = wrote > 0 ? buf.subarray(wrote) : buf;
  this.outbound.push(shared ? tail.slice() : tail);
  this.outboundBytes += tail.length;
  this.outHead = 0;
}

/**
 * Write staged frames larger than the gather buffer through it, one full
 * buffer per write, instead of into a fresh allocation of their whole size
 * (a 1 MiB delivery). After a short write the unsent bytes of the buffer are
 * copied out and the frames after them queue on `outbound` as they are.
 */
function writeGathered(conn: Conn, parts: Uint8Array[]) {
  if (gather.length < GATHER_MAX) gather = new Uint8Array(GATHER_MAX);
  let i = 0;
  let from = 0;
  while (i < parts.length) {
    let o = 0;
    while (i < parts.length && o < gather.length) {
      const part = parts[i]!;
      const n = Math.min(part.length - from, gather.length - o);
      if (n <= 24) for (let j = 0; j < n; j++) gather[o + j] = part[from + j]!;
      else gather.set(from === 0 && n === part.length ? part : part.subarray(from, from + n), o);
      o += n;
      from += n;
      if (from === part.length) {
        i++;
        from = 0;
      }
    }
    const wrote = conn.socket.write(gather.subarray(0, o));
    if (wrote === o) continue;
    const tail = gather.slice(Math.max(0, wrote), o);
    conn.outbound.push(tail);
    conn.outboundBytes += tail.length;
    if (i < parts.length) {
      const rest = from > 0 ? parts[i]!.subarray(from) : parts[i]!;
      conn.outbound.push(rest);
      conn.outboundBytes += rest.length;
      for (let k = i + 1; k < parts.length; k++) {
        conn.outbound.push(parts[k]!);
        conn.outboundBytes += parts[k]!.length;
      }
    }
    conn.outHead = 0;
    return;
  }
  conn.socket.flush?.();
  wakeOutput(conn);
}

/**
 * Queue several frames, in order, behind any write already in flight.
 *
 * @param frames Complete frames. They are not sent until earlier writes finish.
 * @returns The write chain. A closed connection drops the frames.
 */
export function sendMany(this: Conn, frames: Uint8Array[]) {
  if (this.closed) return SENT;
  // Nothing is waiting on the socket: stage the burst and write it once.
  // A promise per frame capped the fan-out consume rate.
  if (this.writesPending === 0 && this.outHead >= this.outbound.length) {
    for (let i = 0; i < frames.length; i++) {
      const frame = frames[i]!;
      this.staged.push(frame);
      this.stagedBytes += frame.length;
    }
    if (this.stagedBytes >= COALESCE_LIMIT) flushCoalesced.call(this);
    else armCoalesce.call(this);
    return SENT;
  }
  this.writesPending++;
  const bytes = frames.reduce((n, frame) => n + frame.length, 0);
  this.writeQueuedBytes += bytes;
  this.writeChain = this.writeChain.then(() => {
    this.writesPending--;
    this.writeQueuedBytes -= bytes;
    if (this.closed) return;
    if (this.staged.length) flushCoalesced.call(this);
    for (const frame of frames) { this.outbound.push(frame); this.outboundBytes += frame.length; }
    this.flush();
  });
  return this.writeChain;
}

/**
 * Write queued frames until the socket accepts a partial write.
 *
 * A short write keeps the unsent tail at the front of the queue. The socket
 * `drain` callback calls this again.
 */
export function flush(this: Conn) {
  if (this.staged.length) {
    for (const frame of this.staged) { this.outbound.push(frame); this.outboundBytes += frame.length; }
    this.staged = [];
    this.stagedBytes = 0;
  }
  const q = this.outbound;
  let i = this.outHead;
  while (i < q.length) {
    const frame = q[i]!;
    const n = this.socket.write(frame);
    this.outboundBytes -= Math.max(0, n);
    if (n === frame.length) {
      i++;
      continue;
    }
    if (n > 0) q[i] = frame.subarray(n);
    this.outHead = i;
    if (i > 64) {
      this.outbound = q.slice(i);
      this.outHead = 0;
    }
    return;
  }
  this.outHead = 0;
  q.length = 0;
  this.socket.flush?.();
  rewindArena(this);
  wakeOutput(this);
}

function wakeOutput(conn: Conn) {
  if (!conn.outputBlocked || conn.outboundBytes + conn.stagedBytes + conn.writeQueuedBytes + (conn.socket.pendingBytes?.() ?? 0) >= 1024 * 1024) return;
  conn.outputBlocked = false;
  queueMicrotask(() => {
    if (!conn.closed && !conn.connClosed) for (const wake of conn.outputWakers.values()) wake();
  });
}

/** How many staged publishes to enqueue before checking consumer credit again. */
const PUBLISH_BATCH = 64;

/** Fallback when the socket stays quiet. Ack bytes wake the wait sooner. */
const SOCKET_WAIT_MS = 1;

/**
 * A consumer that does not ack still has to get its confirms. After this
 * long, one batch is enqueued anyway.
 */
const HOLD_MS = 20;

/** A connection.close-ok frame (type 1, channel 0, class 10 method 51) in `data`. */
function hasCloseOk(data: Uint8Array): boolean {
  for (let i = 0; i + 11 <= data.length; i++) {
    if (data[i] === 1 && data[i + 1] === 0 && data[i + 2] === 0 && data[i + 7] === 0 && data[i + 8] === 10 && data[i + 9] === 0 && data[i + 10] === 51) return true;
  }
  return false;
}

/**
 * Append inbound bytes and handle every complete frame they contain.
 *
 * @param data The next chunk from the socket. It may hold a partial frame.
 * An AMQP 0-9-1 header switches the connection into frame mode and sends
 * `connection.start`. An AMQP 1.0 header hands the connection to `Amqp10Session`.
 * A bad frame end closes the connection. A channel error closes channel 0
 * with that code; any other error closes the connection with 541.
 * A second call while a burst is running queues `data` and shares that run.
 */
export function push(this: Conn, data: Uint8Array): Promise<void> {
  if (this.closeOkWait) {
    if (hasCloseOk(data)) this.closeOkWait();
    return Promise.resolve();
  }
  if (data.length) {
    let retained = this.buf.length + this.publishBytes + data.length;
    for (const chunk of this.fresh) retained += chunk.length;
    for (const ch of this.channels.values()) retained += ch.got + ch.txBytes;
    if (retained > 64 * 1024 * 1024) return this.connClose(506, "RESOURCE_ERROR - inbound buffering exceeds 64 MiB");
    this.fresh.push(data);
    const wake = this.wakeInbound;
    if (wake) {
      this.wakeInbound = null;
      wake();
    }
  }
  if (this.inboundRunning) return this.inboundDone;
  this.inboundRunning = true;
  this.inboundDone = driveInbound.call(this).finally(() => {
    this.inboundRunning = false;
  });
  return this.inboundDone.then(() => {
    if (this.fresh.length || this.deferredPublish.length) return this.push(new Uint8Array());
  });
}

/** Pressure belongs to the destinations of the next ordered publish. */
function publishAheadOfAcks(conn: Conn): boolean {
  return conn.deferredPublish[0]?.pressured() ?? false;
}

function hasConsumers(conn: Conn): boolean {
  for (const ch of conn.channels.values()) if (ch.consumers.size || ch.deliveries.size) return true;
  return false;
}

function resumeReads(conn: Conn) {
  if (!conn.readsPaused) return;
  conn.readsPaused = false;
  conn.socket.resume?.();
}

function startPublish(conn: Conn) {
  const op = conn.deferredPublish.shift()!;
  conn.inFlightBytes += op.bytes;
  conn.inFlightPublishes++;
  op.run();
}

/** Topology and transaction methods must observe earlier completed publishes. */
async function publishFence(conn: Conn) {
  while (conn.deferredPublish.length) startPublish(conn);
  resumeReads(conn);
  while (conn.inFlightPublishes > 0) await conn.publishSettles();
}

/**
 * Wait until the socket delivers more bytes, or until {@link SOCKET_WAIT_MS}.
 * The caller parses those bytes before enqueueing another publish group.
 */
function waitForSocket(this: Conn): Promise<void> {
  if (this.fresh.length && !this.readsPaused) return Promise.resolve();
  return new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (this.wakeInbound === finish) this.wakeInbound = null;
      resolve();
    };
    const timer = setTimeout(finish, SOCKET_WAIT_MS);
    this.wakeInbound = finish;
  });
}

/**
 * Parse queued bytes, then enqueue parked publishes.
 *
 * Ack frames in the buffer run during the parse. Publishes are parked and
 * started afterwards. While this connection's consumers are behind, further
 * publishes stay parked until ack bytes arrive, so the ready queue stays near
 * one prefetch window instead of the whole burst.
 */
async function driveInbound(this: Conn): Promise<void> {
  let holdStarted = 0;
  // A one-frame read must not start its confirm before the rest of a burst
  // arrives. The store flushes a single waiter on the next turn, which is
  // before the following socket read, so that frame would fsync alone.
  let loneQuiet = false;
  try {
    while (!this.closed && !this.connClosed) {
      if (this.readsPaused && !publishAheadOfAcks(this) && this.inFlightBytes < 32 * 1024 * 1024) resumeReads(this);
      if (!this.readsPaused) await parseAvailable.call(this);
      if (this.connClosed || this.closed) return;
      if (this.inFlightBytes >= 32 * 1024 * 1024) {
        if (!hasConsumers(this) && this.socket.pause && !this.readsPaused) {
          this.readsPaused = true;
          this.socket.pause();
        }
        await this.publishSettles();
        continue;
      }
      let releaseHold = false;
      if (this.deferredPublish.length > 0 && publishAheadOfAcks(this)) {
        const mixed = hasConsumers(this);
        if (!mixed && this.socket.pause && !this.readsPaused) {
          this.readsPaused = true;
          this.socket.pause();
        }
        const now = Date.now();
        if (holdStarted === 0) holdStarted = now;
        if (!mixed || now - holdStarted < HOLD_MS) {
          await waitForSocket.call(this);
          continue;
        }
        // One batch, then hold again, so a quiet consumer cannot stall confirms.
        releaseHold = true;
        holdStarted = now;
      } else if (!publishAheadOfAcks(this)) {
        holdStarted = 0;
      }
      // A second parked publish is a pipeline. Unparsed bytes may be a
      // consumer ack; those must not put the next confirm back on the wait.
      if (this.deferredPublish.length > 1) {
        this.broker.store.setLoneMode(false);
      }
      // Only the first frame of an idle group waits. A pipeline that already
      // has a staged row keeps enqueueing, so 128 waiters still share one fsync
      // and a hot socket does not pause a millisecond per frame. Once a confirm
      // has stayed alone, the next one skips that wait. With the sync worker
      // there is nothing to gather: confirms that arrive during its fsync
      // share the next one, so the publish is not delayed.
      const learnedLone = this.broker.store.immediateLone;
      if (
        !learnedLone &&
        !loneQuiet &&
        !releaseHold &&
        !this.broker.store.syncsAside &&
        this.deferredPublish.length === 1 &&
        this.deferredPublish[0]!.batchConfirm &&
        !this.fresh.length &&
        !this.broker.store.stagedWithoutFlush
      ) {
        await waitForSocket.call(this);
        if (this.fresh.length) {
          this.broker.store.setLoneMode(false);
          continue;
        }
        // The socket stayed quiet. This confirm is alone.
        this.broker.store.setLoneMode(true);
        loneQuiet = true;
      }
      let started = 0;
      while (this.deferredPublish.length && started < PUBLISH_BATCH) {
        if ((!releaseHold && publishAheadOfAcks(this)) || this.inFlightBytes >= 32 * 1024 * 1024) break;
        startPublish(this);
        started++;
      }
      if (this.fresh.length) {
        loneQuiet = false;
        continue;
      }
      if (this.deferredPublish.length > 0) continue;
      if (this.buf.length >= 7) {
        const size = new DataView(this.buf.buffer, this.buf.byteOffset, this.buf.byteLength).getUint32(3);
        if (this.buf.length >= size + 8) continue;
      }
      resumeReads(this);
      return;
    }
  } catch (err) {
    if (err instanceof ChanError && err.code !== 506) await this.chanClose(0, err.code, err.message);
    else if (err instanceof ChanError) await this.connClose(err.code, err.message);
    else await this.connClose(541, "INTERNAL_ERROR");
  } finally {
    if (this.closed || this.connClosed) {
      this.deferredPublish = [];
      this.publishBytes = this.inFlightBytes;
      this.fresh = [];
      this.buf = new Uint8Array();
      this.bodyFrameOpen = false;
      this.readsPaused = false;
    }
  }
}

/** Fold bytes that arrived during this parse onto the inbound buffer. */
function absorbFresh(this: Conn) {
  if (!this.fresh.length) return;
  // Each socket read is its own buffer, never written again, so a read with
  // nothing left over before it is parsed where it is. A copy into a fresh
  // allocation was 11% of a 64 KiB publish.
  if (this.buf.length === 0 && this.fresh.length === 1) {
    this.buf = this.fresh[0]!;
    this.fresh = [];
    return;
  }
  let extra = 0;
  for (const chunk of this.fresh) extra += chunk.length;
  const next = new Uint8Array(this.buf.length + extra);
  next.set(this.buf);
  let offset = this.buf.length;
  for (const chunk of this.fresh) {
    next.set(chunk, offset);
    offset += chunk.length;
  }
  this.fresh = [];
  this.buf = next;
}

/**
 * The one home of every queue an exchange publish routes to, or null.
 *
 * Null for a headers exchange, an exchange this process does not know, a key
 * that routes nowhere yet, or queues on more than one home.
 */
function exchangeHome(this: Conn, exchange: string, routingKey: string): string | null {
  const vhost = this.vhost || "/";
  const ex = this.broker.exchanges.get(this.broker.key(vhost, exchange));
  if (!ex || ex.kind === "headers" || ex.hashOn) return null;
  let queues: string[];
  try {
    queues = this.broker.route(vhost, exchange, routingKey, []);
  } catch {
    return null;
  }
  let home: string | null = null;
  for (const name of queues) {
    const q = this.broker.queues.get(this.broker.key(vhost, name));
    if (!q?.home) return null;
    if (home === null) home = q.home;
    else if (home !== q.home) return null;
  }
  return home;
}

/**
 * Move this connection to the queue's home before the naming frame is handled.
 *
 * One move per connection. The frame stays in `buf` so the home parses it.
 * Returns true when the socket has been handed off and this process must stop.
 */
function tryMigrate(this: Conn, buf: Uint8Array, at: number): boolean {
  if (!this.handed || this.migrated || this.ownsExclusive || buf.length - at < 11 || buf[at] !== 1) return false;
  // The handoff carries only pristine channel settings, not publish closures,
  // deliveries or transaction operations. Keep a connection with live work on
  // this worker and use the normal forwarding path instead.
  if (this.deferredPublish.length || this.inFlightPublishes || this.stagedBytes || this.outboundBytes || this.writesPending) return false;
  for (const ch of this.channels.values()) {
    // A direct reply-to consumer is registered on this worker (`replySinks`).
    if (ch.tx || ch.publish || ch.nextPub !== 1 || ch.consumers.size || ch.deliveries.size || ch.parked.length || ch.replyAddr) return false;
  }
  const size = (buf[at + 3]! << 24) | (buf[at + 4]! << 16) | (buf[at + 5]! << 8) | buf[at + 6]!;
  if (buf.length - at < 8 + size) return false;
  const cls = (buf[at + 7]! << 8) | buf[at + 8]!;
  const meth = (buf[at + 9]! << 8) | buf[at + 10]!;
  const args = new R(buf.subarray(at + 11, at + 7 + size));
  let queue = "";
  let home: string | null = null;
  if (cls === 60 && meth === 40) {
    args.u16();
    const exchange = args.shortstr();
    queue = args.shortstr();
    // Most applications publish through a named exchange. When every queue it
    // routes to has one home, move there too, so the publish is not forwarded
    // between processes. Mixed homes, no route yet, and headers exchanges
    // (the headers are in the next frame) stay here and forward.
    if (exchange !== "") {
      home = exchangeHome.call(this, exchange, queue);
      if (!home) return false;
    } else if (queue.startsWith("amq.rabbitmq.reply-to.")) {
      // A direct reply goes to the requester's worker; so does its publisher.
      home = replyWorker(this, queue);
      if (!home) return false;
    }
  } else if (cls === 60 && meth === 20) {
    args.u16();
    queue = args.shortstr();
  } else {
    return false;
  }
  if (home === null) {
    // A known queue keeps the home it was placed on; a locator may have moved it off the hash.
    const known = this.broker.queues.get(this.broker.key(this.vhost || "/", queue));
    home = known ? known.home : this.broker.homeOf(this.vhost || "/", queue, false);
  }
  if (!home || this.broker.isLocalHome(home)) return false;
  // The home parses from the naming frame on.
  if (at > 0) this.buf = buf.subarray(at);
  this.migrated = true;
  this.closed = true;
  this.connClosed = true;
  const state = {
    user: this.user,
    vhost: this.vhost || "/",
    wantsBlocked: this.wantsBlocked,
    cancelNotify: this.cancelNotify,
    principal: principalToWire(this.broker.principalOf(this.user)),
    channels: [...this.channels.entries()].map(([id, ch]) => ({
      id,
      confirm: ch.confirm,
      prefetch: ch.prefetch,
    })),
  };
  const bytes = Buffer.from(this.buf);
  const socket = this.handed;
  this.handed = null;
  socket.pause();
  socket.removeAllListeners("data");
  (process as unknown as { send?: (message: unknown, handle?: unknown) => void }).send?.(
    { type: "migrate", home, state, bytes },
    socket,
  );
  return true;
}

/**
 * Handle every complete frame already buffered.
 *
 * @returns Nothing. A partial frame stays in `buf`. Persistent publishes are
 * parked on `deferredPublish` instead of being enqueued here.
 */
async function parseAvailable(this: Conn): Promise<void> {
  absorbFresh.call(this);
  if (this.stage === "amqp10") {
    const bytes = this.buf;
    this.buf = new Uint8Array(0);
    this.amqp10!.feed(bytes);
    return;
  }
  if (this.stage === "header") {
    if (this.buf.length < 8) return;
    const head = this.buf.subarray(0, 8);
    const amqp10 = head[0] === 65 && head[1] === 77 && head[2] === 81 && head[3] === 80 && ((head[4] === 3 && head[5] === 1 && head[6] === 0 && head[7] === 0) || (head[4] === 0 && head[5] === 1 && head[6] === 0 && head[7] === 0));
    if (amqp10) {
      this.stage = "amqp10";
      this.amqp10 = new Amqp10Session(
        this.broker,
        (bytes) => void this.send(bytes),
        () => this.socket.end(),
      );
      this.amqp10.peerCN = this.peerCN;
      const bytes = this.buf;
      this.buf = new Uint8Array(0);
      this.amqp10.feed(bytes);
      return;
    }
    const ok = head[0] === 65 && head[1] === 77 && head[2] === 81 && head[3] === 80 && head[4] === 0 && head[5] === 0 && head[6] === 9 && head[7] === 1;
    if (!ok) {
      this.socket.end();
      return;
    }
    this.buf = this.buf.subarray(8);
    this.stage = "frames";
    await this.send(
      methodFrame(
        0,
        method(10, 10, (w) => {
          w.u8(0);
          w.u8(9);
          writeTable(w, [
            ["capabilities", { t: "F", v: [
              ["publisher_confirms", { t: "t", v: true }],
              ["exchange_exchange_bindings", { t: "t", v: true }],
              ["basic.nack", { t: "t", v: true }],
              ["consumer_cancel_notify", { t: "t", v: true }],
              ["connection.blocked", { t: "t", v: true }],
              ["consumer_priorities", { t: "t", v: true }],
              ["authentication_failure_close", { t: "t", v: true }],
              ["per_consumer_qos", { t: "t", v: true }],
              ["direct_reply_to", { t: "t", v: true }],
            ] }],
            ["product", { t: "S", v: "QueueForge" }],
            // RabbitMQ's own clients read these. PerfTest stops on a missing version.
            ["version", { t: "S", v: SERVER_VERSION }],
            ["queueforge_version", { t: "S", v: QUEUEFORGE_VERSION }],
            ["platform", { t: "S", v: `Bun ${Bun.version}` }],
            ["copyright", { t: "S", v: "Copyright (c) QueueForge" }],
            ["information", { t: "S", v: "https://github.com/danielbarion/queueforge" }],
            ["cluster_name", { t: "S", v: this.broker.clusterName() }],
          ]);
          w.longstr(this.peerCN ? "PLAIN EXTERNAL" : "PLAIN");
          w.longstr("en_US");
        }),
      ),
    );
  }
  // Frames are read at `pos` in a local view. `this.buf` is written back only
  // before an await, a handoff, or a return: two views per frame showed up
  // in the profile.
  let buf = this.buf;
  let pos = 0;
  const commit = () => {
    if (pos > 0) this.buf = buf.subarray(pos);
    buf = this.buf;
    pos = 0;
  };
  try {
    while (!this.connClosed) {
      if (this.fresh.length) {
        commit();
        absorbFresh.call(this);
        buf = this.buf;
      }
      if (this.bodyFrameOpen) {
        // The rest of a large body frame: copied into the body, then its end.
        const avail = buf.length - pos;
        if (avail === 0) return;
        const take = Math.min(this.bodyFrameLeft, avail);
        if (take > 0) {
          this.streamBody(this.bodyFrameChannel, take, buf.subarray(pos, pos + take));
          pos += take;
          this.bodyFrameLeft -= take;
          if (this.bodyFrameLeft > 0) return;
        }
        if (pos >= buf.length) return;
        this.bodyFrameOpen = false;
        if (buf[pos++] !== FRAME_END) {
          commit();
          await this.connClose(501, "FRAME_ERROR");
          return;
        }
        const body = this.onBody(this.bodyFrameChannel, EMPTY_BODY);
        if (body) {
          commit();
          await body;
          buf = this.buf;
        }
        if (!hasConsumers(this) && (this.deferredPublish.length >= PUBLISH_BATCH || this.publishBytes >= 16 * 1024 * 1024)) return;
        continue;
      }
      const rest = buf.length - pos;
      if (rest < 7) return;
      if (tryMigrate.call(this, buf, pos)) {
        pos = 0;
        buf = this.buf;
        return;
      }
      const type = buf[pos]!;
      const channel = (buf[pos + 1]! << 8) | buf[pos + 2]!;
      const size = ((buf[pos + 3]! << 24) | (buf[pos + 4]! << 16) | (buf[pos + 5]! << 8) | buf[pos + 6]!) >>> 0;
      if (rest < 8 + size) {
        // A large body frame is copied into its body as it arrives, so the
        // read buffer is not concatenated again on every read until it is whole.
        if (type === 3 && size >= STREAM_FRAME && this.streamBody(channel, size, buf.subarray(pos + 7))) {
          this.bodyFrameChannel = channel;
          this.bodyFrameLeft = size - (rest - 7);
          this.bodyFrameOpen = true;
          pos = buf.length;
        }
        return;
      }
      const start = pos + 7;
      const end = buf[start + size];
      pos = start + size + 1;
      if (end !== FRAME_END) {
        commit();
        await this.connClose(501, "FRAME_ERROR");
        return;
      }
      if (type === 8) continue;
      if (type === 3) {
        // A body that arrives in one frame is copied straight out of the read
        // buffer: the store and the queue must not hold a view of it, and a
        // view first, then a copy in finishPublish, was a second object. A
        // large body already has its own buffer, which onBody copies into.
        const c = this.channels.get(channel);
        const whole = c !== undefined && c.publish !== null && c.body === null && c.got === 0 && size === c.bodySize;
        const body = this.onBody(channel, whole ? buf.slice(start, start + size) : buf.subarray(start, start + size), whole);
        if (body) {
          commit();
          await body;
          buf = this.buf;
        }
        if (!hasConsumers(this) && (this.deferredPublish.length >= PUBLISH_BATCH || this.publishBytes >= 16 * 1024 * 1024)) return;
        continue;
      }
      const payload = buf.subarray(start, start + size);
      // basic.publish, its header, and its body do not await. Awaiting a resolved
      // async function still allocates a promise per frame and caps the pipeline.
      if (
        type === 1 &&
        payload.length >= 4 &&
        payload[0] === 0 &&
        payload[1] === 60 &&
        payload[2] === 0 &&
        payload[3] === 40
      ) {
        this.beginPublish(this.ch(channel), payload);
        continue;
      }
      if (
        type === 1 &&
        payload.length >= 4 &&
        payload[0] === 0 &&
        payload[1] === 60 &&
        payload[2] === 0 &&
        payload[3] === 80
      ) {
        const ack = this.ack(this.ch(channel), payload);
        if (ack) {
          commit();
          await ack;
          buf = this.buf;
        }
        continue;
      }
      if (type === 1) {
        commit();
        const cls = payload.length >= 4 ? (payload[0]! << 8) | payload[1]! : 0;
        const mid = payload.length >= 4 ? (payload[2]! << 8) | payload[3]! : 0;
        // Settlements and close frames remain readable while publishing is held.
        if (!(cls === 60 && (mid === 90 || mid === 120)) && (cls !== 10 && cls !== 20 || mid === 40 || mid === 50)) await publishFence(this);
        await this.onMethod(channel, payload);
        buf = this.buf;
        if (cls === 90 && mid === 20 && !hasConsumers(this) && this.deferredPublish.length) return;
      } else if (type === 2) {
        const header = this.onHeader(channel, payload);
        if (header) {
          commit();
          await header;
          buf = this.buf;
        }
      }
      // Dispatch a bounded publish group before reading another pure-publisher burst.
      if (!hasConsumers(this) && (this.deferredPublish.length >= PUBLISH_BATCH || this.publishBytes >= 16 * 1024 * 1024)) return;
    }
  } finally {
    commit();
  }
}

Conn.prototype.send = send;
Conn.prototype.outFrame = function (this: Conn, n: number) {
  return outFrame(this, n);
};
Conn.prototype.sendMany = sendMany;
Conn.prototype.flush = flush;
Conn.prototype.push = push;

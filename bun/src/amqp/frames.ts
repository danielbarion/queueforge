/**
 * AMQP frame read and write for one connection.
 *
 * Owns the outbound queue and the inbound byte buffer. Method bodies live
 * in the sibling files; this file only frames them.
 */
import { driveAmqp10 } from "../protocols/index.ts";
import { method, methodFrame, R, writeTable } from "../codec.ts";
import { ChanError } from "../broker/index.ts";
import { Conn } from "./listen.ts";

/** Every AMQP frame ends with this octet. A mismatch closes the connection. */
const FRAME_END = 0xce;

/**
 * Queue one frame behind any write already in flight.
 *
 * @param frame A complete frame, including its end octet.
 * @returns The write chain. Later frames wait on this promise.
 */
export function send(this: Conn, frame: Uint8Array) {
  return this.sendMany([frame]);
}

/** Bytes gathered before one write. A larger burst still flushes in order. */
const COALESCE_LIMIT = 64 * 1024;

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
  queueMicrotask(() => {
    this.coalesceScheduled = false;
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
    for (const frame of this.staged) this.outbound.push(frame);
    this.staged = [];
    this.stagedBytes = 0;
    flush.call(this);
    return;
  }
  const parts = this.staged;
  this.staged = [];
  this.stagedBytes = 0;
  let buf: Uint8Array;
  if (parts.length === 1) buf = parts[0]!;
  else {
    let n = 0;
    for (let i = 0; i < parts.length; i++) n += parts[i]!.length;
    buf = new Uint8Array(n);
    let o = 0;
    for (let i = 0; i < parts.length; i++) {
      const part = parts[i]!;
      buf.set(part, o);
      o += part.length;
    }
  }
  const wrote = this.socket.write(buf);
  if (wrote === buf.length) {
    this.socket.flush?.();
    return;
  }
  this.outbound.push(wrote > 0 ? buf.subarray(wrote) : buf);
  this.outHead = 0;
}

/**
 * Queue several frames, in order, behind any write already in flight.
 *
 * @param frames Complete frames. They are not sent until earlier writes finish.
 * @returns The write chain. A closed connection drops the frames.
 */
export function sendMany(this: Conn, frames: Uint8Array[]) {
  if (this.closed) return Promise.resolve();
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
    return Promise.resolve();
  }
  this.writesPending++;
  this.writeChain = this.writeChain.then(() => {
    this.writesPending--;
    if (this.closed) return;
    if (this.staged.length) flushCoalesced.call(this);
    for (const frame of frames) this.outbound.push(frame);
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
    for (const frame of this.staged) this.outbound.push(frame);
    this.staged = [];
    this.stagedBytes = 0;
  }
  const q = this.outbound;
  let i = this.outHead;
  while (i < q.length) {
    const frame = q[i]!;
    const n = this.socket.write(frame);
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
}

/** How many staged publishes to enqueue before checking consumer credit again. */
const PUBLISH_BATCH = 64;

/**
 * Ready messages above this, while this connection still has unacked
 * deliveries, means prefetch is full. Further confirms wait for ack bytes
 * so one burst cannot bury the consumer. Deep enough that 128 in-flight
 * confirms still share one group commit.
 */
const READY_BACKLOG = 256;

/** Fallback when the socket stays quiet. Ack bytes wake the wait sooner. */
const SOCKET_WAIT_MS = 1;

/**
 * A consumer that does not ack still has to get its confirms. After this
 * long, one batch is enqueued anyway.
 */
const HOLD_MS = 20;

/**
 * Append inbound bytes and handle every complete frame they contain.
 *
 * @param data The next chunk from the socket. It may hold a partial frame.
 * An AMQP 0-9-1 header switches the connection into frame mode and sends
 * `connection.start`. An AMQP 1.0 header is handed to `driveAmqp10`.
 * A bad frame end closes the connection. A channel error closes channel 0
 * with that code; any other error closes the connection with 541.
 * A second call while a burst is running queues `data` and shares that run.
 */
export function push(this: Conn, data: Uint8Array): Promise<void> {
  if (data.length) {
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

/** Messages sitting on ready queues. A deep total means prefetch credit is full. */
function readyBacklog(conn: Conn): number {
  let n = 0;
  for (const q of conn.broker.queues.values()) n += q.ready.length;
  return n;
}

/**
 * True when this connection has delivered messages it has not acked yet and
 * the ready queues are already past {@link READY_BACKLOG}.
 */
function publishAheadOfAcks(conn: Conn): boolean {
  if (readyBacklog(conn) <= READY_BACKLOG) return false;
  for (const ch of conn.channels.values()) {
    if (ch.deliveries.size > 0) return true;
  }
  return false;
}

/**
 * Wait until the socket delivers more bytes, or until {@link SOCKET_WAIT_MS}.
 * The caller parses those bytes before enqueueing another publish group.
 */
function waitForSocket(this: Conn): Promise<void> {
  if (this.fresh.length) return Promise.resolve();
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
      await parseAvailable.call(this);
      if (this.connClosed || this.closed) return;
      let releaseHold = false;
      if (this.deferredPublish.length > 0 && publishAheadOfAcks(this)) {
        const now = Date.now();
        if (holdStarted === 0) holdStarted = now;
        if (now - holdStarted < HOLD_MS) {
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
      // has stayed alone, the next one skips that wait.
      const learnedLone = this.broker.store.immediateLone;
      if (
        !learnedLone &&
        !loneQuiet &&
        !releaseHold &&
        this.deferredPublish.length === 1 &&
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
        if (!releaseHold && publishAheadOfAcks(this)) break;
        const op = this.deferredPublish.shift()!;
        void op();
        started++;
      }
      if (this.fresh.length) {
        loneQuiet = false;
        continue;
      }
      if (this.deferredPublish.length > 0) continue;
      return;
    }
  } catch (err) {
    if (err instanceof ChanError) await this.chanClose(0, err.code, err.message);
    else await this.connClose(541, "INTERNAL_ERROR");
  }
}

/** Fold bytes that arrived during this parse onto the inbound buffer. */
function absorbFresh(this: Conn) {
  if (!this.fresh.length) return;
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
 * Handle every complete frame already buffered.
 *
 * @returns Nothing. A partial frame stays in `buf`. Persistent publishes are
 * parked on `deferredPublish` instead of being enqueued here.
 */
async function parseAvailable(this: Conn): Promise<void> {
  absorbFresh.call(this);
  if (this.stage === "amqp10") {
    this.buf = await driveAmqp10(this.buf, this.amqp10, (frame) => this.socket.write(frame), this.broker);
    return;
  }
  if (this.stage === "header") {
    if (this.buf.length < 8) return;
    const head = this.buf.subarray(0, 8);
    const amqp10 = head[0] === 65 && head[1] === 77 && head[2] === 81 && head[3] === 80 && ((head[4] === 3 && head[5] === 1 && head[6] === 0 && head[7] === 0) || (head[4] === 0 && head[5] === 1 && head[6] === 0 && head[7] === 0));
    if (amqp10) {
      this.stage = "amqp10";
      this.buf = await driveAmqp10(this.buf, this.amqp10, (frame) => this.socket.write(frame), this.broker);
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
              ["consumer_cancel_notify", { t: "t", v: true }],
              ["basic.nack", { t: "t", v: true }],
            ] }],
          ]);
          w.longstr("PLAIN");
          w.longstr("en_US");
        }),
      ),
    );
  }
  while (!this.connClosed) {
    absorbFresh.call(this);
    if (this.buf.length < 7) return;
    const r = new R(this.buf);
    const type = r.u8();
    const channel = r.u16();
    const size = r.u32();
    if (this.buf.length < 7 + size + 1) return;
    const payload = this.buf.subarray(7, 7 + size);
    const end = this.buf[7 + size];
    this.buf = this.buf.subarray(8 + size);
    if (end !== FRAME_END) {
      await this.connClose(501, "FRAME_ERROR");
      return;
    }
    if (type === 8) continue;
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
      if (ack) await ack;
      continue;
    }
    if (type === 1) await this.onMethod(channel, payload);
    else if (type === 2) {
      const header = this.onHeader(channel, payload);
      if (header) await header;
    } else if (type === 3) {
      const body = this.onBody(channel, payload);
      if (body) await body;
    }
  }
}

Conn.prototype.send = send;
Conn.prototype.sendMany = sendMany;
Conn.prototype.flush = flush;
Conn.prototype.push = push;

/**
 * AMQP frame read and write for one connection.
 *
 * Owns the outbound queue and the inbound byte buffer. Method bodies live
 * in the sibling files; this file only frames them.
 */
import { Amqp10Session } from "../amqp10/session.ts";
import { method, methodFrame, R, writeTable } from "../codec.ts";
import { ChanError } from "../broker/index.ts";
import { Conn } from "./listen.ts";

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
  let shared = false;
  if (parts.length === 1) buf = parts[0]!;
  else {
    let n = 0;
    for (let i = 0; i < parts.length; i++) n += parts[i]!.length;
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
      buf.set(part, o);
      o += part.length;
    }
  }
  const wrote = this.socket.write(buf);
  if (wrote === buf.length) {
    this.socket.flush?.();
    return;
  }
  const tail = wrote > 0 ? buf.subarray(wrote) : buf;
  this.outbound.push(shared ? tail.slice() : tail);
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
 * The one home of every queue an exchange publish routes to, or null.
 *
 * Null for a headers exchange, an exchange this process does not know, a key
 * that routes nowhere yet, or queues on more than one home.
 */
function exchangeHome(this: Conn, exchange: string, routingKey: string): string | null {
  const vhost = this.vhost || "/";
  const ex = this.broker.exchanges.get(this.broker.key(vhost, exchange));
  if (!ex || ex.kind === "headers") return null;
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
  if (!this.handed || this.migrated || buf.length - at < 11 || buf[at] !== 1) return false;
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
      if (rest < 8 + size) return;
      const start = pos + 7;
      const payload = buf.subarray(start, start + size);
      const end = buf[start + size];
      pos = start + size + 1;
      if (end !== FRAME_END) {
        commit();
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
        if (ack) {
          commit();
          await ack;
          buf = this.buf;
        }
        continue;
      }
      if (type === 1) {
        commit();
        await this.onMethod(channel, payload);
        buf = this.buf;
      } else if (type === 2) {
        const header = this.onHeader(channel, payload);
        if (header) {
          commit();
          await header;
          buf = this.buf;
        }
      } else if (type === 3) {
        const body = this.onBody(channel, payload);
        if (body) {
          commit();
          await body;
          buf = this.buf;
        }
      }
    }
  } finally {
    commit();
  }
}

Conn.prototype.send = send;
Conn.prototype.sendMany = sendMany;
Conn.prototype.flush = flush;
Conn.prototype.push = push;

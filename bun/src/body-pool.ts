/**
 * Reuse of large publish bodies.
 *
 * Every 64 KiB publish used to allocate a fresh body, and that external
 * memory made JavaScriptCore run a full collection every few MiB (about 40 a
 * second in the 64 KiB cells). A body here comes from a free list instead,
 * and goes back to it only when nothing can read it again.
 *
 * Reuse is opt-in per body, never opt-out. A body is released only after this
 * exact chain, each step observed here:
 *
 * 1. allocated by {@link poolBody} for a publish frame (FRESH);
 * 2. enqueued once, on a local classic or lone quorum queue, by the
 *    single-destination publish path, and kept whole in memory (ARMED);
 * 3. handed to exactly one consumer, which is an AMQP connection of this
 *    process that wrote it with `deliver` (DELIVERED);
 * 4. acked by that consumer (RETIRING);
 * 5. then, checked when a body is next needed: the store holds no row with
 *    the body, the connection is open and has written every byte it was
 *    given (nothing staged, queued, or held by the socket), and the buffer
 *    has not been reused since step 1.
 *
 * Any other event on the body (a second enqueue, a second hand-off, a
 * republish, a forward to another process, a trace copy, a nack, a requeue)
 * marks it TAINTED, and a tainted body is left to the garbage collector as
 * before. A path that nothing here knows about cannot complete the chain, so
 * a missed case costs the reuse, not the bytes.
 *
 * Each use carries the buffer's generation. A delivery or ack that sees a
 * body whose buffer moved on is a broken invariant: it is logged and reuse
 * stops for the life of the process. `QUEUEFORGE_BODY_POOL=0` turns reuse off,
 * and `QUEUEFORGE_BODY_POOL_CHECK=1` also hashes each body when it is
 * enqueued and checks it at delivery and before reuse (tests run with it).
 */

/** Smallest pooled body: the publish path's `DIRECT_BODY`. */
export const POOL_MIN = 16 * 1024;
/** Largest pooled body. Larger ones keep their own allocation. */
const POOL_MAX = 1024 * 1024;
/** Free buffers kept per process, in bytes. Past it a released buffer is dropped. */
const FREE_BYTES = 16 * 1024 * 1024;
/** Acked bodies waiting for the store and the socket, in bytes. Past it the oldest is dropped. */
const RETIRING_BYTES = 64 * 1024 * 1024;
/** Retiring entries looked at per allocation, so a stuck head cannot make one publish slow. */
const SCAN = 32;

const FRESH = 0;
const ARMED = 1;
const DELIVERED = 2;
const RETIRING = 3;
const TAINTED = 4;

/** What the pool needs from an AMQP connection: whether it still holds written bytes. */
export type PoolConn = {
  closed: boolean;
  staged: unknown[];
  writesPending: number;
  outHead: number;
  outbound: unknown[];
  reuseWrites: boolean;
  held: { n: number } | null;
};

/** What the pool needs from the store: whether a row still references its body. */
export type PoolStore = { holdsBody(rowId: number): boolean };

/** One backing buffer, reused across bodies. */
type Slot = { buf: ArrayBuffer; cls: number; gen: number };

/** One body's use of a slot. */
type Use = {
  slot: Slot;
  gen: number;
  state: number;
  handoffs: number;
  conn: PoolConn | null;
  store: PoolStore | null;
  rowId: number | null;
  sum: number;
  body: Uint8Array | null;
};

const uses = new WeakMap<Uint8Array, Use>();
/** Free slots by size class (log2 of the slot size). */
const free = new Map<number, Slot[]>();
let freeBytes = 0;
let retiring: Use[] = [];
let retiringHead = 0;
let retiringBytes = 0;

let enabled = process.env.QUEUEFORGE_BODY_POOL !== "0";
const check = process.env.QUEUEFORGE_BODY_POOL_CHECK === "1";

/** Counters for tests and the profile. */
export const poolStats = { allocated: 0, reused: 0, released: 0, tainted: 0, dropped: 0, violations: 0 };

function classOf(n: number): number {
  return Math.max(14, 32 - Math.clz32(n - 1));
}

/**
 * A buffer for `n` body bytes, every one written before it is read, or null
 * when `n` is outside the pooled range or reuse is off.
 */
export function poolBody(n: number): Uint8Array | null {
  if (!enabled || n < POOL_MIN || n > POOL_MAX) return null;
  if (retiringHead < retiring.length) reclaim();
  const cls = classOf(n);
  const list = free.get(cls);
  let slot = list?.pop();
  if (slot) {
    freeBytes -= slot.buf.byteLength;
    poolStats.reused++;
  } else {
    // Its own buffer, not a slice of Bun's shared pool, and no zero fill.
    const b = Buffer.allocUnsafeSlow(1 << cls);
    slot = { buf: b.buffer as ArrayBuffer, cls, gen: 0 };
    poolStats.allocated++;
  }
  const body = new Uint8Array(slot.buf, 0, n);
  uses.set(body, { slot, gen: slot.gen, state: FRESH, handoffs: 0, conn: null, store: null, rowId: null, sum: 0, body: null });
  return body;
}

/** The use of a pooled body still on its buffer, or null (not pooled, or stale). */
function useOf(body: Uint8Array): Use | null {
  if (body.length < POOL_MIN) return null;
  const use = uses.get(body);
  if (use === undefined) return null;
  if (use.gen !== use.slot.gen) {
    violation("a body was read after its buffer was reused");
    return null;
  }
  return use;
}

function violation(what: string) {
  poolStats.violations++;
  if (enabled) console.error(`queueforge-bun: body pool: ${what}; body reuse is now off`);
  enabled = false;
  free.clear();
  freeBytes = 0;
  retiring = [];
  retiringHead = 0;
  retiringBytes = 0;
}

function taintUse(use: Use) {
  if (use.state === TAINTED) return;
  use.state = TAINTED;
  use.conn = null;
  use.store = null;
  poolStats.tainted++;
}

/** This body has left the reuse chain: it goes to the garbage collector. */
export function taintBody(body: Uint8Array) {
  if (body.length < POOL_MIN) return;
  const use = useOf(body);
  if (use) taintUse(use);
}

/** A body about to be routed: a pooled body that was already routed once is now shared. */
export function notePublish(body: Uint8Array) {
  if (body.length < POOL_MIN) return;
  const use = useOf(body);
  if (use && use.state !== FRESH) taintUse(use);
}

/**
 * An enqueue of `body`. `eligible` is set only by the single-destination
 * publish path, for a message kept whole on a local classic or lone quorum
 * queue. Every other enqueue of a pooled body taints it.
 */
export function noteEnqueue(body: Uint8Array, eligible: boolean, store: PoolStore, rowId: number | null) {
  if (body.length < POOL_MIN) return;
  const use = useOf(body);
  if (!use) return;
  if (!eligible || use.state !== FRESH) {
    taintUse(use);
    return;
  }
  use.state = ARMED;
  use.store = store;
  use.rowId = rowId;
  if (check) use.sum = hash(body);
}

/** A queue gave `body` to a consumer, of any kind. */
export function noteHandoff(body: Uint8Array) {
  if (body.length < POOL_MIN) return;
  const use = useOf(body);
  if (!use || use.state === TAINTED) return;
  if (++use.handoffs !== 1 || use.state !== ARMED) taintUse(use);
}

/** An AMQP connection of this process is writing `body` in basic.deliver. */
export function noteDeliver(body: Uint8Array, conn: PoolConn) {
  if (body.length < POOL_MIN) return;
  const use = useOf(body);
  if (!use || use.state === TAINTED) return;
  if (check && use.state !== FRESH && hash(body) !== use.sum) {
    violation("a body changed between its enqueue and its delivery");
    return;
  }
  if (use.state !== ARMED || use.handoffs !== 1) {
    taintUse(use);
    return;
  }
  use.state = DELIVERED;
  use.conn = conn;
}

/** The consumer acked the message holding `body`; `rowId` is its stored row. */
export function noteAck(body: Uint8Array, rowId: number | null) {
  if (body.length < POOL_MIN) return;
  const use = useOf(body);
  if (!use || use.state === TAINTED) return;
  if (use.state !== DELIVERED || use.rowId !== rowId) {
    taintUse(use);
    return;
  }
  use.state = RETIRING;
  use.body = body;
  retiring.push(use);
  retiringBytes += use.slot.buf.byteLength;
  while (retiringBytes > RETIRING_BYTES && retiringHead < retiring.length) drop(retiring[retiringHead++]!);
}

/** The connection has handed every byte it was given to the kernel. */
function drained(conn: PoolConn): boolean {
  if (conn.closed) return false;
  if (conn.staged.length !== 0 || conn.writesPending !== 0 || conn.outHead < conn.outbound.length) return false;
  // Bun's own sockets copy what a write accepts; a node socket holds a
  // written buffer until its callback (`Conn.held`). Anything else is unknown.
  return conn.reuseWrites || (conn.held !== null && conn.held.n === 0);
}

/** Move retiring bodies nothing can read any more to the free lists. */
function reclaim() {
  let kept = 0;
  const end = Math.min(retiring.length, retiringHead + SCAN);
  for (let i = retiringHead; i < end; i++) {
    const use = retiring[i]!;
    if (use.state !== RETIRING) {
      retiringBytes -= use.slot.buf.byteLength;
      continue;
    }
    const conn = use.conn!;
    if (conn.closed) {
      drop(use);
      continue;
    }
    if (!drained(conn) || (use.rowId !== null && use.store!.holdsBody(use.rowId))) {
      retiring[retiringHead + kept++] = use;
      continue;
    }
    release(use);
  }
  // Waiting entries move up behind the head; the rest of the list follows them.
  const rest = retiring.length - end;
  for (let i = 0; i < rest; i++) retiring[retiringHead + kept + i] = retiring[end + i]!;
  retiring.length = retiringHead + kept + rest;
  if (retiringHead > 1024 && retiringHead * 2 > retiring.length) {
    retiring = retiring.slice(retiringHead);
    retiringHead = 0;
  }
}

function release(use: Use) {
  retiringBytes -= use.slot.buf.byteLength;
  const body = use.body!;
  if (check && hash(body) !== use.sum) {
    violation("a body changed before its buffer was reused");
    return;
  }
  const slot = use.slot;
  // Every view of this buffer is stale from here: `useOf` reports a read.
  slot.gen++;
  use.state = TAINTED;
  use.conn = null;
  use.store = null;
  use.body = null;
  poolStats.released++;
  if (check) new Uint8Array(slot.buf).fill(0xa5);
  if (freeBytes + slot.buf.byteLength > FREE_BYTES) return;
  let list = free.get(slot.cls);
  if (!list) free.set(slot.cls, (list = []));
  list.push(slot);
  freeBytes += slot.buf.byteLength;
}

function drop(use: Use) {
  retiringBytes -= use.slot.buf.byteLength;
  use.state = TAINTED;
  use.conn = null;
  use.store = null;
  use.body = null;
  poolStats.dropped++;
}

/** FNV-1a over the whole body: the check mode only. */
function hash(b: Uint8Array): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < b.length; i++) h = Math.imul(h ^ b[i]!, 0x01000193);
  return h >>> 0;
}

/** Test hook: reset the pool and turn reuse on or off. */
export function resetPool(on = true) {
  enabled = on;
  free.clear();
  freeBytes = 0;
  retiring = [];
  retiringHead = 0;
  retiringBytes = 0;
  for (const k of Object.keys(poolStats) as Array<keyof typeof poolStats>) poolStats[k] = 0;
}

/** Test hook: run the reclaim scan now. */
export function reclaimNow() {
  while (retiringHead < retiring.length) {
    const before = retiring.length - retiringHead;
    reclaim();
    if (retiring.length - retiringHead >= before) break;
  }
}

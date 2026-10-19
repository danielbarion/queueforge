import { beforeEach, expect, test } from "bun:test";
import { noteAck, noteDeliver, noteEnqueue, noteHandoff, notePublish, poolBody, poolStats, reclaimNow, resetPool, taintBody, type PoolConn } from "../src/body-pool.ts";

/**
 * The body pool (src/body-pool.ts) reuses a buffer only after the whole
 * single-queue chain: enqueue, one AMQP delivery, ack, store row gone, socket
 * drained. Anything else must leave the buffer alone.
 */
const N = 64 * 1024;

function conn(over: Partial<PoolConn> = {}): PoolConn {
  return { closed: false, staged: [], writesPending: 0, outHead: 0, outbound: [], reuseWrites: true, held: null, ...over };
}

function store(held = new Set<number>()) {
  return { held, holdsBody: (id: number) => held.has(id) };
}

/** One body through publish, enqueue, deliver and ack. */
function chain(body: Uint8Array, c: PoolConn, s: ReturnType<typeof store>, rowId: number | null) {
  notePublish(body);
  noteEnqueue(body, true, s, rowId);
  noteHandoff(body);
  noteDeliver(body, c);
  noteAck(body, rowId);
}

beforeEach(() => resetPool(true));

test("a body that went through the whole chain is reused, and its old view is stale", () => {
  const a = poolBody(N)!;
  expect(a.length).toBe(N);
  chain(a, conn(), store(), 7);
  reclaimNow();
  expect(poolStats.released).toBe(1);
  const b = poolBody(N)!;
  expect(b.buffer).toBe(a.buffer);
  expect(poolStats.reused).toBe(1);
  // The old view is a different use: touching it is a broken invariant.
  noteHandoff(a);
  expect(poolStats.violations).toBe(1);
  expect(poolBody(N)).toBeNull();
});

test("sizes outside the pooled range are not pooled", () => {
  expect(poolBody(16 * 1024 - 1)).toBeNull();
  expect(poolBody(1024 * 1024 + 1)).toBeNull();
  const small = poolBody(20_000)!;
  expect(small.length).toBe(20_000);
  expect(small.buffer.byteLength).toBe(32 * 1024);
});

test("off turns pooling off", () => {
  resetPool(false);
  expect(poolBody(N)).toBeNull();
});

const shared: Array<[string, (b: Uint8Array, c: PoolConn, s: ReturnType<typeof store>) => void]> = [
  ["fanout: a second enqueue", (b, c, s) => {
    noteEnqueue(b, true, s, 1);
    noteEnqueue(b, false, s, 2);
    noteHandoff(b);
    noteDeliver(b, c);
    noteAck(b, 1);
  }],
  ["an enqueue the single-queue path did not make", (b, c, s) => {
    noteEnqueue(b, false, s, 1);
    noteHandoff(b);
    noteDeliver(b, c);
    noteAck(b, 1);
  }],
  ["a requeue: a second hand-off", (b, c, s) => {
    noteEnqueue(b, true, s, 1);
    noteHandoff(b);
    noteDeliver(b, c);
    noteHandoff(b);
    noteDeliver(b, c);
    noteAck(b, 1);
  }],
  ["a consumer that is not an AMQP deliver (shovel, basic.get, MQTT)", (b, _c, s) => {
    noteEnqueue(b, true, s, 1);
    noteHandoff(b);
    noteAck(b, 1);
  }],
  ["a republish after the enqueue (shovel, federation)", (b, c, s) => {
    noteEnqueue(b, true, s, 1);
    notePublish(b);
    noteHandoff(b);
    noteDeliver(b, c);
    noteAck(b, 1);
  }],
  ["a trace or forward copy", (b, c, s) => {
    noteEnqueue(b, true, s, 1);
    noteHandoff(b);
    taintBody(b);
    noteDeliver(b, c);
    noteAck(b, 1);
  }],
  ["a taint after the ack", (b, c, s) => {
    noteEnqueue(b, true, s, 1);
    noteHandoff(b);
    noteDeliver(b, c);
    noteAck(b, 1);
    taintBody(b);
  }],
  ["an ack for a different row", (b, c, s) => {
    noteEnqueue(b, true, s, 1);
    noteHandoff(b);
    noteDeliver(b, c);
    noteAck(b, 2);
  }],
  ["a deliver without a hand-off", (b, c, s) => {
    noteEnqueue(b, true, s, 1);
    noteDeliver(b, c);
    noteAck(b, 1);
  }],
];

for (const [name, run] of shared) {
  test(`not reused: ${name}`, () => {
    const b = poolBody(N)!;
    run(b, conn(), store());
    reclaimNow();
    expect(poolStats.released).toBe(0);
    expect(poolBody(N)!.buffer).not.toBe(b.buffer);
    expect(poolStats.violations).toBe(0);
  });
}

test("waits while the store still holds the row", () => {
  const s = store(new Set([5]));
  const b = poolBody(N)!;
  chain(b, conn(), s, 5);
  reclaimNow();
  expect(poolStats.released).toBe(0);
  s.held.delete(5);
  reclaimNow();
  expect(poolStats.released).toBe(1);
});

const busy: Array<[string, PoolConn, (c: PoolConn) => void]> = [
  ["staged frames", conn({ staged: [1] }), (c) => (c.staged = [])],
  ["a queued write", conn({ writesPending: 1 }), (c) => (c.writesPending = 0)],
  ["an unsent tail", conn({ outbound: [1], outHead: 0 }), (c) => (c.outHead = 1)],
  ["a node socket still holding a write", conn({ reuseWrites: false, held: { n: 2 } }), (c) => (c.held!.n = 0)],
];

for (const [name, c, settle] of busy) {
  test(`waits for the socket: ${name}`, () => {
    const b = poolBody(N)!;
    chain(b, c, store(), null);
    reclaimNow();
    expect(poolStats.released).toBe(0);
    settle(c);
    reclaimNow();
    expect(poolStats.released).toBe(1);
  });
}

test("a socket whose writes are not tracked never releases", () => {
  const b = poolBody(N)!;
  chain(b, conn({ reuseWrites: false, held: null }), store(), null);
  reclaimNow();
  expect(poolStats.released).toBe(0);
});

test("a closed connection drops the body instead of reusing it", () => {
  const c = conn();
  const b = poolBody(N)!;
  c.staged = [1];
  chain(b, c, store(), null);
  c.closed = true;
  reclaimNow();
  expect(poolStats.released).toBe(0);
  expect(poolStats.dropped).toBe(1);
});

import { expect, test } from "bun:test";
import { SENT } from "../src/amqp/frames.ts";
import type { Ch, Conn } from "../src/amqp/listen.ts";
import { sendConfirm } from "../src/amqp/publish.ts";

/**
 * Confirm coalescing (`sendConfirm` in src/amqp/publish.ts) rewrites the last
 * staged basic.ack to `multiple`. With confirms finishing out of order (a
 * fan-out forwarded to other cores), a staged ack for a later tag must never
 * be rewritten down to a lower `multiple`: that tag would never be confirmed.
 */
function harness() {
  const wire: Uint8Array[] = [];
  const conn = {
    staged: [] as Uint8Array[],
    send(frame: Uint8Array) {
      this.staged.push(frame);
      return SENT;
    },
    outFrame: (n: number) => new Uint8Array(n),
  };
  const c = { confirmLow: 0, confirmAhead: null, ackFrame: null, ackTag: 0 } as unknown as Ch;
  /** Write the staged frames: their bytes as they are now. */
  const flush = () => {
    for (const f of conn.staged) wire.push(f.slice());
    conn.staged = [];
  };
  /** What a client confirms from the written acks (amqplib's `handleConfirm`). */
  const confirmed = () => {
    flush();
    const got = new Set<number>();
    for (const f of wire) {
      const view = new DataView(f.buffer, f.byteOffset);
      const tag = Number(view.getBigUint64(11));
      if (f[19] === 1) for (let t = 1; t <= tag; t++) got.add(t);
      else got.add(tag);
    }
    return got;
  };
  const settle = (tag: number) => sendConfirm(conn as unknown as Conn, 1, c, tag, false);
  return { settle, flush, confirmed, wire };
}

test("an out-of-order ack still staged is not rewritten down to a lower multiple", () => {
  const h = harness();
  // The order traced from a stalled 3-core broker (tags 472, 501, 493, 498):
  // 5 settles early and is staged alone, then 2 completes the run to 2 while
  // that frame is still staged. The frames go out between steps, so no later
  // rewrite reaches 5 again.
  h.settle(1);
  h.flush();
  h.settle(5);
  h.settle(2);
  h.flush();
  h.settle(4);
  h.flush();
  h.settle(3);
  expect([...h.confirmed()].sort((a, b) => a - b)).toEqual([1, 2, 3, 4, 5]);
});

test("in-order confirms still coalesce into one multiple ack", () => {
  const h = harness();
  for (let t = 1; t <= 50; t++) h.settle(t);
  expect(h.confirmed().size).toBe(50);
  expect(h.wire.length).toBe(1);
});

test("every tag is confirmed under random out-of-order settles and flushes", () => {
  let seed = 12345;
  const rand = () => ((seed = (seed * 1103515245 + 12345) >>> 0) / 0x100000000);
  for (let round = 0; round < 200; round++) {
    const h = harness();
    const n = 50 + Math.floor(rand() * 200);
    // Settle order: mostly ascending, with tags held back by up to 30 places.
    const order = Array.from({ length: n }, (_, i) => ({ tag: i + 1, at: i + rand() * 30 }));
    order.sort((a, b) => a.at - b.at);
    for (const { tag } of order) {
      h.settle(tag);
      if (rand() < 0.2) h.flush();
    }
    const got = h.confirmed();
    for (let t = 1; t <= n; t++) if (!got.has(t)) throw new Error(`round ${round}: tag ${t} of ${n} never confirmed`);
  }
});

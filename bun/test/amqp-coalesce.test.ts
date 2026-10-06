import { expect, test } from "bun:test";
import { Conn } from "../src/amqp/listen.ts";
import "../src/amqp/frames.ts";
import type { Broker } from "../src/broker/index.ts";

/**
 * Fake socket that records each write. `partialAt` makes the first write
 * accept only that many bytes.
 */
function capture(partialAt?: number) {
  const writes: number[][] = [];
  let flushes = 0;
  let partial = partialAt != null;
  const socket = {
    write(b: Uint8Array) {
      if (partial) {
        partial = false;
        const n = Math.min(partialAt ?? 0, b.length);
        writes.push(Array.from(b.subarray(0, n)));
        return n;
      }
      writes.push(Array.from(b));
      return b.length;
    },
    flush() {
      flushes++;
    },
    end() {},
  };
  const conn = new Conn(socket, undefined as unknown as Broker);
  return {
    conn,
    writes,
    flushes: () => flushes,
  };
}

test("a staged frame is written before the caller continues", async () => {
  const cap = capture();
  let seenAtResume = -1;
  await (async () => {
    await cap.conn.send(Uint8Array.of(1, 2, 0xce));
    seenAtResume = cap.writes.length;
  })();
  expect(seenAtResume).toBe(1);
  expect(cap.writes).toEqual([[1, 2, 0xce]]);
  expect(cap.flushes()).toBe(1);
});

test("a burst of frames shares one socket write", async () => {
  const cap = capture();
  await (async () => {
    cap.conn.send(Uint8Array.of(1, 1, 0xce));
    cap.conn.send(Uint8Array.of(1, 2, 0xce));
    await cap.conn.send(Uint8Array.of(1, 3, 0xce));
  })();
  expect(cap.writes).toEqual([[1, 1, 0xce, 1, 2, 0xce, 1, 3, 0xce]]);
  expect(cap.flushes()).toBe(1);
});

test("a short write keeps the unsent tail ahead of the next frame", async () => {
  const cap = capture(2);
  await (async () => {
    cap.conn.send(Uint8Array.of(1, 2, 3));
    await cap.conn.send(Uint8Array.of(4, 5, 6));
  })();
  await cap.conn.send(Uint8Array.of(7));
  expect(cap.writes.flat()).toEqual([1, 2, 3, 4, 5, 6, 7]);
});

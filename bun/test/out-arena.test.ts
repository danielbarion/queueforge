import { expect, test } from "bun:test";
import { Conn } from "../src/amqp/listen.ts";
import "../src/amqp/frames.ts";
import { encodeDeliver, encodeSettle } from "../src/codec.ts";
import type { Broker } from "../src/broker/index.ts";

/**
 * The outbound frame arena (`Conn.arena`): deliveries and confirms encoded
 * back to back go out as one uncopied write, and bytes a socket may still
 * hold are never overwritten.
 */

type Socket = { write(b: Uint8Array): number; flush(): void; end(): void };

function conn(socket: Socket, copies: boolean) {
  const c = new Conn(socket, undefined as unknown as Broker);
  c.reuseWrites = copies;
  // An arena is not zeroed: start from garbage so a byte an encoder leaves
  // unwritten shows up as a wrong frame.
  c.arena = new Uint8Array(4096).fill(0xaa);
  return c;
}

const msg = (i: number) => ({
  exchange: "",
  routingKey: "jobs",
  redelivered: false,
  propRaw: new Uint8Array(0),
  body: new TextEncoder().encode(`body-${i}`.padEnd(40, "."))
});

/** The bytes the generic encoder would produce, for comparison. */
const plain = (i: number) => encodeDeliver(1, "ctag", i, msg(i))!;
const settle = (i: number) => encodeSettle(1, i, false);

test("frames encoded into the arena are written as one view without a copy", async () => {
  const writes: Uint8Array[] = [];
  const c = conn({ write: (b) => (writes.push(b), b.length), flush() {}, end() {} }, true);
  await (async () => {
    for (let i = 1; i <= 3; i++) c.send(encodeDeliver(1, "ctag", i, msg(i), undefined, 0, c)!);
    await c.send(encodeSettle(1, 9, false, c));
  })();
  expect(writes.length).toBe(1);
  // One view of the arena itself: nothing was gathered into another buffer.
  expect(writes[0]!.buffer).toBe(c.arena!.buffer);
  const want = [...plain(1), ...plain(2), ...plain(3), ...settle(9)];
  expect([...writes[0]!]).toEqual(want);
  // The socket copied the bytes, so the arena starts over.
  expect(c.arenaPos).toBe(0);
});

test("a socket that keeps the buffers it was given never sees them overwritten", async () => {
  // node:net semantics: `write` queues the very buffer.
  const kept: Array<{ view: Uint8Array; copy: number[] }> = [];
  const c = conn({ write: (b) => (kept.push({ view: b, copy: [...b] }), b.length), flush() {}, end() {} }, false);
  for (let round = 0; round < 400; round++) {
    await (async () => {
      c.send(encodeDeliver(1, "ctag", round, msg(round), undefined, 0, c)!);
      await c.send(encodeSettle(1, round, false, c));
    })();
  }
  // 400 rounds pass several arenas; every kept buffer still holds what was written.
  for (const k of kept) expect([...k.view]).toEqual(k.copy);
});

test("an unsent tail after a short write is not overwritten by later frames", async () => {
  let accept = 10;
  const out: number[] = [];
  const c = conn(
    {
      write(b) {
        const n = Math.min(accept, b.length);
        out.push(...b.subarray(0, n));
        return n;
      },
      flush() {},
      end() {},
    },
    true,
  );
  await (async () => {
    c.send(encodeDeliver(1, "ctag", 1, msg(1), undefined, 0, c)!);
    await c.send(encodeDeliver(1, "ctag", 2, msg(2), undefined, 0, c)!);
  })();
  // The tail waits on `outbound`; a new burst encodes behind it.
  await c.send(encodeDeliver(1, "ctag", 3, msg(3), undefined, 0, c)!);
  accept = 1 << 30;
  c.flush();
  await c.send(encodeSettle(1, 4, false, c));
  expect(out).toEqual([...plain(1), ...plain(2), ...plain(3), ...settle(4)]);
});

test("frames larger than a quarter of the arena get their own buffer", () => {
  const c = conn({ write: (b) => b.length, flush() {}, end() {} }, true);
  const before = c.arenaPos;
  const big = c.outFrame(32 * 1024);
  expect(big.buffer).not.toBe(c.arena!.buffer);
  expect(c.arenaPos).toBe(before);
  expect(big.length).toBe(32 * 1024);
});

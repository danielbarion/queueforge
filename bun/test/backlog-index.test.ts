import { describe, expect, test } from "bun:test";
import { LiveIndex } from "../src/fast-log.ts";
import { pickConsumer } from "../src/broker/routing.ts";
import type { Consumer, QueueLive } from "../src/broker/index.ts";

describe("live row index", () => {
  test("stores, overwrites and forgets rows past its first capacity", () => {
    const index = new LiveIndex();
    for (let id = 1; id <= 5000; id++) index.set(id, id % 7, id * 10, id + 1);
    expect(index.size).toBe(5000);
    const s = index.slot(4321)!;
    expect([index.chunk[s], index.at[s], index.len[s]]).toEqual([4321 % 7, 43210, 4322]);
    index.set(4321, 99, 5, 6);
    const again = index.slot(4321)!;
    expect(again).toBe(s);
    expect([index.chunk[again], index.at[again], index.len[again]]).toEqual([99, 5, 6]);
    expect(index.delete(4321)).toBe(99);
    expect(index.delete(4321)).toBeUndefined();
    expect(index.has(4321)).toBe(false);
    // A freed slot is reused before the arrays grow.
    index.set(9999, 1, 2, 3);
    expect(index.slot(9999)).toBe(s);
  });

  test("a drained index gives back its arrays", () => {
    const index = new LiveIndex();
    for (let id = 1; id <= 3000; id++) index.set(id, 1, 0, 1);
    expect(index.chunk.length).toBeGreaterThanOrEqual(3000);
    for (let id = 1; id <= 3000; id++) index.delete(id);
    expect(index.size).toBe(0);
    expect(index.chunk.length).toBe(1024);
    index.set(7, 2, 3, 4);
    expect(index.slot(7)).toBe(0);
  });
});

describe("consumer choice", () => {
  const consumer = (priority: number, wants: () => boolean) => ({ priority, want: wants }) as unknown as Consumer;
  const queue = (consumers: unknown[]) => ({ argsParsed: {}, consumers, rr: 0 }) as unknown as QueueLive;

  test("round-robin among the highest priority that wants a delivery", () => {
    let busy = false;
    const low = consumer(0, () => true);
    const a = consumer(5, () => !busy);
    const b = consumer(5, () => true);
    const q = queue([low, a, b]);
    expect(pickConsumer(q)).toBe(a);
    expect(pickConsumer(q)).toBe(b);
    expect(pickConsumer(q)).toBe(a);
    busy = true;
    expect(pickConsumer(q)).toBe(b);
    expect(pickConsumer(q)).toBe(b);
  });

  test("falls to a lower priority only when no higher one wants, and null when none do", () => {
    let open = true;
    const low = consumer(0, () => true);
    const high = consumer(9, () => open);
    const q = queue([high, low]);
    expect(pickConsumer(q)).toBe(high);
    open = false;
    expect(pickConsumer(q)).toBe(low);
    expect(pickConsumer(queue([consumer(1, () => false)]))).toBeNull();
    expect(pickConsumer(queue([]))).toBeNull();
  });
});

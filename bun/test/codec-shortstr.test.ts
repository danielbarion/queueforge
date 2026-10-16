import { expect, test } from "bun:test";
import { R, W } from "../src/codec.ts";

// shortstr builds ASCII names from char codes and falls back to UTF-8 for
// anything else; both must give the same string the encoder wrote.
test("shortstr reads ASCII, UTF-8 and long names", () => {
  const names = ["", "amq.direct", "orders.eu-west.created", "café", "очередь", "队列-1", "x".repeat(64), "y".repeat(65), "é".repeat(100)];
  for (const name of names) {
    const w = new W();
    w.shortstr(name);
    w.u8(7);
    const r = new R(w.concat());
    expect(r.shortstr()).toBe(name);
    expect(r.u8()).toBe(7);
  }
});

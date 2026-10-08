import { expect, test } from "bun:test";
import { homeHash, pickConsumer, queueHome } from "../src/broker/routing.ts";

// The home hash is shared with Rust and PHP (docs/raft.md, section 9).
test("classic queue home hash matches the shared spec", () => {
  expect(homeHash("/", "orders")).toBe(0x58c6545457c80e72n);
  expect(queueHome("/", "orders", [{ id: "n3" }, { id: "n1" }, { id: "n2" }])).toBe("n3");
});

// A single-active-consumer queue whose consumers all left is swept every
// second. Picking from an empty list used to throw and end the process.
test("a single-active queue with no consumers picks nobody", () => {
  const q = { argsParsed: { singleActive: true }, consumers: [], rr: 0 } as unknown as Parameters<typeof pickConsumer>[0];
  expect(pickConsumer(q)).toBeNull();
});

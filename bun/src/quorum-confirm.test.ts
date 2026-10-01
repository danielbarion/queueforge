import { expect, test } from "bun:test";
import { durableMajority } from "./quorum-confirm.ts";

// The live AMQP path calls durableMajority from Broker.enqueueQuorum.
// quorum-failover.test.ts drives that path with a real client: confirm, kill -9, deliver once.

test("memory copies do not confirm and durable copies do", () => {
  expect(durableMajority(3, ["memory", "memory", "durable"])).toBe(false);
  expect(durableMajority(3, ["durable", "durable", "memory"])).toBe(true);
  expect(durableMajority(1, ["durable"])).toBe(true);
  expect(durableMajority(1, ["memory"])).toBe(false);
});

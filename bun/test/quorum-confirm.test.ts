import { expect, test } from "bun:test";
import { durableMajority, selectQuorumPeers } from "../src/quorum-confirm.ts";

// The live AMQP path calls durableMajority from Broker.enqueueQuorum.
// quorum-failover.test.ts drives that path with a real client: confirm, kill -9, deliver once.

test("memory copies do not confirm and durable copies do", () => {
  expect(durableMajority(3, ["memory", "memory", "durable"])).toBe(false);
  expect(durableMajority(3, ["durable", "durable", "memory"])).toBe(true);
  expect(durableMajority(1, ["durable"])).toBe(true);
  expect(durableMajority(1, ["memory"])).toBe(false);
});

test("a lagging extra peer is skipped once another peer can complete the majority", () => {
  const idle = new Map<string, number>();
  expect(selectQuorumPeers(["b", "c"], idle, 1)).toEqual(["b", "c"]);
  const lagging = new Map<string, number>([["c", 32]]);
  expect(selectQuorumPeers(["b", "c"], lagging, 1)).toEqual(["b"]);
  const both = new Map<string, number>([
    ["b", 100],
    ["c", 40],
  ]);
  expect(selectQuorumPeers(["b", "c"], both, 1)).toEqual(["c"]);
  expect(selectQuorumPeers(["c"], both, 1)).toEqual(["c"]);
});

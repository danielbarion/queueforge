import { expect, test } from "bun:test";
import type { Broker } from "../src/broker/index.ts";
import { Consensus } from "../src/raft/glue.ts";

test("a reply without a feature list keeps the peer's features", () => {
  const consensus = new Consensus({} as Broker, () => {});
  consensus.noteFeatures("n2", { features: ["enqueue_many", "bin_frames"] });
  expect(consensus.manyPeers.has("n2")).toBe(true);
  expect(consensus.binPeers.has("n2")).toBe(true);
  // The `true` that answers a forwarded enqueue.
  consensus.noteFeatures("n2", {}, false);
  consensus.noteFeatures("n2", undefined, false);
  expect(consensus.manyPeers.has("n2")).toBe(true);
  expect(consensus.binPeers.has("n2")).toBe(true);
  // A hello without the list: a peer that takes neither.
  consensus.noteFeatures("n2", {});
  expect(consensus.manyPeers.has("n2")).toBe(false);
  expect(consensus.binPeers.has("n2")).toBe(false);
});

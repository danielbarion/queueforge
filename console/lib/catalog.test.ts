import { describe, expect, test } from "bun:test";
import { definitionCounts, firstGauge, parseConnections, parseExchanges } from "./catalog";

describe("catalog", () => {
  test("exchanges need an items list", () => {
    expect(parseExchanges({ error: "no" })).toBeNull();
    expect(parseExchanges({ items: [{ name: "amq.direct", type: "direct", durable: true }] })).toEqual([
      { name: "amq.direct", type: "direct", durable: true },
    ]);
  });

  test("connections without rows keep the count", () => {
    expect(parseConnections({ items: [], total_count: 4 })).toEqual({ rows: [], total: 4 });
    expect(parseConnections({ items: [{ name: "c1", user: "guest", peer_host: "10.0.0.2", peer_port: 50000, channels: 2 }] })?.rows).toEqual([
      { name: "c1", user: "guest", peerHost: "10.0.0.2", peerPort: 50000, channels: 2 },
    ]);
  });

  test("definition counts accept a map of exchanges", () => {
    expect(definitionCounts({ vhosts: [{ name: "/" }], queues: [{}], exchanges: { "amq.direct": "direct" }, bindings: [] })).toEqual({
      vhosts: 1,
      queues: 1,
      exchanges: 1,
      bindings: 0,
    });
  });

  test("free disk uses the first gauge and does not add a second", () => {
    const text = "rabbitmq_disk_space_available_bytes 100\nrabbitmq_disk_space_available_bytes 250\n";
    expect(firstGauge(text, "rabbitmq_disk_space_available_bytes")).toBe(100);
  });
});

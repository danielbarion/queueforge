import { expect, test } from "bun:test";
import { probeFleet } from "./fleet";
import type { BrokerCall } from "./client";
import type { BrokerTarget } from "../stores/broker";
const target: BrokerTarget = { id: "fleet-test", name: "Configured Rust", kind: "rust", url: "http://broker.test:15672" };
const reply = (body: unknown, status = 200, text = ""): BrokerCall => ({ body, status, text });
async function probe(responses: Record<string, BrokerCall | Error>) {
  const original = globalThis.fetch;
  const paths: string[] = [];
  globalThis.fetch = (async (_input: unknown, init?: RequestInit) => {
    const request = JSON.parse(String(init?.body)) as { path: string; url: string; method: string };
    expect(request.url).toBe(target.url); expect(request.method).toBe("GET"); paths.push(request.path);
    const response = responses[request.path] ?? reply(null, 404);
    if (response instanceof Error) throw response;
    return Response.json(response);
  }) as typeof fetch;
  try { return { snapshot: await probeFleet(target), paths }; }
  finally { globalThis.fetch = original; }
}

test("fleet reads actual counters and distinguishes absent probes from success", async () => {
  const { snapshot: row, paths } = await probe({ "/api/overview": reply({ rabbitmq_version: "4.0", object_totals: { connections: 0, queues: 2 } }), "/api/queues/%2F?page_size=500": reply([{ name: "a", messages_ready: 3, messages_unacknowledged: 1 }, { name: "b", messages_ready: 2, messages_unacknowledged: 0 }]) });
  expect(paths).toHaveLength(7); expect(row.target).toBe(target); expect(row.stale).toBe(false);
  expect(row.health).toBeNull(); expect(row.ready).toBeNull(); expect(row.implementation).toBeNull();
  expect(row.version).toBe("4.0"); expect(row.connections).toBe(0); expect(row.queues).toBe(2);
  expect(row.readyMessages).toBe(5); expect(row.unackedMessages).toBe(1); expect(row.memoryBytes).toBeNull(); expect(row.diskBytes).toBeNull();
});

test("fleet retains login and permission uncertainty without exposing protected fields", async () => {
  const { snapshot: row } = await probe({ "/healthz": reply("ok"), "/api/overview": reply({ object_totals: { connections: 99 } }, 401), "/api/queues/%2F?page_size=500": reply([{ name: "secret", messages_ready: 99 }], 403) });
  expect(row.health).toBe(true); expect(row.loginNeeded).toBe(true); expect(row.stale).toBe(true);
  expect(row.connections).toBeNull(); expect(row.queues).toBeNull(); expect(row.readyMessages).toBeNull();
  expect(row.errors.some((error) => error.includes("login needed"))).toBe(true); expect(row.errors.some((error) => error.includes("permission denied"))).toBe(true);
});

test("fleet never sums an incomplete queue page or unavailable counters", async () => {
  let { snapshot: row } = await probe({ "/api/overview": reply({}), "/api/queues/%2F?page_size=500": reply({ items: [{ name: "one", messages_ready: 5, messages_unacknowledged: 2 }], total_count: 10 }) });
  expect(row.queues).toBe(10); expect(row.readyMessages).toBeNull(); expect(row.unackedMessages).toBeNull();
  ({ snapshot: row } = await probe({ "/api/overview": reply({}), "/api/queues/%2F?page_size=500": reply([{ name: "one" }]) }));
  expect(row.queues).toBe(1); expect(row.readyMessages).toBeNull(); expect(row.unackedMessages).toBeNull();
});

test("fleet preserves malformed data as unknown", async () => {
  const { snapshot: row } = await probe({ "/api/overview": reply([]), "/api/queues/%2F?page_size=500": reply({ items: [{}] }), "/api/nodes": reply([{ mem_used: 10 }]) });
  expect(row.stale).toBe(true); expect(row.queues).toBeNull(); expect(row.readyMessages).toBeNull(); expect(row.nodes).toBeNull(); expect(row.memoryBytes).toBeNull();
});

test("fleet records unreachable and failed readiness without guessing implementation", async () => {
  const { snapshot: row } = await probe({ "/healthz": new Error("offline"), "/readyz": reply("not ready", 503), "/api/overview": reply({}), "/api/queues/%2F?page_size=500": reply([]) });
  expect(row.health).toBeNull(); expect(row.ready).toBe(false); expect(row.stale).toBe(true); expect(row.implementation).toBeNull(); expect(row.errors.some((error) => error.includes("offline"))).toBe(true);
});

test("fleet prefers local resource gauges and labels node fallback scope", async () => {
  const { snapshot: row } = await probe({ "/api/identity": reply({ kind: "bun" }), "/api/overview": reply({}), "/api/queues/%2F?page_size=500": reply([]), "/api/nodes": reply([{ name: "node-a", mem_used: 10, disk_free: 50 }, { name: "node-b", mem_used: 20, disk_free: 60 }]), "/metrics": reply(null, 200, "rabbitmq_process_resident_memory_bytes 123\n") });
  expect(row.implementation).toBe("bun"); expect(row.memoryBytes).toBe(123); expect(row.diskBytes).toBe(50); expect(row.nodes).toEqual(["node-a", "node-b"]); expect(row.resourceScope).toContain("first reported node");
  expect(row.latencyMs).toBeGreaterThanOrEqual(0); expect(row.sampledAt).toBeGreaterThan(0);
});

test("fleet accepts wrapped node responses and declared peer names", async () => {
  const { snapshot: row } = await probe({ "/api/overview": reply({ object_totals: { queues: 99 }, queue_totals: { messages_ready: 99 } }), "/api/queues/%2F?page_size=500": reply([{ name: "local", messages_ready: 1, messages_unacknowledged: 0 }]), "/api/nodes": reply({ items: [{ name: "a", mem_used: 12, peers: [{ name: "b" }, { name: "a" }, {}] }] }) });
  expect(row.nodes).toEqual(["a", "b"]); expect(row.memoryBytes).toBe(12); expect(row.queues).toBe(1); expect(row.readyMessages).toBe(1);
});

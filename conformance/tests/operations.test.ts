import { expect, test } from "bun:test";
import { channel, collect, confirmChannel, eventually, mgmt, nextMessage, sleep, uniq } from "../lib.ts";

test("Management HTTP API :: overview, queue and connection listings answer", async () => {
  const ch = await channel();
  const q = uniq("mgmt");
  await ch.assertQueue(q, { durable: true });
  expect((await mgmt("/api/overview")).ok).toBe(true);
  const queue = await mgmt(`/api/queues/%2f/${q}`);
  expect(queue.ok).toBe(true);
  expect(((await queue.json()) as { name: string }).name).toBe(q);
  const conns = (await (await mgmt("/api/connections")).json()) as unknown[];
  expect(conns.length).toBeGreaterThan(0);
});

test("Definitions import and export :: export lists queues, exchanges, bindings and users", async () => {
  const ch = await channel();
  const q = uniq("def-q");
  const x = uniq("def-x");
  await ch.assertQueue(q, { durable: true });
  await ch.assertExchange(x, "direct", { durable: true });
  await ch.bindQueue(q, x, "k");
  const defs = (await (await mgmt("/api/definitions")).json()) as {
    queues: Array<{ name: string }>;
    exchanges: Array<{ name: string }>;
    bindings: Array<{ source: string; destination: string }>;
    users: Array<{ name: string }>;
  };
  expect(defs.queues.some((d) => d.name === q)).toBe(true);
  expect(defs.exchanges.some((d) => d.name === x)).toBe(true);
  expect(defs.bindings.some((b) => b.source === x && b.destination === q)).toBe(true);
  expect(defs.users.some((u) => u.name === "admin")).toBe(true);
  await ch.deleteQueue(q);
  await ch.deleteExchange(x);
});

test("Definitions import and export :: import creates queues, exchanges and exchange-to-exchange bindings", async () => {
  const q = uniq("imp-q");
  const a = uniq("imp-a");
  const b = uniq("imp-b");
  const body = {
    queues: [{ name: q, vhost: "/", durable: true, auto_delete: false, arguments: {} }],
    exchanges: [
      { name: a, vhost: "/", type: "fanout", durable: true, auto_delete: false, internal: false, arguments: {} },
      { name: b, vhost: "/", type: "fanout", durable: true, auto_delete: false, internal: false, arguments: {} },
    ],
    bindings: [
      { source: a, vhost: "/", destination: b, destination_type: "exchange", routing_key: "", arguments: {} },
      { source: b, vhost: "/", destination: q, destination_type: "queue", routing_key: "", arguments: {} },
    ],
  };
  const res = await mgmt("/api/definitions", { method: "POST", body: JSON.stringify(body) });
  expect(res.ok).toBe(true);
  const ch = await channel();
  ch.publish(a, "", Buffer.from("imported"));
  expect((await nextMessage(ch, q, 2000, { noAck: true }))?.content.toString()).toBe("imported");
  await ch.deleteQueue(q);
  await ch.deleteExchange(a);
  await ch.deleteExchange(b);
});

test("Policies :: a message-ttl policy applies to a matching queue", async () => {
  const prefix = uniq("pol");
  const name = uniq("policy");
  const res = await mgmt(`/api/policies/%2f/${name}`, {
    method: "PUT",
    body: JSON.stringify({ pattern: `^${prefix}`, definition: { "message-ttl": 100 }, "apply-to": "queues", priority: 1 }),
  });
  expect(res.ok).toBe(true);
  const ch = await confirmChannel();
  const q = `${prefix}-q`;
  await ch.assertQueue(q, { durable: true });
  await sleep(200);
  ch.sendToQueue(q, Buffer.from("x"));
  await ch.waitForConfirms();
  await sleep(400);
  expect((await ch.checkQueue(q)).messageCount).toBe(0);
  await mgmt(`/api/policies/%2f/${name}`, { method: "DELETE" });
});

test("Operator policies :: an operator max-length caps a larger queue argument", async () => {
  const prefix = uniq("opol");
  const name = uniq("op-policy");
  const res = await mgmt(`/api/operator-policies/%2f/${name}`, {
    method: "PUT",
    body: JSON.stringify({ pattern: `^${prefix}`, definition: { "max-length": 1 }, "apply-to": "queues", priority: 1 }),
  });
  expect(res.ok).toBe(true);
  const ch = await confirmChannel();
  const q = `${prefix}-q`;
  await ch.assertQueue(q, { durable: true, arguments: { "x-max-length": 10 } });
  await sleep(200);
  for (const v of ["a", "b", "c"]) ch.sendToQueue(q, Buffer.from(v));
  await ch.waitForConfirms();
  const got = await collect(ch, q, 3, 500, { noAck: true });
  expect(got.map((m) => m.content.toString())).toEqual(["c"]);
  await mgmt(`/api/operator-policies/%2f/${name}`, { method: "DELETE" });
});

test("Shovel :: a dynamic shovel moves messages from one queue to another", async () => {
  const ch = await confirmChannel();
  const src = uniq("shovel-src");
  const dst = uniq("shovel-dst");
  await ch.assertQueue(src, { durable: true });
  await ch.assertQueue(dst, { durable: true });
  const name = uniq("shovel");
  const res = await mgmt(`/api/parameters/shovel/%2f/${name}`, {
    method: "PUT",
    body: JSON.stringify({
      value: { "src-protocol": "amqp091", "src-uri": "amqp://", "src-queue": src, "dest-protocol": "amqp091", "dest-uri": "amqp://", "dest-queue": dst },
    }),
  });
  expect(res.ok).toBe(true);
  ch.sendToQueue(src, Buffer.from("moved"), { persistent: true });
  await ch.waitForConfirms();
  const msg = await nextMessage(ch, dst, 5000, { noAck: true });
  expect(msg?.content.toString()).toBe("moved");
  await mgmt(`/api/parameters/shovel/%2f/${name}`, { method: "DELETE" });
  await ch.deleteQueue(src);
  await ch.deleteQueue(dst);
});

test("Event exchange :: declaring a queue publishes queue.created", async () => {
  const ch = await channel();
  const events = uniq("events");
  await ch.assertQueue(events, { durable: true, exclusive: true });
  await ch.bindQueue(events, "amq.rabbitmq.event", "queue.created");
  const made = uniq("evented");
  await ch.assertQueue(made, { durable: true });
  const got = await collect(ch, events, 20, 1500, { noAck: true });
  const names = got.map((m) => String(m.properties.headers?.name ?? ""));
  expect(names).toContain(made);
  await ch.deleteQueue(made);
});

test("Message tracing :: with tracing on, a publish is copied to amq.rabbitmq.trace", async () => {
  const on = await mgmt("/api/vhosts/%2f", { method: "PUT", body: JSON.stringify({ tracing: true }) });
  expect(on.ok).toBe(true);
  try {
    const ch = await channel();
    const trace = uniq("trace");
    await ch.assertQueue(trace, { durable: true, exclusive: true });
    await ch.bindQueue(trace, "amq.rabbitmq.trace", "publish.#");
    const q = uniq("traced");
    await ch.assertQueue(q, { durable: true });
    await sleep(100);
    ch.sendToQueue(q, Buffer.from("watched"));
    const copies = await collect(ch, trace, 10, 1500, { noAck: true });
    expect(copies.some((m) => m.content.toString() === "watched")).toBe(true);
  } finally {
    await mgmt("/api/vhosts/%2f", { method: "PUT", body: JSON.stringify({ tracing: false }) });
  }
});

test("Feature flags :: the feature flag list answers", async () => {
  const res = await mgmt("/api/feature-flags");
  expect(res.ok).toBe(true);
  const flags = (await res.json()) as Array<{ name: string; state: string }>;
  expect(flags.length).toBeGreaterThan(0);
  expect(flags.every((f) => typeof f.name === "string" && typeof f.state === "string")).toBe(true);
});

test("Memory and disk alarms :: the node reports its alarm state", async () => {
  const res = await mgmt("/api/nodes");
  expect(res.ok).toBe(true);
  const nodes = (await res.json()) as Array<Record<string, unknown>>;
  expect(typeof nodes[0]?.mem_alarm).toBe("boolean");
  expect(typeof nodes[0]?.disk_free_alarm).toBe("boolean");
  await eventually(() => true);
});

/**
 * RabbitMQ behaviour a production deployment leans on beyond the basics:
 * consumer acknowledgement timeouts, stream filtering, header and property
 * hashing, federation of exchanges and queues, and the 4.x queue arguments
 * clients send whether or not the broker acts on them.
 */
import { expect, test } from "bun:test";
import type { ConsumeMessage } from "amqplib";
import { channel, closeCode, collect, confirmChannel, eventually, mgmt, nextMessage, sleep, uniq } from "../lib.ts";

/**
 * RabbitMQ 4.3 enforces delivery acknowledgement timeouts on quorum queues
 * only. A client that advertises consumer_cancel_notify (amqplib does) gets
 * basic.cancel for the timed-out consumer; the channel stays open and the
 * message goes back to the queue.
 */
async function heldPastTimeout(declare: Record<string, unknown>, consume: Record<string, unknown> = {}) {
  const ch = await channel();
  let channelClosed = false;
  ch.on("close", () => {
    channelClosed = true;
  });
  const q = uniq("ctimeout");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "quorum", ...declare } });
  ch.sendToQueue(q, Buffer.from("held"), { persistent: true });
  const got: ConsumeMessage[] = [];
  let cancelled = false;
  await ch.consume(
    q,
    (m) => {
      if (m) got.push(m);
      else cancelled = true;
    },
    { noAck: false, arguments: consume },
  );
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  // RabbitMQ evaluates timeouts on a periodic tick, so allow up to a minute.
  expect(await eventually(() => cancelled, 75000)).toBe(true);
  expect(channelClosed).toBe(false);
  const other = await channel();
  const back = await nextMessage(other, q, 3000, { noAck: true });
  expect(back?.content.toString()).toBe("held");
  expect(back?.fields.redelivered).toBe(true);
  await other.deleteQueue(q);
}

test("Consumer acknowledgement timeout :: a delivery held past the queue's x-consumer-timeout cancels the consumer and is requeued", async () => {
  await heldPastTimeout({ "x-consumer-timeout": 1000 });
}, 90000);

test("Consumer acknowledgement timeout :: x-consumer-timeout on basic.consume sets the timeout for one consumer", async () => {
  await heldPastTimeout({}, { "x-consumer-timeout": 1000 });
}, 90000);

test("Consumer acknowledgement timeout :: a delivery acked in time keeps the consumer", async () => {
  const ch = await channel();
  const q = uniq("ctimeout-ok");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "quorum", "x-consumer-timeout": 1000 } });
  let cancelled = false;
  const got: ConsumeMessage[] = [];
  await ch.consume(q, (m) => {
    if (!m) {
      cancelled = true;
      return;
    }
    got.push(m);
    ch.ack(m);
  });
  ch.sendToQueue(q, Buffer.from("quick"), { persistent: true });
  expect(await eventually(() => got.length === 1, 3000)).toBe(true);
  await sleep(2500);
  ch.sendToQueue(q, Buffer.from("again"), { persistent: true });
  expect(await eventually(() => got.length === 2, 3000)).toBe(true);
  expect(cancelled).toBe(false);
  await ch.deleteQueue(q);
});

test("Consumer acknowledgement timeout :: a classic queue refuses x-consumer-timeout with 406", async () => {
  const ch = await channel();
  expect(await closeCode(ch.assertQueue(uniq("ctimeout-cq"), { durable: true, arguments: { "x-consumer-timeout": 1000 } }))).toBe(406);
});

async function streamQueue(ch: Awaited<ReturnType<typeof confirmChannel>>, args: Record<string, unknown> = {}) {
  const q = uniq("sfilter");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "stream", ...args } });
  return q;
}

/** Publish one confirmed batch, so each batch lands in its own stream chunk. */
async function batch(ch: Awaited<ReturnType<typeof confirmChannel>>, q: string, value: string | null, n: number) {
  for (let i = 0; i < n; i++) {
    const headers = value === null ? {} : { "x-stream-filter-value": value };
    ch.sendToQueue(q, Buffer.from(`${value ?? "none"}-${i}`), { persistent: true, headers });
  }
  await ch.waitForConfirms();
  await sleep(100);
}

async function readStream(q: string, args: Record<string, unknown>, ms = 1500): Promise<string[]> {
  const ch = await channel();
  await ch.prefetch(100);
  const got: string[] = [];
  const { consumerTag } = await ch.consume(
    q,
    (m) => {
      if (!m) return;
      got.push(m.content.toString());
      ch.ack(m);
    },
    { noAck: false, arguments: { "x-stream-offset": "first", ...args } },
  );
  await sleep(ms);
  await ch.cancel(consumerTag);
  return got;
}

test("Stream filtering :: x-stream-filter delivers the matching values and skips other chunks", async () => {
  const ch = await confirmChannel();
  const q = await streamQueue(ch);
  await batch(ch, q, "red", 5);
  await batch(ch, q, "blue", 5);
  await batch(ch, q, "red", 5);
  const got = await readStream(q, { "x-stream-filter": "red" });
  expect(got.filter((m) => m.startsWith("red")).length).toBe(10);
  expect(got.filter((m) => m.startsWith("blue")).length).toBe(0);
  const both = await readStream(q, { "x-stream-filter": ["red", "blue"] });
  expect(both.length).toBe(15);
  await ch.deleteQueue(q);
});

test("Stream filtering :: x-stream-match-unfiltered also delivers messages without a filter value", async () => {
  const ch = await confirmChannel();
  const q = await streamQueue(ch);
  await batch(ch, q, "red", 3);
  await batch(ch, q, null, 3);
  await batch(ch, q, "blue", 3);
  const strict = await readStream(q, { "x-stream-filter": "red" });
  expect(strict.filter((m) => m.startsWith("none")).length).toBe(0);
  const loose = await readStream(q, { "x-stream-filter": "red", "x-stream-match-unfiltered": true });
  expect(loose.filter((m) => m.startsWith("red")).length).toBe(3);
  expect(loose.filter((m) => m.startsWith("none")).length).toBe(3);
  expect(loose.filter((m) => m.startsWith("blue")).length).toBe(0);
  await ch.deleteQueue(q);
});

async function hashSplit(ch: Awaited<ReturnType<typeof channel>>, args: Record<string, unknown>) {
  const x = uniq("chash-arg");
  const a = uniq("chash-arg-a");
  const b = uniq("chash-arg-b");
  await ch.assertExchange(x, "x-consistent-hash", { durable: false, arguments: args });
  await ch.assertQueue(a, { durable: true });
  await ch.assertQueue(b, { durable: true });
  await ch.bindQueue(a, x, "1");
  await ch.bindQueue(b, x, "1");
  return { x, a, b };
}

test("Consistent-hash exchange :: hash-header routes on a header value instead of the routing key", async () => {
  const ch = await channel();
  const { x, a, b } = await hashSplit(ch, { "hash-header": "tenant" });
  // One routing key, many header values: the header decides.
  for (let i = 0; i < 200; i++) ch.publish(x, "one-key", Buffer.from(`t${i}`), { headers: { tenant: `tenant-${i}` } });
  for (let i = 0; i < 5; i++) ch.publish(x, `key-${i}`, Buffer.from("same"), { headers: { tenant: "fixed" } });
  await sleep(500);
  const fromA = await collect(ch, a, 205, 1500, { noAck: true });
  const fromB = await collect(ch, b, 205, 1500, { noAck: true });
  expect(fromA.length + fromB.length).toBe(205);
  expect(fromA.length).toBeGreaterThan(30);
  expect(fromB.length).toBeGreaterThan(30);
  const sameA = fromA.filter((m) => m.content.toString() === "same").length;
  expect([0, 5]).toContain(sameA);
});

test("Consistent-hash exchange :: hash-property routes on message_id", async () => {
  const ch = await channel();
  const { x, a, b } = await hashSplit(ch, { "hash-property": "message_id" });
  for (let i = 0; i < 200; i++) ch.publish(x, "one-key", Buffer.from(`m${i}`), { messageId: `id-${i}` });
  for (let i = 0; i < 5; i++) ch.publish(x, `key-${i}`, Buffer.from("same"), { messageId: "fixed" });
  await sleep(500);
  const fromA = await collect(ch, a, 205, 1500, { noAck: true });
  const fromB = await collect(ch, b, 205, 1500, { noAck: true });
  expect(fromA.length + fromB.length).toBe(205);
  expect(fromA.length).toBeGreaterThan(30);
  expect(fromB.length).toBeGreaterThan(30);
  const sameA = fromA.filter((m) => m.content.toString() === "same").length;
  expect([0, 5]).toContain(sameA);
});

async function putUpstream(name: string, value: Record<string, unknown>) {
  const res = await mgmt(`/api/parameters/federation-upstream/%2f/${name}`, {
    method: "PUT",
    body: JSON.stringify({ value: { uri: "amqp://", ...value } }),
  });
  expect(res.ok).toBe(true);
}

async function putPolicy(name: string, pattern: string, applyTo: string, definition: Record<string, unknown>) {
  const res = await mgmt(`/api/policies/%2f/${name}`, {
    method: "PUT",
    body: JSON.stringify({ pattern, "apply-to": applyTo, definition, priority: 10 }),
  });
  expect(res.ok).toBe(true);
}

test("Federation :: a federated exchange receives what is published to its upstream exchange", async () => {
  const ch = await channel();
  const up = uniq("fed-up");
  const down = uniq("fed-down");
  const q = uniq("fed-q");
  await ch.assertExchange(up, "topic", { durable: true });
  await ch.assertExchange(down, "topic", { durable: true });
  await ch.assertQueue(q, { durable: true });
  await ch.bindQueue(q, down, "orders.#");
  const upstream = uniq("fed-upstream");
  await putUpstream(upstream, { exchange: up });
  const policy = uniq("fed-policy");
  await putPolicy(policy, `^${down}$`, "exchanges", { "federation-upstream": upstream });
  // The link starts asynchronously: publish until one copy arrives.
  let got = null;
  for (let i = 0; i < 40 && !got; i++) {
    ch.publish(up, "orders.new", Buffer.from("federated"));
    got = await nextMessage(ch, q, 250, { noAck: true });
  }
  expect(got?.content.toString()).toBe("federated");
  // Routing keys the downstream binding does not match stay upstream.
  await collect(ch, q, 100, 500, { noAck: true });
  ch.publish(up, "other.key", Buffer.from("not-bound"));
  expect(await nextMessage(ch, q, 600, { noAck: true })).toBeNull();
  await mgmt(`/api/policies/%2f/${policy}`, { method: "DELETE" });
  await mgmt(`/api/parameters/federation-upstream/%2f/${upstream}`, { method: "DELETE" });
  await ch.deleteQueue(q);
});

test("Federation :: a federated queue pulls messages from its upstream queue for its consumers", async () => {
  const ch = await channel();
  const up = uniq("fedq-up");
  const down = uniq("fedq-down");
  await ch.assertQueue(up, { durable: true });
  await ch.assertQueue(down, { durable: true });
  const upstream = uniq("fedq-upstream");
  await putUpstream(upstream, { queue: up });
  const policy = uniq("fedq-policy");
  await putPolicy(policy, `^${down}$`, "queues", { "federation-upstream": upstream });
  const got: string[] = [];
  const cch = await channel();
  await cch.consume(down, (m) => m && got.push(m.content.toString()), { noAck: true });
  ch.sendToQueue(up, Buffer.from("pulled"), { persistent: true });
  expect(await eventually(() => got.length === 1, 8000)).toBe(true);
  expect(got[0]).toBe("pulled");
  await mgmt(`/api/policies/%2f/${policy}`, { method: "DELETE" });
  await mgmt(`/api/parameters/federation-upstream/%2f/${upstream}`, { method: "DELETE" });
  await ch.deleteQueue(up);
  await ch.deleteQueue(down);
}, 15000);

test("Federation :: upstream status is listed by the management API", async () => {
  const ch = await channel();
  const up = uniq("fed-st-up");
  const down = uniq("fed-st-down");
  await ch.assertExchange(up, "fanout", { durable: true });
  await ch.assertExchange(down, "fanout", { durable: true });
  const upstream = uniq("fed-st-upstream");
  await putUpstream(upstream, { exchange: up });
  const policy = uniq("fed-st-policy");
  await putPolicy(policy, `^${down}$`, "exchanges", { "federation-upstream": upstream });
  const running = await eventually(async () => {
    const res = await mgmt("/api/federation-links/%2f");
    if (!res.ok) return false;
    const links = (await res.json()) as Array<{ upstream?: string; exchange?: string; status?: string }>;
    return links.some((l) => l.upstream === upstream && l.exchange === down && l.status === "running");
  }, 8000);
  expect(running).toBe(true);
  await mgmt(`/api/policies/%2f/${policy}`, { method: "DELETE" });
  await mgmt(`/api/parameters/federation-upstream/%2f/${upstream}`, { method: "DELETE" });
}, 15000);

test("Quorum queues :: 4.x arguments (initial group size, dead-letter strategy, delivery limit) are accepted", async () => {
  const ch = await channel();
  const q = uniq("qq-args");
  await ch.assertQueue(q, {
    durable: true,
    arguments: {
      "x-queue-type": "quorum",
      "x-quorum-initial-group-size": 1,
      "x-delivery-limit": 5,
      "x-dead-letter-strategy": "at-most-once",
      "x-overflow": "reject-publish",
    },
  });
  const res = await mgmt(`/api/queues/%2f/${q}`);
  const info = (await res.json()) as { arguments?: Record<string, unknown>; type?: string };
  expect(info.type).toBe("quorum");
  expect(info.arguments?.["x-quorum-initial-group-size"]).toBe(1);
  await ch.deleteQueue(q);
});

test("Stream queues :: segment size, initial cluster size and max-age arguments are accepted", async () => {
  const ch = await channel();
  const q = uniq("st-args");
  await ch.assertQueue(q, {
    durable: true,
    arguments: {
      "x-queue-type": "stream",
      "x-stream-max-segment-size-bytes": 1_000_000,
      "x-initial-cluster-size": 1,
      "x-max-age": "7D",
      "x-max-length-bytes": 50_000_000,
    },
  });
  const res = await mgmt(`/api/queues/%2f/${q}`);
  const info = (await res.json()) as { arguments?: Record<string, unknown>; type?: string };
  expect(info.type).toBe("stream");
  expect(info.arguments?.["x-max-age"]).toBe("7D");
  await ch.deleteQueue(q);
});

test("Stream queues :: an invalid x-max-age is refused with 406", async () => {
  const ch = await channel();
  expect(await closeCode(ch.assertQueue(uniq("st-bad"), { durable: true, arguments: { "x-queue-type": "stream", "x-max-age": "forever" } }))).toBe(406);
});

test("Durable classic queues :: x-queue-version is accepted", async () => {
  const ch = await channel();
  const q = uniq("cq-v2");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-version": 2 } });
  ch.sendToQueue(q, Buffer.from("v2"), { persistent: true });
  expect((await nextMessage(ch, q, 2000, { noAck: true }))?.content.toString()).toBe("v2");
  await ch.deleteQueue(q);
});

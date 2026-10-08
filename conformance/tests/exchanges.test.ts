import { expect, test } from "bun:test";
import { channel, closeCode, collect, confirmChannel, nextMessage, sleep, uniq } from "../lib.ts";

test("Exchange-to-exchange bindings :: a message routes through two exchanges", async () => {
  const ch = await channel();
  const src = uniq("e2e-src");
  const dst = uniq("e2e-dst");
  const q = uniq("e2e-q");
  await ch.assertExchange(src, "topic", { durable: false });
  await ch.assertExchange(dst, "direct", { durable: false });
  await ch.assertQueue(q, { durable: true });
  await ch.bindExchange(dst, src, "a.*");
  await ch.bindQueue(q, dst, "a.b");
  ch.publish(src, "a.b", Buffer.from("hop"));
  expect((await nextMessage(ch, q, 2000, { noAck: true }))?.content.toString()).toBe("hop");
  await ch.unbindExchange(dst, src, "a.*");
  ch.publish(src, "a.b", Buffer.from("gone"));
  expect(await nextMessage(ch, q, 400, { noAck: true })).toBeNull();
});

test("Internal exchanges :: a client publish to an internal exchange is refused", async () => {
  const ch = await channel();
  const x = uniq("internal");
  await ch.assertExchange(x, "fanout", { durable: false, internal: true });
  ch.publish(x, "", Buffer.from("no"));
  // The refusal closes the channel. The broker handles frames in order, so
  // the next call on it gets that close.
  expect(await closeCode(ch.checkExchange(x))).toBe(403);
});

test("Internal exchanges :: an internal exchange still receives an exchange-to-exchange hop", async () => {
  const ch = await channel();
  const front = uniq("front");
  const inner = uniq("inner");
  const q = uniq("inner-q");
  await ch.assertExchange(front, "fanout", { durable: false });
  await ch.assertExchange(inner, "fanout", { durable: false, internal: true });
  await ch.assertQueue(q, { durable: true });
  await ch.bindExchange(inner, front, "");
  await ch.bindQueue(q, inner, "");
  ch.publish(front, "", Buffer.from("through"));
  expect((await nextMessage(ch, q, 2000, { noAck: true }))?.content.toString()).toBe("through");
});

test("Passive declare, delete if-unused :: a passive declare of a missing queue is 404", async () => {
  const ch = await channel();
  expect(await closeCode(ch.checkQueue(uniq("missing")))).toBe(404);
});

test("Passive declare, delete if-unused :: a passive declare of a missing exchange is 404", async () => {
  const ch = await channel();
  expect(await closeCode(ch.checkExchange(uniq("missing-x")))).toBe(404);
});

test("Passive declare, delete if-unused :: if-unused refuses a queue with a consumer", async () => {
  const ch = await channel();
  const q = uniq("ifunused");
  await ch.assertQueue(q, { durable: true });
  await ch.consume(q, () => {});
  const other = await channel();
  expect(await closeCode(other.deleteQueue(q, { ifUnused: true }))).toBe(406);
});

test("Passive declare, delete if-unused :: if-empty refuses a queue with messages", async () => {
  const ch = await confirmChannel();
  const q = uniq("ifempty");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("x"));
  await ch.waitForConfirms();
  const other = await channel();
  expect(await closeCode(other.deleteQueue(q, { ifEmpty: true }))).toBe(406);
});

test("Consistent-hash exchange :: one routing key always lands on the same queue, and keys spread", async () => {
  const ch = await channel();
  const x = uniq("chash");
  const a = uniq("chash-a");
  const b = uniq("chash-b");
  await ch.assertExchange(x, "x-consistent-hash", { durable: false });
  await ch.assertQueue(a, { durable: true });
  await ch.assertQueue(b, { durable: true });
  await ch.bindQueue(a, x, "1");
  await ch.bindQueue(b, x, "1");
  for (let i = 0; i < 200; i++) ch.publish(x, `key-${i}`, Buffer.from(String(i)));
  for (let i = 0; i < 5; i++) ch.publish(x, "same-key", Buffer.from("same"));
  await sleep(500);
  const fromA = await collect(ch, a, 205, 1500, { noAck: true });
  const fromB = await collect(ch, b, 205, 1500, { noAck: true });
  expect(fromA.length + fromB.length).toBe(205);
  expect(fromA.length).toBeGreaterThan(30);
  expect(fromB.length).toBeGreaterThan(30);
  const sameA = fromA.filter((m) => m.content.toString() === "same").length;
  const sameB = fromB.filter((m) => m.content.toString() === "same").length;
  expect([sameA, sameB].sort()).toEqual([0, 5]);
});

test("Local random exchange :: a message reaches one bound queue", async () => {
  const ch = await channel();
  const x = uniq("lrandom");
  const q = uniq("lrandom-q");
  await ch.assertExchange(x, "x-local-random", { durable: false });
  await ch.assertQueue(q, { durable: true });
  await ch.bindQueue(q, x, "");
  ch.publish(x, "anything", Buffer.from("local"));
  expect((await nextMessage(ch, q, 2000, { noAck: true }))?.content.toString()).toBe("local");
});

test("Delayed messages :: x-delay holds the message until it is due", async () => {
  const ch = await channel();
  const x = uniq("delayed");
  const q = uniq("delayed-q");
  await ch.assertExchange(x, "x-delayed-message", { durable: false, arguments: { "x-delayed-type": "direct" } });
  await ch.assertQueue(q, { durable: true });
  await ch.bindQueue(q, x, "k");
  const sent = Date.now();
  ch.publish(x, "k", Buffer.from("later"), { headers: { "x-delay": 600 } });
  expect(await nextMessage(ch, q, 300, { noAck: true })).toBeNull();
  const msg = await nextMessage(ch, q, 2000, { noAck: true });
  expect(msg?.content.toString()).toBe("later");
  expect(Date.now() - sent).toBeGreaterThanOrEqual(550);
});

test("Passive declare, delete if-unused :: deleting a missing queue or exchange succeeds", async () => {
  const ch = await channel();
  const res = await ch.deleteQueue(uniq("never-declared"));
  expect(res.messageCount).toBe(0);
  await ch.deleteExchange(uniq("never-declared-x"));
  // The channel is still open.
  expect((await ch.checkExchange("amq.direct")).toString()).toBeDefined();
});

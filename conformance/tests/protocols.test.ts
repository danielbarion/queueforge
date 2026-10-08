import { expect, test } from "bun:test";
import { channel, collect, confirmChannel, nextMessage, rpc, sleep, uniq } from "../lib.ts";

test("AMQP 0-9-1 :: publish and consume round trip", async () => {
  const ch = await channel();
  const q = uniq("rt");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("hello"), { headers: { a: 1 }, contentType: "text/plain" });
  const msg = await nextMessage(ch, q);
  expect(msg?.content.toString()).toBe("hello");
  expect(msg?.properties.contentType).toBe("text/plain");
  expect(msg?.properties.headers?.a).toBe(1);
  ch.ack(msg!);
});

test("AMQP 0-9-1 :: queue.purge removes ready messages and reports the count", async () => {
  const ch = await confirmChannel();
  const q = uniq("purge");
  await ch.assertQueue(q, { durable: true });
  for (let i = 0; i < 3; i++) ch.sendToQueue(q, Buffer.from(String(i)));
  await ch.waitForConfirms();
  const res = await ch.purgeQueue(q);
  expect(res.messageCount).toBe(3);
  const after = await ch.checkQueue(q);
  expect(after.messageCount).toBe(0);
});

test("AMQP 0-9-1 :: basic.qos global=true is applied per consumer, as RabbitMQ 4 does", async () => {
  const ch = await channel();
  const a = uniq("gq-a");
  const b = uniq("gq-b");
  await ch.assertQueue(a, { durable: true });
  await ch.assertQueue(b, { durable: true });
  for (let i = 0; i < 3; i++) {
    ch.sendToQueue(a, Buffer.from("a"));
    ch.sendToQueue(b, Buffer.from("b"));
  }
  await sleep(100);
  // RabbitMQ 4 no longer shares a channel-wide limit: each consumer gets 2.
  await ch.prefetch(2, true);
  let got = 0;
  await ch.consume(a, () => got++);
  await ch.consume(b, () => got++);
  await sleep(500);
  expect(got).toBe(4);
  await ch.deleteQueue(a);
  await ch.deleteQueue(b);
});

test("AMQP 0-9-1 :: tx.rollback discards the publishes in the transaction", async () => {
  const ch = await channel();
  const q = uniq("tx-rb");
  await ch.assertQueue(q, { durable: true });
  await rpc(ch, "TxSelect", {});
  ch.sendToQueue(q, Buffer.from("discard-me"));
  await rpc(ch, "TxRollback", {});
  await sleep(200);
  const info = await ch.checkQueue(q);
  expect(info.messageCount).toBe(0);
});

test("AMQP 0-9-1 :: tx.commit publishes what the transaction held", async () => {
  const ch = await channel();
  const q = uniq("tx-commit");
  await ch.assertQueue(q, { durable: true });
  await rpc(ch, "TxSelect", {});
  ch.sendToQueue(q, Buffer.from("kept"));
  await sleep(100);
  expect((await ch.checkQueue(q)).messageCount).toBe(0);
  await rpc(ch, "TxCommit", {});
  expect((await ch.checkQueue(q)).messageCount).toBe(1);
});

test("AMQP 0-9-1 :: many publishes keep their order on one queue", async () => {
  const ch = await confirmChannel();
  const q = uniq("order");
  await ch.assertQueue(q, { durable: true });
  for (let i = 0; i < 200; i++) ch.sendToQueue(q, Buffer.from(String(i)));
  await ch.waitForConfirms();
  const got = await collect(ch, q, 200, 3000, { noAck: true });
  expect(got.map((m) => Number(m.content.toString()))).toEqual([...Array(200).keys()]);
});

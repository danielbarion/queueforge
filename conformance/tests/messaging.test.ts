import { expect, test } from "bun:test";
import type { Message } from "amqplib";
import { channel, closeCode, collect, confirmChannel, connect, eventually, nextMessage, rpc, sleep, uniq } from "../lib.ts";

test("Mandatory returns :: an unroutable message returns before its publisher confirm", async () => {
  const ch = await confirmChannel();
  const key = uniq("unroutable");
  const exchange = uniq("unroutable-exchange");
  await ch.assertExchange(exchange, "direct", { durable: false });
  const events: string[] = [];
  let returned: (Message & { fields: Message["fields"] & { replyCode: number } }) | undefined;
  ch.on("return", (message) => { returned = message; events.push("return"); });
  await new Promise<void>((resolve, reject) => {
    ch.publish(exchange, key, Buffer.from("unroutable-body"), { mandatory: true, correlationId: "return-id" }, (error) => {
      events.push("confirm");
      if (error) reject(error); else resolve();
    });
  });
  expect(events).toEqual(["return", "confirm"]);
  expect(returned?.fields.replyCode).toBe(312);
  expect(returned?.fields.routingKey).toBe(key);
  expect(returned?.content.toString()).toBe("unroutable-body");
  expect(returned?.properties.correlationId).toBe("return-id");
  await ch.deleteExchange(exchange);
});

test("Acks, nacks, rejects :: a nack with requeue redelivers with the redelivered flag", async () => {
  const ch = await channel();
  const q = uniq("nack");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("again"));
  const first = await nextMessage(ch, q);
  expect(first?.fields.redelivered).toBe(false);
  ch.nack(first!, false, true);
  const second = await nextMessage(ch, q);
  expect(second?.content.toString()).toBe("again");
  expect(second?.fields.redelivered).toBe(true);
  ch.ack(second!);
});

test("Acks, nacks, rejects :: multiple ack settles every earlier delivery", async () => {
  const ch = await channel();
  const q = uniq("multi-ack");
  await ch.assertQueue(q, { durable: true });
  for (let i = 0; i < 3; i++) ch.sendToQueue(q, Buffer.from(String(i)));
  const got = await collect(ch, q, 3);
  ch.ack(got[2]!, true);
  await sleep(200);
  const info = await ch.checkQueue(q);
  expect(info.messageCount).toBe(0);
});

test("Acks, nacks, rejects :: unacked messages are requeued when the connection closes", async () => {
  const c = await connect();
  const ch = await c.createChannel();
  const q = uniq("requeue-close");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("held"));
  expect((await nextMessage(ch, q))?.content.toString()).toBe("held");
  await c.close();
  const other = await channel();
  const back = await nextMessage(other, q, 2000);
  expect(back?.content.toString()).toBe("held");
  expect(back?.fields.redelivered).toBe(true);
  other.ack(back!);
  await other.deleteQueue(q);
});

test("Prefetch :: a consumer gets no more than its prefetch unacked", async () => {
  const ch = await channel();
  const q = uniq("prefetch");
  await ch.assertQueue(q, { durable: true });
  for (let i = 0; i < 5; i++) ch.sendToQueue(q, Buffer.from(String(i)));
  await sleep(100);
  await ch.prefetch(2);
  let got = 0;
  await ch.consume(q, () => got++);
  await sleep(400);
  expect(got).toBe(2);
});

test("Prefetch :: basic.qos global=true still limits each consumer", async () => {
  const ch = await channel();
  const a = uniq("pg-a");
  const b = uniq("pg-b");
  await ch.assertQueue(a, { durable: true });
  await ch.assertQueue(b, { durable: true });
  for (let i = 0; i < 5; i++) {
    ch.sendToQueue(a, Buffer.from("a"));
    ch.sendToQueue(b, Buffer.from("b"));
  }
  await sleep(100);
  await ch.prefetch(3, true);
  let got = 0;
  await ch.consume(a, () => got++);
  await ch.consume(b, () => got++);
  await sleep(400);
  expect(got).toBe(6);
  await ch.deleteQueue(a);
  await ch.deleteQueue(b);
});

test("basic.get :: get returns the message and the count left behind it", async () => {
  const ch = await confirmChannel();
  const q = uniq("get");
  await ch.assertQueue(q, { durable: true });
  for (let i = 0; i < 3; i++) ch.sendToQueue(q, Buffer.from(String(i)));
  await ch.waitForConfirms();
  const msg = await ch.get(q, { noAck: true });
  expect(msg && msg.content.toString()).toBe("0");
  expect(msg && msg.fields.messageCount).toBe(2);
  const next = await ch.get(q, { noAck: true });
  expect(next && next.fields.messageCount).toBe(1);
});

test("Transactions :: an ack inside a rolled-back transaction is undone", async () => {
  const ch = await channel();
  const q = uniq("tx-ack");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("m"));
  await sleep(100);
  await rpc(ch, "TxSelect", {});
  const msg = await ch.get(q);
  expect(msg).toBeTruthy();
  ch.ack(msg as never);
  await rpc(ch, "TxRollback", {});
  // RabbitMQ keeps the rolled-back delivery unacked on the channel; recover returns it.
  await ch.recover();
  const again = await nextMessage(ch, q, 1500);
  expect(again?.content.toString()).toBe("m");
});

test("Transactions :: a nack inside a committed transaction takes effect at commit", async () => {
  const ch = await channel();
  const q = uniq("tx-nack");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("n"));
  await sleep(100);
  await rpc(ch, "TxSelect", {});
  const msg = await ch.get(q);
  ch.nack(msg as never, false, false);
  await sleep(100);
  await rpc(ch, "TxCommit", {});
  await sleep(100);
  expect((await ch.checkQueue(q)).messageCount).toBe(0);
});

test("basic.recover :: recover with requeue redelivers unacked messages", async () => {
  const ch = await channel();
  const q = uniq("recover");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("r"));
  const seen: boolean[] = [];
  await ch.consume(q, (m) => m && seen.push(m.fields.redelivered));
  await eventually(() => seen.length === 1);
  await ch.recover();
  expect(await eventually(() => seen.length === 2)).toBe(true);
  expect(seen).toEqual([false, true]);
});

test("basic.recover :: recover without requeue is refused with 540, as RabbitMQ does", async () => {
  const ch = await channel();
  const q = uniq("recover-nr");
  await ch.assertQueue(q, { durable: true });
  expect(await closeCode(rpc(ch, "BasicRecover", { requeue: false }))).toBe(540);
  const other = await channel();
  await other.deleteQueue(q);
});

test("Consumer cancel notification :: deleting a queue cancels its consumers", async () => {
  const ch = await channel();
  const q = uniq("cancel-notify");
  await ch.assertQueue(q, { durable: true });
  let cancelled = false;
  await ch.consume(q, (m) => {
    if (m === null) cancelled = true;
  });
  const other = await channel();
  await other.deleteQueue(q);
  expect(await eventually(() => cancelled, 2000)).toBe(true);
});

test("connection.blocked :: the server advertises connection.blocked", async () => {
  const c = await connect();
  const props = (c as unknown as { connection: { serverProperties: { capabilities?: Record<string, unknown> } } }).connection.serverProperties;
  expect(props.capabilities?.["connection.blocked"]).toBe(true);
});

test("Direct reply-to :: a reply to amq.rabbitmq.reply-to reaches the requester", async () => {
  const ch = await channel();
  const service = uniq("rpc-service");
  await ch.assertQueue(service, { durable: true });
  const replies: string[] = [];
  await ch.consume("amq.rabbitmq.reply-to", (m) => m && replies.push(m.content.toString()), { noAck: true });
  ch.sendToQueue(service, Buffer.from("ping"), { replyTo: "amq.rabbitmq.reply-to", correlationId: "c1" });
  const server = await channel();
  const req = await nextMessage(server, service);
  expect(req?.properties.replyTo).toStartWith("amq.rabbitmq.reply-to.");
  server.sendToQueue(req!.properties.replyTo, Buffer.from("pong"), { correlationId: req!.properties.correlationId });
  server.ack(req!);
  expect(await eventually(() => replies.length === 1, 2000)).toBe(true);
  expect(replies).toEqual(["pong"]);
});

test("Validated user-id :: a user-id that does not match the login is refused", async () => {
  const ch = await channel();
  const q = uniq("userid");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("x"), { userId: "someone-else" });
  expect(await closeCode(ch.checkQueue(q))).toBe(406);
});

test("Validated user-id :: a matching user-id is delivered", async () => {
  const ch = await channel();
  const q = uniq("userid-ok");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("x"), { userId: "admin" });
  const msg = await nextMessage(ch, q, 2000, { noAck: true });
  expect(msg?.properties.userId).toBe("admin");
});

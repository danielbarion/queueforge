import { expect, test } from "bun:test";
import { channel, closeCode, collect, confirmChannel, connect, eventually, nextMessage, sleep, uniq } from "../lib.ts";

test("Durable classic queues :: a persistent message on a durable queue is delivered", async () => {
  const ch = await confirmChannel();
  const q = uniq("durable");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "classic" } });
  ch.sendToQueue(q, Buffer.from("kept"), { persistent: true });
  await ch.waitForConfirms();
  const msg = await nextMessage(ch, q);
  expect(msg?.content.toString()).toBe("kept");
  expect(msg?.properties.deliveryMode).toBe(2);
  ch.ack(msg!);
  await ch.deleteQueue(q);
});

test("Quorum queues :: declare, confirm, consume", async () => {
  const ch = await confirmChannel();
  const q = uniq("quorum");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "quorum" } });
  ch.sendToQueue(q, Buffer.from("q1"), { persistent: true });
  await ch.waitForConfirms();
  const msg = await nextMessage(ch, q);
  expect(msg?.content.toString()).toBe("q1");
  ch.ack(msg!);
  await ch.deleteQueue(q);
});

test("Stream queues :: consumers read from an offset without removing messages", async () => {
  const ch = await confirmChannel();
  const q = uniq("stream");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "stream" } });
  for (let i = 0; i < 5; i++) ch.sendToQueue(q, Buffer.from(String(i)), { persistent: true });
  await ch.waitForConfirms();
  await ch.prefetch(10);
  const read = async () => {
    const got: string[] = [];
    const ok = await ch.consume(
      q,
      (m) => {
        if (!m) return;
        got.push(m.content.toString());
        ch.ack(m);
      },
      { arguments: { "x-stream-offset": "first" } },
    );
    await eventually(() => got.length >= 5, 3000);
    await ch.cancel(ok.consumerTag);
    return got;
  };
  expect(await read()).toEqual(["0", "1", "2", "3", "4"]);
  // A second reader from the start sees the same messages.
  expect(await read()).toEqual(["0", "1", "2", "3", "4"]);
  await ch.deleteQueue(q);
});

async function readStream(ch: Awaited<ReturnType<typeof confirmChannel>>, q: string, offset: unknown, want: number, ms = 3000) {
  const got: Array<{ body: string; offset: unknown }> = [];
  const ok = await ch.consume(
    q,
    (m) => {
      if (!m) return;
      got.push({ body: m.content.toString(), offset: m.properties.headers?.["x-stream-offset"] });
      ch.ack(m);
    },
    { arguments: { "x-stream-offset": offset } },
  );
  await eventually(() => got.length >= want, ms);
  await sleep(150);
  await ch.cancel(ok.consumerTag);
  return got;
}

test("Stream queues :: a numeric offset starts there, and each delivery carries x-stream-offset", async () => {
  const ch = await confirmChannel();
  const q = uniq("stream-num");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "stream" } });
  for (let i = 0; i < 5; i++) ch.sendToQueue(q, Buffer.from(String(i)));
  await ch.waitForConfirms();
  await ch.prefetch(10);
  const all = await readStream(ch, q, "first", 5);
  const third = Number(all[2]!.offset);
  const tail = await readStream(ch, q, third, 3);
  expect(tail.map((m) => m.body)).toEqual(["2", "3", "4"]);
  expect(tail.map((m) => Number(m.offset))).toEqual([third, third + 1, third + 2]);
  await ch.deleteQueue(q);
});

test("Stream queues :: offset next sees only messages published after it subscribed", async () => {
  const ch = await confirmChannel();
  const q = uniq("stream-next");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "stream" } });
  ch.sendToQueue(q, Buffer.from("old"));
  await ch.waitForConfirms();
  await ch.prefetch(10);
  const got: string[] = [];
  const ok = await ch.consume(q, (m) => m && (got.push(m.content.toString()), ch.ack(m)), { arguments: { "x-stream-offset": "next" } });
  await sleep(300);
  const pub = await confirmChannel();
  pub.sendToQueue(q, Buffer.from("new"));
  await pub.waitForConfirms();
  await eventually(() => got.length >= 1, 3000);
  await sleep(150);
  expect(got).toEqual(["new"]);
  await ch.cancel(ok.consumerTag);
  await ch.deleteQueue(q);
});

test("Stream queues :: a stream must be durable", async () => {
  const ch = await channel();
  expect(await closeCode(ch.assertQueue(uniq("stream-transient"), { durable: false, exclusive: true, arguments: { "x-queue-type": "stream" } }))).toBe(406);
});

test("Stream queues :: consuming needs a prefetch", async () => {
  const ch = await channel();
  const q = uniq("stream-noqos");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "stream" } });
  expect(await closeCode(ch.consume(q, () => {}))).toBe(406);
  const other = await channel();
  await other.deleteQueue(q);
});

test("Exclusive, auto-delete, server-named :: a server-named queue gets a generated name", async () => {
  const ch = await channel();
  const ok = await ch.assertQueue("", { exclusive: true });
  expect(ok.queue.length).toBeGreaterThan(0);
});

test("Exclusive, auto-delete, server-named :: an exclusive queue is locked to its connection", async () => {
  const ch = await channel();
  const q = uniq("excl");
  await ch.assertQueue(q, { exclusive: true });
  const other = await channel();
  expect(await closeCode(other.checkQueue(q).then(() => other.consume(q, () => {})))).toBe(405);
});

test("Exclusive, auto-delete, server-named :: an exclusive queue goes when its connection closes", async () => {
  const c = await connect();
  const ch = await c.createChannel();
  const q = uniq("excl-gone");
  await ch.assertQueue(q, { exclusive: true });
  await c.close();
  const other = await channel();
  const gone = await eventually(async () => (await closeCode(other.checkQueue(q))) === 404, 3000);
  expect(gone).toBe(true);
});

test("Exclusive, auto-delete, server-named :: an auto-delete queue goes after its last consumer cancels", async () => {
  const ch = await channel();
  const q = uniq("autodel");
  await ch.assertQueue(q, { durable: true, autoDelete: true });
  const ok = await ch.consume(q, () => {});
  await ch.cancel(ok.consumerTag);
  await sleep(300);
  const other = await channel();
  expect(await closeCode(other.checkQueue(q))).toBe(404);
});

test("Queue message TTL :: an expired message is not delivered", async () => {
  const ch = await confirmChannel();
  const q = uniq("ttl");
  await ch.assertQueue(q, { durable: true, arguments: { "x-message-ttl": 100 } });
  ch.sendToQueue(q, Buffer.from("old"));
  await ch.waitForConfirms();
  await sleep(300);
  expect(await nextMessage(ch, q, 300, { noAck: true })).toBeNull();
});

test("Queue message TTL :: a TTL of zero is accepted", async () => {
  const ch = await confirmChannel();
  const q = uniq("ttl0");
  await ch.assertQueue(q, { durable: true, arguments: { "x-message-ttl": 0 } });
  ch.sendToQueue(q, Buffer.from("instant"));
  await ch.waitForConfirms();
  expect(await nextMessage(ch, q, 300, { noAck: true })).toBeNull();
});

test("Queue message TTL :: a per-message expiration is honoured", async () => {
  const ch = await confirmChannel();
  const q = uniq("ttl-msg");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("old"), { expiration: "100" });
  ch.sendToQueue(q, Buffer.from("fresh"));
  await ch.waitForConfirms();
  await sleep(300);
  const got = await collect(ch, q, 2, 500, { noAck: true });
  expect(got.map((m) => m.content.toString())).toEqual(["fresh"]);
});

test("Queue expiry :: an unused queue is deleted after x-expires", async () => {
  const ch = await channel();
  const q = uniq("expires");
  await ch.assertQueue(q, { durable: true, arguments: { "x-expires": 300 } });
  // A passive declare counts as use and restarts the timer, so look once.
  await sleep(1500);
  const other = await channel();
  expect(await closeCode(other.checkQueue(q))).toBe(404);
});

test("Queue expiry :: basic.get counts as use", async () => {
  const ch = await channel();
  const q = uniq("expires-get");
  await ch.assertQueue(q, { durable: true, arguments: { "x-expires": 800 } });
  for (let i = 0; i < 4; i++) {
    await sleep(400);
    await ch.get(q, { noAck: true });
  }
  expect((await ch.checkQueue(q)).queue).toBe(q);
});

test("Length limits :: x-max-length drops from the head", async () => {
  const ch = await confirmChannel();
  const q = uniq("maxlen");
  await ch.assertQueue(q, { durable: true, arguments: { "x-max-length": 2 } });
  for (const v of ["a", "b", "c"]) ch.sendToQueue(q, Buffer.from(v));
  await ch.waitForConfirms();
  const got = await collect(ch, q, 3, 500, { noAck: true });
  expect(got.map((m) => m.content.toString())).toEqual(["b", "c"]);
});

test("Length limits :: x-max-length of zero is accepted and keeps nothing", async () => {
  const ch = await confirmChannel();
  const q = uniq("maxlen0");
  await ch.assertQueue(q, { durable: true, arguments: { "x-max-length": 0 } });
  ch.sendToQueue(q, Buffer.from("x"));
  await ch.waitForConfirms();
  expect((await ch.checkQueue(q)).messageCount).toBe(0);
});

test("Length limits :: reject-publish nacks once the queue is full", async () => {
  const ch = await confirmChannel();
  const q = uniq("maxlen-rp");
  await ch.assertQueue(q, { durable: true, arguments: { "x-max-length": 1, "x-overflow": "reject-publish" } });
  const first = await new Promise<boolean>((r) => ch.sendToQueue(q, Buffer.from("1"), {}, (err) => r(!err)));
  const second = await new Promise<boolean>((r) => ch.sendToQueue(q, Buffer.from("2"), {}, (err) => r(!err)));
  expect([first, second]).toEqual([true, false]);
});

async function dlxPair(ch: Awaited<ReturnType<typeof channel>>) {
  const dlx = uniq("dlx");
  const dead = uniq("dead");
  const q = uniq("dlx-src");
  await ch.assertExchange(dlx, "fanout", { durable: false });
  await ch.assertQueue(dead, { durable: true });
  await ch.bindQueue(dead, dlx, "");
  await ch.assertQueue(q, { durable: true, arguments: { "x-dead-letter-exchange": dlx } });
  return { dlx, dead, q };
}

test("Dead-letter exchanges :: a rejected message is dead-lettered with x-death", async () => {
  const ch = await channel();
  const { dead, q } = await dlxPair(ch);
  ch.sendToQueue(q, Buffer.from("bad"));
  const msg = await nextMessage(ch, q);
  ch.reject(msg!, false);
  const out = await nextMessage(ch, dead);
  expect(out?.content.toString()).toBe("bad");
  const death = (out?.properties.headers?.["x-death"] as Array<Record<string, unknown>>)?.[0];
  expect(death?.queue).toBe(q);
  expect(death?.reason).toBe("rejected");
  expect(Number(death?.count)).toBe(1);
  ch.ack(out!);
});

test("Dead-letter exchanges :: the x-death count rises each time the same message dies again", async () => {
  const ch = await channel();
  // q dead-letters to `dead`; `dead` expires its messages back into q.
  const toDead = uniq("to-dead");
  const toQ = uniq("to-q");
  const q = uniq("loop-q");
  const dead = uniq("loop-dead");
  await ch.assertExchange(toDead, "fanout", { durable: false });
  await ch.assertExchange(toQ, "fanout", { durable: false });
  await ch.assertQueue(q, { durable: true, arguments: { "x-dead-letter-exchange": toDead } });
  await ch.assertQueue(dead, { durable: true, arguments: { "x-dead-letter-exchange": toQ, "x-message-ttl": 50 } });
  await ch.bindQueue(dead, toDead, "");
  await ch.bindQueue(q, toQ, "");
  ch.sendToQueue(q, Buffer.from("again"));
  let msg = await nextMessage(ch, q, 3000);
  ch.reject(msg!, false);
  msg = await nextMessage(ch, q, 3000);
  ch.reject(msg!, false);
  msg = await nextMessage(ch, q, 3000);
  const deaths = msg?.properties.headers?.["x-death"] as Array<Record<string, unknown>>;
  const forQ = deaths?.find((d) => d.queue === q && d.reason === "rejected");
  expect(Number(forQ?.count)).toBe(2);
  ch.ack(msg!);
  await ch.deleteQueue(q);
  await ch.deleteQueue(dead);
});

test("Dead-letter exchanges :: an expired message is dead-lettered", async () => {
  const ch = await channel();
  const dlx = uniq("dlx-ttl");
  const dead = uniq("dead-ttl");
  const q = uniq("ttl-src");
  await ch.assertExchange(dlx, "fanout", { durable: false });
  await ch.assertQueue(dead, { durable: true });
  await ch.bindQueue(dead, dlx, "");
  await ch.assertQueue(q, { durable: true, arguments: { "x-dead-letter-exchange": dlx, "x-message-ttl": 100 } });
  ch.sendToQueue(q, Buffer.from("timed"));
  const out = await nextMessage(ch, dead, 3000);
  expect(out?.content.toString()).toBe("timed");
  const death = (out?.properties.headers?.["x-death"] as Array<Record<string, unknown>>)?.[0];
  expect(death?.reason).toBe("expired");
});

test("Single active consumer :: only the first consumer receives, and it stays active with a full prefetch", async () => {
  const ch1 = await channel();
  const q = uniq("sac");
  await ch1.assertQueue(q, { durable: true, arguments: { "x-single-active-consumer": true } });
  await ch1.prefetch(1);
  const ch2 = await channel();
  await ch2.prefetch(1);
  const first: string[] = [];
  const second: string[] = [];
  await ch1.consume(q, (m) => m && first.push(m.content.toString()));
  await ch2.consume(q, (m) => m && second.push(m.content.toString()));
  for (const v of ["1", "2", "3"]) ch1.sendToQueue(q, Buffer.from(v));
  await sleep(600);
  expect(first).toEqual(["1"]);
  expect(second).toEqual([]);
  await ch1.deleteQueue(q);
});

test("Single active consumer :: the next consumer takes over when the active one cancels", async () => {
  const ch1 = await channel();
  const q = uniq("sac-fail");
  await ch1.assertQueue(q, { durable: true, arguments: { "x-single-active-consumer": true } });
  const ch2 = await channel();
  const second: string[] = [];
  const ok1 = await ch1.consume(q, () => {}, { noAck: true });
  await ch2.consume(q, (m) => m && second.push(m.content.toString()), { noAck: true });
  await ch1.cancel(ok1.consumerTag);
  await sleep(200);
  ch1.sendToQueue(q, Buffer.from("takeover"));
  expect(await eventually(() => second.length === 1, 2000)).toBe(true);
  await ch1.deleteQueue(q);
});

test("Delivery limit :: a quorum message is dropped after x-delivery-limit returns", async () => {
  const ch = await channel();
  const q = uniq("dlimit");
  await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "quorum", "x-delivery-limit": 2 } });
  ch.sendToQueue(q, Buffer.from("poison"), { persistent: true });
  // RabbitMQ allows `limit` returns: delivered limit+1 times, then dropped.
  let deliveries = 0;
  for (;;) {
    const msg = await nextMessage(ch, q, 600);
    if (!msg) break;
    deliveries++;
    ch.reject(msg, true);
    if (deliveries > 10) break;
  }
  expect(deliveries).toBe(3);
  await ch.deleteQueue(q);
});

test("Queue leader locator :: a declare with x-queue-leader-locator is accepted", async () => {
  const ch = await channel();
  const q = uniq("locator");
  const ok = await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "quorum", "x-queue-leader-locator": "balanced" } });
  expect(ok.queue).toBe(q);
  await ch.deleteQueue(q);
});

test("Queue leader locator :: an unknown locator value is refused", async () => {
  const ch = await channel();
  expect(
    await closeCode(ch.assertQueue(uniq("locator-bad"), { durable: true, arguments: { "x-queue-type": "quorum", "x-queue-leader-locator": "nowhere" } })),
  ).toBe(406);
});

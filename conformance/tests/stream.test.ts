/**
 * The RabbitMQ stream protocol on its own port, and super streams.
 * Driven by rabbitmq-stream-js-client, RabbitMQ's JavaScript stream client.
 */
import { afterAll, expect, test } from "bun:test";
import { connect as streamConnect, Offset, type Client, type Message } from "rabbitmq-stream-js-client";
import { channel, sleep, uniq } from "../lib.ts";

const PORT = Number(process.env.QF_STREAM_PORT ?? 5552);
const clients: Client[] = [];

afterAll(async () => {
  for (const c of clients.splice(0)) await c.close().catch(() => {});
});

async function client(): Promise<Client> {
  const c = await streamConnect({ hostname: "127.0.0.1", port: PORT, username: "admin", password: "devpassword12", vhost: "/" });
  clients.push(c);
  return c;
}

/** Every message a consumer from `offset` gets within `ms`. */
async function read(c: Client, stream: string, offset: Offset, ms = 800): Promise<Message[]> {
  const got: Message[] = [];
  const consumer = await c.declareConsumer({ stream, offset }, (m) => {
    got.push(m);
  });
  await sleep(ms);
  await c.closeConsumer(consumer.extendedId);
  return got;
}

const text = (ms: Message[]) => ms.map((m) => m.content.toString());

async function newStream(c: Client): Promise<string> {
  const stream = uniq("st");
  await c.createStream({ stream, arguments: {} });
  return stream;
}

test("Stream protocol :: publish, then consume from the first offset", async () => {
  const c = await client();
  const stream = await newStream(c);
  const pub = await c.declarePublisher({ stream });
  for (const b of ["a", "b", "c"]) await pub.send(Buffer.from(b));
  await pub.flush();
  const got = await read(c, stream, Offset.first());
  expect(text(got)).toEqual(["a", "b", "c"]);
  expect(got.map((m) => m.offset)).toEqual([0n, 1n, 2n]);
});

test("Stream protocol :: stream stats name the first and the committed chunk", async () => {
  const c = await client();
  const stream = await newStream(c);
  const pub = await c.declarePublisher({ stream });
  for (const b of ["s0", "s1", "s2"]) await pub.send(Buffer.from(b));
  await pub.flush();
  await sleep(300);
  const stats = await c.streamStatsRequest(stream);
  // RabbitMQ may put several messages in one chunk; a chunk id is the
  // offset of its first message, so it never passes the last offset.
  expect(stats.firstChunkId).toBe(0n);
  expect(stats.committedChunkId).toBeGreaterThanOrEqual(0n);
  expect(stats.committedChunkId).toBeLessThanOrEqual(2n);
  expect(stats.lastChunkId).toBeGreaterThanOrEqual(stats.committedChunkId);
});

test("Stream protocol :: a numeric offset starts there; next sees only new messages", async () => {
  const c = await client();
  const stream = await newStream(c);
  const pub = await c.declarePublisher({ stream });
  for (const b of ["m0", "m1", "m2"]) await pub.send(Buffer.from(b));
  await pub.flush();
  await sleep(200);
  expect(text(await read(c, stream, Offset.offset(1n)))).toEqual(["m1", "m2"]);
  const later: Message[] = [];
  const consumer = await c.declareConsumer({ stream, offset: Offset.next() }, (m) => {
    later.push(m);
  });
  await sleep(300);
  await pub.send(Buffer.from("m3"));
  await pub.flush();
  await sleep(500);
  await c.closeConsumer(consumer.extendedId);
  expect(text(later)).toEqual(["m3"]);
});

test("Stream protocol :: publish confirms carry the publishing ids", async () => {
  const c = await client();
  const stream = await newStream(c);
  const pub = await c.declarePublisher({ stream });
  const confirmed: bigint[] = [];
  pub.on("publish_confirm", (err, ids) => {
    if (err === null) confirmed.push(...ids);
  });
  const sent = [];
  for (let i = 0; i < 3; i++) sent.push((await pub.send(Buffer.from(`x${i}`))).publishingId);
  await pub.flush();
  await sleep(500);
  expect(confirmed.sort()).toEqual(sent.sort());
});

test("Stream protocol :: a named publisher is deduplicated by publishing id", async () => {
  const c = await client();
  const stream = await newStream(c);
  const publisherRef = uniq("pub");
  const pub = await c.declarePublisher({ stream, publisherRef });
  await pub.basicSend(1n, Buffer.from("one"));
  await pub.basicSend(2n, Buffer.from("two"));
  await pub.flush();
  await sleep(300);
  expect(await pub.getLastPublishingId()).toBe(2n);
  // Ids at or below the last one are dropped.
  await pub.basicSend(2n, Buffer.from("two-again"));
  await pub.basicSend(3n, Buffer.from("three"));
  await pub.flush();
  await sleep(300);
  expect(text(await read(c, stream, Offset.first()))).toEqual(["one", "two", "three"]);
});

test("Stream protocol :: a stored consumer offset can be queried back", async () => {
  const c = await client();
  const stream = await newStream(c);
  const pub = await c.declarePublisher({ stream });
  for (const b of ["a", "b", "c"]) await pub.send(Buffer.from(b));
  await pub.flush();
  const consumerRef = uniq("ref");
  const got: Message[] = [];
  const consumer = await c.declareConsumer({ stream, offset: Offset.first(), consumerRef }, (m) => {
    got.push(m);
  });
  await sleep(500);
  await consumer.storeOffset(1n);
  await sleep(200);
  expect(await c.queryOffset({ reference: consumerRef, stream })).toBe(1n);
  await c.closeConsumer(consumer.extendedId);
});

test("Stream protocol :: AMQP 0-9-1 and stream protocol share a stream", async () => {
  const ch = await channel();
  const c = await client();
  const stream = uniq("st091");
  await ch.assertQueue(stream, { durable: true, arguments: { "x-queue-type": "stream" } });
  ch.sendToQueue(stream, Buffer.from("from-091"), { persistent: true, headers: { tag: "t" }, contentType: "text/plain" });
  await sleep(300);
  const pub = await c.declarePublisher({ stream });
  await pub.send(Buffer.from("from-stream"), { applicationProperties: { color: "red" } });
  await pub.flush();
  await sleep(300);
  const got = await read(c, stream, Offset.first());
  expect(text(got)).toEqual(["from-091", "from-stream"]);
  expect(got[0]!.applicationProperties?.tag).toBe("t");
  await ch.prefetch(10);
  const seen: string[] = [];
  const headers: unknown[] = [];
  const ok = await ch.consume(
    stream,
    (m) => {
      if (!m) return;
      seen.push(m.content.toString());
      headers.push(m.properties.headers?.color);
      ch.ack(m);
    },
    { arguments: { "x-stream-offset": "first" } },
  );
  await sleep(600);
  await ch.cancel(ok.consumerTag);
  expect(seen).toEqual(["from-091", "from-stream"]);
  expect(headers[1]).toBe("red");
});

test("Stream protocol :: single active consumer: one named consumer reads, the next takes over", async () => {
  const c = await client();
  const stream = await newStream(c);
  const pub = await c.declarePublisher({ stream });
  const consumerRef = uniq("sac");
  const a: string[] = [];
  const b: string[] = [];
  const first = await c.declareConsumer({ stream, offset: Offset.first(), consumerRef, singleActive: true }, (m) => {
    a.push(m.content.toString());
  });
  const second = await c.declareConsumer({ stream, offset: Offset.first(), consumerRef, singleActive: true }, (m) => {
    b.push(m.content.toString());
  });
  await pub.send(Buffer.from("1"));
  await pub.flush();
  await sleep(500);
  expect(a).toEqual(["1"]);
  expect(b).toEqual([]);
  await c.closeConsumer(first.extendedId);
  await sleep(300);
  await pub.send(Buffer.from("2"));
  await pub.flush();
  await sleep(500);
  expect(b).toContain("2");
  await c.closeConsumer(second.extendedId);
});

test("Stream protocol :: metadata names the stream and a missing one is an error", async () => {
  const c = await client();
  const stream = await newStream(c);
  const [meta] = await c.queryMetadata({ streams: [stream] });
  expect(meta?.streamName).toBe(stream);
  expect(meta?.responseCode).toBe(1);
  expect(meta?.leader?.port).toBe(PORT);
  const [missing] = await c.queryMetadata({ streams: [uniq("none")] });
  expect(missing?.responseCode).toBe(2);
});

test("Stream protocol :: deleting a stream removes it", async () => {
  const ch = await channel();
  const c = await client();
  const stream = await newStream(c);
  await c.deleteStream({ stream });
  await sleep(200);
  const err = await ch.checkQueue(stream).then(
    () => "exists",
    () => "gone",
  );
  expect(err).toBe("gone");
});

test("Super streams :: created partitions are streams bound to an exchange", async () => {
  const c = await client();
  const ch = await channel();
  const superStream = uniq("sup");
  await c.createSuperStream({ streamName: superStream }, undefined, 3);
  const partitions = await c.partitionsQuery({ superStream });
  expect(partitions).toEqual([`${superStream}-0`, `${superStream}-1`, `${superStream}-2`]);
  await ch.checkExchange(superStream);
  for (const p of partitions) await ch.checkQueue(p);
  expect(await c.routeQuery({ routingKey: "1", superStream })).toEqual([`${superStream}-1`]);
});

test("Super streams :: a hashing publisher keeps a key on one partition, and a super stream consumer reads them all", async () => {
  const c = await client();
  const superStream = uniq("suph");
  await c.createSuperStream({ streamName: superStream }, undefined, 3);
  const pub = await c.declareSuperStreamPublisher({ superStream }, (_content, opts) => opts.messageProperties?.messageId);
  const keys = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
  for (const k of keys) for (let i = 0; i < 2; i++) await pub.send(Buffer.from(`${k}:${i}`), { messageProperties: { messageId: k } });
  // A super stream publisher has no flush; closing sends what is queued.
  await pub.close();
  await sleep(300);
  const where = new Map<string, Set<string>>();
  for (const p of await c.partitionsQuery({ superStream })) {
    for (const m of await read(c, p, Offset.first(), 400)) {
      const key = m.content.toString().split(":")[0]!;
      where.set(key, (where.get(key) ?? new Set()).add(p));
    }
  }
  expect([...where.keys()].sort()).toEqual([...keys].sort());
  for (const parts of where.values()) expect(parts.size).toBe(1);
  const all: string[] = [];
  const consumer = await c.declareSuperStreamConsumer({ superStream, offset: Offset.first() }, (m) => {
    all.push(m.content.toString());
  });
  await sleep(800);
  await consumer.close();
  expect(all.length).toBe(keys.length * 2);
});

test("Super streams :: deleting a super stream removes its partitions", async () => {
  const c = await client();
  const ch = await channel();
  const superStream = uniq("supd");
  await c.createSuperStream({ streamName: superStream }, undefined, 2);
  await c.deleteSuperStream({ streamName: superStream });
  await sleep(200);
  const gone = await ch.checkQueue(`${superStream}-0`).then(
    () => "exists",
    () => "gone",
  );
  expect(gone).toBe("gone");
});


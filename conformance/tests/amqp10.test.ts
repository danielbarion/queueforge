/**
 * AMQP 1.0 on port 5672, with RabbitMQ 4's v2 addresses:
 * /queues/:queue and /exchanges/:exchange/:key.
 */
import { expect, test } from "bun:test";
// eslint-disable-next-line @typescript-eslint/no-require-imports
const rhea = require("rhea");
import { channel, collect, nextMessage, sleep, uniq } from "../lib.ts";

const PORT = Number(process.env.QF_AMQP_PORT ?? 5672);

type Conn = {
  raw: any;
  sender: (address: string | null) => Promise<any>;
  receiver: (address: string, opts?: object) => Promise<any>;
  close: () => void;
};

/** Open an AMQP 1.0 connection, or reject with the SASL or open error. */
function open(password = "devpassword12"): Promise<Conn> {
  const container = rhea.create_container({ id: uniq("c10") });
  return new Promise((done, fail) => {
    const raw = container.connect({
      host: "127.0.0.1",
      port: PORT,
      username: "admin",
      password,
      hostname: "vhost:/",
      reconnect: false,
      idle_time_out: 0,
    });
    raw.on("connection_error", (ctx: any) => fail(new Error(String(ctx.connection.error?.condition ?? "connection error"))));
    raw.on("error", (err: any) => fail(err));
    raw.on("disconnected", (ctx: any) => fail(new Error(String(ctx.error?.message ?? "disconnected"))));
    raw.on("connection_open", () =>
      done({
        raw,
        sender: (address) =>
          new Promise((ok, no) => {
            const s = raw.open_sender({ target: { address }, autosettle: true });
            s.once("sendable", () => ok(s));
            s.once("sender_open", () => setTimeout(() => ok(s), 50));
            s.once("sender_error", (ctx: any) => no(new Error(String(ctx.sender.error?.condition))));
          }),
        receiver: (address, opts = {}) =>
          new Promise((ok, no) => {
            const r = raw.open_receiver({ source: { address }, autoaccept: false, credit_window: 0, ...opts });
            // A refused attach is answered with an attach and then a detach carrying the error.
            r.once("receiver_open", () => setTimeout(() => ok(r), 150));
            r.once("receiver_error", (ctx: any) => no(new Error(String(ctx.receiver.error?.condition))));
          }),
        close: () => raw.close(),
      }),
    );
  });
}

/** Send one message and resolve with the outcome name: accepted, released, rejected or modified. */
function send(sender: any, msg: object): Promise<string> {
  return new Promise((done) => {
    const d = sender.send(msg);
    for (const ev of ["accepted", "released", "rejected", "modified"]) sender.on(ev, (ctx: any) => ctx.delivery === d && done(ev));
  });
}

/** Every message the receiver gets within `ms`. */
function gather(receiver: any, ms: number): Promise<any[]> {
  const got: any[] = [];
  receiver.on("message", (ctx: any) => got.push(ctx));
  return sleep(ms).then(() => got);
}

const data = (s: string | Buffer) => rhea.message.data_section(typeof s === "string" ? Buffer.from(s) : s);
const bodyText = (m: any) => Buffer.from(m.body?.content ?? m.body ?? "").toString();

async function durableQueue(): Promise<string> {
  const ch = await channel();
  const q = uniq("q10");
  await ch.assertQueue(q, { durable: true });
  return q;
}

test("AMQP 1.0 :: send to /queues and receive it back", async () => {
  const q = await durableQueue();
  const c = await open();
  const s = await c.sender(`/queues/${q}`);
  expect(await send(s, { body: data("one"), message_id: "m-1", content_type: "text/plain" })).toBe("accepted");
  const r = await c.receiver(`/queues/${q}`);
  const wait = gather(r, 500);
  r.add_credit(5);
  const got = await wait;
  expect(got.length).toBe(1);
  expect(bodyText(got[0].message)).toBe("one");
  expect(got[0].message.message_id).toBe("m-1");
  got[0].delivery.accept();
  c.close();
});

test("AMQP 1.0 :: /exchanges address routes by key and maps properties to 0-9-1", async () => {
  const ch = await channel();
  const q = await durableQueue();
  const key = uniq("k");
  await ch.bindQueue(q, "amq.direct", key);
  const c = await open();
  const s = await c.sender(`/exchanges/amq.direct/${key}`);
  const outcome = await send(s, {
    body: data("routed"),
    content_type: "application/json",
    correlation_id: "corr-7",
    message_id: "mid-7",
    application_properties: { color: "blue", n: 3 },
  });
  expect(outcome).toBe("accepted");
  const m = await nextMessage(ch, q);
  expect(m?.content.toString()).toBe("routed");
  expect(m?.properties.contentType).toBe("application/json");
  expect(m?.properties.correlationId).toBe("corr-7");
  expect(m?.properties.messageId).toBe("mid-7");
  expect(m?.properties.headers?.color).toBe("blue");
  expect(Number(m?.properties.headers?.n)).toBe(3);
  c.close();
});

test("AMQP 1.0 :: a 0-9-1 publish is received over 1.0 with its properties", async () => {
  const ch = await channel();
  const q = await durableQueue();
  ch.sendToQueue(q, Buffer.from("from-091"), { contentType: "text/plain", messageId: "x-1", headers: { tag: "t" } });
  const c = await open();
  const r = await c.receiver(`/queues/${q}`);
  const wait = gather(r, 500);
  r.add_credit(1);
  const [got] = await wait;
  expect(bodyText(got.message)).toBe("from-091");
  expect(got.message.content_type).toBe("text/plain");
  expect(got.message.message_id).toBe("x-1");
  expect(got.message.application_properties?.tag).toBe("t");
  got.delivery.accept();
  c.close();
});

test("AMQP 1.0 :: link credit limits deliveries", async () => {
  const ch = await channel();
  const q = await durableQueue();
  for (let i = 0; i < 5; i++) ch.sendToQueue(q, Buffer.from(`m${i}`), { persistent: true });
  await sleep(100);
  const c = await open();
  const r = await c.receiver(`/queues/${q}`);
  const got: any[] = [];
  r.on("message", (ctx: any) => {
    got.push(ctx);
    ctx.delivery.accept();
  });
  r.add_credit(2);
  await sleep(400);
  expect(got.length).toBe(2);
  r.add_credit(3);
  await sleep(400);
  expect(got.length).toBe(5);
  expect(got.map((g) => bodyText(g.message))).toEqual(["m0", "m1", "m2", "m3", "m4"]);
  c.close();
});

test("AMQP 1.0 :: bodies larger than a frame go both ways", async () => {
  const ch = await channel();
  const q = await durableQueue();
  const big = Buffer.alloc(1_000_000, 7);
  big[0] = 1;
  big[big.length - 1] = 9;
  const c = await open();
  const s = await c.sender(`/queues/${q}`);
  expect(await send(s, { body: data(big) })).toBe("accepted");
  const m = await nextMessage(ch, q);
  expect(m?.content.length).toBe(big.length);
  expect(m?.content.equals(big)).toBe(true);
  ch.sendToQueue(q, big);
  const r = await c.receiver(`/queues/${q}`);
  const wait = gather(r, 1000);
  r.add_credit(1);
  const [got] = await wait;
  expect(Buffer.from(got.message.body.content).equals(big)).toBe(true);
  got.delivery.accept();
  c.close();
});

test("AMQP 1.0 :: accepted removes, released requeues, rejected dead-letters", async () => {
  const ch = await channel();
  const dlq = await durableQueue();
  const q = uniq("q10dlx");
  await ch.assertQueue(q, { durable: true, arguments: { "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dlq } });
  for (const b of ["a", "b", "c"]) ch.sendToQueue(q, Buffer.from(b), { persistent: true });
  await sleep(100);
  const c = await open();
  const r = await c.receiver(`/queues/${q}`);
  const first = gather(r, 400);
  r.add_credit(3);
  const got = await first;
  expect(got.map((g) => bodyText(g.message))).toEqual(["a", "b", "c"]);
  // rhea folds a batch of dispositions into ranges and can merge the
  // first two outcomes, so each is sent on its own turn.
  got[0].delivery.accept();
  await sleep(50);
  got[1].delivery.release();
  await sleep(50);
  got[2].delivery.reject({ condition: "test:rejected" });
  await sleep(200);
  const again = gather(r, 400);
  r.add_credit(1);
  const back = await again;
  expect(back.map((g) => bodyText(g.message))).toEqual(["b"]);
  back[0].delivery.accept();
  const dead = await collect(ch, dlq, 1, 1000);
  expect(dead.map((m) => m.content.toString())).toEqual(["c"]);
  c.close();
});

test("AMQP 1.0 :: unroutable message is released", async () => {
  const c = await open();
  const s = await c.sender(`/exchanges/amq.direct/${uniq("nobody")}`);
  expect(await send(s, { body: data("lost") })).toBe("released");
  c.close();
});

test("AMQP 1.0 :: the to property addresses a null target", async () => {
  const ch = await channel();
  const q = await durableQueue();
  const c = await open();
  const s = await c.sender(null);
  expect(await send(s, { to: `/queues/${q}`, body: data("anon") })).toBe("accepted");
  expect((await nextMessage(ch, q))?.content.toString()).toBe("anon");
  c.close();
});

test("AMQP 1.0 :: attaching to a missing queue fails with not-found", async () => {
  const c = await open();
  const err = await c.receiver(`/queues/${uniq("missing")}`).then(
    () => "attached",
    (e: Error) => e.message,
  );
  expect(err).toBe("amqp:not-found");
  c.close();
});

test("AMQP 1.0 :: SASL PLAIN refuses a wrong password", async () => {
  const err = await open("wrong").then(
    (c) => {
      c.close();
      return "open";
    },
    (e: Error) => e.message,
  );
  expect(err).not.toBe("open");
});

test("AMQP 1.0 :: drain hands back unused credit", async () => {
  const ch = await channel();
  const q = await durableQueue();
  ch.sendToQueue(q, Buffer.from("only"), { persistent: true });
  await sleep(100);
  const c = await open();
  const r = await c.receiver(`/queues/${q}`);
  const got: any[] = [];
  r.on("message", (ctx: any) => {
    got.push(ctx);
    ctx.delivery.accept();
  });
  const drained = new Promise<number>((done) => r.once("receiver_drained", () => done(r.credit)));
  r.add_credit(10);
  r.drain_credit();
  const left = await Promise.race([drained, sleep(2000).then(() => -1)]);
  expect(left).toBe(0);
  expect(got.map((g) => bodyText(g.message))).toEqual(["only"]);
  c.close();
});

test("AMQP 1.0 :: an amqp-value body round-trips between 1.0 clients", async () => {
  const q = await durableQueue();
  const c = await open();
  const s = await c.sender(`/queues/${q}`);
  expect(await send(s, { body: { greeting: "hi", n: 2 }, subject: "value" })).toBe("accepted");
  const r = await c.receiver(`/queues/${q}`);
  const wait = gather(r, 500);
  r.add_credit(1);
  const [got] = await wait;
  expect(got.message.body).toEqual({ greeting: "hi", n: 2 });
  got.delivery.accept();
  c.close();
});

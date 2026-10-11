import { expect, test } from "bun:test";
import { Conn } from "../src/amqp/listen.ts";
import "../src/amqp/index.ts";
import type { Broker } from "../src/broker/index.ts";
import type { LiveMsg } from "../src/broker/index.ts";
import { bodyFrame, contentHeaderFrame, emptyProps, method, methodFrame } from "../src/codec.ts";

function fixture() {
  const q = { home: "local", consumers: [{}], ready: { length: 256 } };
  const permissions: unknown[] = [];
  let published = 0, pauses = 0, resumes = 0;
  const broker = {
    blocked: false, perms: permissions,
    queues: new Map([["/q", q]]),
    key: (vhost: string, name: string) => vhost + name,
    route: (_vhost: string, _exchange: string, name: string) => [name],
    isLocalHome: (home: string) => home === "local",
    topicWriteAllowed: () => true,
    publish: () => { published++; return "ack"; },
    store: { immediateLone: true, stagedWithoutFlush: false, setLoneMode() {} },
  } as unknown as Broker;
  const conn = new Conn({ write: b => b.length, end() {}, pause: () => { pauses++; }, resume: () => { resumes++; } }, broker);
  conn.stage = "frames";
  conn.writeEx = "";
  conn.permRef = permissions;
  conn.permLen = 0;
  return { conn, q, published: () => published, pauses: () => pauses, resumes: () => resumes };
}

function publish(queue = "q", size = 1) {
  return Buffer.concat([
    methodFrame(1, method(60, 40, w => { w.u16(0); w.shortstr(""); w.shortstr(queue); w.u8(0); })),
    contentHeaderFrame(1, size, emptyProps()),
    bodyFrame(1, new Uint8Array(size), 131072),
  ]);
}

test("a non-confirm publisher pauses only for its destination and resumes", async () => {
  const f = fixture();
  let done = false;
  const pending = f.conn.push(publish()).then(() => { done = true; });
  await Bun.sleep(5);
  expect(done).toBe(false);
  expect(f.published()).toBe(0);
  expect(f.pauses()).toBe(1);
  f.q.ready.length = 0;
  await pending;
  expect(f.published()).toBe(1);
  expect(f.resumes()).toBe(1);
  expect(f.conn.publishBytes).toBe(0);
  expect(f.conn.ch(1).chunks).toHaveLength(0);
});

test("an unrelated queue and a queue without consumers do not hold publishing", async () => {
  const f = fixture();
  await f.conn.push(publish("other"));
  f.q.consumers = [];
  await f.conn.push(publish());
  expect(f.published()).toBe(2);
  expect(f.pauses()).toBe(0);
});

test("consumer cancellation releases a paused publisher", async () => {
  const f = fixture();
  const pending = f.conn.push(publish());
  await Bun.sleep(2);
  expect(f.pauses()).toBe(1);
  f.q.consumers = [];
  await pending;
  expect(f.published()).toBe(1);
  expect(f.resumes()).toBe(1);
});

test("destination pressure includes fanout and CC/BCC routes within the vhost", () => {
  for (const header of ["CC", "BCC"]) {
    const f = fixture();
    const ch = f.conn.ch(1);
    ch.publish = { exchange: "", routingKey: "other", mandatory: false, immediate: false };
    ch.headers = [[header, { t: "A", v: [{ t: "S", v: "q" }] }]];
    f.conn.finishPublish(1, ch);
    expect(f.conn.deferredPublish[0]!.pressured()).toBe(true);
    f.conn.broker.queues.delete("/q");
    f.conn.broker.queues.set("other-vhostq", f.q as never);
    expect(f.conn.deferredPublish[0]!.pressured()).toBe(false);
  }
  const f = fixture();
  f.conn.broker.route = () => ["q", "other"];
  const ch = f.conn.ch(1);
  ch.publish = { exchange: "", routingKey: "fanout", mandatory: false, immediate: false };
  f.conn.finishPublish(1, ch);
  expect(f.conn.deferredPublish[0]!.pressured()).toBe(true);
});

test("ACKs remain readable behind a held publish on a consumer connection", async () => {
  const f = fixture();
  f.conn.ch(1).consumers.set("consumer", "q");
  let acked = false;
  f.conn.ack = () => { acked = true; f.q.ready.length = 0; };
  await f.conn.push(Buffer.concat([publish(), methodFrame(1, method(60, 80, w => { w.u64(1); w.u8(0); }))]));
  expect(acked).toBe(true);
  expect(f.published()).toBe(1);
  expect(f.pauses()).toBe(0);
});

test("a topology method observes earlier non-confirm publishes", async () => {
  const f = fixture();
  f.q.ready.length = 0;
  const events: string[] = [];
  f.conn.onMethod = async () => { events.push(`purge after ${f.published()}`); };
  await f.conn.push(Buffer.concat([publish(), methodFrame(1, method(50, 30, () => {}))]));
  expect(events).toEqual(["purge after 1"]);
});

test("a topology fence waits for a publish already waiting on remote acceptance", async () => {
  const f = fixture();
  f.q.ready.length = 0;
  let release!: () => void;
  f.conn.broker.publish = () => new Promise<"ack">(resolve => { release = () => resolve("ack"); });
  await f.conn.push(publish());
  let purged = false;
  f.conn.onMethod = async () => { purged = true; };
  const pending = f.conn.push(methodFrame(1, method(50, 30, () => {})));
  await Bun.sleep(2);
  expect(purged).toBe(false);
  release();
  await pending;
  expect(purged).toBe(true);
  expect(f.conn.publishBytes).toBe(0);
});

test("buffered reads while paused yield to drain timers", async () => {
  const f = fixture();
  const first = f.conn.push(publish());
  const second = f.conn.push(publish());
  setTimeout(() => { f.q.ready.length = 0; }, 5);
  await Promise.all([first, second]);
  expect(f.published()).toBe(2);
});

test("a fragmented body completes before publisher reads are paused", async () => {
  const f = fixture();
  const bytes = publish("q", 256);
  await f.conn.push(bytes.subarray(0, bytes.length - 8));
  expect(f.pauses()).toBe(0);
  const pending = f.conn.push(bytes.subarray(bytes.length - 8));
  await Bun.sleep(5);
  expect(f.pauses()).toBe(1);
  f.q.ready.length = 0;
  await pending;
  expect(f.published()).toBe(1);
});

test("transaction publishes and ACKs retain order before commit completes", async () => {
  const f = fixture();
  const ch = f.conn.ch(1);
  ch.tx = true;
  ch.consumers.set("consumer", "q");
  await f.conn.push(publish());
  expect(f.published()).toBe(0);
  ch.txBatch.push(() => { expect(f.published()).toBe(1); f.q.ready.length = 0; });
  await f.conn.txCommit(1, ch);
  await f.conn.push(new Uint8Array());
  expect(ch.txBytes).toBe(0);
  expect(f.published()).toBe(1);
});

test("auto-ack tags advance without retaining manual settlement state", () => {
  const f = fixture();
  const ch = f.conn.ch(1);
  const tags: number[] = [];
  f.conn.deliver = (_channel, _tag, deliveryTag) => { tags.push(deliveryTag); };
  for (let i = 0; i < 1000; i++) f.conn.handOff(1, ch, "auto", "q", { id: String(i) } as LiveMsg, true);
  expect(tags[999]).toBe(1000);
  expect(ch.deliveries.size).toBe(0);
  expect(ch.byConsumer.size).toBe(0);
  expect(ch.globalUnacked).toBe(0);
  f.conn.handOff(1, ch, "manual", "q", { id: "manual" } as LiveMsg);
  expect(ch.deliveries.size).toBe(1);
  expect(ch.byConsumer.get("manual")).toBe(1);
  expect(ch.globalUnacked).toBe(1);
});

test("parked auto-ack deliveries keep their settlement mode", () => {
  const f = fixture();
  const ch = f.conn.ch(1);
  f.conn.deliver = () => {};
  ch.parked.push({ channel: 1, tag: "auto", queue: "q", msg: { id: "auto", body: Uint8Array.of(1), propRaw: emptyProps() } as LiveMsg, noAck: true });
  f.conn.flushParked(ch);
  expect(ch.parked).toHaveLength(0);
  expect(ch.deliveries.size).toBe(0);
});

test("slow socket output consumes delivery credit and drain wakes consumers", async () => {
  const f = fixture();
  const ch = f.conn.ch(1);
  f.conn.socket.write = () => 0;
  let woken = 0;
  f.conn.outputWakers.set("consumer", () => { woken++; });
  await f.conn.send(new Uint8Array(2 * 1024 * 1024));
  expect(f.conn.creditOk(ch, "consumer")).toBe(false);
  expect(f.conn.outboundBytes).toBe(2 * 1024 * 1024);
  f.conn.socket.write = b => b.length;
  f.conn.flush();
  await Bun.sleep(0);
  expect(f.conn.outboundBytes).toBe(0);
  expect(f.conn.creditOk(ch, "consumer")).toBe(true);
  expect(woken).toBe(1);
});

test("asynchronous publish admission keeps outstanding bytes bounded", async () => {
  const f = fixture();
  f.q.ready.length = 0;
  let maximum = 0;
  for (let i = 0; i < 5; i++) {
    f.conn.publishBytes += 8 * 1024 * 1024;
    f.conn.deferredPublish.push({ channel: 1, bytes: 8 * 1024 * 1024, pressured: () => false,
      run: () => new Promise<void>(resolve => {
        setTimeout(() => { maximum = Math.max(maximum, f.conn.inFlightBytes); resolve(); }, 2);
      }) });
  }
  await f.conn.push(new Uint8Array());
  await Promise.all(f.conn.inFlightPublishes);
  expect(maximum).toBeLessThanOrEqual(32 * 1024 * 1024);
  expect(f.conn.publishBytes).toBe(0);
});

test("transaction limits include settlements and payloads across channels", () => {
  const f = fixture();
  const ch = f.conn.ch(1);
  ch.tx = true;
  f.conn.txOperations = 65536;
  expect(() => f.conn.ack(ch, method(60, 80, w => { w.u64(1); w.u8(0); }))).toThrow("transaction staging limit");
  f.conn.txOperations = 0;
  f.conn.stageTransaction(ch, () => {}, 64 * 1024 * 1024);
  expect(() => f.conn.stageTransaction(f.conn.ch(2), () => {}, 1)).toThrow("transaction staging limit");
});

test("deferred publishes remain on their worker when a later queue has a remote home", async () => {
  const f = fixture();
  f.q.ready.length = 0;
  f.conn.handed = { pause() {}, removeAllListeners() {} } as never;
  f.conn.broker.queues.set("/remote", { home: "other", consumers: [], ready: { length: 0 } } as never);
  await f.conn.push(Buffer.concat([publish(), publish("remote")]));
  await Promise.all(f.conn.inFlightPublishes);
  expect(f.conn.migrated).toBe(false);
  expect(f.published()).toBe(2);
});

test("parked manual deliveries requeue on close while auto-ack bodies only release memory", async () => {
  const f = fixture();
  const ch = f.conn.ch(1);
  const returned: string[] = [];
  let releases = 0;
  f.conn.broker.nack = (_vhost, _queue, id, requeue) => { expect(requeue).toBe(true); returned.push(id); };
  const message = { id: "manual", body: Uint8Array.of(1), propRaw: emptyProps() } as LiveMsg;
  ch.parked.push({ channel: 1, tag: "c", queue: "q", msg: message, release: () => { releases++; } });
  ch.parked.push({ channel: 1, tag: "c", queue: "q", msg: { ...message, id: "auto" }, noAck: true });
  f.conn.parkedBytes = 2 * (message.body.length + message.propRaw.length);
  f.conn.parkedOperations = 2;
  await f.conn.requeueChannel(ch);
  expect(returned).toEqual(["manual"]);
  expect(releases).toBe(1);
  expect(ch.parked).toHaveLength(0);
  expect(f.conn.parkedBytes).toBe(0);
  expect(f.conn.parkedOperations).toBe(0);
});

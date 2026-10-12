import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/index.ts";
import type { Config } from "../src/config.ts";
import type { LiveMsg } from "../src/broker/model.ts";
import { Store } from "../src/store.ts";

function config(dir: string): Config {
  return {
    amqp: "127.0.0.1:0", management: "127.0.0.1:0", metrics: "127.0.0.1:0",
    mqtt: null, stomp: null, stream: null, dataDir: dir,
    fsync: "every_n_ms", fsyncIntervalMs: 50, fsyncEveryN: 1, tls: false,
    nodeId: "bun-a", clusterListen: null, members: [], defaultQueueType: "classic",
  };
}

async function stream(broker: Broker, args: Record<string, string | number> = {}) {
  await broker.declareQueue({ vhost: "/", name: "s", durable: true, exclusive: false, autoDelete: false, passive: false, args: { "x-queue-type": "stream", ...args } });
  return broker.queues.get(broker.key("/", "s"))!;
}

/** 8 KiB bodies numbered from 0. 3072 of them is 24 MiB: the oldest 8 MiB leave memory. */
function append(broker: Broker, q: ReturnType<Broker["queues"]["get"]> & object, from: number, count: number) {
  for (let i = from; i < from + count; i++) {
    const body = new Uint8Array(8 * 1024);
    new DataView(body.buffer).setUint32(0, i);
    broker.appendStream(q, { body, exchange: "", routingKey: "s", headers: [], propRaw: new Uint8Array([0, 0]) });
  }
}

const number = (m: LiveMsg) => new DataView(m.body.buffer, m.body.byteOffset).getUint32(0);

async function read(broker: Broker, offset: Field) {
  const got: number[] = [];
  const offsets: number[] = [];
  await broker.consume("/", "s", {
    tag: `r-${Math.random()}`, session: 1, noAck: false, exclusive: false, streamOffset: offset,
    want: () => true,
    deliver: (m) => { got.push(number(m)); offsets.push(Number(m.id.slice(2))); },
  });
  broker.pumpStream(broker.queues.get(broker.key("/", "s"))!);
  return { got, offsets };
}
type Field = { t: "S"; v: string } | { t: "l"; v: number };

function withBroker(name: string, body: (broker: Broker, store: Store, dir: string) => Promise<void>) {
  test(name, async () => {
    const dir = mkdtempSync(join(tmpdir(), "qf-stream-"));
    const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50);
    try {
      await body(new Broker(config(dir), store), store, dir);
    } finally {
      store.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
}

withBroker("a reader behind the resident window reads the store in ranges, in order", async (broker, store) => {
  const q = await stream(broker);
  append(broker, q, 0, 3072);
  let ranges = 0;
  let points = 0;
  const range = store.readStreamRange.bind(store);
  const point = store.readStreamEntry.bind(store);
  store.readStreamRange = (...args) => { ranges++; return range(...args); };
  store.readStreamEntry = (...args) => { points++; return point(...args); };
  const { got, offsets } = await read(broker, { t: "S", v: "first" });
  expect(got).toEqual([...Array(3072).keys()]);
  expect(offsets).toEqual(got);
  // 1024 paged entries of 8 KiB at 512 a range.
  expect(points).toBe(0);
  expect(ranges).toBe(2);
});

withBroker("only the resident tail keeps an entry object", async (broker) => {
  const q = await stream(broker);
  append(broker, q, 0, 3072);
  const s = broker.streamOf(q);
  expect(s.log.length - s.logHead).toBe(2048);
  expect(s.coldTs.length - s.coldHead).toBe(1024);
  expect(s.bytes).toBe(3072 * 8 * 1024);
});

withBroker("retention trims cold and resident entries alike", async (broker) => {
  // 12 MiB kept: the whole cold part and the oldest resident entries go.
  const q = await stream(broker, { "x-max-length-bytes": 12 * 1024 * 1024 });
  append(broker, q, 0, 3072);
  const s = broker.streamOf(q);
  expect(s.next - s.first).toBe(1536);
  expect(s.first).toBe(1536);
  const { got } = await read(broker, { t: "S", v: "first" });
  expect(got).toEqual([...Array(1536).keys()].map((i) => i + 1536));
  expect((await read(broker, { t: "S", v: "last" })).got).toEqual([3071]);
});

withBroker("a restart rebuilds the cold index from the store", async (broker, store, dir) => {
  const q = await stream(broker);
  append(broker, q, 0, 100);
  store.flushStreamAppends();
  const again = new Broker(config(dir), store);
  const q2 = await stream(again);
  const s = again.streamOf(q2);
  expect([s.first, s.next, s.coldTs.length]).toEqual([0, 100, 100]);
  append(again, q2, 100, 10);
  const got: number[] = [];
  await again.consume("/", "s", {
    tag: "r", session: 1, noAck: false, exclusive: false, streamOffset: { t: "l", v: 95 },
    want: () => true, deliver: (m) => got.push(number(m)),
  });
  again.pumpStream(q2);
  expect(got).toEqual([95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109]);
});

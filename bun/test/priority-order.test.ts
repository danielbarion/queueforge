import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/index.ts";
import type { Config } from "../src/config.ts";
import { Store } from "../src/store.ts";

/**
 * A priority queue delivers its highest priority first and, within one
 * priority, the oldest first: after a burst at one priority (which appends),
 * after a requeue (back to the head of its own priority, not of the queue),
 * and after a restart (rows load in insert order, then sort).
 */

function cfg(dir: string): Config {
  return {
    amqp: "127.0.0.1:0",
    management: "127.0.0.1:0",
    metrics: "127.0.0.1:0",
    mqtt: null,
    stomp: null,
    stream: null,
    dataDir: dir,
    fsync: "every_n_ms",
    fsyncIntervalMs: 20,
    fsyncEveryN: 1,
    tls: false,
    nodeId: "a",
    clusterListen: null,
    members: [],
    defaultQueueType: "classic",
  };
}

async function open(dir: string) {
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 20);
  const broker = new Broker(cfg(dir), store);
  broker.load();
  return { store, broker };
}

function publish(broker: Broker, label: string, priority: number) {
  return broker.publish({
    vhost: "/",
    exchange: "",
    routingKey: "p",
    body: new TextEncoder().encode(label),
    headers: [],
    propRaw: new Uint8Array(),
    persistent: true,
    priority,
    expiration: "",
    confirm: true,
  });
}

test("priority order holds across bursts, requeues and a restart", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-prio-"));
  try {
    let { store, broker } = await open(dir);
    await broker.declareQueue({ vhost: "/", name: "p", durable: true, exclusive: false, autoDelete: false, passive: false, args: { "x-max-priority": 5 } });
    const order: Array<[string, number]> = [["a1", 1], ["a2", 1], ["c9", 9], ["b3", 3], ["a3", 1], ["b4", 3], ["c5", 5]];
    await Promise.all(order.map(([label, p]) => publish(broker, label, p)));
    // 9 is above x-max-priority 5, so it ranks as 5; c9 is older than c5.
    const expected = ["c9", "c5", "b3", "b4", "a1", "a2", "a3"];
    const q = broker.queues.get(broker.key("/", "p"))!;
    expect([...q.ready].map((m) => new TextDecoder().decode(m.body))).toEqual(expected);

    // Take every message, then requeue some: each goes back to the head of
    // its own priority. Requeued to the head of the queue, a1 would come out
    // before c5.
    const got: string[] = [];
    await broker.consume("/", "p", { tag: "c", session: 1, noAck: false, exclusive: false, want: () => true, deliver: (item) => got.push(item.id) });
    await broker.kick("/", "p", 1);
    for (let i = 0; i < 50 && got.length < 7; i++) await Bun.sleep(5);
    expect(got).toHaveLength(7);
    const id = (label: string) => got[expected.indexOf(label)]!;
    const labels = () => [...q.ready].map((m) => new TextDecoder().decode(m.body));
    q.consumers.length = 0;
    broker.nack("/", "p", id("c5"), true);
    broker.nack("/", "p", id("a1"), true);
    expect(labels()).toEqual(["c5", "a1"]);
    broker.nack("/", "p", id("b3"), true);
    expect(labels()).toEqual(["c5", "b3", "a1"]);
    broker.nack("/", "p", id("c9"), true);
    expect(labels()).toEqual(["c9", "c5", "b3", "a1"]);
    await store.whenDurable();
    store.close();

    ({ store, broker } = await open(dir));
    const again = broker.queues.get(broker.key("/", "p"))!;
    const after: string[] = [];
    for (const m of again.ready) {
      broker.materialize(m);
      after.push(new TextDecoder().decode(m.body));
    }
    expect(after).toEqual(["c9", "c5", "b3", "b4", "a1", "a2", "a3"]);
    store.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

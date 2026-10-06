import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Conn } from "../src/amqp/listen.ts";
import "../src/amqp/index.ts";
import { Broker } from "../src/broker/index.ts";
import type { Cluster } from "../src/cluster.ts";
import { method, writeTable } from "../src/codec.ts";
import type { Config } from "../src/config.ts";
import { Store } from "../src/store.ts";

/**
 * A slow follower drop used to let pump take every ready body, because prefetch
 * was counted only when basic.deliver was written. The claim has to reserve
 * that credit first.
 */
test("a slow quorum drop keeps unacked inside prefetch", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qpref-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50);
  const cfg: Config = {
    amqp: "127.0.0.1:0",
    management: "127.0.0.1:0",
    metrics: "127.0.0.1:0",
    mqtt: null,
    stomp: null,
    stream: null,
    dataDir: dir,
    fsync: "every_n_ms",
    fsyncIntervalMs: 50,
    fsyncEveryN: 1,
    tls: false,
    nodeId: "a",
    clusterListen: null,
    members: [{ id: "a", addr: "127.0.0.1:1" }],
    defaultQueueType: "classic",
  };
  const broker = new Broker(cfg, store);
  let releaseDrops: () => void = () => {};
  const gate = new Promise<void>((resolve) => {
    releaseDrops = resolve;
  });
  try {
    await broker.declareQueue({
      vhost: "/",
      name: "q",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: { "x-queue-type": "quorum" },
    });
    broker.cluster = {
      peerIds: () => ["b"],
      call: async () => {
        await gate;
        return true;
      },
    } as unknown as Cluster;
    const conn = new Conn({ write: (frame) => frame.length, end() {} }, broker);
    const ch = conn.ch(1);
    ch.prefetch = 2;
    const payload = method(60, 20, (w) => {
      w.u16(0);
      w.shortstr("q");
      w.shortstr("c");
      w.u8(0);
      writeTable(w, []);
    });
    await conn.consume(1, ch, payload);
    const q = broker.queues.get(broker.key("/", "q"));
    if (!q) throw new Error("queue missing");
    for (let n = 0; n < 8; n++) {
      expect(
        broker.enqueueLocal(
          q,
          {
            body: new Uint8Array([1]),
            exchange: "",
            routingKey: "q",
            headers: [],
            propRaw: new Uint8Array(),
            persistent: false,
            priority: 0,
            expiration: "",
            id: `id-${n}`,
          },
          0,
        ),
      ).toBe(true);
    }
    expect(q.unacked.size).toBe(2);
    expect(q.ready.length).toBe(6);
    releaseDrops();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(q.unacked.size).toBe(2);
    expect(q.ready.length).toBe(6);
    expect(ch.byConsumer.get("c")).toBe(2);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

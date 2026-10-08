import { expect, test } from "bun:test";
import { cpSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Conn } from "../src/amqp/listen.ts";
import "../src/amqp/consume.ts";
import "../src/amqp/frames.ts";
import "../src/amqp/publish.ts";
import { Broker } from "../src/broker/index.ts";
import type { Config } from "../src/config.ts";
import { Store } from "../src/store.ts";

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
    fsyncIntervalMs: 400,
    fsyncEveryN: 1,
    tls: false,
    nodeId: "bun-a",
    clusterListen: null,
    members: [],
    defaultQueueType: "classic",
  };
}

function brokerWithQueue(intervalMs: number) {
  const dir = mkdtempSync(join(tmpdir(), "qf-fast-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", intervalMs);
  const broker = new Broker(cfg(dir), store);
  return { dir, store, broker };
}

test("a local classic confirm resolves only after the covering fsync", async () => {
  const { dir, store, broker } = brokerWithQueue(400);
  try {
    await broker.declareQueue({
      vhost: "/",
      name: "q",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: {},
    });
    const started = performance.now();
    const pending = broker.publish({
      vhost: "/",
      exchange: "",
      routingKey: "q",
      body: Uint8Array.of(9, 8, 7),
      headers: [],
      propRaw: Uint8Array.of(0x10, 0x00, 0x02),
      persistent: true,
      priority: 0,
      expiration: "",
      confirm: true,
    });
    expect(typeof pending).toBe("object");
    expect(broker.prom.confirmed).toBe(0);
    expect(store.fullFlushCount).toBe(0);
    expect(store.listMessages()).toHaveLength(0);
    const result = await pending;
    expect(result).toBe("ack");
    expect(broker.prom.confirmed).toBe(1);
    expect(store.fullFlushCount).toBe(1);
    expect(store.listMessages()).toHaveLength(1);
    expect(performance.now() - started).toBeLessThan(10);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a learned lone confirm acks in the same turn after the fsync", async () => {
  const { dir, store, broker } = brokerWithQueue(400);
  try {
    await broker.declareQueue({
      vhost: "/",
      name: "q",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: {},
    });
    store.immediateLone = true;
    const pending = broker.publish({
      vhost: "/",
      exchange: "",
      routingKey: "q",
      body: Uint8Array.of(4, 5, 6),
      headers: [],
      propRaw: Uint8Array.of(0x10, 0x00, 0x02),
      persistent: true,
      priority: 0,
      expiration: "",
      confirm: true,
    });
    expect(pending).toBe("ack");
    expect(broker.prom.confirmed).toBe(1);
    expect(store.fullFlushCount).toBe(1);
    expect(store.confirmsBeforeFsync).toBe(0);
    expect(store.listMessages()).toHaveLength(1);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("128 local classic confirms share one fsync", async () => {
  const { dir, store, broker } = brokerWithQueue(400);
  try {
    await broker.declareQueue({
      vhost: "/",
      name: "q",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: {},
    });
    const body = new Uint8Array(256);
    const props = Uint8Array.of(0x10, 0x00, 0x02);
    const started = performance.now();
    const waits = [];
    for (let i = 0; i < 128; i++) {
      waits.push(
        broker.publish({
          vhost: "/",
          exchange: "",
          routingKey: "q",
          body,
          headers: [],
          propRaw: props,
          persistent: true,
          priority: 0,
          expiration: "",
          confirm: true,
        }),
      );
    }
    expect(store.fullFlushCount).toBe(1);
    const results = await Promise.all(waits);
    expect(results.every((item) => item === "ack")).toBe(true);
    expect(broker.prom.confirmed).toBe(128);
    expect(store.fullFlushCount).toBe(1);
    expect(store.listMessages()).toHaveLength(128);
    expect(performance.now() - started).toBeLessThan(250);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("basic.ack multiple settles each tag", async () => {
  const { dir, store, broker } = brokerWithQueue(400);
  try {
    const socket = {
      write: () => 0,
      end() {},
      flush() {},
    };
    const conn = new Conn(socket, broker);
    const c = conn.ch(1);
    c.deliveries.set(1, { vhost: "/", queue: "q", id: "gone-1", consumer: "c" });
    c.deliveries.set(2, { vhost: "/", queue: "q", id: "gone-2", consumer: "c" });
    c.byConsumer.set("c", 2);
    c.globalUnacked = 2;
    const pending = conn.settle(c, 2, true, false, false);
    if (pending) await pending;
    expect(c.deliveries.size).toBe(0);
    expect(c.globalUnacked).toBe(0);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a parked publish confirm is written after the fsync", async () => {
  const { dir, store, broker } = brokerWithQueue(400);
  try {
    await broker.declareQueue({
      vhost: "/",
      name: "q",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: {},
    });
    const writes: Uint8Array[] = [];
    const socket = {
      write(b: Uint8Array) {
        writes.push(b.slice());
        return b.length;
      },
      end() {},
      flush() {},
    };
    const conn = new Conn(socket, broker);
    conn.vhost = "/";
    conn.user = "guest";
    broker.perms = [...broker.perms, { user: "guest", vhost: "/", configure: ".*", write: ".*", read: ".*" }];
    const c = conn.ch(1);
    c.confirm = true;
    c.deliveryMode = 2;
    c.headers = [];
    c.propRaw = Uint8Array.of(0x10, 0x00, 0x02);
    c.chunks = [Uint8Array.of(1, 2, 3, 4)];
    c.bodySize = 4;
    c.got = 4;
    c.publish = { exchange: "", routingKey: "q", mandatory: false, immediate: false };
    expect(conn.finishPublish(1, c)).toBeUndefined();
    expect(conn.deferredPublish).toHaveLength(1);
    expect(store.fullFlushCount).toBe(0);
    expect(writes).toHaveLength(0);
    const parked = conn.deferredPublish.shift()!;
    const pending = parked();
    expect(broker.prom.confirmed).toBe(0);
    expect(store.fullFlushCount).toBe(0);
    expect(writes).toHaveLength(0);
    await pending;
    expect(broker.prom.confirmed).toBe(1);
    expect(store.fullFlushCount).toBe(1);
    expect(writes.length).toBeGreaterThan(0);
    const bytes = writes.flatMap((frame) => [...frame]);
    expect(bytes[0]).toBe(1);
    expect(bytes[7]).toBe(0);
    expect(bytes[8]).toBe(60);
    expect(bytes[9]).toBe(0);
    expect(bytes[10]).toBe(80);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a noAck consumer does not confirm before the append is fsynced", async () => {
  for (const lone of [false, true]) {
    const dir = mkdtempSync(join(tmpdir(), "qf-fast-noack-"));
    const path = join(dir, "t.sqlite");
    const store = new Store(path, "every_n_ms", 400);
    const broker = new Broker(cfg(dir), store);
    const body = Uint8Array.of(9, 4, lone ? 1 : 0);
    let closed = false;
    try {
      await broker.declareQueue({
        vhost: "/",
        name: "q",
        durable: true,
        exclusive: false,
        autoDelete: false,
        passive: false,
        args: {},
      });
      store.immediateLone = lone;
      const seen: Uint8Array[] = [];
      await broker.consume("/", "q", {
        tag: "c",
        session: 1,
        noAck: true,
        exclusive: false,
        want: () => true,
        deliver: (msg) => {
          seen.push(msg.body);
        },
      });
      const pending = broker.publish({
        vhost: "/",
        exchange: "",
        routingKey: "q",
        body,
        headers: [],
        propRaw: Uint8Array.of(0x10, 0x00, 0x02),
        persistent: true,
        priority: 0,
        expiration: "",
        confirm: true,
      });
      expect(await pending).toBe("ack");
      expect(seen).toHaveLength(1);
      expect(seen[0]).toEqual(body);
      expect(broker.prom.confirmed).toBe(1);
      expect(store.fullFlushCount).toBe(1);
      expect(store.confirmsBeforeFsync).toBe(0);
      expect(store.stagedWithoutFlush).toBe(false);
      expect(store.listMessages()).toHaveLength(0);
      const crashPath = join(dir, "crash.sqlite");
      cpSync(`${path}.dlog`, `${crashPath}.dlog`, { recursive: true });
      const crashed = new Store(crashPath, "every_n_ms", 400);
      try {
        const kept = crashed.listMessages();
        expect(kept).toHaveLength(1);
        expect(new Uint8Array(kept[0]!.body)).toEqual(body);
      } finally {
        crashed.close();
      }
      store.close();
      closed = true;
      const again = new Store(path, "every_n_ms", 400);
      try {
        expect(again.listMessages()).toHaveLength(0);
      } finally {
        again.close();
      }
    } finally {
      if (!closed) store.close();
      rmSync(dir, { recursive: true, force: true });
    }
  }
});

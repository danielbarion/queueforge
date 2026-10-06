import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/index.ts";
import type { Config } from "../src/config.ts";
import { ReadyQueue, type LiveMsg } from "../src/broker/model.ts";
import { Store } from "../src/store.ts";

function msg(n: number, bytes = 4): LiveMsg {
  return {
    id: `m-${n}`,
    rowId: null,
    body: new Uint8Array(bytes).fill(n & 0xff),
    exchange: "",
    routingKey: "q",
    headers: [],
    propRaw: new Uint8Array(),
    persistent: false,
    priority: 0,
    expiresAt: null,
    redelivered: false,
  };
}

test("ready shift stays in order after the chunk boundary", () => {
  const ready = new ReadyQueue();
  for (let i = 0; i < 300; i++) ready.push(msg(i));
  expect(ready.length).toBe(300);
  expect(ready.bytes).toBe(300 * 4);
  for (let i = 0; i < 300; i++) {
    const got = ready.shift();
    expect(got?.id).toBe(`m-${i}`);
  }
  expect(ready.length).toBe(0);
  expect(ready.bytes).toBe(0);
  expect(ready.shift()).toBeUndefined();
});

test("a publish after a delivery is still readable", () => {
  const ready = new ReadyQueue();
  for (let i = 0; i < 5; i++) {
    ready.push(msg(i));
    expect(ready.shift()?.id).toBe(`m-${i}`);
    expect(ready.length).toBe(0);
  }
  for (let i = 0; i < 150; i++) ready.push(msg(i));
  for (let i = 0; i < 40; i++) expect(ready.shift()?.id).toBe(`m-${i}`);
  for (let i = 150; i < 180; i++) ready.push(msg(i));
  const rest: string[] = [];
  for (;;) {
    const got = ready.shift();
    if (!got) break;
    rest.push(got.id);
  }
  expect(rest[0]).toBe("m-40");
  expect(rest[rest.length - 1]).toBe("m-179");
  expect(rest).toHaveLength(140);
});

test("each classic publish reaches a waiting consumer", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-ready-deliver-"));
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
    nodeId: "bun-a",
    clusterListen: null,
    members: [],
    defaultQueueType: "classic",
  };
  const broker = new Broker(cfg, store);
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
    const got: string[] = [];
    await broker.consume("/", "q", {
      tag: "c",
      session: 1,
      noAck: true,
      exclusive: false,
      want: () => true,
      deliver: (item) => got.push(item.id),
    });
    const body = new Uint8Array([1]);
    for (let i = 0; i < 5; i++) {
      expect(
        await broker.enqueue("/", "q", {
          body,
          exchange: "",
          routingKey: "q",
          headers: [],
          propRaw: new Uint8Array(),
          persistent: true,
          priority: 0,
          expiration: "",
        }),
      ).toBe(true);
    }
    expect(got).toHaveLength(5);
    expect(got.every((id) => id.length > 0)).toBe(true);
    await broker.declareQueue({
      vhost: "/",
      name: "stored",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: {},
    });
    const propRaw = Uint8Array.of(0x10, 0x00, 0x02);
    expect(
      await broker.enqueue("/", "stored", {
        body,
        exchange: "",
        routingKey: "stored",
        headers: [],
        propRaw,
        persistent: true,
        priority: 0,
        expiration: "",
      }),
    ).toBe(true);
    await store.whenDurable();
    const row = store.listMessages().find((item) => item.queue === "stored");
    const meta = JSON.parse(row?.meta ?? "{}") as {
      routingKey: string;
      propRaw: string;
      persistent: boolean;
      expiresAt: null;
      headers: unknown[];
    };
    expect(meta.routingKey).toBe("stored");
    expect(meta.persistent).toBe(true);
    expect(meta.expiresAt).toBeNull();
    expect(meta.headers).toEqual([]);
    expect(Buffer.from(meta.propRaw, "base64")).toEqual(Buffer.from(propRaw));
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("requeue and extract keep the body total", () => {
  const ready = new ReadyQueue();
  ready.push(msg(1));
  ready.push(msg(2));
  const first = ready.shift()!;
  ready.unshift(first);
  expect(ready.at(0)?.id).toBe("m-1");
  expect(ready.bytes).toBe(8);
  const taken = ready.extract((item) => item.id === "m-2");
  expect(taken.map((item) => item.id)).toEqual(["m-2"]);
  expect(ready.length).toBe(1);
  expect(ready.bytes).toBe(4);
});

test("classic enqueue uses the ready byte total for max-length-bytes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-ready-bytes-"));
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
    nodeId: "bun-a",
    clusterListen: null,
    members: [],
    defaultQueueType: "classic",
  };
  const broker = new Broker(cfg, store);
  try {
    await broker.declareQueue({
      vhost: "/",
      name: "bounded",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: { "x-max-length-bytes": 250, "x-overflow": "reject-publish" },
    });
    const body = new Uint8Array(100);
    const publish = () =>
      broker.enqueue("/", "bounded", {
        body,
        exchange: "",
        routingKey: "bounded",
        headers: [],
        propRaw: new Uint8Array(),
        persistent: false,
        priority: 0,
        expiration: "",
      });
    expect(await publish()).toBe(true);
    expect(await publish()).toBe(true);
    expect(await publish()).toBe(false);
    const ready = broker.queues.get(broker.key("/", "bounded"))?.ready;
    expect(ready?.length).toBe(2);
    expect(ready?.bytes).toBe(200);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

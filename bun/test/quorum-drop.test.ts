import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/index.ts";
import type { Cluster } from "../src/cluster.ts";
import type { Config } from "../src/config.ts";
import { Store } from "../src/store.ts";

function cfg(nodeId: string, members: Config["members"]): Config {
  return {
    amqp: "127.0.0.1:0",
    management: "127.0.0.1:0",
    metrics: "127.0.0.1:0",
    mqtt: null,
    stomp: null,
    stream: null,
    dataDir: "",
    fsync: "every_n_ms",
    fsyncIntervalMs: 50,
    fsyncEveryN: 1,
    tls: false,
    nodeId,
    clusterListen: null,
    members,
    defaultQueueType: "classic",
  };
}

function body(): Uint8Array {
  return new Uint8Array([1]);
}

test("a follower drop keeps the other quorum copies in append order", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qdrop-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50);
  const broker = new Broker(
    cfg("c", [
      { id: "a", addr: "127.0.0.1:1" },
      { id: "b", addr: "127.0.0.1:2" },
      { id: "c", addr: "127.0.0.1:3" },
    ]),
    store,
  );
  broker.cfg = { ...broker.cfg, dataDir: dir };
  try {
    expect(broker.isQuorumLeader()).toBe(false);
    await broker.declareQueue({
      vhost: "/",
      name: "q",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: { "x-queue-type": "quorum" },
    });
    const q = broker.queues.get(broker.key("/", "q"));
    if (!q) throw new Error("queue missing");
    for (let n = 0; n < 64; n++) {
      expect(
        broker.enqueueLocal(
          q,
          {
            body: body(),
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
    expect(q.ready.length).toBe(0);
    expect(q.replicas.size).toBe(64);
    broker.dropLocal("/", "q", "id-40");
    expect(q.replicas.has("id-40")).toBe(false);
    expect([...q.replicas.keys()]).toEqual(
      Array.from({ length: 64 }, (_, n) => `id-${n}`).filter((id) => id !== "id-40"),
    );
    broker.dropLocal("/", "q", "id-0");
    expect([...q.replicas.keys()][0]).toBe("id-1");
    broker.noteConsumed("/", "q", "id-1");
    const before = broker.consumed.length;
    broker.noteConsumed("/", "q", "id-1");
    expect(broker.consumed.length).toBe(before);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a quorum body stays queued until its confirm releases the gate", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qgate-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50);
  const broker = new Broker(cfg("a", []), store);
  broker.cfg = { ...broker.cfg, dataDir: dir };
  try {
    expect(broker.isQuorumLeader()).toBe(true);
    await broker.declareQueue({
      vhost: "/",
      name: "q",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: { "x-queue-type": "quorum" },
    });
    const q = broker.queues.get(broker.key("/", "q"));
    if (!q) throw new Error("queue missing");
    const got: string[] = [];
    await broker.consume("/", "q", {
      tag: "c",
      session: 1,
      noAck: true,
      exclusive: false,
      want: () => true,
      deliver: (item) => got.push(item.id),
    });
    const confirmGate = { hold: true };
    expect(
      broker.enqueueLocal(
        q,
        {
          body: body(),
          exchange: "",
          routingKey: "q",
          headers: [],
          propRaw: new Uint8Array(),
          persistent: false,
          priority: 0,
          expiration: "",
          id: "held",
          confirmGate,
        },
        0,
      ),
    ).toBe(true);
    broker.pump(q);
    expect(got).toEqual([]);
    expect(q.ready.length).toBe(1);
    expect(await broker.get("/", "q", true)).toBeNull();
    expect(q.ready.length).toBe(1);
    confirmGate.hold = false;
    broker.pump(q);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(got).toEqual(["held"]);
    expect(q.ready.length).toBe(0);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("enqueueQuorum does not deliver the body before the local fsync", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qenq-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 80);
  const broker = new Broker(cfg("a", []), store);
  broker.cfg = { ...broker.cfg, dataDir: dir };
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
    const q = broker.queues.get(broker.key("/", "q"));
    if (!q) throw new Error("queue missing");
    const got: string[] = [];
    await broker.consume("/", "q", {
      tag: "c",
      session: 1,
      noAck: true,
      exclusive: false,
      want: () => true,
      deliver: (item) => got.push(item.id),
    });
    let confirmed = false;
    const before = store.fullFlushCount;
    const pending = broker
      .enqueue("/", "q", {
        body: body(),
        exchange: "",
        routingKey: "q",
        headers: [],
        propRaw: new Uint8Array(),
        persistent: true,
        priority: 0,
        expiration: "",
      })
      .then((ok) => {
        confirmed = true;
        return ok;
      });
    // The lone fsync is the next turn. This turn still has the body held.
    expect(confirmed).toBe(false);
    expect(got).toEqual([]);
    expect(q.ready.length).toBe(1);
    expect(q.ready.at(0)?.confirmGate?.hold).toBe(true);
    expect(store.fullFlushCount).toBe(before);
    expect(await pending).toBe(true);
    expect(confirmed).toBe(true);
    expect(store.fullFlushCount).toBe(before + 1);
    expect(store.confirmsBeforeFsync).toBe(0);
    expect(q.ready.at(0)?.confirmGate?.hold ?? false).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(got).toHaveLength(1);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

const members = [
  { id: "a", addr: "127.0.0.1:1" },
  { id: "b", addr: "127.0.0.1:2" },
  { id: "c", addr: "127.0.0.1:3" },
];

test("a quorum confirm does not wait for a flush staged after its covering fsync", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qwait-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 300);
  const broker = new Broker(cfg("a", members), store);
  broker.cfg = { ...broker.cfg, dataDir: dir };
  let releasePeer: () => void = () => {};
  const peerGate = new Promise<void>((resolve) => {
    releasePeer = resolve;
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
      peerIds: () => ["b", "c"],
      call: async (_peer: string, op: string) => {
        if (op === "quorum_append") await peerGate;
        return true;
      },
    } as unknown as Cluster;
    const before = store.fullFlushCount;
    const pending = broker.publish({
      vhost: "/",
      exchange: "",
      routingKey: "q",
      body: body(),
      headers: [],
      propRaw: new Uint8Array(),
      persistent: true,
      priority: 0,
      expiration: "",
      confirm: true,
    });
    const deadline = performance.now() + 2000;
    while (store.fullFlushCount === before || store.stagedWithoutFlush) {
      if (performance.now() > deadline) throw new Error("covering fsync did not finish");
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    store.insertMessage("/", "later", body(), "{}");
    expect(store.stagedWithoutFlush).toBe(true);
    const flushes = store.fullFlushCount;
    releasePeer();
    expect(await pending).toBe("ack");
    expect(store.fullFlushCount).toBe(flushes);
    expect(store.stagedWithoutFlush).toBe(true);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a local classic confirm still waits for its covering fsync", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-cwait-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 200);
  const broker = new Broker(cfg("a", []), store);
  broker.cfg = { ...broker.cfg, dataDir: dir };
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
    expect(
      await broker.publish({
        vhost: "/",
        exchange: "",
        routingKey: "q",
        body: body(),
        headers: [],
        propRaw: new Uint8Array(),
        persistent: true,
        priority: 0,
        expiration: "",
        confirm: true,
      }),
    ).toBe("ack");
    // One waiter flushes on the next turn. The ack is that fsync, not the 200 ms interval.
    expect(performance.now() - started).toBeLessThan(50);
    expect(store.fullFlushCount).toBe(1);
    expect(store.confirmsBeforeFsync).toBe(0);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("an ack does not drop a quorum message the claim already dropped", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qack-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50);
  const broker = new Broker(cfg("a", members), store);
  broker.cfg = { ...broker.cfg, dataDir: dir };
  let drops = 0;
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
      peerIds: () => ["b", "c"],
      call: async (_peer: string, op: string) => {
        if (op === "quorum_drop") drops++;
        return true;
      },
    } as unknown as Cluster;
    let delivered = "";
    await broker.consume("/", "q", {
      tag: "c",
      session: 1,
      noAck: false,
      exclusive: false,
      want: () => true,
      deliver: (item) => {
        delivered = item.id;
      },
    });
    expect(
      await broker.publish({
        vhost: "/",
        exchange: "",
        routingKey: "q",
        body: body(),
        headers: [],
        propRaw: new Uint8Array(),
        persistent: false,
        priority: 0,
        expiration: "",
        confirm: true,
      }),
    ).toBe("ack");
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(delivered).not.toBe("");
    expect(drops).toBe(2);
    await broker.ack("/", "q", delivered);
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(drops).toBe(2);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a quorum delivery reads the durable body back from the store", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qbody-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 40);
  const broker = new Broker(cfg("a", []), store);
  broker.cfg = { ...broker.cfg, dataDir: dir };
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
    const payload = new Uint8Array([9, 8, 7]);
    const props = new Uint8Array([4, 5, 6]);
    expect(
      await broker.publish({
        vhost: "/",
        exchange: "",
        routingKey: "q",
        body: payload,
        headers: [],
        propRaw: props,
        persistent: true,
        priority: 0,
        expiration: "",
        confirm: true,
      }),
    ).toBe("ack");
    const q = broker.queues.get(broker.key("/", "q"));
    if (!q) throw new Error("queue missing");
    expect(q.ready.at(0)?.body.byteLength).toBe(0);
    expect(q.ready.at(0)?.propRaw.byteLength).toBe(0);
    expect(q.ready.at(0)?.routingKey).toBe("");
    expect(q.ready.bytes).toBe(payload.byteLength);
    let got = new Uint8Array();
    let gotProps = new Uint8Array();
    let gotKey = "";
    await broker.consume("/", "q", {
      tag: "c",
      session: 1,
      noAck: true,
      exclusive: false,
      want: () => true,
      deliver: (item) => {
        got = item.body;
        gotProps = item.propRaw;
        gotKey = item.routingKey;
      },
    });
    broker.pump(q);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect([...got]).toEqual([9, 8, 7]);
    expect([...gotProps]).toEqual([4, 5, 6]);
    expect(gotKey).toBe("q");
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("consumed ids stay listed and a duplicate does not grow the list", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qseen-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50);
  const broker = new Broker(cfg("a", []), store);
  try {
    for (let i = 0; i < 1000; i++) broker.noteConsumed("/", "q", `id-${i}`);
    broker.noteConsumed("vé", "q", "id-0");
    broker.noteConsumed("/", "q", "id-0");
    const rows = broker.consumed;
    expect(rows).toHaveLength(1001);
    expect(rows[0]).toEqual({ vhost: "/", queue: "q", id: "id-0" });
    expect(rows[999]).toEqual({ vhost: "/", queue: "q", id: "id-999" });
    expect(rows[1000]).toEqual({ vhost: "vé", queue: "q", id: "id-0" });
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("an ack retries a quorum drop a peer missed", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qack2-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50);
  const broker = new Broker(cfg("a", members), store);
  broker.cfg = { ...broker.cfg, dataDir: dir };
  let drops = 0;
  let failNext = true;
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
      peerIds: () => ["b", "c"],
      call: async (_peer: string, op: string) => {
        if (op === "quorum_drop") {
          drops++;
          if (failNext) {
            failNext = false;
            throw new Error("down");
          }
        }
        return true;
      },
    } as unknown as Cluster;
    let delivered = "";
    await broker.consume("/", "q", {
      tag: "c",
      session: 1,
      noAck: false,
      exclusive: false,
      want: () => true,
      deliver: (item) => {
        delivered = item.id;
      },
    });
    expect(
      await broker.publish({
        vhost: "/",
        exchange: "",
        routingKey: "q",
        body: body(),
        headers: [],
        propRaw: new Uint8Array(),
        persistent: false,
        priority: 0,
        expiration: "",
      }),
    ).toBe("ack");
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(delivered).not.toBe("");
    expect(drops).toBe(2);
    await broker.ack("/", "q", delivered);
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(drops).toBe(4);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

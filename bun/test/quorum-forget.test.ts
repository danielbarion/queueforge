import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/index.ts";
import { ConsumedSet } from "../src/broker/consumed-set.ts";
import type { Cluster } from "../src/cluster.ts";
import type { Config } from "../src/config.ts";
import { Store } from "../src/store.ts";

test("consumed ids can be deleted, and the set matches a Set through compaction", () => {
  const set = new ConsumedSet();
  const model = new Set<string>();
  let seed = 7;
  const rand = (n: number) => {
    seed = (Math.imul(seed, 1103515245) + 12345) >>> 0;
    return seed % n;
  };
  for (let step = 0; step < 60_000; step++) {
    const id = `m-${rand(5000)}`;
    const queue = rand(2) ? "q" : "é";
    const key = `${queue}\0${id}`;
    if (rand(3) === 0) {
      expect(set.delete("/", queue, id)).toBe(model.delete(key));
    } else {
      expect(set.add("/", queue, id)).toBe(!model.has(key));
      model.add(key);
    }
  }
  expect(set.size).toBe(model.size);
  const listed = set.list().map((e) => `${e.queue}\0${e.id}`);
  expect(new Set(listed)).toEqual(model);
  expect(listed.length).toBe(model.size);
  for (const key of model) {
    const [queue, id] = key.split("\0") as [string, string];
    expect(set.delete("/", queue, id)).toBe(true);
  }
  expect(set.size).toBe(0);
  expect(set.list()).toEqual([]);
});

function cfg(nodeId: string, members: Config["members"], dataDir: string): Config {
  return {
    amqp: "127.0.0.1:0", management: "127.0.0.1:0", metrics: "127.0.0.1:0",
    mqtt: null, stomp: null, stream: null, dataDir, fsync: "every_n_ms",
    fsyncIntervalMs: 50, fsyncEveryN: 1, tls: false, nodeId, clusterListen: null,
    members, defaultQueueType: "classic",
  };
}

/** A broker `a` in a three-member cluster without Raft; `connected` and `fail` drive the fake peers. */
function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "qf-qforget-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50);
  const members = [{ id: "a", addr: "127.0.0.1:1" }, { id: "b", addr: "127.0.0.1:2" }, { id: "c", addr: "127.0.0.1:3" }];
  const broker = new Broker(cfg("a", members, dir), store);
  const connected = new Set(["b", "c"]);
  const fail = new Set<string>();
  const calls: Array<{ peer: string; payload: Record<string, unknown> }> = [];
  broker.cluster = {
    peerIds: () => [...connected],
    replicate: async () => {},
    call: async (peer: string, op: string, payload: Record<string, unknown>) => {
      expect(op).toBe("quorum_drop");
      calls.push({ peer, payload });
      if (fail.has(peer)) throw new Error("down");
      return true;
    },
  } as unknown as Cluster;
  const ids = () => broker.consumed.map((e) => e.id);
  return { broker, connected, fail, calls, ids, close: () => { store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

test("a drop every member accepted is forgotten; one with a member away is kept until it connects", async () => {
  const f = fixture();
  try {
    f.broker.noteConsumed("/", "q", "all");
    expect(await f.broker.dropOnPeers("/", "q", "all")).toEqual({ ok: true, complete: true });
    expect(f.ids()).toEqual([]);
    // Every member reached: receivers are not asked to keep it.
    expect(f.calls.every((c) => c.payload.keep === false)).toBe(true);

    f.connected.delete("c");
    f.calls.length = 0;
    f.broker.noteConsumed("/", "q", "away");
    expect(await f.broker.dropOnPeers("/", "q", "away")).toEqual({ ok: true, complete: false });
    expect(f.ids()).toEqual(["away"]);
    // `b` keeps it too, so it can hand it on if `a` is gone when `c` is back.
    expect(f.calls).toEqual([{ peer: "b", payload: { vhost: "/", queue: "q", id: "away", keep: true } }]);

    f.fail.add("b");
    f.connected.add("c");
    f.broker.noteConsumed("/", "q", "refused");
    expect(await f.broker.dropOnPeers("/", "q", "refused")).toEqual({ ok: false, complete: false });
    expect(f.ids()).toEqual(["away", "refused"]);

    // A peer connects: the retry pass forgets what every member now accepts.
    f.fail.clear();
    f.broker.retryConsumedDrops();
    await f.broker.dropRetry;
    expect(f.ids()).toEqual([]);
  } finally {
    f.close();
  }
});

test("a member answering quorum_drop keeps the id only when the sender saw a member away", () => {
  const f = fixture();
  try {
    f.broker.dropLocal("/", "q", "plain", false);
    expect(f.ids()).toEqual([]);
    f.broker.dropLocal("/", "q", "kept");
    expect(f.ids()).toEqual(["kept"]);
  } finally {
    f.close();
  }
});

test("a get handed out by the leader skips the leader when it counts every member", async () => {
  const f = fixture();
  try {
    f.connected.delete("b");
    f.broker.noteConsumed("/", "q", "got");
    // `b` led and dropped its copy; `c` is the only other member to ask.
    expect(await f.broker.dropOnPeers("/", "q", "got", ["b"])).toEqual({ ok: true, complete: true });
    expect(f.calls.map((c) => c.peer)).toEqual(["c"]);
    expect(f.ids()).toEqual([]);
  } finally {
    f.close();
  }
});

test("an acked quorum delivery leaves no id when every member dropped it at the claim", async () => {
  const f = fixture();
  try {
    expect(f.broker.isQuorumLeader()).toBe(true);
    await f.broker.declareQueue({ vhost: "/", name: "q", durable: true, exclusive: false, autoDelete: false, passive: false, args: { "x-queue-type": "quorum" } });
    const q = f.broker.queues.get(f.broker.key("/", "q"))!;
    const got: string[] = [];
    await f.broker.consume("/", "q", { tag: "c", session: 1, noAck: false, exclusive: false, want: () => true, deliver: (item) => got.push(item.id) });
    const put = (id: string) =>
      f.broker.enqueueLocal(q, { body: new Uint8Array([1]), exchange: "", routingKey: "q", headers: [], propRaw: new Uint8Array(), persistent: false, priority: 0, expiration: "", id }, 0);
    const delivered = async (n: number) => {
      const deadline = Date.now() + 2000;
      while (got.length < n && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
      expect(got.length).toBe(n);
    };
    put("everyone");
    await delivered(1);
    await f.broker.ack("/", "q", "everyone");
    expect(f.ids()).toEqual([]);

    f.connected.delete("c");
    put("c-away");
    await delivered(2);
    await f.broker.ack("/", "q", "c-away");
    expect(f.ids()).toEqual(["c-away"]);
  } finally {
    f.close();
  }
});

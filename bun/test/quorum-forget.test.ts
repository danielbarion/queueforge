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
  const forgetPeers = new Set(["b", "c"]);
  const calls: Array<{ peer: string; op: string; payload: Record<string, unknown> }> = [];
  let deleteSyncs = 0;
  const syncDeletes = store.syncDeletes.bind(store);
  store.syncDeletes = () => {
    deleteSyncs++;
    syncDeletes();
  };
  broker.cluster = {
    peerIds: () => [...connected],
    replicate: async () => {},
    consensus: { node: null, siblingsOnly: () => false, groupForNewQueue: () => null, forgetPeers },
    call: async (peer: string, op: string, payload: Record<string, unknown>) => {
      calls.push({ peer, op, payload });
      if (fail.has(peer)) throw new Error("down");
      return true;
    },
  } as unknown as Cluster;
  const ids = () => broker.consumed.map((e) => e.id);
  const forget = async () => {
    if (broker.forgetTimer) clearTimeout(broker.forgetTimer);
    broker.forgetTimer = null;
    await broker.runForgetRound();
    if (broker.forgetTimer) clearTimeout(broker.forgetTimer);
    broker.forgetTimer = null;
  };
  return {
    broker, connected, fail, calls, ids, forget, forgetPeers,
    deleteSyncs: () => deleteSyncs,
    close: () => {
      if (broker.forgetTimer) clearTimeout(broker.forgetTimer);
      store.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

test("an id is forgotten only after every member accepted its drop and a forget round made the deletes durable", async () => {
  const f = fixture();
  try {
    f.broker.noteConsumed("/", "q", "all");
    expect(await f.broker.dropOnPeers("/", "q", "all")).toEqual({ ok: true, complete: true });
    // Every member answered, but their deletes may not be on disk yet.
    expect(f.ids()).toEqual(["all"]);
    await f.forget();
    expect(f.ids()).toEqual([]);
    expect(f.deleteSyncs()).toBe(1);
    expect(f.calls.filter((c) => c.op === "quorum_forget").map((c) => c.peer).sort()).toEqual(["b", "c"]);
    expect(f.calls.find((c) => c.op === "quorum_forget")!.payload).toEqual({ items: [{ vhost: "/", queue: "q", id: "all" }] });

    f.connected.delete("c");
    f.calls.length = 0;
    f.broker.noteConsumed("/", "q", "away");
    expect(await f.broker.dropOnPeers("/", "q", "away")).toEqual({ ok: true, complete: false });
    await f.forget();
    expect(f.ids()).toEqual(["away"]);
    expect(f.calls).toEqual([{ peer: "b", op: "quorum_drop", payload: { vhost: "/", queue: "q", id: "away" } }]);

    f.fail.add("b");
    f.connected.add("c");
    f.broker.noteConsumed("/", "q", "refused");
    expect(await f.broker.dropOnPeers("/", "q", "refused")).toEqual({ ok: false, complete: false });
    expect(f.ids()).toEqual(["away", "refused"]);

    // A peer connects: the retry pass completes both drops, the forget round removes them.
    f.fail.clear();
    f.broker.retryConsumedDrops();
    await f.broker.dropRetry;
    expect(f.ids()).toEqual(["away", "refused"]);
    await f.forget();
    expect(f.ids()).toEqual([]);
  } finally {
    f.close();
  }
});

test("a forget round that a member misses keeps the ids for the next one", async () => {
  const f = fixture();
  try {
    f.broker.noteConsumed("/", "q", "x");
    await f.broker.dropOnPeers("/", "q", "x");
    // A member of another build does not answer quorum_forget: nothing is sent.
    f.forgetPeers.delete("c");
    await f.forget();
    expect(f.ids()).toEqual(["x"]);
    expect(f.calls.some((c) => c.op === "quorum_forget")).toBe(false);
    f.forgetPeers.add("c");
    f.fail.add("c");
    await f.forget();
    expect(f.ids()).toEqual(["x"]);
    f.fail.clear();
    await f.forget();
    expect(f.ids()).toEqual([]);
  } finally {
    f.close();
  }
});

test("a member answering quorum_drop keeps the id until a forget arrives", () => {
  const f = fixture();
  try {
    f.broker.dropLocal("/", "q", "theirs");
    expect(f.ids()).toEqual(["theirs"]);
    f.broker.applyForget([{ vhost: "/", queue: "q", id: "theirs" }, { vhost: "/", queue: "q" }]);
    expect(f.ids()).toEqual([]);
    expect(f.deleteSyncs()).toBe(1);
  } finally {
    f.close();
  }
});

test("a get handed out by the leader counts the leader as answered", async () => {
  const f = fixture();
  try {
    f.connected.delete("b");
    f.broker.noteConsumed("/", "q", "got");
    // `b` led and handed it out; `c` is the only other member asked here.
    expect(await f.broker.dropOnPeers("/", "q", "got", ["b"])).toEqual({ ok: true, complete: true });
    expect(f.calls.map((c) => c.peer)).toEqual(["c"]);
  } finally {
    f.close();
  }
});

test("an acked quorum delivery is forgotten once every member dropped it at the claim", async () => {
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
    expect(f.ids()).toEqual(["everyone"]);
    await f.forget();
    expect(f.ids()).toEqual([]);

    f.connected.delete("c");
    put("c-away");
    await delivered(2);
    await f.broker.ack("/", "q", "c-away");
    await f.forget();
    expect(f.ids()).toEqual(["c-away"]);
  } finally {
    f.close();
  }
});

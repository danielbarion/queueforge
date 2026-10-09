import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker, type QueueLive } from "../src/broker/index.ts";
import type { Cluster } from "../src/cluster.ts";
import type { Config } from "../src/config.ts";
import { Consensus } from "../src/raft/glue.ts";
import { META, QUORUM, type RaftNode } from "../src/raft/node.ts";
import { Store } from "../src/store.ts";
import { encodeQuorumAppend } from "../src/wire.ts";

async function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "qf-raft-snapshot-"));
  const store = new Store(join(dir, "bun.sqlite"), "always", 50);
  const cfg: Config = {
    amqp: "127.0.0.1:0", management: "127.0.0.1:0", metrics: "127.0.0.1:0",
    mqtt: null, stomp: null, stream: null, dataDir: dir, fsync: "always",
    fsyncIntervalMs: 50, fsyncEveryN: 1, tls: false, nodeId: "c", clusterListen: null,
    members: [{ id: "a", addr: "127.0.0.1:1" }, { id: "c", addr: "127.0.0.1:2" }], defaultQueueType: "classic",
  };
  const broker = new Broker(cfg, store);
  broker.load();
  const queues: QueueLive[] = [];
  for (const name of ["target", "absent", "other"]) {
    await broker.declareQueue({ vhost: "/", name, durable: true, exclusive: false, autoDelete: false, passive: false, args: { "x-queue-type": "quorum" } });
    queues.push(broker.queues.get(broker.key("/", name))!);
  }
  queues[2]!.raftGroup = "q:other";
  const consensus = new Consensus(broker, () => {});
  let leader = "a";
  consensus.node = { leader: () => leader, dropGroup: () => {}, addGroup: () => {}, expedite: () => {} } as unknown as RaftNode;
  broker.cluster = { consensus, peers: new Map() } as unknown as Cluster;
  const append = (q: QueueLive, id: string) => {
    const src = { body: Buffer.from(id), exchange: "", routingKey: q.name, headers: [], propRaw: new Uint8Array(), persistent: true, priority: 0, expiration: "", id };
    broker.enqueueLocal(q, src, 0);
    const data = encodeQuorumAppend({ ...src, vhost: "/", queue: q.name, messageId: id });
    consensus.live.set(`/\0${q.name}\0${id}`, data);
    return data;
  };
  const install = (state: unknown) => (consensus as unknown as { install(group: string, state: unknown): Promise<void> }).install(QUORUM, state);
  const installMeta = (state: unknown) => (consensus as unknown as { install(group: string, state: unknown): Promise<void> }).install(META, state);
  return { broker, store, cfg, dir, consensus, queues, append, install, installMeta, promote: () => { leader = "c"; broker.promoteIfLeader(); }, close: () => { store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

test("quorum snapshots replace ready, replicas and unacked in leader order", async () => {
  const f = await fixture();
  try {
    const [q, absent, other] = f.queues as [QueueLive, QueueLive, QueueLive];
    const second = f.append(q, "second");
    const first = f.append(q, "first");
    q.ready.push(q.replicas.get("second")!); q.replicas.delete("second");
    q.unacked.set("first", q.replicas.get("first")!); q.replicas.delete("first");
    f.append(q, "removed-replica");
    f.append(q, "removed-ready");
    q.ready.push(q.replicas.get("removed-ready")!); q.replicas.delete("removed-ready");
    f.append(q, "removed-unacked");
    q.unacked.set("removed-unacked", q.replicas.get("removed-unacked")!); q.replicas.delete("removed-unacked");
    f.append(absent, "absent-body");
    const untouched = f.append(other, "untouched");
    await f.install({ queues: [{ vhost: "/", queue: q.name, messages: [first, second, first, untouched] }] });
    expect([...q.replicas.keys()]).toEqual(["first", "second"]);
    expect(q.ready.length).toBe(0); expect(q.unacked.size).toBe(0);
    expect(absent.replicas.size).toBe(0);
    expect([...other.replicas.keys()]).toEqual(["untouched"]);
    expect([...f.consensus.live.keys()]).toEqual(["/\0other\0untouched", "/\0target\0first", "/\0target\0second"]);
    expect(f.store.listMessages().map(row => Buffer.from(row.body).toString())).toEqual(["untouched", "first", "second"]);
    expect(f.broker.consumed.map(m => m.id).sort()).toEqual(["absent-body", "removed-ready", "removed-replica", "removed-unacked"]);
    // Reinstalling is idempotent and does not leave duplicate durable bodies.
    await f.install({ queues: [{ messages: [first, second] }] });
    expect(f.store.listMessages()).toHaveLength(3);
    f.promote();
    expect(q.ready.extract(() => true).map(m => m.id)).toEqual(["first", "second"]);
    expect(absent.ready.length).toBe(0);
  } finally { f.close(); }
});

test("an empty quorum snapshot removes bodies without changing other groups", async () => {
  const f = await fixture();
  try {
    f.append(f.queues[0]!, "gone");
    f.append(f.queues[2]!, "kept");
    await f.install({ queues: [] });
    expect(f.store.listMessages().map(row => Buffer.from(row.body).toString())).toEqual(["kept"]);
    f.promote();
    expect(f.queues[0]!.ready.length).toBe(0);
    expect(f.queues[2]!.ready.at(0)?.id).toBe("kept");
  } finally { f.close(); }
});


test("portable metadata accepts Rust named vhosts and password_hash users", async () => {
  const f = await fixture();
  try {
    f.broker.applySnapshot({ users: [{ name: "portable", password_hash: "wire-hash", tags: ["management"] }], vhosts: [{ name: "tenant" }], permissions: [], queues: [], exchanges: [], bindings: [] } as unknown as ReturnType<Broker["snapshot"]>);
    expect(f.broker.vhosts.has("tenant")).toBe(true);
    expect([...f.broker.vhosts].every((name) => typeof name === "string")).toBe(true);
    expect(f.broker.users.get("portable")).toEqual({ hash: "wire-hash", tags: ["management"] });
    expect(f.store.listUsers().find((user) => user.name === "portable")?.hash).toBe("wire-hash");
  } finally { f.close(); }
});

test("authoritative metadata replaces credentials and topology without losing surviving queue bodies", async () => {
  const f = await fixture();
  try {
    const q = f.queues[0]!;
    f.append(q, "ready");
    const ready = q.replicas.get("ready")!;
    q.replicas.delete("ready"); q.ready.push(ready);
    f.append(q, "unacked");
    const unacked = q.replicas.get("unacked")!;
    q.replicas.delete("unacked"); q.unacked.set("unacked", unacked);
    f.append(f.queues[1]!, "deleted-body");
    const snapshot = f.broker.snapshot();
    snapshot.queues = snapshot.queues.filter(row => row.name === "target");
    snapshot.users = [{ name: "replacement", hash: "new-hash", password_hash: "new-hash", tags: ["administrator"] }];
    snapshot.permissions = [{ user: "replacement", vhost: "/", configure: ".*", write: ".*", read: ".*" }];
    snapshot.exchangeBindings = [{ vhost: "/", source: "amq.direct", destination: "amq.topic", routingKey: "route", routing_key: "route" }];
    await f.installMeta(snapshot);
    await f.installMeta(snapshot);
    expect(f.broker.queues.get(f.broker.key("/", "target"))).toBe(q);
    expect(q.ready.at(0)).toBe(ready);
    expect(q.unacked.get("unacked")).toBe(unacked);
    expect(f.broker.queues.has(f.broker.key("/", "absent"))).toBe(false);
    expect(f.store.listQueues().map(row => row.name)).toEqual(["target"]);
    expect(f.store.listMessages().map(row => Buffer.from(row.body).toString()).sort()).toEqual(["ready", "unacked"]);
    expect([...f.broker.users.keys()]).toEqual(["replacement"]);
    expect(f.store.listUsers().map(row => row.name)).toEqual(["replacement"]);
    expect(f.broker.e2e).toHaveLength(1);
    expect(f.store.listExchangeBindings()).toHaveLength(1);

    // A bad late row must not leave earlier user changes in memory or SQLite.
    const before = JSON.stringify(f.broker.snapshot());
    const invalid = structuredClone(snapshot);
    invalid.users[0]!.hash = "must-not-commit";
    invalid.bindings.push({ vhost: "/", exchange: "amq.direct", queue: "missing", routingKey: "route", routing_key: "route", args: [] });
    await expect(f.installMeta(invalid)).rejects.toThrow();
    expect(JSON.stringify(f.broker.snapshot())).toBe(before);
    expect(f.store.listUsers()[0]!.hash).toBe("new-hash");
    const invalidArgs = structuredClone(snapshot);
    invalidArgs.queues[0]!.args["x-message-ttl"] = -1;
    await expect(f.installMeta(invalidArgs)).rejects.toThrow();
    expect(JSON.stringify(f.broker.snapshot())).toBe(before);

    await f.installMeta({ users: [], vhosts: ["/"], permissions: [], queues: [], exchanges: [], bindings: [] });
    expect(f.store.listQueues()).toHaveLength(0);
    expect(f.store.listMessages()).toHaveLength(0);
    expect(f.store.listUsers()).toHaveLength(0);
    expect(f.store.listExchangeBindings()).toHaveLength(0);
    // Legacy peer hello remains additive; only the Raft installer replaces.
    f.broker.applySnapshot(snapshot);
    f.broker.applySnapshot({ ...snapshot, users: [], vhosts: ["/"], permissions: [], queues: [], exchanges: [], bindings: [], exchangeBindings: [], policies: [] });
    expect(f.broker.queues.has(f.broker.key("/", "target"))).toBe(true);
    expect(f.broker.users.has("replacement")).toBe(true);
  } finally { f.close(); }
});

test("metadata policies retain declared arguments and separate operator policies after restart", async () => {
  const f = await fixture();
  let reopened: Store | undefined;
  try {
    const snapshot = f.broker.snapshot();
    snapshot.queues = [{ ...snapshot.queues[0]!, args: { "x-message-ttl": 123 } }];
    const state = { ...snapshot, policies: [
      { vhost: "/", name: "caps", pattern: ".*", apply_to: "queues", priority: 1, operator: false, definition: { "message-ttl": 321 } },
      { vhost: "/", name: "caps", pattern: ".*", apply_to: "queues", priority: 1, operator: true, definition: { "max-length": 50, "federation-upstream-set": "all" } },
    ] };
    await f.installMeta(state);
    expect(f.broker.snapshot().queues[0]!.args["x-max-length"]).toBeUndefined();
    expect(f.broker.snapshot().policies!.find(row => row.operator)?.definition).toMatchObject({ "federation-upstream-set": "all" });
    expect(f.store.listPolicies()).toHaveLength(1);
    expect(f.store.listParameters("operator-policies")).toHaveLength(1);
    f.store.close();
    reopened = new Store(join(f.dir, "bun.sqlite"), "always", 50);
    const broker = new Broker(f.cfg, reopened);
    broker.load();
    const q = broker.queues.get(broker.key("/", "target"))!;
    expect(q.declaredArgs).toEqual({ "x-message-ttl": 123 });
    expect(q.argsParsed.maxLength).toBe(50);
    expect(broker.policies).toHaveLength(1);
    expect(broker.operatorPolicies).toHaveLength(1);
  } finally { reopened?.close(); f.close(); }
});

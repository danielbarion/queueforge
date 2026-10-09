import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker, type QueueLive } from "../src/broker/index.ts";
import type { Cluster } from "../src/cluster.ts";
import type { Config } from "../src/config.ts";
import { Consensus } from "../src/raft/glue.ts";
import { QUORUM, type RaftNode } from "../src/raft/node.ts";
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
  const queues: QueueLive[] = [];
  for (const name of ["target", "absent", "other"]) {
    await broker.declareQueue({ vhost: "/", name, durable: true, exclusive: false, autoDelete: false, passive: false, args: { "x-queue-type": "quorum" } });
    queues.push(broker.queues.get(broker.key("/", name))!);
  }
  queues[2]!.raftGroup = "q:other";
  const consensus = new Consensus(broker, () => {});
  let leader = "a";
  consensus.node = { leader: () => leader } as unknown as RaftNode;
  broker.cluster = { consensus, peers: new Map() } as unknown as Cluster;
  const append = (q: QueueLive, id: string) => {
    const src = { body: Buffer.from(id), exchange: "", routingKey: q.name, headers: [], propRaw: new Uint8Array(), persistent: true, priority: 0, expiration: "", id };
    broker.enqueueLocal(q, src, 0);
    const data = encodeQuorumAppend({ ...src, vhost: "/", queue: q.name, messageId: id });
    consensus.live.set(`/\0${q.name}\0${id}`, data);
    return data;
  };
  const install = (state: unknown) => (consensus as unknown as { install(group: string, state: unknown): Promise<void> }).install(QUORUM, state);
  return { broker, store, consensus, queues, append, install, promote: () => { leader = "c"; broker.promoteIfLeader(); }, close: () => { store.close(); rmSync(dir, { recursive: true, force: true }); } };
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

import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "./broker.ts";
import { Cluster } from "./cluster.ts";
import type { Config } from "./config.ts";
import { Store } from "./store.ts";
import { encodeQuorumAppend } from "./wire.ts";

/**
 * Drive Cluster.dispatch through its listen socket.
 * A full queue rejects the body; the reply envelope must say so.
 */
test("rejected quorum_append replies ok false", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-qappend-"));
  const probe = Bun.listen({ hostname: "127.0.0.1", port: 0, socket: { data() {} } });
  const port = probe.port;
  probe.stop(true);
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
    clusterListen: `127.0.0.1:${port}`,
    members: [{ id: "bun-a", addr: `127.0.0.1:${port}` }],
    defaultQueueType: "classic",
  };
  const broker = new Broker(cfg, store);
  const cluster = new Cluster(broker);
  broker.cluster = cluster;
  cluster.start();
  try {
    await broker.declareQueue({
      vhost: "/",
      name: "full",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: { "x-queue-type": "quorum", "x-max-length": 0, "x-overflow": "reject-publish" },
    });
    await broker.declareQueue({
      vhost: "/",
      name: "open",
      durable: true,
      exclusive: false,
      autoDelete: false,
      passive: false,
      args: { "x-queue-type": "quorum" },
    });
    const rejected = await append(port, "full", "nope");
    expect(rejected.ok).toBe(false);
    expect(broker.queues.get(broker.key("/", "full"))?.ready.length).toBe(0);
    const stored = await append(port, "open", "kept");
    expect(stored.ok).toBe(true);
    expect(broker.queues.get(broker.key("/", "open"))?.ready.length).toBe(1);
  } finally {
    cluster.stop();
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

function append(port: number, queue: string, body: string): Promise<{ ok?: boolean; id?: number }> {
  const payload = encodeQuorumAppend({
    vhost: "/",
    queue,
    messageId: `m-${queue}`,
    body: new TextEncoder().encode(body),
    exchange: "",
    routingKey: queue,
    persistent: true,
  });
  const line = JSON.stringify({ op: "quorum_append", id: 7, payload });
  return new Promise((resolve, reject) => {
    const sock = connect({ host: "127.0.0.1", port });
    let buf = "";
    const timer = setTimeout(() => {
      sock.destroy();
      reject(new Error("quorum_append reply timed out"));
    }, 2000);
    sock.on("data", (chunk) => {
      buf += chunk.toString();
      const idx = buf.indexOf("\n");
      if (idx < 0) return;
      clearTimeout(timer);
      sock.end();
      resolve(JSON.parse(buf.slice(0, idx)) as { ok?: boolean; id?: number });
    });
    sock.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
    sock.on("connect", () => sock.write(`${line}\n`));
  });
}

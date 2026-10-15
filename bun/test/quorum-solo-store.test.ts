import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, readdirSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

/** Every file named `name` under `dir`. */
function find(dir: string, name: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) out.push(...find(path, name));
    else if (entry === name) out.push(path);
  }
  return out;
}

// On worker processes a quorum queue's group has one voter, and a persistent
// publish skips the Raft log: the queue's store row is its durable copy
// (STORE_ONLY_PREFIX). A kill -9 must keep every confirmed, unacked message
// and none that was acked, even after the group took a snapshot, whose
// install on restart used to rebuild the queue from the log alone.
test("store-only quorum messages survive kill -9 and a group snapshot", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-quorum-solo-"));
  const [portA, portM] = [43360, 43370];
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${portA}"
management = "127.0.0.1:${portM}"
metrics = "127.0.0.1:${portM + 1}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
`,
  );
  // Its own process group, so the kill reaches the supervisor and its workers.
  const start = (): ChildProcess => spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "ignore", "inherit"],
    env: { ...process.env, QUEUEFORGE_CORES: "2" },
    detached: true,
  });
  const url = `amqp://admin:devpassword12@127.0.0.1:${portA}/%2f`;
  const connect = async () => {
    for (let i = 0; i < 150; i++) {
      try {
        return await amqp.connect(url);
      } catch {
        await Bun.sleep(100);
      }
    }
    throw new Error("broker did not start");
  };
  const kill = async (child: ChildProcess, signal: NodeJS.Signals) => {
    const exited = new Promise((resolve) => child.once("exit", resolve));
    try {
      process.kill(-child.pid!, signal);
    } catch {
      /* already gone */
    }
    await exited;
    // The workers are in the same group; give the kernel a moment to reap them.
    await Bun.sleep(300);
  };
  let child = start();
  try {
    const conn = await connect();
    // The kill below closes it under the client.
    conn.on("error", () => {});
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue("solo", { durable: true, arguments: { "x-queue-type": "quorum" } });

    // Transient publishes still commit through the group's log: enough of
    // them to pass the 10,000-entry compaction mark.
    const consumer = await conn.createChannel();
    await consumer.prefetch(500);
    let drained = 0;
    const { consumerTag } = await consumer.consume("solo", (m) => {
      if (!m) return;
      drained++;
      consumer.ack(m);
    });
    const TRANSIENT = 10_050;
    for (let n = 0; n < TRANSIENT; n++) ch.sendToQueue("solo", Buffer.from(`t${n}`), { persistent: false });
    await ch.waitForConfirms();
    for (let t = 0; t < 300 && drained < TRANSIENT; t++) await Bun.sleep(50);
    expect(drained).toBe(TRANSIENT);
    await consumer.cancel(consumerTag);
    let snapshots: string[] = [];
    for (let t = 0; t < 100 && snapshots.length === 0; t++) {
      snapshots = find(join(dir, "data"), "snapshot.json").filter((p) => p.includes("q-") || p.includes("q:"));
      if (!snapshots.length) await Bun.sleep(100);
    }
    expect(snapshots.length).toBeGreaterThan(0);

    // Persistent publishes skip the log. Ack the first half.
    for (let n = 0; n < 200; n++) ch.sendToQueue("solo", Buffer.from(`p${n}`), { persistent: true });
    await ch.waitForConfirms();
    const taker = await conn.createChannel();
    for (const c of [ch, consumer, taker]) c.on("error", () => {});
    for (let n = 0; n < 100; n++) {
      const m = await taker.get("solo", { noAck: false });
      if (!m) throw new Error(`message ${n} missing before the kill`);
      expect(m.content.toString()).toBe(`p${n}`);
      expect(m.fields.redelivered).toBe(false);
      taker.ack(m);
    }
    // The deletes ride the next group commit (10 ms).
    await Bun.sleep(500);
    await kill(child, "SIGKILL");

    child = start();
    const again = await connect();
    const read = await again.createChannel();
    const kept: string[] = [];
    for (let t = 0; t < 200; t++) {
      const m = await read.get("solo", { noAck: true }).catch(() => false);
      if (m && typeof m === "object") kept.push(m.content.toString());
      else if (kept.length >= 100) break;
      else await Bun.sleep(100);
    }
    expect(kept).toEqual(Array.from({ length: 100 }, (_, n) => `p${n + 100}`));
    await again.close();
  } finally {
    await kill(child, "SIGTERM");
  }
}, 120_000);

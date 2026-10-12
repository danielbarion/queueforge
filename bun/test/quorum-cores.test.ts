import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";
import { soloQueueGroup, soloVoter } from "../src/raft/node.ts";

test("a one-voter group names its voter", () => {
  const group = soloQueueGroup("n2", "/", "orders");
  expect(group.startsWith("q:s1:")).toBe(true);
  expect(soloVoter(group)).toBe("n2");
  expect(soloVoter("q:v2:2f:6f7264657273")).toBeNull();
  expect(soloVoter("quorum")).toBeNull();
});

// The workers of one broker share its disk: a quorum queue gets one voter,
// its leader, instead of a Raft group across every sibling. Publishers and
// consumers on the other workers still reach it, and it survives a restart.
test("quorum queues on worker processes have one voter and keep their messages", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-quorum-cores-"));
  const [portA, portM] = [43340, 43350];
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
  const start = (): ChildProcess => spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "ignore", "inherit"],
    env: { ...process.env, QUEUEFORGE_CORES: "3" },
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
  const stop = async (child: ChildProcess) => {
    const exited = new Promise((resolve) => child.once("exit", resolve));
    child.kill("SIGTERM");
    await exited;
  };
  let child = start();
  try {
    const setupConn = await connect();
    const setup = await setupConn.createChannel();
    const QUEUES = 4;
    for (let i = 0; i < QUEUES; i++) await setup.assertQueue(`qq-${i}`, { durable: true, arguments: { "x-queue-type": "quorum" } });
    const info = await fetch(`http://127.0.0.1:${portM}/api/queues/%2f/qq-0`, {
      headers: { authorization: "Basic " + btoa("admin:devpassword12") },
    }).then((r) => r.json()) as { members?: string[]; leader?: string; raft_group?: string };
    expect(info.members).toHaveLength(1);
    expect(info.members![0]).toBe(info.leader!);
    expect(soloVoter(String(info.raft_group))).toBe(info.leader!);
    // Each connection lands on the next worker, so most of these publishers
    // and consumers are on a process that is not the queue's voter.
    const N = 300;
    const got: number[][] = Array.from({ length: QUEUES }, () => []);
    const consumers: amqp.ChannelModel[] = [];
    for (let i = 0; i < QUEUES; i++) {
      const conn = await connect();
      consumers.push(conn);
      const ch = await conn.createChannel();
      await ch.prefetch(50);
      await ch.consume(`qq-${i}`, (m) => {
        if (!m) return;
        got[i]!.push(Number(m.content.toString()));
        ch.ack(m);
      });
    }
    const pubConn = await connect();
    const pub = await pubConn.createConfirmChannel();
    for (let n = 0; n < N; n++) for (let i = 0; i < QUEUES; i++) pub.sendToQueue(`qq-${i}`, Buffer.from(String(n)), { persistent: true });
    await pub.waitForConfirms();
    for (let t = 0; t < 100 && got.some((list) => list.length < N); t++) await Bun.sleep(100);
    for (let i = 0; i < QUEUES; i++) expect(got[i]).toEqual(Array.from({ length: N }, (_, n) => n));
    for (const conn of consumers) await conn.close();
    // Confirmed and unconsumed: kept across a restart of every worker.
    for (let n = 0; n < 50; n++) pub.sendToQueue("qq-1", Buffer.from(`k${n}`), { persistent: true });
    await pub.waitForConfirms();
    await pubConn.close();
    await setup.close();
    await setupConn.close();
    await stop(child);
    expect(readFileSync(join(dir, "data", "broker-id"), "utf8").trim()).not.toBe("");
    child = start();
    const again = await connect();
    const ch = await again.createChannel();
    const kept: string[] = [];
    for (let t = 0; t < 100 && kept.length < 50; t++) {
      const m = await ch.get("qq-1", { noAck: true }).catch(() => false);
      if (m && typeof m === "object") kept.push(m.content.toString());
      else await Bun.sleep(100);
    }
    expect(kept).toEqual(Array.from({ length: 50 }, (_, n) => `k${n}`));
    await again.close();
  } finally {
    // SIGTERM lets the supervisor stop its workers before it exits.
    await stop(child);
  }
}, 60_000);

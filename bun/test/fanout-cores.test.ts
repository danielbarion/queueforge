import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// With one child per core, the queues of one fanout exchange are homed on
// different children. A publish reaches each of them once, in order, and the
// copies bound for one child share a single forward.
test("a fanout across child processes delivers every copy in order", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-fanout-cores-"));
  const [portA, portM] = [43310, 43320];
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
  const child: ChildProcess = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "ignore", "inherit"],
    env: { ...process.env, QUEUEFORGE_CORES: "3" },
  });
  const url = `amqp://admin:devpassword12@127.0.0.1:${portA}/%2f`;
  const QUEUES = 9;
  const N = 200;
  try {
    let setupConn: amqp.ChannelModel | null = null;
    for (let i = 0; i < 150 && !setupConn; i++) {
      try {
        setupConn = await amqp.connect(url);
      } catch {
        await Bun.sleep(100);
      }
    }
    expect(setupConn).not.toBeNull();
    const setup = await setupConn!.createChannel();
    await setup.assertExchange("fan", "fanout", { durable: true });
    for (let i = 0; i < QUEUES; i++) {
      await setup.assertQueue(`fan-${i}`, { durable: true });
      await setup.bindQueue(`fan-${i}`, "fan", "");
    }
    await setup.close();
    await setupConn!.close();

    const got: number[][] = Array.from({ length: QUEUES }, () => []);
    const consumers: amqp.ChannelModel[] = [];
    for (let i = 0; i < QUEUES; i++) {
      const conn = await amqp.connect(url);
      consumers.push(conn);
      const ch = await conn.createChannel();
      await ch.consume(`fan-${i}`, (m) => {
        if (!m) return;
        got[i]!.push(Number(m.content.toString()));
        ch.ack(m);
      });
    }
    const pub = await amqp.connect(url);
    const ch = await pub.createConfirmChannel();
    for (let n = 0; n < N; n++) ch.publish("fan", "", Buffer.from(String(n)), { persistent: true });
    await ch.waitForConfirms();
    for (let i = 0; i < 200 && got.some((g) => g.length < N); i++) await Bun.sleep(50);
    for (const list of got) {
      expect(list.length).toBe(N);
      expect(list).toEqual([...Array(N).keys()]);
    }
    await pub.close();
    for (const conn of consumers) await conn.close();
  } finally {
    child.kill("SIGTERM");
    await new Promise((resolve) => child.once("exit", resolve));
  }
}, 60_000);

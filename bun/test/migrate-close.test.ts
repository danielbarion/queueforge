import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// A consumer connection moves to the child that homes its queue. When its
// socket drops without a connection.close, that child must still requeue the
// unacked deliveries, or they stay unacked until the broker restarts.
test("a dropped consumer on a migrated connection gets its unacked messages requeued", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-migrate-close-"));
  const [portA, portM] = [43410, 43420];
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
  const QUEUES = 6;
  const N = 5;
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
    const setup = await setupConn!.createConfirmChannel();
    for (let q = 0; q < QUEUES; q++) {
      await setup.assertQueue(`mc-${q}`, { durable: true });
      for (let n = 0; n < N; n++) setup.sendToQueue(`mc-${q}`, Buffer.from(String(n)), { persistent: true });
    }
    await setup.waitForConfirms();
    await setupConn!.close();

    // Take every message unacked, then drop the sockets.
    for (let q = 0; q < QUEUES; q++) {
      const conn = await amqp.connect(url);
      conn.on("error", () => {});
      const ch = await conn.createChannel();
      ch.on("error", () => {});
      await ch.prefetch(N);
      let got = 0;
      await new Promise<void>((resolve) => {
        void ch.consume(`mc-${q}`, (m) => {
          if (m && ++got === N) resolve();
        });
      });
      (conn as unknown as { connection: { stream: { destroy(): void } } }).connection.stream.destroy();
    }

    const again = await amqp.connect(url);
    const ch = await again.createChannel();
    const back = Array.from({ length: QUEUES }, () => 0);
    for (let q = 0; q < QUEUES; q++) {
      await ch.consume(`mc-${q}`, (m) => {
        if (!m) return;
        expect(m.fields.redelivered).toBe(true);
        back[q]!++;
        ch.ack(m);
      });
    }
    for (let i = 0; i < 100 && back.some((n) => n < N); i++) await Bun.sleep(50);
    expect(back).toEqual(Array(QUEUES).fill(N));
    await again.close();
  } finally {
    child.kill("SIGTERM");
    await new Promise((resolve) => child.once("exit", resolve));
  }
}, 30_000);

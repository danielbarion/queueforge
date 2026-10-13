import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

const root = join(import.meta.dir, "..");

// The copies of one fanout publish share one log record. A kill -9 after the
// confirms, and after acks of some copies, must restart with every copy that
// was not acked, in each queue, once.
test("fanout copies sharing a log record survive kill -9 with partial acks", async () => {
  const salt = Number(process.hrtime.bigint() % 400n);
  const port = 29150 + salt;
  const mgmt = port + 1000;
  const dir = mkdtempSync(join(tmpdir(), "qf-fan-crash-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${port}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${mgmt + 1}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
`,
  );
  const start = (): ChildProcess =>
    spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], { cwd: root, stdio: "ignore", env: { ...process.env, QUEUEFORGE_CORES: "1" } });
  const url = `amqp://admin:devpassword12@127.0.0.1:${port}/%2f`;
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
  const QUEUES = 6;
  const N = 300;
  let child = start();
  try {
    const conn = await connect();
    conn.on("error", () => {});
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange("fan", "fanout", { durable: true });
    for (let q = 0; q < QUEUES; q++) {
      await ch.assertQueue(`fc-${q}`, { durable: true });
      await ch.bindQueue(`fc-${q}`, "fan", "");
    }
    for (let n = 0; n < N; n++) ch.publish("fan", "", Buffer.from(`m${n}`.padEnd(2048, ".")), { persistent: true });
    await ch.waitForConfirms();
    // Ack the even messages of queue 0 and every message of queue 1.
    const acker = await conn.createChannel();
    for (const q of [0, 1]) {
      let seen = 0;
      await new Promise<void>((resolve) => {
        void acker.consume(`fc-${q}`, (m) => {
          if (!m) return;
          const n = Number(m.content.toString().slice(1).replace(/\.+$/, ""));
          if (q === 1 || n % 2 === 0) acker.ack(m);
          if (++seen === N) resolve();
        });
      });
    }
    // Acks take the next group commit; give it a few intervals.
    await Bun.sleep(300);
    child.kill("SIGKILL");
    await new Promise((resolve) => child.once("exit", resolve));
    // The publishes were written as shared records: "QFML" then type 3.
    const logDir = join(dir, "data", "bun.sqlite.dlog");
    let groups = 0;
    for (const name of readdirSync(logDir)) {
      const bytes = readFileSync(join(logDir, name));
      for (let at = bytes.indexOf("QFML"); at >= 0; at = bytes.indexOf("QFML", at + 4)) if (bytes[at + 4] === 3) groups++;
    }
    expect(groups).toBeGreaterThanOrEqual(N - 10);

    child = start();
    const again = await connect();
    const read = await again.createChannel();
    const expected = (q: number) => (q === 0 ? N / 2 : q === 1 ? 0 : N);
    for (let q = 0; q < QUEUES; q++) {
      const { messageCount } = await read.checkQueue(`fc-${q}`);
      expect(messageCount).toBe(expected(q));
    }
    // Queue 0 kept the odd ones, each once and with its body.
    const odd: number[] = [];
    for (;;) {
      const m = await read.get("fc-0", { noAck: true });
      if (!m) break;
      expect(m.content.length).toBe(2048);
      odd.push(Number(m.content.toString().slice(1).replace(/\.+$/, "")));
    }
    expect(odd).toEqual(Array.from({ length: N / 2 }, (_, i) => 2 * i + 1));
    await again.close();
  } finally {
    child.kill("SIGKILL");
    await new Promise((resolve) => child.once("exit", resolve));
  }
}, 60_000);

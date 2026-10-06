/**
 * A publish burst on one connection must not bury basic.ack.
 * Prefetch is small, confirms stay in flight, and the ready queue stays shallow.
 */
import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

const root = join(import.meta.dir, "..");

test("pipelined publishes leave room for consumer acks", async () => {
  const salt = Number(process.hrtime.bigint() % 500n);
  const amqpPort = 28150 + salt;
  const mgmt = amqpPort + 1000;
  const metrics = mgmt + 1;
  const dir = mkdtempSync(join(tmpdir(), "qf-fan-backlog-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${amqpPort}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${metrics}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
[tls]
enabled = false
`,
  );
  const child: ChildProcess = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: root,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const ready = async () => {
    for (let i = 0; i < 50; i++) {
      try {
        const res = await fetch(`http://127.0.0.1:${mgmt}/readyz`);
        if (res.ok && (await res.text()).includes("ready")) return;
      } catch {
        /* not up */
      }
      await Bun.sleep(100);
    }
    throw new Error("broker did not become ready");
  };
  try {
    await ready();
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    conn.on("error", () => {});
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue("fan-backlog", { durable: true });
    await ch.prefetch(8);
    let got = 0;
    let resolveDone: () => void = () => {};
    const done = new Promise<void>((resolve) => {
      resolveDone = resolve;
    });
    const { consumerTag } = await ch.consume("fan-backlog", (msg) => {
      if (!msg) return;
      ch.ack(msg);
      got++;
      if (got === 2000) resolveDone();
    });
    expect(consumerTag.length).toBeGreaterThan(0);
    let maxReady = 0;
    let sampling = true;
    const sampler = (async () => {
      while (sampling) {
        try {
          const text = await (await fetch(`http://127.0.0.1:${metrics}/metrics`)).text();
          for (const line of text.split("\n")) {
            if (!line.startsWith("rabbitmq_queue_messages_ready{")) continue;
            const n = Number(line.trim().split(/\s+/).pop());
            if (n > maxReady) maxReady = n;
          }
        } catch {
          /* metrics not up yet */
        }
        await Bun.sleep(10);
      }
    })();
    const pending: Array<Promise<void>> = [];
    for (let i = 0; i < 2000; i++) {
      pending.push(
        new Promise((resolve, reject) => {
          ch.sendToQueue("fan-backlog", Buffer.alloc(256, i & 0xff), { persistent: true }, (err) => (err ? reject(err) : resolve()));
        }),
      );
      if (pending.length >= 128) {
        await Promise.race(pending);
        for (let j = pending.length - 1; j >= 0; j--) {
          const settled = await Promise.race([pending[j]!, Promise.resolve("open")]);
          if (settled !== "open") pending.splice(j, 1);
        }
      }
    }
    await Promise.all(pending);
    await done;
    sampling = false;
    await sampler;
    console.log(`fan-backlog consumed=${got} max_ready=${maxReady}`);
    expect(got).toBe(2000);
    expect(maxReady).toBeLessThan(400);
    await conn.close();
  } finally {
    if (child.exitCode == null) child.kill("SIGKILL");
  }
}, 30_000);

import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

/**
 * Publisher confirms of a burst coalesce into `multiple` acks (`sendConfirm`
 * in src/amqp/publish.ts). Every publish must still be confirmed exactly once,
 * including returned mandatory publishes.
 */
async function boot(portA: number, portM: number): Promise<ChildProcess> {
  const dir = mkdtempSync(join(tmpdir(), "qf-bun-confirm-"));
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
[tls]
enabled = false
`,
  );
  const child = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
    env: { ...process.env, QUEUEFORGE_CORES: "1" },
  });
  for (let i = 0; i < 80; i++) {
    try {
      const res = await fetch(`http://127.0.0.1:${portM}/readyz`);
      if (res.ok && (await res.text()).includes("ready")) return child;
    } catch {
      /* not up */
    }
    await Bun.sleep(100);
  }
  child.kill();
  throw new Error("broker did not become ready");
}

test("a confirm burst coalesces into multiple acks and confirms every publish once", async () => {
  const child = await boot(25694, 35694);
  try {
    const conn = await amqp.connect("amqp://admin:devpassword12@127.0.0.1:25694/%2f");
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue("coalesce", { durable: true });
    const acks: Array<{ tag: number; multiple: boolean }> = [];
    ch.on("ack", (f: { deliveryTag: number; multiple: boolean }) => acks.push({ tag: f.deliveryTag, multiple: f.multiple }));
    ch.on("return", () => {});
    const total = 2000;
    const done = new Array<number>(total).fill(0);
    const waits: Promise<void>[] = [];
    for (let i = 0; i < total; i++) {
      waits.push(
        new Promise<void>((resolve, reject) => {
          const cb = (err: unknown) => {
            done[i]!++;
            if (err) reject(err);
            else resolve();
          };
          // Every 97th publish is mandatory and unroutable: basic.return, then its ack.
          if (i % 97 === 0) ch.publish("", "no-such-queue", Buffer.from(`r${i}`), { mandatory: true, persistent: true }, cb);
          else ch.sendToQueue("coalesce", Buffer.from(`m${i}`), { persistent: true }, cb);
        }),
      );
    }
    await Promise.all(waits);
    expect(done.every((n) => n === 1)).toBe(true);
    // Returned publishes are acked at once and durable ones after the fsync,
    // so tags may arrive out of order; a multiple ack never covers a tag that
    // was not settled (each callback above ran once, without an error).
    expect(Math.max(...acks.map((a) => a.tag))).toBe(total);
    expect(acks.some((a) => a.multiple)).toBe(true);
    expect(acks.length).toBeLessThan(total);
    const q = await ch.checkQueue("coalesce");
    expect(q.messageCount).toBe(total - Math.ceil(total / 97));
    await conn.close();
  } finally {
    // The whole group: a supervisor's workers outlive a kill of the supervisor alone.
    try {
      process.kill(-child.pid!, "SIGKILL");
    } catch {
      child.kill("SIGKILL");
    }
  }
}, 30_000);

import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

async function boot(portA: number, portM: number): Promise<ChildProcess> {
  const dir = mkdtempSync(join(tmpdir(), "qf-names-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${portA}"
management = "127.0.0.1:${portM}"
metrics = "127.0.0.1:${portM + 1}"
[data]
dir = "${dir}/data"
`,
  );
  const child = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "ignore", "inherit"],
  });
  for (let i = 0; i < 100; i++) {
    try {
      const res = await fetch(`http://127.0.0.1:${portM}/readyz`);
      if (res.ok) return child;
    } catch {
      /* not up */
    }
    await Bun.sleep(100);
  }
  child.kill();
  throw new Error("broker did not become ready");
}

// basic.publish and basic.ack are parsed in place in the read buffer when
// the frame size matches the names; every name length, ASCII or not, must
// route and settle exactly as through the generic reader.
test("publish names of every length route and their acks settle", async () => {
  const child = await boot(43400, 43410);
  try {
    const conn = await amqp.connect("amqp://admin:devpassword12@127.0.0.1:43400/%2f");
    const ch = await conn.createConfirmChannel();
    const q = (await ch.assertQueue("names", { durable: false, exclusive: true })).queue;
    const keys = [
      ...Array.from({ length: 255 }, (_, i) => "k".repeat(i + 1)),
      "é", "ключ", "键".repeat(85),
    ];
    for (const ex of ["x", "e".repeat(127), "amq.direct"]) {
      if (ex !== "amq.direct") await ch.assertExchange(ex, "direct", { durable: false, autoDelete: true });
      for (const key of keys) await ch.bindQueue(q, ex, key);
      for (const key of keys) ch.publish(ex, key, Buffer.from(key));
      await ch.waitForConfirms();
      for (const key of keys) {
        const m = await ch.get(q, { noAck: false });
        expect(m && m.fields.routingKey).toBe(key);
        expect(m && m.fields.exchange).toBe(ex);
        expect(m && m.content.toString()).toBe(key);
        ch.ack(m as amqp.GetMessage);
      }
    }
    // Every ack landed: nothing is left unacked to come back on close.
    await ch.close();
    const check = await conn.createChannel();
    expect((await check.checkQueue(q)).messageCount).toBe(0);
    await conn.close();
  } finally {
    child.kill();
  }
}, 60_000);

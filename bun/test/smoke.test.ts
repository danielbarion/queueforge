import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

async function boot(portA: number, portM: number): Promise<ChildProcess> {
  const dir = mkdtempSync(join(tmpdir(), "qf-bun-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${portA}"
management = "127.0.0.1:${portM}"
metrics = "127.0.0.1:${portM + 1}"
[data]
dir = "${dir}/data"
fsync_policy = "always"
[tls]
enabled = false
`,
  );
  const child = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "pipe", "pipe"],
  });
  for (let i = 0; i < 50; i++) {
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

test("health, login, and amqp publish/consume", async () => {
  const child = await boot(25672, 35672);
  try {
    const health = await fetch("http://127.0.0.1:35672/healthz");
    expect(await health.text()).toContain("ok");
    const login = await fetch("http://127.0.0.1:35672/api/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username: "admin", password: "devpassword12" }),
    });
    expect(login.status).toBe(200);
    const conn = await amqp.connect("amqp://admin:devpassword12@127.0.0.1:25672/%2f");
    const ch = await conn.createChannel();
    await ch.assertQueue("smoke", { durable: true });
    ch.sendToQueue("smoke", Buffer.from("hello"));
    const msg = await new Promise<amqp.ConsumeMessage | null>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no delivery")), 3000);
      void ch.consume("smoke", (m) => {
        clearTimeout(t);
        resolve(m);
      });
    });
    expect(msg?.content.toString()).toBe("hello");
    if (msg) ch.ack(msg);
    await conn.close();
  } finally {
    child.kill();
  }
});

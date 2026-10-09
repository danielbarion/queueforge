import { expect, test } from "bun:test";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// With several cores and TLS on, the parent still hands every socket to a
// child, and the child runs the handshake. Connections on different children
// publish and consume one queue through the cluster.

test("AMQPS with one child per core", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-tls-cores-"));
  const made = spawnSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", join(dir, "key.pem"), "-out", join(dir, "cert.pem"), "-days", "2", "-subj", "/CN=localhost"]);
  expect(made.status).toBe(0);
  const [portA, portM] = [43210, 43220];
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${portA}"
management = "127.0.0.1:${portM}"
metrics = "127.0.0.1:${portM + 1}"
[data]
dir = "${dir}/data"
[tls]
enabled = true
cert_path = "${dir}/cert.pem"
key_path = "${dir}/key.pem"
`,
  );
  const child: ChildProcess = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "ignore", "inherit"],
    env: { ...process.env, QUEUEFORGE_CORES: "2" },
  });
  const url = `amqps://admin:devpassword12@127.0.0.1:${portA}/%2f`;
  const opts = { rejectUnauthorized: false };
  try {
    let first: amqp.ChannelModel | null = null;
    for (let i = 0; i < 150 && !first; i++) {
      try {
        first = await amqp.connect(url, opts);
      } catch {
        await Bun.sleep(100);
      }
    }
    expect(first).not.toBeNull();
    const setup = await first!.createChannel();
    await setup.assertQueue("tq", { durable: true });
    await setup.close();
    await first!.close();

    // Round robin puts consecutive connections on different children.
    const consumers = await Promise.all([0, 1].map(() => amqp.connect(url, opts)));
    const publishers = await Promise.all([0, 1].map(() => amqp.connect(url, opts)));
    const got = new Set<string>();
    for (const conn of consumers) {
      const ch = await conn.createChannel();
      await ch.consume("tq", (m) => {
        if (m) {
          got.add(m.content.toString());
          ch.ack(m);
        }
      });
    }
    for (const [p, conn] of publishers.entries()) {
      const ch = await conn.createConfirmChannel();
      for (let i = 0; i < 50; i++) ch.sendToQueue("tq", Buffer.from(`${p}-${i}`), { persistent: true });
      await ch.waitForConfirms();
      await ch.close();
    }
    for (let i = 0; i < 100 && got.size < 100; i++) await Bun.sleep(50);
    expect(got.size).toBe(100);
    for (const conn of [...consumers, ...publishers]) await conn.close();
  } finally {
    child.kill("SIGTERM");
    await new Promise((resolve) => child.once("exit", resolve));
  }
}, 60_000);

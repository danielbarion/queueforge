import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// x-queue-leader-locator places a classic queue as RabbitMQ does: client-local on
// the node the client is connected to, balanced on the member with the fewest
// queues. Every member, Bun or Rust, then routes to that home.

const root = join(import.meta.dir, "..");
const rustBin = join(import.meta.dir, "../../rust/target/debug/queueforge");
const AUTH = { authorization: `Basic ${btoa("admin:devpassword12")}` };

function spawnNode(kind: "bun" | "rust", cfg: string): ChildProcess {
  if (kind === "bun") return spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], { cwd: root, stdio: "ignore" });
  return spawn(rustBin, ["--config", cfg, "--dev-bootstrap"], { stdio: "ignore" });
}

async function waitReady(mgmt: number) {
  for (let i = 0; i < 120; i++) {
    try {
      const res = await fetch(`http://127.0.0.1:${mgmt}/readyz`);
      if (res.ok && (await res.text()).includes("ready")) return;
    } catch {
      /* starting */
    }
    await Bun.sleep(50);
  }
  throw new Error(`not ready ${mgmt}`);
}

async function homes(mgmt: number): Promise<Record<string, string>> {
  const res = await fetch(`http://127.0.0.1:${mgmt}/api/queues/%2F`, { headers: AUTH });
  const body = (await res.json()) as { items?: Array<{ name: string; node: string }> } | Array<{ name: string; node: string }>;
  const rows = Array.isArray(body) ? body : (body.items ?? []);
  return Object.fromEntries(rows.map((r) => [r.name, r.node]));
}

async function roundTrip(fromAmqp: number, toAmqp: number, queue: string, body: string) {
  const pub = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${fromAmqp}/%2f`);
  const pch = await pub.createConfirmChannel();
  await new Promise<void>((resolve, reject) => pch.sendToQueue(queue, Buffer.from(body), { persistent: true }, (err) => (err ? reject(err) : resolve())));
  await pch.close();
  await pub.close();
  const sub = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${toAmqp}/%2f`);
  const sch = await sub.createChannel();
  let got: string | null = null;
  for (let i = 0; i < 50 && got == null; i++) {
    const msg = await sch.get(queue, { noAck: true });
    if (msg) got = msg.content.toString();
    else await Bun.sleep(40);
  }
  await sch.close();
  await sub.close();
  return got;
}

const shapes: Array<{ label: string; kinds: Array<"bun" | "rust">; base: number }> = [
  { label: "bun-bun", kinds: ["bun", "bun"], base: 42210 },
  { label: "bun-rust", kinds: ["bun", "rust"], base: 42410 },
];

for (const shape of shapes) {
  test.skipIf(shape.kinds.includes("rust") && !existsSync(rustBin))(`leader locator places classic queues (${shape.label})`, async () => {
    const ids = ["a", "b"];
    const ports = [0, 1].map((i) => ({
      amqp: shape.base + i * 2,
      mgmt: shape.base + 40 + i * 2,
      metrics: shape.base + 41 + i * 2,
      cluster: shape.base + 80 + i * 2,
    }));
    const members = ports.map((p, i) => `{ id = "${ids[i]}", addr = "127.0.0.1:${p.cluster}" }`).join(", ");
    const kids: ChildProcess[] = [];
    try {
      for (let i = 0; i < 2; i++) {
        const dir = mkdtempSync(join(tmpdir(), `qf-locator-${shape.label}-${i}-`));
        const cfg = join(dir, "qf.toml");
        await Bun.write(
          cfg,
          `[listeners]
amqp = "127.0.0.1:${ports[i]!.amqp}"
management = "127.0.0.1:${ports[i]!.mgmt}"
metrics = "127.0.0.1:${ports[i]!.metrics}"
[data]
dir = "${dir}/data"
[tls]
enabled = false
[logging]
level = "warn"
[cluster]
node_id = "${ids[i]}"
listen = "127.0.0.1:${ports[i]!.cluster}"
members = [${members}]
`,
        );
        kids.push(spawnNode(shape.kinds[i]!, cfg));
      }
      for (const p of ports) await waitReady(p.mgmt);
      await Bun.sleep(400);

      const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${ports[0]!.amqp}/%2f`);
      const ch = await conn.createChannel();
      for (let i = 0; i < 6; i++) {
        await ch.assertQueue(`local-${i}`, { durable: true, arguments: { "x-queue-leader-locator": "client-local" } });
      }
      for (let i = 0; i < 3; i++) {
        await ch.assertQueue(`spread-${i}`, { durable: true, arguments: { "x-queue-leader-locator": "balanced" } });
      }
      await ch.close();
      await conn.close();
      await Bun.sleep(300);

      for (const p of ports) {
        const seen = await homes(p.mgmt);
        for (let i = 0; i < 6; i++) expect(seen[`local-${i}`]).toBe("a");
        // a homes six, b none: every balanced queue goes to b.
        for (let i = 0; i < 3; i++) expect(seen[`spread-${i}`]).toBe("b");
      }

      // The second member places its own client-local queue, and a declare on
      // the first member afterwards does not move it.
      const other = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${ports[1]!.amqp}/%2f`);
      const och = await other.createChannel();
      await och.assertQueue("from-b", { durable: true, arguments: { "x-queue-leader-locator": "client-local" } });
      await och.close();
      await other.close();
      await Bun.sleep(300);
      const again = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${ports[0]!.amqp}/%2f`);
      const ach = await again.createChannel();
      await ach.assertQueue("from-b", { durable: true, arguments: { "x-queue-leader-locator": "client-local" } });
      await ach.close();
      await again.close();
      for (const p of ports) expect((await homes(p.mgmt))["from-b"]).toBe("b");

      expect(await roundTrip(ports[1]!.amqp, ports[0]!.amqp, "local-0", "to-a")).toBe("to-a");
      expect(await roundTrip(ports[0]!.amqp, ports[1]!.amqp, "spread-0", "to-b")).toBe("to-b");
      expect(await roundTrip(ports[0]!.amqp, ports[0]!.amqp, "from-b", "via-b")).toBe("via-b");
    } finally {
      for (const kid of kids) kid.kill("SIGKILL");
    }
  }, 60_000);
}

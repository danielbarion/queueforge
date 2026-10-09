import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// At-least-once dead-lettering to a queue homed on another member: the copy is
// kept, across a restart of the source node, until the target's home is back.

const root = join(import.meta.dir, "..");
const URL = (port: number) => `amqp://admin:devpassword12@127.0.0.1:${port}/%2f`;

async function waitReady(mgmt: number) {
  for (let i = 0; i < 160; i++) {
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

async function stop(child: ChildProcess, signal: NodeJS.Signals) {
  if (child.exitCode != null || child.signalCode != null) return;
  const done = new Promise((resolve) => child.once("exit", resolve));
  child.kill(signal);
  await done;
}

test("an at-least-once dead letter waits for a remote home that is down", async () => {
  const ids = ["a", "b"];
  const base = 42610;
  const ports = [0, 1].map((i) => ({ amqp: base + i * 2, mgmt: base + 40 + i * 2, metrics: base + 41 + i * 2, cluster: base + 80 + i * 2 }));
  const members = ports.map((p, i) => `{ id = "${ids[i]}", addr = "127.0.0.1:${p.cluster}" }`).join(", ");
  const cfgs: string[] = [];
  for (let i = 0; i < 2; i++) {
    const dir = mkdtempSync(join(tmpdir(), `qf-dlx-${i}-`));
    const cfg = join(dir, "qf.toml");
    await Bun.write(
      cfg,
      `[listeners]
amqp = "127.0.0.1:${ports[i]!.amqp}"
management = "127.0.0.1:${ports[i]!.mgmt}"
metrics = "127.0.0.1:${ports[i]!.metrics}"
[data]
dir = "${dir}/data"
fsync_policy = "always"
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
    cfgs.push(cfg);
  }
  const start = (i: number) => spawn("bun", ["src/main.ts", "--config", cfgs[i]!, "--dev-bootstrap"], { cwd: root, stdio: "ignore" });
  let a = start(0);
  let b = start(1);
  try {
    for (const p of ports) await waitReady(p.mgmt);
    await Bun.sleep(400);

    const cb = await amqp.connect(URL(ports[1]!.amqp));
    const chb = await cb.createChannel();
    await chb.assertQueue("dead", { durable: true, arguments: { "x-queue-leader-locator": "client-local" } });
    await chb.close();
    await cb.close();

    const ca = await amqp.connect(URL(ports[0]!.amqp));
    const cha = await ca.createConfirmChannel();
    await cha.assertQueue("src", {
      durable: true,
      arguments: {
        "x-queue-leader-locator": "client-local",
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "dead",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
      },
    });
    await Bun.sleep(300);

    await stop(b, "SIGKILL");
    await new Promise<void>((resolve, reject) => cha.sendToQueue("src", Buffer.from("kept"), { persistent: true }, (err) => (err ? reject(err) : resolve())));
    const msg = await cha.get("src", { noAck: false });
    expect(msg && msg.content.toString()).toBe("kept");
    cha.nack(msg as amqp.Message, false, false);
    await Bun.sleep(300);
    await cha.close();
    await ca.close();

    // The source restarts while the target's home is still down.
    await stop(a, "SIGTERM");
    a = start(0);
    await Bun.sleep(1500);
    b = start(1);
    for (const p of ports) await waitReady(p.mgmt);

    const cr = await amqp.connect(URL(ports[1]!.amqp));
    const chr = await cr.createChannel();
    let got: string | null = null;
    for (let i = 0; i < 150 && got == null; i++) {
      const m = await chr.get("dead", { noAck: true });
      if (m) got = m.content.toString();
      else await Bun.sleep(100);
    }
    expect(got).toBe("kept");
    await Bun.sleep(500);
    expect(await chr.get("dead", { noAck: true })).toBe(false);
    await chr.close();
    await cr.close();
  } finally {
    await stop(a, "SIGKILL");
    await stop(b, "SIGKILL");
  }
}, 90_000);

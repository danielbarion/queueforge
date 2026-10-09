import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// The raft feature flag behaves as a RabbitMQ feature flag: a new cluster
// turns it on by itself, an upgraded one waits for an operator, and enabling
// it on one member enables it on every member.

const root = join(import.meta.dir, "..");
const rustBin = join(import.meta.dir, "../../rust/target/debug/queueforge");
const AUTH = { authorization: `Basic ${btoa("admin:devpassword12")}`, "content-type": "application/json" };

function spawnNode(kind: "bun" | "rust", cfg: string, raft: boolean): ChildProcess {
  const env = { ...process.env, QUEUEFORGE_RAFT: raft ? "1" : "0" };
  if (kind === "bun") return spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], { cwd: root, stdio: "ignore", env });
  return spawn(rustBin, ["--config", cfg, "--dev-bootstrap"], { stdio: "ignore", env });
}

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

async function raftState(mgmt: number): Promise<string | undefined> {
  const res = await fetch(`http://127.0.0.1:${mgmt}/api/feature-flags`, { headers: AUTH });
  const rows = (await res.json()) as Array<{ name: string; state: string }>;
  return rows.find((r) => r.name === "raft")?.state;
}

async function eventually(check: () => Promise<boolean>, ms = 8000) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    if (await check()) return true;
    await Bun.sleep(100);
  }
  return false;
}

async function stop(child: ChildProcess) {
  if (child.exitCode != null || child.signalCode != null) return;
  const done = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  await done;
}

/** Publish to the quorum queue `qq` through one member and read it back through another. */
async function roundTrip(fromAmqp: number, toAmqp: number, body: string): Promise<string | null> {
  let got: string | null = null;
  for (let attempt = 0; attempt < 40 && got == null; attempt++) {
    try {
      const pub = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${fromAmqp}/%2f`);
      const pch = await pub.createConfirmChannel();
      await new Promise<void>((resolve, reject) => pch.sendToQueue("qq", Buffer.from(body), { persistent: true }, (err) => (err ? reject(err) : resolve())));
      await pch.close();
      await pub.close();
      const sub = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${toAmqp}/%2f`);
      const sch = await sub.createChannel();
      for (let i = 0; i < 30 && got == null; i++) {
        const m = await sch.get("qq", { noAck: false });
        if (m) {
          got = m.content.toString();
          sch.ack(m);
        } else await Bun.sleep(50);
      }
      await sch.close();
      await sub.close();
    } catch {
      await Bun.sleep(200);
    }
  }
  return got;
}

const shapes: Array<{ label: string; kinds: Array<"bun" | "rust">; base: number }> = [
  { label: "bun-bun", kinds: ["bun", "bun"], base: 42810 },
  { label: "bun-rust", kinds: ["bun", "rust"], base: 43010 },
];

for (const shape of shapes) {
  test.skipIf(shape.kinds.includes("rust") && !existsSync(rustBin))(`raft is a feature flag (${shape.label})`, async () => {
    const ids = ["a", "b"];
    const ports = [0, 1].map((i) => ({ amqp: shape.base + i * 2, mgmt: shape.base + 40 + i * 2, metrics: shape.base + 41 + i * 2, cluster: shape.base + 80 + i * 2 }));
    const members = ports.map((p, i) => `{ id = "${ids[i]}", addr = "127.0.0.1:${p.cluster}" }`).join(", ");
    const config = (dir: string, i: number) => {
      const cfg = join(dir, "qf.toml");
      return Bun.write(
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
      ).then(() => cfg);
    };

    // A new cluster.
    let kids: ChildProcess[] = [];
    try {
      for (let i = 0; i < 2; i++) kids.push(spawnNode(shape.kinds[i]!, await config(mkdtempSync(join(tmpdir(), `qf-ff-new-${i}-`)), i), true));
      for (const p of ports) await waitReady(p.mgmt);
      for (const p of ports) expect(await eventually(async () => (await raftState(p.mgmt)) === "enabled")).toBe(true);
    } finally {
      for (const kid of kids) await stop(kid);
    }

    // A cluster created without Raft, then upgraded.
    const cfgs: string[] = [];
    for (let i = 0; i < 2; i++) cfgs.push(await config(mkdtempSync(join(tmpdir(), `qf-ff-old-${i}-`)), i));
    kids = cfgs.map((cfg, i) => spawnNode(shape.kinds[i]!, cfg, false));
    try {
      for (const p of ports) await waitReady(p.mgmt);
      const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${ports[0]!.amqp}/%2f`);
      const ch = await conn.createChannel();
      await ch.assertQueue("qq", { durable: true, arguments: { "x-queue-type": "quorum" } });
      await ch.close();
      await conn.close();
      for (const kid of kids) await stop(kid);

      // Rolling: the first member is upgraded while the second still runs the old build.
      kids[0] = spawnNode(shape.kinds[0]!, cfgs[0]!, true);
      kids[1] = spawnNode(shape.kinds[1]!, cfgs[1]!, false);
      for (const p of ports) await waitReady(p.mgmt);
      await Bun.sleep(800);
      expect(await raftState(ports[0]!.mgmt)).toBe("disabled");
      const early = await fetch(`http://127.0.0.1:${ports[0]!.mgmt}/api/feature-flags/raft/enable`, { method: "PUT", headers: AUTH, body: "{}" });
      expect(early.status).toBe(400);
      expect(await roundTrip(ports[0]!.amqp, ports[1]!.amqp, "mixed-version")).toBe("mixed-version");
      await stop(kids[1]!);
      kids[1] = spawnNode(shape.kinds[1]!, cfgs[1]!, true);
      for (const p of ports) await waitReady(p.mgmt);
      await Bun.sleep(1500);
      for (const p of ports) expect(await raftState(p.mgmt)).toBe("disabled");

      const disable = await fetch(`http://127.0.0.1:${ports[1]!.mgmt}/api/feature-flags/raft/disable`, { method: "POST", headers: AUTH });
      expect(disable.status).toBe(400);
      const unknown = await fetch(`http://127.0.0.1:${ports[0]!.mgmt}/api/feature-flags/no_such_flag/enable`, { method: "PUT", headers: AUTH, body: "{}" });
      expect(unknown.status).toBe(400);
      const enable = await fetch(`http://127.0.0.1:${ports[0]!.mgmt}/api/feature-flags/raft/enable`, { method: "PUT", headers: AUTH, body: "{}" });
      expect(enable.status).toBe(204);
      for (const p of ports) expect(await eventually(async () => (await raftState(p.mgmt)) === "enabled")).toBe(true);

      // Quorum queues work on Raft from either member.
      expect(await roundTrip(ports[1]!.amqp, ports[0]!.amqp, "on-raft")).toBe("on-raft");
    } finally {
      for (const kid of kids) await stop(kid);
    }
  }, 90_000);
}

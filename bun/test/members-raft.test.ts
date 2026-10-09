import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// With Raft on, a member change commits through the meta log, as RabbitMQ's
// Khepri store does: it needs a majority, every member applies the same list,
// and the Raft voters follow it.

const root = join(import.meta.dir, "..");
const AUTH = { authorization: `Basic ${btoa("admin:devpassword12")}` };

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

async function eventually(check: () => Promise<boolean>, ms = 10_000) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    if (await check().catch(() => false)) return true;
    await Bun.sleep(100);
  }
  return false;
}

async function kill(child: ChildProcess) {
  if (child.exitCode != null || child.signalCode != null) return;
  const done = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGKILL");
  await done;
}

test("forgetting a member commits through Raft and needs a majority", async () => {
  const ids = ["a", "b", "c"];
  const base = 43610;
  const ports = ids.map((_, i) => ({ amqp: base + i * 2, mgmt: base + 40 + i * 2, metrics: base + 41 + i * 2, cluster: base + 80 + i * 2 }));
  const members = ports.map((p, i) => `{ id = "${ids[i]}", addr = "127.0.0.1:${p.cluster}" }`).join(", ");
  const dirs: string[] = [];
  const kids: ChildProcess[] = [];
  try {
    for (let i = 0; i < 3; i++) {
      const dir = mkdtempSync(join(tmpdir(), `qf-members-${i}-`));
      dirs.push(dir);
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
      kids.push(spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], { cwd: root, stdio: "ignore" }));
    }
    for (const p of ports) await waitReady(p.mgmt);
    expect(await eventually(async () => {
      const res = await fetch(`http://127.0.0.1:${ports[0]!.mgmt}/api/feature-flags`, { headers: AUTH });
      return ((await res.json()) as Array<{ name: string; state: string }>).some((f) => f.name === "raft" && f.state === "enabled");
    })).toBe(true);
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${ports[0]!.amqp}/%2f`);
    const ch = await conn.createChannel();
    await ch.assertQueue("qq-members", { durable: true, arguments: { "x-queue-type": "quorum" } });
    await ch.close();
    await conn.close();

    await kill(kids[2]!);
    const forget = await fetch(`http://127.0.0.1:${ports[0]!.mgmt}/api/nodes/c`, { method: "DELETE", headers: AUTH });
    expect(forget.status).toBe(204);

    // b applied the same list from the meta log.
    const metaLog = readFileSync(join(dirs[1]!, "data", "raft", "meta", "log.jsonl"), "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line) as { kind: string; data: unknown });
    const change = metaLog.filter((e) => e.kind === "members").pop();
    expect((change?.data as Array<{ id: string }>).map((m) => m.id).sort()).toEqual(["a", "b"]);
    expect(await eventually(async () => {
      const saved = JSON.parse(readFileSync(join(dirs[1]!, "data", "members.json"), "utf8")) as Array<{ id: string }>;
      return saved.map((m) => m.id).sort().join() === "a,b";
    })).toBe(true);
    // The queue's Raft voters follow the member list.
    expect(await eventually(async () => {
      const res = await fetch(`http://127.0.0.1:${ports[1]!.mgmt}/api/queues/%2F`, { headers: AUTH });
      const row = ((await res.json()) as Array<{ name: string; members?: string[] }>).find((r) => r.name === "qq-members");
      return [...(row?.members ?? [])].sort().join() === "a,b";
    })).toBe(true);

    // a alone is not a majority of a and b: the change is refused.
    await kill(kids[1]!);
    const refused = await fetch(`http://127.0.0.1:${ports[0]!.mgmt}/api/nodes/b`, { method: "DELETE", headers: AUTH });
    expect(refused.status).toBe(503);
    const saved = JSON.parse(readFileSync(join(dirs[0]!, "data", "members.json"), "utf8")) as Array<{ id: string }>;
    expect(saved.map((m) => m.id).sort()).toEqual(["a", "b"]);
  } finally {
    for (const kid of kids) await kill(kid);
  }
}, 90_000);

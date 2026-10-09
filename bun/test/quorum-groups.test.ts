import { expect, test } from "bun:test";
import { queueGroup } from "../src/raft/node.ts";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// One Raft group per quorum queue, as RabbitMQ runs one Ra cluster per queue:
// each queue elects its own leader, and losing one leader fails over only the
// queues it led.

const root = join(import.meta.dir, "..");
const AUTH = { authorization: `Basic ${btoa("admin:devpassword12")}` };
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

type Row = { name: string; leader?: string | null; members?: string[]; raft_group?: string };

async function queueRow(mgmt: number, name: string): Promise<Row | undefined> {
  const res = await fetch(`http://127.0.0.1:${mgmt}/api/queues/%2F`, { headers: AUTH });
  return ((await res.json()) as Row[]).find((r) => r.name === name);
}

async function eventually(check: () => Promise<boolean>, ms = 10_000) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    if (await check().catch(() => false)) return true;
    await Bun.sleep(100);
  }
  return false;
}

async function publish(port: number, queue: string, bodies: string[]) {
  const conn = await amqp.connect(URL(port));
  const ch = await conn.createConfirmChannel();
  for (const body of bodies) ch.sendToQueue(queue, Buffer.from(body), { persistent: true });
  await ch.waitForConfirms();
  await ch.close();
  await conn.close();
}

async function drain(port: number, queue: string, want: number): Promise<string[]> {
  const conn = await amqp.connect(URL(port));
  const ch = await conn.createChannel();
  const got: string[] = [];
  for (let i = 0; i < 200 && got.length < want; i++) {
    const m = await ch.get(queue, { noAck: false });
    if (m) {
      got.push(m.content.toString());
      ch.ack(m);
    } else await Bun.sleep(50);
  }
  await ch.close();
  await conn.close();
  return got;
}

test("each quorum queue has its own Raft group and leader", async () => {
  const ids = ["a", "b", "c"];
  const base = 43410;
  const ports = ids.map((_, i) => ({ amqp: base + i * 2, mgmt: base + 40 + i * 2, metrics: base + 41 + i * 2, cluster: base + 80 + i * 2 }));
  const members = ports.map((p, i) => `{ id = "${ids[i]}", addr = "127.0.0.1:${p.cluster}" }`).join(", ");
  const dirs: string[] = [];
  const kids: ChildProcess[] = [];
  try {
    for (let i = 0; i < 3; i++) {
      const dir = mkdtempSync(join(tmpdir(), `qf-qgroup-${i}-`));
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

    // Declared on a, b and c in turn: client-local makes each its leader.
    for (let i = 0; i < 3; i++) {
      const conn = await amqp.connect(URL(ports[i]!.amqp));
      const ch = await conn.createChannel();
      await ch.assertQueue(`qq-${ids[i]}`, { durable: true, arguments: { "x-queue-type": "quorum" } });
      await ch.close();
      await conn.close();
    }
    for (let i = 0; i < 3; i++) {
      const name = `qq-${ids[i]}`;
      for (const p of ports) {
        expect(await eventually(async () => (await queueRow(p.mgmt, name))?.leader === ids[i])).toBe(true);
      }
      const row = (await queueRow(ports[0]!.mgmt, name))!;
      expect(row.raft_group).toBe(queueGroup("/", name));
      expect([...row.members!].sort()).toEqual(ids);
    }

    // Publish through a follower, read through another follower.
    await publish(ports[1]!.amqp, "qq-a", ["a1", "a2", "a3"]);
    await publish(ports[2]!.amqp, "qq-b", ["b1"]);
    expect(await drain(ports[2]!.amqp, "qq-a", 1)).toEqual(["a1"]);

    // A message delivered and not acked when its leader dies is delivered again.
    await publish(ports[1]!.amqp, "qq-a", ["held"]);
    const holder = await amqp.connect(URL(ports[0]!.amqp));
    const hch = await holder.createChannel();
    let held: string | null = null;
    for (let i = 0; i < 100 && held == null; i++) {
      const m = await hch.get("qq-a", { noAck: false });
      if (m) held = m.content.toString();
      else await Bun.sleep(30);
    }
    expect(held).toBe("a2");
    holder.on("error", () => {});

    // Kill a: only qq-a needs a new leader; qq-b and qq-c keep theirs.
    kids[0]!.kill("SIGKILL");
    await new Promise((resolve) => kids[0]!.once("exit", resolve));
    expect(await eventually(async () => {
      const row = await queueRow(ports[1]!.mgmt, "qq-a");
      return row?.leader === "b" || row?.leader === "c";
    })).toBe(true);
    expect((await queueRow(ports[1]!.mgmt, "qq-b"))?.leader).toBe("b");
    expect((await queueRow(ports[2]!.mgmt, "qq-c"))?.leader).toBe("c");
    let rest: string[] = [];
    expect(await eventually(async () => {
      rest = rest.concat(await drain(ports[1]!.amqp, "qq-a", 3 - rest.length));
      return rest.length >= 3;
    }, 15_000)).toBe(true);
    expect(rest.sort()).toEqual(["a2", "a3", "held"]);
    expect(await drain(ports[2]!.amqp, "qq-b", 1)).toEqual(["b1"]);

    // Deleting a queue stops its group and removes its files.
    const groupDirs = (dir: string) => readdirSync(join(dir, "data", "raft")).filter((d) => d.startsWith("q-"));
    expect(groupDirs(dirs[1]!).length).toBe(3);
    const conn = await amqp.connect(URL(ports[1]!.amqp));
    const ch = await conn.createChannel();
    await ch.deleteQueue("qq-c");
    await ch.close();
    await conn.close();
    expect(await eventually(async () => groupDirs(dirs[1]!).length === 2 && groupDirs(dirs[2]!).length === 2)).toBe(true);
    expect(existsSync(join(dirs[1]!, "data", "raft", "quorum"))).toBe(true);
  } finally {
    for (const kid of kids) kid.kill("SIGKILL");
  }
}, 90_000);

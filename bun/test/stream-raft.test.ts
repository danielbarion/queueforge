import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// A stream replicated as RabbitMQ's are: its own Raft group, confirms after a
// majority commit, the same offsets on every member, reads on any member, and
// a new writer after the leader is lost.

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

async function publish(port: number, bodies: string[]) {
  const conn = await amqp.connect(URL(port));
  const ch = await conn.createConfirmChannel();
  for (const body of bodies) ch.sendToQueue("events", Buffer.from(body), { persistent: true, headers: { tag: body } });
  await ch.waitForConfirms();
  await ch.close();
  await conn.close();
}

/** Read `want` entries from offset `from` on the member at `port`. */
async function read(port: number, from: number, want: number): Promise<Array<{ offset: number; body: string; tag: unknown }>> {
  const conn = await amqp.connect(URL(port));
  const ch = await conn.createChannel();
  await ch.prefetch(500);
  const got: Array<{ offset: number; body: string; tag: unknown }> = [];
  await ch.consume("events", (m) => {
    if (!m) return;
    got.push({ offset: Number(m.properties.headers?.["x-stream-offset"]), body: m.content.toString(), tag: m.properties.headers?.tag });
    ch.ack(m);
  }, { arguments: { "x-stream-offset": from } });
  for (let i = 0; i < 100 && got.length < want; i++) await Bun.sleep(50);
  await ch.close();
  await conn.close();
  return got;
}

test("a stream is replicated with one Raft group", async () => {
  const ids = ["a", "b", "c"];
  const base = 43810;
  const ports = ids.map((_, i) => ({ amqp: base + i * 2, mgmt: base + 40 + i * 2, metrics: base + 41 + i * 2, cluster: base + 80 + i * 2 }));
  const members = ports.map((p, i) => `{ id = "${ids[i]}", addr = "127.0.0.1:${p.cluster}" }`).join(", ");
  const cfgs: string[] = [];
  const kids: ChildProcess[] = [];
  const start = (i: number) => spawn("bun", ["src/main.ts", "--config", cfgs[i]!, "--dev-bootstrap"], { cwd: root, stdio: "ignore" });
  try {
    for (let i = 0; i < 3; i++) {
      const dir = mkdtempSync(join(tmpdir(), `qf-stream-raft-${i}-`));
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
      cfgs.push(cfg);
      kids.push(start(i));
    }
    for (const p of ports) await waitReady(p.mgmt);
    expect(await eventually(async () => {
      const res = await fetch(`http://127.0.0.1:${ports[0]!.mgmt}/api/feature-flags`, { headers: AUTH });
      return ((await res.json()) as Array<{ name: string; state: string }>).some((f) => f.name === "raft" && f.state === "enabled");
    })).toBe(true);

    const conn = await amqp.connect(URL(ports[0]!.amqp));
    const ch = await conn.createChannel();
    await ch.assertQueue("events", { durable: true, arguments: { "x-queue-type": "stream" } });
    await ch.close();
    await conn.close();
    const leaderOf = async (mgmt: number) => {
      const res = await fetch(`http://127.0.0.1:${mgmt}/api/queues/%2F`, { headers: AUTH });
      return ((await res.json()) as Array<{ name: string; leader?: string | null; members?: string[] }>).find((r) => r.name === "events");
    };
    expect(await eventually(async () => (await leaderOf(ports[1]!.mgmt))?.leader === "a")).toBe(true);
    expect([...((await leaderOf(ports[1]!.mgmt))?.members ?? [])].sort()).toEqual(ids);

    // Published through b, read through c: the same offsets, bodies and headers.
    await publish(ports[1]!.amqp, Array.from({ length: 100 }, (_, i) => `m${i}`));
    const onC = await read(ports[2]!.amqp, 0, 100);
    expect(onC.map((e) => e.offset)).toEqual(Array.from({ length: 100 }, (_, i) => i));
    expect(onC.map((e) => e.body)).toEqual(Array.from({ length: 100 }, (_, i) => `m${i}`));
    expect(String(onC[7]!.tag)).toBe("m7");
    expect((await read(ports[0]!.amqp, 95, 5)).map((e) => e.body)).toEqual(["m95", "m96", "m97", "m98", "m99"]);

    // The writer goes: another member takes over and appends continue.
    await kill(kids[0]!);
    expect(await eventually(async () => {
      const leader = (await leaderOf(ports[1]!.mgmt))?.leader;
      return leader === "b" || leader === "c";
    })).toBe(true);
    await publish(ports[2]!.amqp, Array.from({ length: 50 }, (_, i) => `n${i}`));
    const after = await read(ports[1]!.amqp, 100, 50);
    expect(after.map((e) => e.offset)).toEqual(Array.from({ length: 50 }, (_, i) => 100 + i));
    expect(after[0]!.body).toBe("n0");

    // The old writer comes back and catches up from the log.
    kids[0] = start(0);
    await waitReady(ports[0]!.mgmt);
    let all: Array<{ offset: number; body: string }> = [];
    expect(await eventually(async () => {
      all = await read(ports[0]!.amqp, 0, 150);
      return all.length === 150;
    }, 20_000)).toBe(true);
    expect(all[149]!.body).toBe("n49");
    expect(new Set(all.map((e) => e.offset)).size).toBe(150);
  } finally {
    for (const kid of kids) await kill(kid);
  }
}, 120_000);

import { expect, test } from "bun:test";
import { queueGroup } from "../src/raft/node.ts";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// Bun and Rust in one Raft cluster: per-queue groups, a delivery waiting
// for its ack surviving a leader loss, and a replicated stream with the
// same offsets on both implementations.

const root = join(import.meta.dir, "..");
const RUST = join(root, "..", "rust", "target", "debug", "queueforge");
const AUTH = { authorization: `Basic ${btoa("admin:devpassword12")}` };
const URL = (port: number) => `amqp://admin:devpassword12@127.0.0.1:${port}/%2f`;
type Kind = "bun" | "rust";
type Ports = { amqp: number; mgmt: number; metrics: number; cluster: number };

async function waitReady(mgmt: number) {
  for (let i = 0; i < 200; i++) {
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

type Row = { name: string; leader?: string | null; members?: string[]; raft_group?: string };

async function queueRow(mgmt: number, name: string): Promise<Row | undefined> {
  const res = await fetch(`http://127.0.0.1:${mgmt}/api/queues/%2F`, { headers: AUTH });
  return ((await res.json()) as Row[]).find((r) => r.name === name);
}

async function publish(port: number, queue: string, bodies: string[]) {
  const conn = await amqp.connect(URL(port));
  const ch = await conn.createConfirmChannel();
  for (const body of bodies) ch.sendToQueue(queue, Buffer.from(body), { persistent: true, headers: { tag: body }, correlationId: `c-${body}` });
  await ch.waitForConfirms();
  await ch.close();
  await conn.close();
}

async function drain(port: number, queue: string, want: number): Promise<Array<{ body: string; tag: unknown; corr?: string; redelivered: boolean }>> {
  const conn = await amqp.connect(URL(port));
  const ch = await conn.createChannel();
  const got: Array<{ body: string; tag: unknown; corr?: string; redelivered: boolean }> = [];
  for (let i = 0; i < 200 && got.length < want; i++) {
    const m = await ch.get(queue, { noAck: false });
    if (m) {
      got.push({ body: m.content.toString(), tag: m.properties.headers?.tag, corr: m.properties.correlationId, redelivered: m.fields.redelivered });
      ch.ack(m);
    } else await Bun.sleep(50);
  }
  await ch.close();
  await conn.close();
  return got;
}

async function readStream(port: number, from: number, want: number): Promise<Array<{ offset: number; body: string; tag: unknown }>> {
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

/** Start a three-member cluster of `kinds`; returns ports, data dirs and a starter. */
async function cluster(kinds: Kind[], base: number) {
  const ids = ["a", "b", "c"];
  const ports: Ports[] = ids.map((_, i) => ({ amqp: base + i * 2, mgmt: base + 40 + i * 2, metrics: base + 41 + i * 2, cluster: base + 80 + i * 2 }));
  const members = ports.map((p, i) => `{ id = "${ids[i]}", addr = "127.0.0.1:${p.cluster}" }`).join(", ");
  const dirs: string[] = [];
  const cfgs: string[] = [];
  for (let i = 0; i < 3; i++) {
    const dir = mkdtempSync(join(tmpdir(), `qf-mixed-raft-${i}-`));
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
    cfgs.push(cfg);
  }
  const start = (i: number) =>
    kinds[i] === "rust"
      ? spawn(RUST, ["--config", cfgs[i]!, "--dev-bootstrap"], { stdio: "ignore" })
      : spawn("bun", ["src/main.ts", "--config", cfgs[i]!, "--dev-bootstrap"], { cwd: root, stdio: "ignore" });
  const kids = [0, 1, 2].map(start);
  for (const p of ports) await waitReady(p.mgmt);
  for (const p of ports) {
    expect(await eventually(async () => {
      const res = await fetch(`http://127.0.0.1:${p.mgmt}/api/feature-flags`, { headers: AUTH });
      return ((await res.json()) as Array<{ name: string; state: string }>).some((f) => f.name === "raft" && f.state === "enabled");
    }, 15_000)).toBe(true);
  }
  return { ids, ports, dirs, kids, start };
}

async function queueGroups(kinds: Kind[], base: number) {
  const c = await cluster(kinds, base);
  const { ids, ports, dirs, kids } = c;
  try {
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
      for (const p of ports) {
        const row = (await queueRow(p.mgmt, name))!;
        expect(row.raft_group).toBe(queueGroup("/", name));
        expect([...row.members!].sort()).toEqual(ids);
      }
    }

    // Publish through a follower, read through another follower, properties intact.
    await publish(ports[1]!.amqp, "qq-a", ["a1", "a2", "a3"]);
    await publish(ports[2]!.amqp, "qq-b", ["b1"]);
    const first = await drain(ports[2]!.amqp, "qq-a", 1);
    expect(first.map((m) => m.body)).toEqual(["a1"]);
    expect(String(first[0]!.tag)).toBe("a1");
    expect(first[0]!.corr).toBe("c-a1");

    // A message delivered and not acked when its leader dies is delivered again.
    await publish(ports[1]!.amqp, "qq-a", ["held"]);
    const holder = await amqp.connect(URL(ports[0]!.amqp));
    holder.on("error", () => {});
    const hch = await holder.createChannel();
    hch.on("error", () => {});
    let held: string | null = null;
    for (let i = 0; i < 100 && held == null; i++) {
      const m = await hch.get("qq-a", { noAck: false });
      if (m) held = m.content.toString();
      else await Bun.sleep(30);
    }
    expect(held).toBe("a2");

    // Kill a: only qq-a needs a new leader; qq-b and qq-c keep theirs.
    await kill(kids[0]!);
    expect(await eventually(async () => {
      const row = await queueRow(ports[1]!.mgmt, "qq-a");
      return row?.leader === "b" || row?.leader === "c";
    })).toBe(true);
    expect((await queueRow(ports[1]!.mgmt, "qq-b"))?.leader).toBe("b");
    expect((await queueRow(ports[2]!.mgmt, "qq-c"))?.leader).toBe("c");
    let rest: Array<{ body: string; tag: unknown; corr?: string; redelivered: boolean }> = [];
    expect(await eventually(async () => {
      rest = rest.concat(await drain(ports[1]!.amqp, "qq-a", 3 - rest.length));
      return rest.length >= 3;
    }, 15_000)).toBe(true);
    expect(rest.map((m) => m.body).sort()).toEqual(["a2", "a3", "held"]);
    expect(rest.find((m) => m.body === "held")!.corr).toBe("c-held");
    expect((await drain(ports[2]!.amqp, "qq-b", 1)).map((m) => m.body)).toEqual(["b1"]);

    // Deleting a queue stops its group and removes its files on every member.
    const groupDirs = (dir: string) => readdirSync(join(dir, "data", "raft")).filter((d) => d.startsWith("q-"));
    expect(groupDirs(dirs[1]!).length).toBe(3);
    expect(groupDirs(dirs[2]!).length).toBe(3);
    const conn = await amqp.connect(URL(ports[1]!.amqp));
    const ch = await conn.createChannel();
    await ch.deleteQueue("qq-c");
    await ch.close();
    await conn.close();
    expect(await eventually(async () => groupDirs(dirs[1]!).length === 2 && groupDirs(dirs[2]!).length === 2)).toBe(true);
    expect(existsSync(join(dirs[1]!, "data", "raft", "quorum"))).toBe(true);
  } finally {
    for (const kid of kids) await kill(kid);
  }
}

async function replicatedStream(kinds: Kind[], base: number) {
  const c = await cluster(kinds, base);
  const { ids, ports, kids } = c;
  try {
    const conn = await amqp.connect(URL(ports[0]!.amqp));
    const ch = await conn.createChannel();
    await ch.assertQueue("events", { durable: true, arguments: { "x-queue-type": "stream" } });
    await ch.close();
    await conn.close();
    for (const p of ports) {
      expect(await eventually(async () => (await queueRow(p.mgmt, "events"))?.leader === "a")).toBe(true);
      expect([...((await queueRow(p.mgmt, "events"))?.members ?? [])].sort()).toEqual(ids);
    }

    // Published through b, read through c and a: the same offsets, bodies and headers.
    await publish(ports[1]!.amqp, "events", Array.from({ length: 100 }, (_, i) => `m${i}`));
    for (const p of [ports[2]!, ports[0]!]) {
      const got = await readStream(p.amqp, 0, 100);
      expect(got.map((e) => e.offset)).toEqual(Array.from({ length: 100 }, (_, i) => i));
      expect(got.map((e) => e.body)).toEqual(Array.from({ length: 100 }, (_, i) => `m${i}`));
      expect(String(got[7]!.tag)).toBe("m7");
    }

    // The writer goes: another member takes over and appends continue.
    await kill(kids[0]!);
    expect(await eventually(async () => {
      const leader = (await queueRow(ports[1]!.mgmt, "events"))?.leader;
      return leader === "b" || leader === "c";
    })).toBe(true);
    await publish(ports[2]!.amqp, "events", Array.from({ length: 50 }, (_, i) => `n${i}`));
    for (const p of [ports[1]!, ports[2]!]) {
      const after = await readStream(p.amqp, 100, 50);
      expect(after.map((e) => e.offset)).toEqual(Array.from({ length: 50 }, (_, i) => 100 + i));
      expect(after[0]!.body).toBe("n0");
    }

    // The old writer comes back and catches up from the log.
    kids[0] = c.start(0);
    await waitReady(ports[0]!.mgmt);
    let all: Array<{ offset: number; body: string }> = [];
    expect(await eventually(async () => {
      all = await readStream(ports[0]!.amqp, 0, 150);
      return all.length === 150;
    }, 20_000)).toBe(true);
    expect(all[149]!.body).toBe("n49");
    expect(new Set(all.map((e) => e.offset)).size).toBe(150);
  } finally {
    for (const kid of kids) await kill(kid);
  }
}

async function membersAndSettings(kinds: Kind[], base: number) {
  const c = await cluster(kinds, base);
  const { ports, dirs, kids } = c;
  const api = (i: number, path: string, init?: RequestInit) =>
    fetch(`http://127.0.0.1:${ports[i]!.mgmt}${path}`, { ...init, headers: { ...AUTH, "content-type": "application/json" } });
  try {
    // Settings made on one implementation are in force on the other.
    expect((await api(0, "/api/user-limits/admin/max-connections", { method: "PUT", body: JSON.stringify({ value: 50 }) })).status).toBe(204);
    expect((await api(1, "/api/topic-permissions/admin/%2F", { method: "PUT", body: JSON.stringify({ exchange: "amq.topic", write: "^w", read: ".*" }) })).status).toBe(201);
    expect((await api(2, "/api/parameters/my-comp/%2F/p1", { method: "PUT", body: JSON.stringify({ value: { a: 1 } }) })).status).toBe(201);
    expect((await api(1, "/api/global-parameters/g1", { method: "PUT", body: JSON.stringify({ value: "x" }) })).status).toBe(201);
    expect((await api(0, "/api/vhost-limits/%2F/max-queues", { method: "PUT", body: JSON.stringify({ value: 100 }) })).status).toBe(204);
    for (let i = 0; i < 3; i++) {
      expect(await eventually(async () => JSON.stringify(await (await api(i, "/api/user-limits/admin")).json()).includes('"max-connections":50'))).toBe(true);
      expect(await eventually(async () => JSON.stringify(await (await api(i, "/api/vhost-limits/%2F")).json()).includes('"max-queues":100'))).toBe(true);
      expect(await eventually(async () => JSON.stringify(await (await api(i, "/api/topic-permissions")).json()).includes('"write":"^w"'))).toBe(true);
      expect(await eventually(async () => JSON.stringify(await (await api(i, "/api/parameters/my-comp")).json()).includes('"name":"p1"'))).toBe(true);
      expect(await eventually(async () => JSON.stringify(await (await api(i, "/api/global-parameters")).json()).includes('"name":"g1"'))).toBe(true);
    }
    expect((await api(0, "/api/topic-permissions/admin/%2F/amq.topic", { method: "DELETE" })).status).toBe(204);
    expect(await eventually(async () => !JSON.stringify(await (await api(1, "/api/topic-permissions")).json()).includes('"write":"^w"'))).toBe(true);
    const defs = (await (await api(2, "/api/definitions")).json()) as { parameters?: Array<{ name: string }>; global_parameters?: Array<{ name: string }> };
    expect(defs.parameters?.some((p) => p.name === "p1")).toBe(true);
    expect(defs.global_parameters?.some((p) => p.name === "g1")).toBe(true);

    const conn = await amqp.connect(URL(ports[0]!.amqp));
    const ch = await conn.createChannel();
    await ch.assertQueue("qq-members", { durable: true, arguments: { "x-queue-type": "quorum" } });
    await ch.close();
    await conn.close();

    // A membership change commits through the meta log on both implementations.
    await kill(kids[2]!);
    expect((await api(0, "/api/nodes/c", { method: "DELETE" })).status).toBe(204);
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
    expect(await eventually(async () => [...((await queueRow(ports[1]!.mgmt, "qq-members"))?.members ?? [])].sort().join() === "a,b")).toBe(true);

    // a alone is not a majority of a and b: the change is refused.
    await kill(kids[1]!);
    expect((await api(0, "/api/nodes/b", { method: "DELETE" })).status).toBe(503);
    const saved = JSON.parse(readFileSync(join(dirs[0]!, "data", "members.json"), "utf8")) as Array<{ id: string }>;
    expect(saved.map((m) => m.id).sort()).toEqual(["a", "b"]);
  } finally {
    for (const kid of kids) await kill(kid);
  }
}

const haveRust = existsSync(RUST);

test.skipIf(!haveRust)("queue groups: Rust a, Bun b, Rust c", () => queueGroups(["rust", "bun", "rust"], 47010), 120_000);
test.skipIf(!haveRust)("queue groups: Bun a, Rust b, Bun c", () => queueGroups(["bun", "rust", "bun"], 47110), 120_000);
test.skipIf(!haveRust)("replicated stream: Rust writer, Bun and Rust readers", () => replicatedStream(["rust", "bun", "rust"], 47210), 150_000);
test.skipIf(!haveRust)("replicated stream: Bun writer, Rust and Bun readers", () => replicatedStream(["bun", "rust", "bun"], 47310), 150_000);
test.skipIf(!haveRust)("membership and settings: Rust a, Bun b, Rust c", () => membersAndSettings(["rust", "bun", "rust"], 47410), 120_000);
test.skipIf(!haveRust)("membership and settings: Bun a, Rust b, Bun c", () => membersAndSettings(["bun", "rust", "bun"], 47510), 120_000);
// The same shapes on a cluster of Rust members only.
test.skipIf(!haveRust)("queue groups: Rust only", () => queueGroups(["rust", "rust", "rust"], 47610), 120_000);
test.skipIf(!haveRust)("replicated stream: Rust only", () => replicatedStream(["rust", "rust", "rust"], 47910), 150_000);
test.skipIf(!haveRust)("membership and settings: Rust only", () => membersAndSettings(["rust", "rust", "rust"], 48010), 120_000);

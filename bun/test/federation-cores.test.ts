/**
 * Federation links and delayed messages with one worker per core behave as
 * in one process: each queue link runs on the worker that homes its
 * downstream queue, exchange links spread over the workers, the link list
 * shows every worker's links, and delayed messages taken by any worker are
 * held by one owner, delivered once, and survive a restart with a different
 * worker count.
 */
import { afterAll, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

const root = join(import.meta.dir, "..");
const AUTH = `Basic ${btoa("admin:devpassword12")}`;
const running = new Set<ChildProcess>();

afterAll(() => {
  for (const p of running) p.kill("SIGKILL");
});

type Broker = { amqp: number; mgmt: number; cfg: string; proc: ChildProcess | null };

async function makeBroker(base: number): Promise<Broker> {
  const dir = mkdtempSync(join(tmpdir(), "qf-fed-cores-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${base}"
management = "127.0.0.1:${base + 1}"
metrics = "127.0.0.1:${base + 2}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
`,
  );
  return { amqp: base, mgmt: base + 1, cfg, proc: null };
}

async function start(b: Broker, cores: number) {
  const proc = spawn("bun", ["src/main.ts", "--config", b.cfg, "--dev-bootstrap"], {
    cwd: root,
    stdio: ["ignore", "ignore", "inherit"],
    env: { ...process.env, QUEUEFORGE_CORES: String(cores) },
  });
  running.add(proc);
  b.proc = proc;
  for (let i = 0; i < 200; i++) {
    try {
      const who = await fetch(`http://127.0.0.1:${b.mgmt}/api/whoami`, { headers: { authorization: AUTH } });
      if (who.ok) {
        const c = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${b.amqp}/%2f`);
        await c.close();
        return;
      }
    } catch {
      /* not up yet */
    }
    await Bun.sleep(100);
  }
  throw new Error("broker did not start");
}

async function stop(b: Broker) {
  const proc = b.proc;
  if (!proc) return;
  b.proc = null;
  const exited = new Promise((done) => proc.once("exit", done));
  proc.kill("SIGTERM");
  await Promise.race([exited, Bun.sleep(5000).then(() => proc.kill("SIGKILL"))]);
  await exited;
  running.delete(proc);
}

async function conn(b: Broker) {
  const c = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${b.amqp}/%2f`);
  c.on("error", () => {});
  return c;
}

function mgmt(b: Broker, path: string, init: RequestInit = {}) {
  return fetch(`http://127.0.0.1:${b.mgmt}${path}`, {
    ...init,
    headers: { authorization: AUTH, "content-type": "application/json", ...(init.headers ?? {}) },
  });
}

/** Queue rows with the worker that homes each, as /api/queues lists them. */
async function queueNodes(b: Broker): Promise<Array<{ name: string; node: string }>> {
  const j = (await (await mgmt(b, "/api/queues/%2f")).json()) as Array<{ name: string; node: string }> | { items: Array<{ name: string; node: string }> };
  return Array.isArray(j) ? j : j.items;
}

/**
 * Get messages from `queue` until `want` arrived and a quiet spell followed,
 * reconnecting when a get fails while the workers are still meeting each other.
 */
async function collect(b: Broker, queue: string, want: number, ms: number): Promise<string[]> {
  const got: string[] = [];
  const end = Date.now() + ms;
  let quietUntil = 0;
  while (Date.now() < end && (got.length < want || Date.now() < quietUntil)) {
    let c: amqp.ChannelModel | null = null;
    try {
      c = await conn(b);
      const ch = await c.createChannel();
      ch.on("error", () => {});
      while (Date.now() < end && (got.length < want || Date.now() < quietUntil)) {
        const m = await ch.get(queue, { noAck: true });
        if (m) {
          got.push(m.content.toString());
          if (got.length >= want) quietUntil = Date.now() + 800;
        } else await Bun.sleep(50);
      }
    } catch {
      await Bun.sleep(200);
    } finally {
      await c?.close().catch(() => {});
    }
  }
  return got;
}

async function eventually(fn: () => boolean | Promise<boolean>, ms = 8000): Promise<boolean> {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await fn()) return true;
    await Bun.sleep(100);
  }
  return fn();
}

test("cores mode: queue and exchange federation run on the owning workers", async () => {
  const b = await makeBroker(44210);
  await start(b, 3);
  try {
    const c = await conn(b);
    const ch = await c.createChannel();
    const N = 6;
    for (let i = 0; i < N; i++) {
      await ch.assertQueue(`cq-up-${i}`, { durable: true });
      await ch.assertQueue(`cq-down-${i}`, { durable: true });
      await ch.assertExchange(`cx-up-${i}`, "direct", { durable: true });
      await ch.assertExchange(`cx-down-${i}`, "direct", { durable: true });
      await ch.assertQueue(`cx-q-${i}`, { durable: true });
      await ch.bindQueue(`cx-q-${i}`, `cx-down-${i}`, "k");
      for (const kind of ["q", "x"]) {
        const value = kind === "q" ? { uri: "amqp://", queue: `cq-up-${i}` } : { uri: "amqp://", exchange: `cx-up-${i}` };
        expect((await mgmt(b, `/api/parameters/federation-upstream/%2f/c${kind}-up-${i}`, { method: "PUT", body: JSON.stringify({ value }) })).ok).toBe(true);
      }
      const policies = [
        [`cq-${i}`, `^cq-down-${i}$`, "queues", `cq-up-${i}`],
        [`cx-${i}`, `^cx-down-${i}$`, "exchanges", `cx-up-${i}`],
      ];
      for (const [name, pattern, applyTo, upstream] of policies) {
        const body = JSON.stringify({ pattern, "apply-to": applyTo, definition: { "federation-upstream": upstream } });
        expect((await mgmt(b, `/api/policies/%2f/${name}`, { method: "PUT", body })).ok).toBe(true);
      }
    }
    // The downstream queues live on more than one worker, so the links must too.
    const queues = { items: await queueNodes(b) };
    const homes = new Set(queues.items.filter((q) => q.name.startsWith("cq-down-")).map((q) => q.node));
    expect(homes.size).toBeGreaterThan(1);

    type Link = { type: string; status: string; node: string; queue?: string; exchange?: string };
    let links: Link[] = [];
    expect(
      await eventually(async () => {
        links = (await (await mgmt(b, "/api/federation-links")).json()) as Link[];
        return links.filter((l) => l.status === "running").length === 2 * N;
      }),
    ).toBe(true);
    // Each queue link reports the worker that homes its downstream queue.
    for (const q of queues.items.filter((x) => x.name.startsWith("cq-down-"))) {
      expect(links.find((l) => l.type === "queue" && l.queue === q.name)?.node).toBe(q.node);
    }
    expect(new Set(links.filter((l) => l.type === "exchange").map((l) => l.node)).size).toBeGreaterThan(1);
    expect((await (await mgmt(b, "/api/federation-links/%2f")).json() as Link[]).length).toBe(2 * N);

    // Exchange federation: an upstream publish reaches every downstream queue once.
    for (let i = 0; i < N; i++) ch.publish(`cx-up-${i}`, "k", Buffer.from(`x${i}`));
    const fromExchanges: string[] = [];
    expect(
      await eventually(async () => {
        for (let i = 0; i < N; i++) {
          const m = await ch.get(`cx-q-${i}`, { noAck: true });
          if (m) fromExchanges.push(m.content.toString());
        }
        return fromExchanges.length >= N;
      }),
    ).toBe(true);
    expect(fromExchanges.sort()).toEqual([...Array(N).keys()].map((i) => `x${i}`).sort());

    // Queue federation pulls for a consumer on any worker's downstream queue.
    for (let i = 0; i < N; i++) for (let k = 0; k < 3; k++) ch.sendToQueue(`cq-up-${i}`, Buffer.from(`q${i}-${k}`), { persistent: true });
    await Bun.sleep(400);
    // Nothing is pulled while the downstream queues have no consumer.
    for (let i = 0; i < N; i++) expect(await ch.get(`cq-down-${i}`, { noAck: true })).toBe(false);
    const got: string[][] = Array.from({ length: N }, () => []);
    const consumers: amqp.ChannelModel[] = [];
    for (let i = 0; i < N; i++) {
      const cc = await conn(b);
      consumers.push(cc);
      const cch = await cc.createChannel();
      await cch.consume(`cq-down-${i}`, (m) => m && got[i]!.push(m.content.toString()), { noAck: true });
    }
    expect(await eventually(() => got.every((g) => g.length === 3))).toBe(true);
    for (let i = 0; i < N; i++) expect(got[i]).toEqual([0, 1, 2].map((k) => `q${i}-${k}`));
    for (const cc of consumers) await cc.close();
    await c.close();
  } finally {
    await stop(b);
  }
}, 90_000);

test("cores mode: delayed messages from every worker are delivered once and survive a restart with fewer workers", async () => {
  const b = await makeBroker(44310);
  await start(b, 3);
  const N = 8;
  try {
    const c = await conn(b);
    const setup = await c.createChannel();
    await setup.assertExchange("cd-later", "x-delayed-message", { durable: true, arguments: { "x-delayed-type": "direct" } });
    // The destination queue must outlive the third worker: keep one homed on n0 or n1.
    let target = "";
    for (let i = 0; i < 12 && !target; i++) {
      await setup.assertQueue(`cd-q-${i}`, { durable: true });
      const node = (await queueNodes(b)).find((x) => x.name === `cd-q-${i}`)?.node;
      if (node === "n0" || node === "n1") target = `cd-q-${i}`;
      else await setup.deleteQueue(`cd-q-${i}`);
    }
    expect(target).not.toBe("");
    await setup.bindQueue(target, "cd-later", "k");
    await c.close();
    // Several connections, so the publishes land on every worker. A consumer
    // keeps each on the worker the parent handed it to (no move to the
    // destination queue's home), so some are taken by n2, which the restart drops.
    for (let i = 0; i < N; i++) {
      const pc = await conn(b);
      const ch = await pc.createConfirmChannel();
      const own = await ch.assertQueue("", { exclusive: true });
      await ch.consume(own.queue, () => {}, { noAck: true });
      ch.publish("cd-later", "k", Buffer.from(`d${i}`), { persistent: true, headers: { "x-delay": i < N / 2 ? 800 : 4000 } });
      await ch.waitForConfirms();
      await pc.close();
    }
    // The short ones arrive while running, each once.
    expect((await collect(b, target, N / 2, 8000)).sort()).toEqual([0, 1, 2, 3].map((i) => `d${i}`));

    // The long ones are still waiting; restart with two workers instead of three.
    await stop(b);
    await start(b, 2);
    expect((await collect(b, target, N / 2, 12_000)).sort()).toEqual([4, 5, 6, 7].map((i) => `d${i}`));
  } finally {
    await stop(b);
  }
}, 90_000);

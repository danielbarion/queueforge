/**
 * One broker as three workers answers clients as one process does: whichever
 * worker a connection lands on, counts, management, replies, cancels, limits
 * and replicated settings are the broker's, not that worker's.
 *
 * The broker starts with the first internal port already taken, so it also
 * shows the workers moving to free ports instead of failing or sharing one.
 */
import { afterAll, beforeAll, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

const [AMQP, MGMT, METRICS, STOMP] = [43510, 43511, 43512, 43513];
const CORES_PORT = Number(process.env.QUEUEFORGE_CORES_PORT ?? 26300);
const URL = `amqp://admin:devpassword12@127.0.0.1:${AMQP}/%2f`;
const AUTH = `Basic ${btoa("admin:devpassword12")}`;
let broker: ChildProcess | null = null;
let squatter: net.Server | null = null;
const open: amqp.ChannelModel[] = [];

const mgmt = (path: string, init: RequestInit = {}) =>
  fetch(`http://127.0.0.1:${MGMT}${path}`, { ...init, headers: { authorization: AUTH, "content-type": "application/json" } });
const uniq = (p: string) => `${p}-${Math.random().toString(36).slice(2, 8)}`;

async function connect(url = URL) {
  const c = await amqp.connect(url);
  c.on("error", () => {});
  open.push(c);
  return c;
}

async function channel() {
  const ch = await (await connect()).createConfirmChannel();
  ch.on("error", () => {});
  return ch;
}

/** One connection per worker and then some: the parent hands them out in turn. */
async function channels(n = 4) {
  const out: amqp.ConfirmChannel[] = [];
  for (let i = 0; i < n; i++) out.push(await channel());
  return out;
}

async function until(check: () => boolean | Promise<boolean>, ms = 3000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await check()) return true;
    await Bun.sleep(20);
  }
  return check();
}

async function getOne(ch: amqp.Channel, q: string, ms = 2000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const m = await ch.get(q, { noAck: true });
    if (m) return m;
    await Bun.sleep(20);
  }
  return null;
}

beforeAll(async () => {
  squatter = net.createServer();
  await new Promise<void>((done) => squatter!.listen(CORES_PORT, "127.0.0.1", done));
  const dir = mkdtempSync(join(tmpdir(), "qf-cores-parity-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${AMQP}"
management = "127.0.0.1:${MGMT}"
metrics = "127.0.0.1:${METRICS}"
stomp = "127.0.0.1:${STOMP}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
`,
  );
  broker = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "ignore", "ignore"],
    env: { ...process.env, QUEUEFORGE_CORES: "3", QUEUEFORGE_CORES_PORT: String(CORES_PORT) },
  });
  const up = await until(async () => {
    try {
      const who = await mgmt("/api/whoami");
      if (!who.ok) return false;
      const c = await amqp.connect(URL);
      await c.close();
      return true;
    } catch {
      return false;
    }
  }, 30_000);
  if (!up) throw new Error("broker did not start");
}, 40_000);

afterAll(async () => {
  for (const c of open) await c.close().catch(() => {});
  broker?.kill("SIGTERM");
  await Bun.sleep(300);
  broker?.kill("SIGKILL");
  squatter?.close();
});

test("a passive declare, the queue list and the queue page show the home's counts", async () => {
  const [pub] = await channels(1);
  const q = uniq("counts");
  await pub!.assertQueue(q, { durable: true });
  for (let i = 0; i < 3; i++) pub!.sendToQueue(q, Buffer.from(String(i)));
  await pub!.waitForConfirms();
  for (const ch of await channels(4)) expect((await ch.checkQueue(q)).messageCount).toBe(3);
  const all = (await (await mgmt("/api/queues")).json()) as Array<{ name: string; messages: number }> | { items: Array<{ name: string; messages: number }> };
  const rows = Array.isArray(all) ? all : all.items;
  expect(rows.find((r) => r.name === q)?.messages).toBe(3);
  const inVhost = (await (await mgmt("/api/queues/%2f")).json()) as Array<{ name: string; messages: number }> | { items: Array<{ name: string; messages: number }> };
  expect((Array.isArray(inVhost) ? inVhost : inVhost.items).find((r) => r.name === q)?.messages).toBe(3);
  const page = (await (await mgmt(`/api/queues/%2f/${q}`)).json()) as { messages_ready: number };
  expect(page.messages_ready).toBe(3);
});

test("if-empty is checked against the home's messages", async () => {
  const [pub, other] = await channels(2);
  const q = uniq("ifempty");
  await pub!.assertQueue(q, { durable: true });
  pub!.sendToQueue(q, Buffer.from("x"));
  await pub!.waitForConfirms();
  let code = 0;
  other!.once("error", (err: { code?: number }) => (code = err.code ?? -1));
  await other!.deleteQueue(q, { ifEmpty: true }).catch(() => {});
  expect(code).toBe(406);
});

test("a basic.get on another worker keeps an x-expires queue alive", async () => {
  const [ch] = await channels(1);
  const q = uniq("expires");
  await ch!.assertQueue(q, { durable: true, arguments: { "x-expires": 600 } });
  for (let i = 0; i < 4; i++) {
    await Bun.sleep(300);
    for (const other of await channels(1)) await other.get(q, { noAck: true });
  }
  expect((await ch!.checkQueue(q)).queue).toBe(q);
});

test("a direct reply reaches a requester on another worker", async () => {
  const [requester, server] = await channels(2);
  const service = uniq("rpc");
  await requester!.assertQueue(service, { durable: true });
  const replies: string[] = [];
  await requester!.consume("amq.rabbitmq.reply-to", (m) => m && replies.push(m.content.toString()), { noAck: true });
  requester!.sendToQueue(service, Buffer.from("ping"), { replyTo: "amq.rabbitmq.reply-to" });
  const req = await getOne(server!, service);
  expect(req?.properties.replyTo).toStartWith("amq.rabbitmq.reply-to.");
  server!.sendToQueue(req!.properties.replyTo, Buffer.from("pong"));
  expect(await until(() => replies.length === 1)).toBe(true);
  expect(replies).toEqual(["pong"]);
});

test("deleting a queue from another worker cancels its consumers", async () => {
  const [consumer, other] = await channels(2);
  const q = uniq("cancel");
  await consumer!.assertQueue(q, { durable: true });
  let cancelled = false;
  await consumer!.consume(q, (m) => {
    if (m === null) cancelled = true;
  });
  await other!.deleteQueue(q);
  expect(await until(() => cancelled)).toBe(true);
});

test("a user's max-connections counts connections on every worker", async () => {
  const user = uniq("limited");
  expect((await mgmt(`/api/users/${user}`, { method: "PUT", body: JSON.stringify({ password: "limited-pw-123", tags: "" }) })).ok).toBe(true);
  await mgmt(`/api/permissions/%2f/${user}`, { method: "PUT", body: JSON.stringify({ configure: ".*", write: ".*", read: ".*" }) });
  expect((await mgmt(`/api/user-limits/${user}/max-connections`, { method: "PUT", body: JSON.stringify({ value: 1 }) })).ok).toBe(true);
  const url = `amqp://${user}:limited-pw-123@127.0.0.1:${AMQP}/%2f`;
  await connect(url);
  // Every later one lands on some worker; none may open.
  for (let i = 0; i < 3; i++) {
    let refused = false;
    try {
      const c = await connect(url);
      await c.createChannel();
    } catch {
      refused = true;
    }
    expect(refused).toBe(true);
  }
});

test("management lists connections from every worker", async () => {
  const before = ((await (await mgmt("/api/connections")).json()) as unknown[] | { items: unknown[] });
  const count = (x: unknown[] | { items: unknown[] }) => (Array.isArray(x) ? x.length : x.items.length);
  await channels(3);
  const after = (await (await mgmt("/api/connections")).json()) as unknown[] | { items: unknown[] };
  expect(count(after)).toBe(count(before) + 3);
});

test("exchange-to-exchange bindings, operator policies and tracing reach every worker", async () => {
  const [setup] = await channels(1);
  const front = uniq("front");
  const inner = uniq("inner");
  const q = uniq("inner-q");
  await setup!.assertExchange(front, "fanout", { durable: false });
  await setup!.assertExchange(inner, "fanout", { durable: false, internal: true });
  await setup!.assertQueue(q, { durable: true });
  await setup!.bindExchange(inner, front, "");
  await setup!.bindQueue(q, inner, "");
  for (const ch of await channels(3)) {
    ch.publish(front, "", Buffer.from("through"));
    await ch.waitForConfirms();
  }
  for (let i = 0; i < 3; i++) expect((await getOne(setup!, q))?.content.toString()).toBe("through");

  const prefix = uniq("opol");
  const name = uniq("op");
  expect((await mgmt(`/api/operator-policies/%2f/${name}`, { method: "PUT", body: JSON.stringify({ pattern: `^${prefix}`, definition: { "max-length": 1 }, "apply-to": "queues" }) })).ok).toBe(true);
  for (const ch of await channels(3)) {
    const capped = `${prefix}-${uniq("q")}`;
    await ch.assertQueue(capped, { durable: true, arguments: { "x-max-length": 10 } });
    for (const v of ["a", "b", "c"]) ch.sendToQueue(capped, Buffer.from(v));
    await ch.waitForConfirms();
    expect((await ch.checkQueue(capped)).messageCount).toBe(1);
  }
  await mgmt(`/api/operator-policies/%2f/${name}`, { method: "DELETE" });

  expect((await mgmt("/api/vhosts/%2f", { method: "PUT", body: JSON.stringify({ tracing: true }) })).ok).toBe(true);
  try {
    const [watcher] = await channels(1);
    const trace = uniq("trace");
    // Exclusive: its connection publishes below and must not leave this worker.
    await watcher!.assertQueue(trace, { durable: true, exclusive: true });
    await watcher!.bindQueue(trace, "amq.rabbitmq.trace", "publish.#");
    const traced = uniq("traced");
    await watcher!.assertQueue(traced, { durable: true });
    watcher!.sendToQueue(traced, Buffer.from("watched"));
    for (const ch of await channels(2)) ch.sendToQueue(traced, Buffer.from("watched"));
    let copies = 0;
    await until(async () => {
      while (await watcher!.get(trace, { noAck: true })) copies++;
      return copies >= 3;
    });
    expect(copies).toBe(3);
  } finally {
    await mgmt("/api/vhosts/%2f", { method: "PUT", body: JSON.stringify({ tracing: false }) });
  }
});

test("a deleted vhost or policy is gone from the next listing", async () => {
  for (let i = 0; i < 5; i++) {
    const vhost = uniq("vh");
    expect((await mgmt(`/api/vhosts/${vhost}`, { method: "PUT", body: "{}" })).ok).toBe(true);
    expect((await mgmt(`/api/vhosts/${vhost}`, { method: "DELETE" })).ok).toBe(true);
    const list = (await (await mgmt("/api/vhosts")).json()) as Array<{ name: string }> | { items: Array<{ name: string }> };
    expect((Array.isArray(list) ? list : list.items).some((v) => v.name === vhost)).toBe(false);
    const pol = uniq("pol");
    await mgmt(`/api/policies/%2f/${pol}`, { method: "PUT", body: JSON.stringify({ pattern: "^nothing$", definition: { "max-length": 5 }, "apply-to": "queues" }) });
    await mgmt(`/api/policies/%2f/${pol}`, { method: "DELETE" });
    const pols = (await (await mgmt("/api/policies/%2f")).json()) as Array<{ name: string }> | { items: Array<{ name: string }> };
    expect((Array.isArray(pols) ? pols : pols.items).some((p) => p.name === pol)).toBe(false);
  }
});

test("a STOMP subscriber gets a SEND's headers from a queue homed on another worker", async () => {
  // STOMP is served by n0: use a queue homed on another worker.
  const [ch] = await channels(1);
  let q = "";
  for (let i = 0; i < 10 && !q; i++) {
    const name = uniq("stq");
    await ch!.assertQueue(name, { durable: true });
    const page = (await (await mgmt(`/api/queues/%2f/${name}`)).json()) as { node: string };
    if (page.node !== "n0") q = name;
  }
  expect(q).not.toBe("");
  const sock = net.connect(STOMP, "127.0.0.1");
  let text = "";
  sock.on("data", (d) => (text += d.toString()));
  const frame = (cmd: string, headers: Record<string, string>, body = "") =>
    sock.write(`${cmd}\n${Object.entries(headers).map(([k, v]) => `${k}:${v}`).join("\n")}\n\n${body}\0`);
  frame("CONNECT", { "accept-version": "1.2", host: "/", login: "admin", passcode: "devpassword12" });
  expect(await until(() => text.includes("CONNECTED"))).toBe(true);
  frame("SUBSCRIBE", { id: "1", destination: `/queue/${q}`, ack: "auto" });
  await Bun.sleep(200);
  frame("SEND", { destination: `/queue/${q}`, "content-type": "text/plain", "x-custom": "yes" }, "hello");
  expect(await until(() => text.includes("MESSAGE"))).toBe(true);
  expect(await until(() => text.includes("hello"))).toBe(true);
  expect(text).toContain("x-custom:yes");
  expect(text).toContain("subscription:1");
  sock.destroy();
});

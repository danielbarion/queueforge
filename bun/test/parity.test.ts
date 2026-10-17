/**
 * RabbitMQ 4.3 parity features that need a running broker: what must survive
 * a restart (delayed messages, federation links, shovels, MQTT retained
 * messages) and the AMQP-side behaviour (acknowledgement timeouts, stream
 * filters, consistent-hash header and property hashing, update-secret).
 */
import { afterAll, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { connect as netConnect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

const root = join(import.meta.dir, "..");
const AUTH = `Basic ${btoa("admin:devpassword12")}`;
const running = new Set<ChildProcess>();

afterAll(() => {
  for (const p of running) p.kill("SIGKILL");
});

type Broker = { amqp: number; mgmt: number; mqtt: number; dir: string; cfg: string; proc: ChildProcess | null };

async function makeBroker(base: number, extra = ""): Promise<Broker> {
  const dir = mkdtempSync(join(tmpdir(), "qf-parity-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `${extra}
[listeners]
amqp = "127.0.0.1:${base}"
management = "127.0.0.1:${base + 1}"
metrics = "127.0.0.1:${base + 2}"
mqtt = "127.0.0.1:${base + 3}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
[tls]
enabled = false
`,
  );
  return { amqp: base, mgmt: base + 1, mqtt: base + 3, dir, cfg, proc: null };
}

async function start(b: Broker) {
  const proc = spawn("bun", ["src/main.ts", "--config", b.cfg, "--dev-bootstrap"], { cwd: root, stdio: ["ignore", "ignore", "inherit"] });
  running.add(proc);
  b.proc = proc;
  for (let i = 0; i < 100; i++) {
    try {
      const res = await fetch(`http://127.0.0.1:${b.mgmt + 1}/readyz`);
      if (res.ok) {
        // The management listener comes up next to metrics.
        const who = await fetch(`http://127.0.0.1:${b.mgmt}/api/whoami`, { headers: { authorization: AUTH } }).catch(() => null);
        if (who?.ok) return;
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
  await Promise.race([exited, Bun.sleep(3000).then(() => proc.kill("SIGKILL"))]);
  await exited;
  running.delete(proc);
}

async function restart(b: Broker) {
  await stop(b);
  await start(b);
}

async function conn(b: Broker, opts: Record<string, unknown> = {}) {
  const c = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${b.amqp}/%2f`, opts);
  c.on("error", () => {});
  return c;
}

function mgmt(b: Broker, path: string, init: RequestInit = {}) {
  return fetch(`http://127.0.0.1:${b.mgmt}${path}`, {
    ...init,
    headers: { authorization: AUTH, "content-type": "application/json", ...(init.headers ?? {}) },
  });
}

async function eventually(fn: () => boolean | Promise<boolean>, ms = 5000): Promise<boolean> {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await fn()) return true;
    await Bun.sleep(50);
  }
  return fn();
}

async function drain(ch: amqp.Channel, q: string, ms = 1500): Promise<string[]> {
  const out: string[] = [];
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const m = await ch.get(q, { noAck: true });
    if (m) out.push(m.content.toString());
    else await Bun.sleep(50);
  }
  return out;
}

async function closeCode(p: Promise<unknown>): Promise<number | null> {
  try {
    await p;
    return null;
  } catch (err) {
    return Number((err as { code?: number }).code ?? -1);
  }
}

test("a delayed message still waiting at shutdown is delivered after the restart", async () => {
  const b = await makeBroker(38100);
  await start(b);
  try {
    let c = await conn(b);
    let ch = await c.createConfirmChannel();
    await ch.assertExchange("later", "x-delayed-message", { durable: true, arguments: { "x-delayed-type": "direct" } });
    await ch.assertQueue("later-q", { durable: true });
    await ch.bindQueue("later-q", "later", "k");
    ch.publish("later", "k", Buffer.from("after-restart"), { persistent: true, headers: { "x-delay": 1500 } });
    await ch.waitForConfirms();
    await c.close();
    await restart(b);
    c = await conn(b);
    ch = await c.createConfirmChannel();
    expect(await ch.get("later-q", { noAck: true })).toBe(false);
    const got = await drain(ch, "later-q", 3000);
    expect(got).toEqual(["after-restart"]);
    await c.close();
  } finally {
    await stop(b);
  }
}, 30000);

test("federation upstreams, policies and links are stored and come back after a restart", async () => {
  const b = await makeBroker(38110);
  await start(b);
  try {
    let c = await conn(b);
    let ch = await c.createChannel();
    await ch.assertExchange("fx-up", "topic", { durable: true });
    await ch.assertExchange("fx-down", "topic", { durable: true });
    await ch.assertQueue("fx-q", { durable: true });
    await ch.bindQueue("fx-q", "fx-down", "orders.*");
    await ch.assertQueue("fq-up", { durable: true });
    await ch.assertQueue("fq-down", { durable: true });
    expect((await mgmt(b, "/api/parameters/federation-upstream/%2f/up-x", { method: "PUT", body: JSON.stringify({ value: { uri: "amqp://", exchange: "fx-up" } }) })).ok).toBe(true);
    expect((await mgmt(b, "/api/parameters/federation-upstream/%2f/up-q", { method: "PUT", body: JSON.stringify({ value: { uri: "amqp://", queue: "fq-up" } }) })).ok).toBe(true);
    expect((await mgmt(b, "/api/parameters/federation-upstream-set/%2f/both", { method: "PUT", body: JSON.stringify({ value: [{ upstream: "up-x" }] }) })).ok).toBe(true);
    expect((await mgmt(b, "/api/policies/%2f/fed-x", { method: "PUT", body: JSON.stringify({ pattern: "^fx-down$", "apply-to": "exchanges", definition: { "federation-upstream-set": "both" } }) })).ok).toBe(true);
    expect((await mgmt(b, "/api/policies/%2f/fed-q", { method: "PUT", body: JSON.stringify({ pattern: "^fq-down$", "apply-to": "queues", definition: { "federation-upstream": "up-q" } }) })).ok).toBe(true);

    const linksRunning = () =>
      eventually(async () => {
        const links = (await (await mgmt(b, "/api/federation-links")).json()) as Array<{ type: string; status: string; upstream: string; exchange?: string; queue?: string; uri: string }>;
        return links.some((l) => l.type === "exchange" && l.exchange === "fx-down" && l.upstream === "up-x" && l.status === "running")
          && links.some((l) => l.type === "queue" && l.queue === "fq-down" && l.upstream === "up-q" && l.status === "running");
      });
    const exchangeFlows = async (body: string) => {
      ch.publish("fx-up", "orders.new", Buffer.from(body));
      ch.publish("fx-up", "other.key", Buffer.from("unbound"));
      return drain(ch, "fx-q", 800);
    };
    expect(await linksRunning()).toBe(true);
    expect(await exchangeFlows("before")).toEqual(["before"]);
    // A new downstream binding is followed upstream.
    await ch.bindQueue("fx-q", "fx-down", "other.*");
    await Bun.sleep(200);
    ch.publish("fx-up", "other.key", Buffer.from("now-bound"));
    expect(await drain(ch, "fx-q", 800)).toEqual(["now-bound"]);
    await ch.unbindQueue("fx-q", "fx-down", "other.*");
    await c.close();

    await restart(b);
    c = await conn(b);
    ch = await c.createChannel();
    expect(await linksRunning()).toBe(true);
    expect(await exchangeFlows("after")).toEqual(["after"]);
    // Queue federation pulls only while the downstream queue has a consumer.
    ch.sendToQueue("fq-up", Buffer.from("pulled"), { persistent: true });
    await Bun.sleep(400);
    expect((await ch.checkQueue("fq-up")).messageCount).toBe(1);
    const got: string[] = [];
    const cc = await c.createChannel();
    await cc.consume("fq-down", (m) => m && got.push(m.content.toString()), { noAck: true });
    expect(await eventually(() => got.length === 1)).toBe(true);
    expect(got).toEqual(["pulled"]);
    // The x-received-from hop list travels with a federated copy.
    const hop = await c.createChannel();
    await hop.assertQueue("fx-hop", { durable: true });
    await hop.bindQueue("fx-hop", "fx-down", "orders.hop");
    await Bun.sleep(200);
    hop.publish("fx-up", "orders.hop", Buffer.from("h"));
    let seen: amqp.GetMessage | false = false;
    await eventually(async () => !!(seen = await hop.get("fx-hop", { noAck: true })));
    const from = (seen as unknown as amqp.GetMessage).properties.headers?.["x-received-from"] as Array<Record<string, unknown>>;
    expect(from?.[0]?.exchange).toBe("fx-up");
    // Removing the policy stops the link.
    await mgmt(b, "/api/policies/%2f/fed-x", { method: "DELETE" });
    expect(await eventually(async () => !((await (await mgmt(b, "/api/federation-links/%2f")).json()) as Array<{ type: string }>).some((l) => l.type === "exchange"))).toBe(true);
    await c.close();
  } finally {
    await stop(b);
  }
}, 45000);

test("shovels: amqp10 addresses, a stream source, a remote amqp10 source, and all restart with the broker", async () => {
  const b = await makeBroker(38120);
  await start(b);
  try {
    let c = await conn(b);
    let ch = await c.createConfirmChannel();
    await ch.prefetch(10);
    for (const q of ["sh-a", "sh-b", "sh-c", "sh-d", "sh-e"]) await ch.assertQueue(q, { durable: true });
    await ch.assertQueue("sh-stream", { durable: true, arguments: { "x-queue-type": "stream" } });
    for (let i = 0; i < 3; i++) ch.sendToQueue("sh-stream", Buffer.from(`s${i}`), { persistent: true });
    await ch.waitForConfirms();
    const put = (name: string, value: Record<string, unknown>) => mgmt(b, `/api/parameters/shovel/%2f/${name}`, { method: "PUT", body: JSON.stringify({ value }) });
    expect((await put("v10", { "src-protocol": "amqp10", "src-uri": "amqp://", "src-address": "/queues/sh-a", "dest-protocol": "amqp10", "dest-uri": "amqp://", "dest-address": "/queues/sh-b" })).ok).toBe(true);
    expect((await put("from-stream", { "src-uri": "amqp://", "src-queue": "sh-stream", "src-consumer-args": { "x-stream-offset": "first" }, "dest-uri": "amqp://", "dest-queue": "sh-c" })).ok).toBe(true);
    const remote = `amqp://admin:devpassword12@127.0.0.1:${b.amqp}/%2f`;
    expect((await put("remote10", { "src-protocol": "amqp10", "src-uri": remote, "src-address": "/queues/sh-d", "dest-protocol": "amqp091", "dest-uri": remote, "dest-queue": "sh-e" })).ok).toBe(true);

    const status = async () => (await (await mgmt(b, "/api/shovels/%2f")).json()) as Array<{ name: string; state: string; type: string }>;
    expect(await eventually(async () => (await status()).filter((s) => s.state === "running" && s.type === "dynamic").length === 3)).toBe(true);
    ch.sendToQueue("sh-a", Buffer.from("via-amqp10"), { persistent: true });
    ch.sendToQueue("sh-d", Buffer.from("over-the-wire"), { persistent: true });
    await ch.waitForConfirms();
    expect(await drain(ch, "sh-b", 1000)).toEqual(["via-amqp10"]);
    expect(await drain(ch, "sh-c", 1000)).toEqual(["s0", "s1", "s2"]);
    expect(await drain(ch, "sh-e", 1500)).toEqual(["over-the-wire"]);
    await c.close();

    await restart(b);
    c = await conn(b);
    ch = await c.createConfirmChannel();
    expect(await eventually(async () => (await status()).filter((s) => s.state === "running").length === 3, 8000)).toBe(true);
    ch.sendToQueue("sh-a", Buffer.from("again"), { persistent: true });
    ch.sendToQueue("sh-d", Buffer.from("again-remote"), { persistent: true });
    await ch.waitForConfirms();
    expect(await drain(ch, "sh-b", 1000)).toEqual(["again"]);
    expect(await drain(ch, "sh-e", 1500)).toEqual(["again-remote"]);
    const params = (await (await mgmt(b, "/api/parameters/shovel/%2f")).json()) as Array<{ name: string; value: Record<string, unknown> }>;
    expect(params.find((p) => p.name === "from-stream")?.value["src-consumer-args"]).toEqual({ "x-stream-offset": "first" });
    await c.close();
  } finally {
    await stop(b);
  }
}, 45000);

/** MQTT 3.1.1 packets, enough for a retained-message round trip. */
function mqttString(text: string): number[] {
  const b = [...new TextEncoder().encode(text)];
  return [b.length >> 8, b.length & 0xff, ...b];
}
function mqttPacket(type: number, body: number[]): Uint8Array {
  const len: number[] = [];
  let n = body.length;
  do {
    let byte = n % 128;
    n = Math.floor(n / 128);
    if (n > 0) byte |= 0x80;
    len.push(byte);
  } while (n > 0);
  return Uint8Array.from([type, ...len, ...body]);
}
async function mqttClient(port: number, clientId: string, version = 4, connectProps: number[] = []) {
  const sock: Socket = netConnect({ host: "127.0.0.1", port });
  const packets: Uint8Array[] = [];
  let buf = Buffer.alloc(0);
  sock.on("data", (d: Buffer) => {
    buf = Buffer.concat([buf, d]);
    for (;;) {
      let len = 0;
      let mult = 1;
      let i = 1;
      for (; i < buf.length && i < 5; i++) {
        len += (buf[i]! & 0x7f) * mult;
        mult *= 128;
        if (!(buf[i]! & 0x80)) break;
      }
      if (i >= buf.length || buf.length < i + 1 + len) return;
      packets.push(new Uint8Array(buf.subarray(0, i + 1 + len)));
      buf = buf.subarray(i + 1 + len);
    }
  });
  await new Promise<void>((resolve) => sock.once("connect", () => resolve()));
  const props = version === 5 ? [connectProps.length, ...connectProps] : [];
  sock.write(mqttPacket(0x10, [...mqttString("MQTT"), version, 0xc2, 0, 60, ...props, ...mqttString(clientId), ...mqttString("admin"), ...mqttString("devpassword12")]));
  await eventually(() => packets.some((p) => p[0] === 0x20));
  return { sock, packets };
}

test("MQTT retained messages are stored and reach a subscriber after a restart; an expired one does not", async () => {
  const b = await makeBroker(38130);
  await start(b);
  try {
    const pub = await mqttClient(b.mqtt, "ret-pub", 5);
    // Publish retained, QoS 0: `keep` with no expiry, `short` with a 1 s message expiry interval (v5 property 0x02).
    pub.sock.write(mqttPacket(0x31, [...mqttString("ret/keep"), 0, ..."kept".split("").map((c) => c.charCodeAt(0))]));
    pub.sock.write(mqttPacket(0x31, [...mqttString("ret/short"), 5, 0x02, 0, 0, 0, 1, ..."short".split("").map((c) => c.charCodeAt(0))]));
    await Bun.sleep(300);
    pub.sock.destroy();
    await restart(b);
    await Bun.sleep(1200);
    const sub = await mqttClient(b.mqtt, "ret-sub");
    sub.sock.write(mqttPacket(0x82, [0, 1, ...mqttString("ret/+"), 0]));
    expect(await eventually(() => sub.packets.some((p) => p[0]! >> 4 === 3))).toBe(true);
    await Bun.sleep(300);
    const publishes = sub.packets.filter((p) => p[0]! >> 4 === 3);
    expect(publishes.length).toBe(1);
    expect(publishes[0]![0]! & 1).toBe(1);
    expect(new TextDecoder().decode(publishes[0]!).endsWith("kept")).toBe(true);
    sub.sock.destroy();
  } finally {
    await stop(b);
  }
}, 30000);

test("MQTT 5 session expiry: a session kept by its interval is gone once the interval passes", async () => {
  const b = await makeBroker(38140);
  await start(b);
  try {
    // Clean start with a 1 s session expiry interval (property 0x11).
    const first = await mqttClient(b.mqtt, "short-session", 5, [0x11, 0, 0, 0, 1]);
    first.sock.write(mqttPacket(0x82, [0, 1, 0, ...mqttString("sess/x"), 1]));
    await eventually(() => first.packets.some((p) => p[0] === 0x90));
    first.sock.destroy();
    const queue = () => mgmt(b, `/api/queues/%2f/${encodeURIComponent("mqtt-subscription-short-sessionqos1")}`);
    await Bun.sleep(300);
    expect((await queue()).status).toBe(200);
    expect(await eventually(async () => (await queue()).status === 404, 4000)).toBe(true);
  } finally {
    await stop(b);
  }
}, 20000);

test("acknowledgement timeouts: queue argument, policy, config default; classic and stream refuse the argument", async () => {
  const b = await makeBroker(38150, "consumer_timeout = 1500");
  await start(b);
  try {
    const c = await conn(b);
    const setup = await c.createChannel();
    const held = async (q: string, args: Record<string, unknown>, consumeArgs: Record<string, unknown> = {}) => {
      await setup.assertQueue(q, { durable: true, arguments: { "x-queue-type": "quorum", ...args } });
      setup.sendToQueue(q, Buffer.from("held"), { persistent: true });
      const ch = await c.createChannel();
      let cancelled = false;
      let closed = false;
      ch.on("close", () => {
        closed = true;
      });
      const got: amqp.ConsumeMessage[] = [];
      await ch.consume(q, (m) => (m ? got.push(m) : (cancelled = true)), { arguments: consumeArgs });
      expect(await eventually(() => got.length === 1)).toBe(true);
      const t0 = Date.now();
      expect(await eventually(() => cancelled, 8000)).toBe(true);
      const took = Date.now() - t0;
      expect(closed).toBe(false);
      const back = await eventually(async () => {
        const m = await setup.get(q, { noAck: true });
        return !!m && m.fields.redelivered && m.content.toString() === "held";
      });
      expect(back).toBe(true);
      await setup.deleteQueue(q);
      return took;
    };
    expect(await held("ct-arg", { "x-consumer-timeout": 300 })).toBeLessThan(1500);
    expect(await held("ct-consume", {}, { "x-consumer-timeout": 300 })).toBeLessThan(1500);
    expect((await mgmt(b, "/api/policies/%2f/ct", { method: "PUT", body: JSON.stringify({ pattern: "^ct-pol$", "apply-to": "queues", definition: { "consumer-timeout": 300 } }) })).ok).toBe(true);
    const listed = (await (await mgmt(b, "/api/policies/%2f")).json()) as Array<{ definition: Record<string, unknown> }> | { items: Array<{ definition: Record<string, unknown> }> };
    expect((Array.isArray(listed) ? listed : listed.items)[0]?.definition["consumer-timeout"]).toBe(300);
    expect(await held("ct-pol", {})).toBeLessThan(1500);
    // Nothing set: the config's consumer_timeout (1.5 s here).
    expect(await held("ct-config", {})).toBeGreaterThan(400);

    // A client without consumer_cancel_notify has the channel closed with 406.
    const plain = await conn(b, { clientProperties: { capabilities: { publisher_confirms: true } } });
    const pch = await plain.createChannel();
    let code = 0;
    pch.on("error", (err: { code?: number }) => {
      code = err.code ?? -1;
    });
    await setup.assertQueue("ct-plain", { durable: true, arguments: { "x-queue-type": "quorum", "x-consumer-timeout": 300 } });
    setup.sendToQueue("ct-plain", Buffer.from("p"), { persistent: true });
    await pch.consume("ct-plain", () => {});
    expect(await eventually(() => code === 406, 6000)).toBe(true);
    expect(await eventually(async () => !!(await setup.get("ct-plain", { noAck: true })))).toBe(true);
    await plain.close().catch(() => {});

    const bad = await c.createChannel();
    bad.on("error", () => {});
    expect(await closeCode(bad.assertQueue("ct-classic", { durable: true, arguments: { "x-consumer-timeout": 1000 } }))).toBe(406);
    const bad2 = await c.createChannel();
    bad2.on("error", () => {});
    expect(await closeCode(bad2.assertQueue("ct-stream", { durable: true, arguments: { "x-queue-type": "stream", "x-consumer-timeout": 1000 } }))).toBe(406);
    await setup.assertQueue("ct-classic-ok", { durable: true });
    const bad3 = await c.createChannel();
    bad3.on("error", () => {});
    expect(await closeCode(bad3.consume("ct-classic-ok", () => {}, { arguments: { "x-consumer-timeout": 1000 } }))).toBe(406);
    const bad4 = await c.createChannel();
    bad4.on("error", () => {});
    expect(await closeCode(bad4.assertQueue("st-bad-age", { durable: true, arguments: { "x-queue-type": "stream", "x-max-age": "10x" } }))).toBe(406);
    await c.close();
  } finally {
    await stop(b);
  }
}, 60000);

test("stream filters, consistent-hash header and property hashing, update-secret", async () => {
  const b = await makeBroker(38160);
  await start(b);
  try {
    const c = await conn(b);
    const ch = await c.createConfirmChannel();
    await ch.assertQueue("sf", { durable: true, arguments: { "x-queue-type": "stream" } });
    for (const v of ["red", "blue", null, "red", "green"]) ch.sendToQueue("sf", Buffer.from(v ?? "none"), { persistent: true, headers: v ? { "x-stream-filter-value": v } : {} });
    await ch.waitForConfirms();
    const read = async (args: Record<string, unknown>) => {
      const r = await c.createChannel();
      await r.prefetch(10);
      const got: string[] = [];
      const { consumerTag } = await r.consume("sf", (m) => {
        if (!m) return;
        got.push(m.content.toString());
        r.ack(m);
      }, { arguments: { "x-stream-offset": "first", ...args } });
      await Bun.sleep(500);
      await r.cancel(consumerTag);
      return got;
    };
    expect(await read({ "x-stream-filter": "red" })).toEqual(["red", "red"]);
    expect(await read({ "x-stream-filter": ["red", "green"] })).toEqual(["red", "red", "green"]);
    expect(await read({ "x-stream-filter": "blue", "x-stream-match-unfiltered": true })).toEqual(["blue", "none"]);
    expect((await read({})).length).toBe(5);

    await ch.assertExchange("ch-h", "x-consistent-hash", { durable: false, arguments: { "hash-header": "tenant" } });
    await ch.assertExchange("ch-p", "x-consistent-hash", { durable: false, arguments: { "hash-property": "correlation_id" } });
    for (const q of ["ch-a", "ch-b"]) {
      await ch.assertQueue(q, { durable: true });
      await ch.bindQueue(q, "ch-h", "1");
      await ch.bindQueue(q, "ch-p", "1");
    }
    for (let i = 0; i < 100; i++) ch.publish("ch-h", "same-key", Buffer.from("h"), { headers: { tenant: `t${i}` } });
    // No header: unroutable.
    for (let i = 0; i < 10; i++) ch.publish("ch-h", "same-key", Buffer.from("none"));
    for (let i = 0; i < 100; i++) ch.publish("ch-p", "same-key", Buffer.from("p"), { correlationId: `c${i}` });
    await ch.waitForConfirms();
    const a = await drain(ch, "ch-a", 600);
    const bb = await drain(ch, "ch-b", 600);
    expect(a.length + bb.length).toBe(200);
    expect(a.filter((m) => m === "h").length).toBeGreaterThan(15);
    expect(bb.filter((m) => m === "h").length).toBeGreaterThan(15);
    expect(a.filter((m) => m === "p").length).toBeGreaterThan(15);
    expect(bb.filter((m) => m === "p").length).toBeGreaterThan(15);
    const exchanges = (await (await mgmt(b, "/api/exchanges/%2f/ch-h")).json()) as { arguments: Record<string, unknown> };
    expect(exchanges.arguments["hash-header"]).toBe("tenant");
    const both = await c.createChannel();
    both.on("error", () => {});
    expect(await closeCode(both.assertExchange("ch-both", "x-consistent-hash", { arguments: { "hash-header": "a", "hash-property": "message_id" } }))).toBe(406);

    // update-secret: an internal user's secret must still be its password.
    await c.updateSecret(Buffer.from("devpassword12"), "same");
    let closed = false;
    c.on("close", () => {
      closed = true;
    });
    // The broker closes the connection (530) instead of answering; amqplib leaves that call pending.
    void c.updateSecret(Buffer.from("not-the-password"), "wrong").catch(() => {});
    expect(await eventually(() => closed, 3000)).toBe(true);
  } finally {
    await stop(b);
  }
}, 45000);

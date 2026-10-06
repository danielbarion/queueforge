import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { closeSync, mkdtempSync, openSync, readSync, readdirSync } from "node:fs";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";
import { queueHome } from "../src/broker/index.ts";

const root = join(import.meta.dir, "..");

async function writeConfig(dir: string, amqpPort: number, mgmt: number) {
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${amqpPort}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${mgmt + 1}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
[tls]
enabled = false
`,
  );
  return cfg;
}

async function waitReady(mgmt: number) {
  for (let i = 0; i < 50; i++) {
    try {
      const res = await fetch(`http://127.0.0.1:${mgmt}/readyz`);
      if (res.ok && (await res.text()).includes("ready")) return;
    } catch {
      /* not up */
    }
    await Bun.sleep(100);
  }
  throw new Error("broker did not become ready");
}

function start(cfg: string): ChildProcess {
  return spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: root,
    stdio: ["ignore", "pipe", "pipe"],
  });
}

test("confirmed durable message survives kill -9", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-kill9-"));
  const amqpPort = 26172;
  const mgmt = 36172;
  const cfg = await writeConfig(dir, amqpPort, mgmt);
  const first = start(cfg);
  try {
    await waitReady(mgmt);
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    conn.on("error", () => {});
    conn.on("close", () => {});
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue("keep", { durable: true });
    const body = Buffer.from("kill9-body");
    await new Promise<void>((resolve, reject) => {
      ch.sendToQueue("keep", body, { persistent: true }, (err) => (err ? reject(err) : resolve()));
    });
    console.log("confirm ack received");
    expect(first.pid).toBeGreaterThan(0);
    // The confirm returns after the interval fsync. A short pause still lets the follower settle.
    await new Promise((resolve) => setTimeout(resolve, 250));
    process.kill(first.pid!, "SIGKILL");
    console.log("sent SIGKILL");
    await new Promise((resolve) => first.on("exit", resolve));
  } finally {
    if (first.exitCode == null && !first.killed) first.kill("SIGKILL");
  }

  const second = start(cfg);
  try {
    await waitReady(mgmt);
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    const ch = await conn.createChannel();
    const msg = await ch.get("keep", { noAck: false });
    if (!msg) throw new Error("expected kill9-body");
    expect(msg.content.toString()).toBe("kill9-body");
    console.log("consumed body=kill9-body");
    ch.ack(msg);
    const again = await ch.get("keep", { noAck: false });
    expect(again).toBe(false);
    await conn.close();
  } finally {
    second.kill("SIGKILL");
  }
});

test("pipelined persistent confirms share one group commit", async () => {
  const salt = Number(process.hrtime.bigint() % 700n);
  const amqpPort = 27200 + salt;
  const mgmt = amqpPort + 1000;
  const metrics = mgmt + 1;
  const dir = mkdtempSync(join(tmpdir(), "qf-batch-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${amqpPort}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${metrics}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 80
[tls]
enabled = false
`,
  );
  const child = start(cfg);
  try {
    await waitReady(mgmt);
    const before = await fetch(`http://127.0.0.1:${metrics}/metrics`).then((res) => res.text());
    const flushBefore = Number(before.match(/queueforge_full_flush_total (\d+)/)?.[1] ?? "0");
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    conn.on("error", () => {});
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue("batch", { durable: true });
    const started = performance.now();
    const pending: Array<Promise<void>> = [];
    for (let i = 0; i < 24; i++) {
      pending.push(
        new Promise((resolve, reject) => {
          ch.sendToQueue("batch", Buffer.from([i]), { persistent: true }, (err) => (err ? reject(err) : resolve()));
        }),
      );
    }
    await Promise.all(pending);
    const elapsed = performance.now() - started;
    const after = await fetch(`http://127.0.0.1:${metrics}/metrics`).then((res) => res.text());
    const flushAfter = Number(after.match(/queueforge_full_flush_total (\d+)/)?.[1] ?? "0");
    const flushes = flushAfter - flushBefore;
    console.log(`bun batch elapsed_ms=${elapsed.toFixed(0)} full_flush ${flushBefore} -> ${flushAfter}`);
    expect(elapsed).toBeGreaterThan(40);
    expect(elapsed).toBeLessThan(800);
    expect(flushes).toBeGreaterThanOrEqual(1);
    expect(flushes).toBeLessThan(12);
    await conn.close();
  } finally {
    if (child.exitCode == null) child.kill("SIGKILL");
  }
});

test("one illegal frame closes that connection and leaves the process up", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-fault-"));
  const amqpPort = 26182;
  const mgmt = 36182;
  const cfg = await writeConfig(dir, amqpPort, mgmt);
  const child = start(cfg);
  try {
    await waitReady(mgmt);
    const pid = child.pid;
    console.log(`broker pid before fault=${pid}`);
    await new Promise<void>((resolve, reject) => {
      const sock = createConnection({ host: "127.0.0.1", port: amqpPort }, () => {
        sock.write(Buffer.from("AMQP\x00\x00\x09\x01"));
      });
      sock.on("data", () => {
        const bad = Buffer.alloc(12);
        bad[0] = 1;
        bad[1] = 0;
        bad[2] = 1;
        bad.writeUInt32BE(4, 3);
        bad[11] = 0x00;
        sock.write(bad);
      });
      sock.on("close", () => {
        console.log("faulted connection closed");
        resolve();
      });
      sock.on("error", () => resolve());
      setTimeout(() => reject(new Error("faulted connection stayed open")), 3000);
    });
    expect(child.exitCode).toBeNull();
    const health = await fetch(`http://127.0.0.1:${mgmt}/healthz`);
    expect(await health.text()).toContain("ok");
    console.log(`broker pid after healthz=${child.pid}`);
    expect(child.pid).toBe(pid);
    expect(child.exitCode).toBeNull();
    console.log("healthz ok");
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    const ch = await conn.createChannel();
    // A non-durable, non-exclusive queue is rejected and closes the connection.
    await ch.assertQueue("still", { durable: true });
    ch.sendToQueue("still", Buffer.from("after-fault"));
    const msg = await new Promise<amqp.ConsumeMessage>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no delivery after fault")), 3000);
      void ch.consume("still", (m) => {
        if (!m) return;
        clearTimeout(t);
        ch.ack(m);
        resolve(m);
      });
    });
    expect(msg.content.toString()).toBe("after-fault");
    console.log(`surviving pid=${child.pid} healthz ok body=after-fault`);
    await conn.close();
    expect(child.exitCode).toBeNull();
  } finally {
    child.kill("SIGKILL");
  }
});

async function metric(port: number, name: string): Promise<number> {
  const text = await (await fetch(`http://127.0.0.1:${port}/metrics`)).text();
  for (const line of text.split("\n")) {
    if (!line.startsWith(name) || line.startsWith("#")) continue;
    const rest = line.slice(name.length).trim();
    if (rest.startsWith("{")) continue;
    return Number(rest.split(/\s+/)[0] ?? 0);
  }
  return 0;
}

async function confirmedOnce(policy: string) {
  const dir = mkdtempSync(join(tmpdir(), `qf-kill9-${policy}-`));
  const amqpPort = 26190 + (policy === "always" ? 2 : policy === "every_n_messages" ? 4 : 0);
  const mgmt = amqpPort + 10000;
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${amqpPort}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${mgmt + 1}"
[data]
dir = "${dir}/data"
fsync_policy = "${policy}"
fsync_interval_ms = 400
fsync_every_n_messages = 1
[tls]
enabled = false
`,
  );
  const first = start(cfg);
  try {
    await waitReady(mgmt);
    const early = await metric(mgmt + 1, "queueforge_confirm_before_fsync_total");
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    conn.on("error", () => {});
    conn.on("close", () => {});
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue("keep", { durable: true });
    const started = performance.now();
    await new Promise<void>((resolve, reject) => {
      ch.sendToQueue("keep", Buffer.from(`body-${policy}`), { persistent: true }, (err) => (err ? reject(err) : resolve()));
    });
    const elapsed = performance.now() - started;
    const after = await metric(mgmt + 1, "queueforge_confirm_before_fsync_total");
    console.log(`${policy} confirm elapsed_ms=${elapsed.toFixed(0)} confirm_before_fsync ${early} -> ${after}`);
    expect(after).toBe(early);
    if (policy === "every_n_ms") expect(elapsed).toBeLessThan(10);
    process.kill(first.pid!, "SIGKILL");
    await new Promise((resolve) => first.on("exit", resolve));
  } finally {
    if (first.exitCode == null && first.pid) {
      try { process.kill(first.pid, "SIGKILL"); } catch { /* already dead */ }
    }
  }
  const second = start(cfg);
  try {
    await waitReady(mgmt);
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    const ch = await conn.createChannel();
    let msg: amqp.GetMessage | false = false;
    for (let i = 0; i < 40 && !msg; i++) {
      msg = await ch.get("keep", { noAck: false });
      if (!msg) await Bun.sleep(50);
    }
    if (!msg) throw new Error(`${policy} body missing after restart`);
    expect(msg.content.toString()).toBe(`body-${policy}`);
    ch.ack(msg);
    expect(await ch.get("keep", { noAck: false })).toBe(false);
    console.log(`${policy} survived kill -9`);
    await conn.close();
  } finally {
    second.kill("SIGKILL");
  }
}

test("every_n_ms confirm survives kill -9", async () => {
  await confirmedOnce("every_n_ms");
});

test("always confirm survives kill -9", async () => {
  await confirmedOnce("always");
});

test("every_n_messages confirm survives kill -9", async () => {
  await confirmedOnce("every_n_messages");
});

test("forwarded classic confirm waits for the home fsync", async () => {
  const members = [{ id: "a" }, { id: "b" }];
  let qname = "bq0";
  for (let i = 0; i < 40; i++) {
    const n = `bq${i}`;
    if (queueHome("/", n, members) === "b") {
      qname = n;
      break;
    }
  }
  const dirA = mkdtempSync(join(tmpdir(), "qf-fa-"));
  const dirB = mkdtempSync(join(tmpdir(), "qf-fb-"));
  const cluster = (id: string, amqpPort: number, mgmt: number, clusterPort: number, dir: string) => `[listeners]
amqp = "127.0.0.1:${amqpPort}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${mgmt + 1}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
[tls]
enabled = false
[cluster]
node_id = "${id}"
listen = "127.0.0.1:${clusterPort}"
members = [
  { id = "a", addr = "127.0.0.1:26440" },
  { id = "b", addr = "127.0.0.1:26442" },
]
`;
  const cfgA = join(dirA, "qf.toml");
  const cfgB = join(dirB, "qf.toml");
  await Bun.write(cfgA, cluster("a", 26340, 36340, 26440, dirA));
  await Bun.write(cfgB, cluster("b", 26342, 36342, 26442, dirB));
  let nodeB = start(cfgB);
  const nodeA = start(cfgA);
  try {
    await waitReady(36340);
    await waitReady(36342);
    await Bun.sleep(400);
    const conn = await amqp.connect("amqp://admin:devpassword12@127.0.0.1:26340/%2f");
    conn.on("error", () => {});
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue(qname, { durable: true });
    const before = await metric(36343, "queueforge_full_flush_total");
    const early = await metric(36341, "queueforge_confirm_before_fsync_total");
    const started = performance.now();
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("forwarded confirm hung")), 2000);
      ch.sendToQueue(qname, Buffer.from("forwarded-body"), { persistent: true }, (err) => {
        clearTimeout(timer);
        if (err) reject(err);
        else resolve();
      });
    });
    const elapsed = performance.now() - started;
    const after = await metric(36343, "queueforge_full_flush_total");
    const counter = await metric(36341, "queueforge_confirm_before_fsync_total");
    console.log(`forwarded classic elapsed_ms=${elapsed.toFixed(0)} home flush ${before} -> ${after} confirm_before_fsync ${early} -> ${counter}`);
    expect(after).toBeGreaterThan(before);
    expect(counter).toBe(early);
    expect(elapsed).toBeLessThan(40);
    await conn.close();
    process.kill(nodeB.pid!, "SIGKILL");
    await new Promise((resolve) => nodeB.on("exit", resolve));
    nodeB = start(cfgB);
    await waitReady(36342);
    const again = await amqp.connect("amqp://admin:devpassword12@127.0.0.1:26342/%2f");
    const getCh = await again.createChannel();
    let msg: amqp.GetMessage | false = false;
    for (let i = 0; i < 40 && !msg; i++) {
      msg = await getCh.get(qname, { noAck: false });
      if (!msg) await Bun.sleep(50);
    }
    if (!msg) throw new Error("forwarded body missing on the home after kill -9");
    expect(msg.content.toString()).toBe("forwarded-body");
    getCh.ack(msg);
    expect(await getCh.get(qname, { noAck: false })).toBe(false);
    console.log("forwarded classic survived kill -9 of the home");
    await again.close();
  } finally {
    if (nodeA.exitCode == null && nodeA.pid) nodeA.kill("SIGKILL");
    if (nodeB.exitCode == null && nodeB.pid) nodeB.kill("SIGKILL");
  }
});

test("closing a connection with an unacked remote delivery does not exit when the home is down", async () => {
  const members = [{ id: "a" }, { id: "b" }];
  let qname = "rq0";
  for (let i = 0; i < 40; i++) {
    const n = `rq${i}`;
    if (queueHome("/", n, members) === "b") {
      qname = n;
      break;
    }
  }
  const dirA = mkdtempSync(join(tmpdir(), "qf-ra-"));
  const dirB = mkdtempSync(join(tmpdir(), "qf-rb-"));
  const cluster = (id: string, amqpPort: number, mgmt: number, clusterPort: number, dir: string) => `[listeners]
amqp = "127.0.0.1:${amqpPort}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${mgmt + 1}"
[data]
dir = "${dir}/data"
fsync_policy = "always"
[tls]
enabled = false
[cluster]
node_id = "${id}"
listen = "127.0.0.1:${clusterPort}"
members = [
  { id = "a", addr = "127.0.0.1:26330" },
  { id = "b", addr = "127.0.0.1:26332" },
]
`;
  const cfgA = join(dirA, "qf.toml");
  const cfgB = join(dirB, "qf.toml");
  await Bun.write(cfgA, cluster("a", 26230, 36230, 26330, dirA));
  await Bun.write(cfgB, cluster("b", 26232, 36232, 26332, dirB));
  const nodeA = start(cfgA);
  const nodeB = start(cfgB);
  try {
    await waitReady(36230);
    await waitReady(36232);
    await Bun.sleep(400);
    const conn = await amqp.connect("amqp://admin:devpassword12@127.0.0.1:26230/%2f");
    conn.on("error", () => {});
    conn.on("close", () => {});
    const ch = await conn.createChannel();
    await ch.assertQueue(qname, { durable: true });
    ch.sendToQueue(qname, Buffer.from("remote-body"), { persistent: true });
    const held = await new Promise<amqp.GetMessage>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("remote get timed out")), 3000);
      const poll = async () => {
        const msg = await ch.get(qname, { noAck: false });
        if (msg) {
          clearTimeout(t);
          resolve(msg);
          return;
        }
        setTimeout(() => void poll(), 50);
      };
      void poll();
    });
    expect(held.content.toString()).toBe("remote-body");
    const pid = nodeA.pid;
    console.log(`broker pid before fault=${pid}`);
    process.kill(nodeB.pid!, "SIGKILL");
    await new Promise((resolve) => nodeB.on("exit", resolve));
    (conn.connection as unknown as { stream: { destroy(): void } }).stream.destroy();
    await Bun.sleep(300);
    expect(nodeA.exitCode).toBeNull();
    const health = await fetch("http://127.0.0.1:36230/healthz");
    expect(await health.text()).toContain("ok");
    console.log(`broker pid after healthz=${nodeA.pid}`);
    expect(nodeA.pid).toBe(pid);
    console.log("healthz ok");
  } finally {
    if (nodeA.exitCode == null) nodeA.kill("SIGKILL");
    if (nodeB.exitCode == null) nodeB.kill("SIGKILL");
  }
});

function logHasBody(dataDir: string, body: string): boolean {
  const dir = join(dataDir, "bun.sqlite.dlog");
  const needle = Buffer.from(body);
  const scratch = Buffer.alloc(65536);
  for (const name of readdirSync(dir)) {
    if (!/^\d+$/.test(name)) continue;
    const fd = openSync(join(dir, name), "r");
    try {
      const n = readSync(fd, scratch, 0, scratch.length, 0);
      if (scratch.subarray(0, n).includes(needle)) return true;
    } finally {
      closeSync(fd);
    }
  }
  return false;
}

test("noAck delivery confirms only after the append is in the log", async () => {
  const salt = Number(process.hrtime.bigint() % 1500n);
  const amqpPort = 41000 + salt;
  const mgmt = amqpPort + 1000;
  const metrics = mgmt + 1;
  const dir = mkdtempSync(join(tmpdir(), "qf-noack-kill9-"));
  const body = `noack-body-${salt}`;
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${amqpPort}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${metrics}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 400
[tls]
enabled = false
`,
  );
  const first = start(cfg);
  try {
    await waitReady(mgmt);
    const before = await metric(metrics, "queueforge_full_flush_total");
    const early = await metric(metrics, "queueforge_confirm_before_fsync_total");
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    conn.on("error", () => {});
    conn.on("close", () => {});
    const ch = await conn.createConfirmChannel();
    await ch.assertQueue("noackq", { durable: true });
    const seen: string[] = [];
    await ch.consume("noackq", (msg) => {
      if (msg) seen.push(msg.content.toString());
    }, { noAck: true });
    const started = performance.now();
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("noAck confirm hung")), 2000);
      ch.sendToQueue("noackq", Buffer.from(body), { persistent: true }, (err) => {
        clearTimeout(timer);
        if (err) reject(err);
        else resolve();
      });
    });
    const elapsed = performance.now() - started;
    const after = await metric(metrics, "queueforge_full_flush_total");
    const counter = await metric(metrics, "queueforge_confirm_before_fsync_total");
    const logged = logHasBody(`${dir}/data`, body);
    console.log(
      `noack every_n_ms elapsed_ms=${elapsed.toFixed(0)} full_flush ${before} -> ${after} confirm_before_fsync ${early} -> ${counter} log_has_body=${logged} delivered=${seen.length}`,
    );
    expect(seen).toEqual([body]);
    expect(after).toBeGreaterThan(before);
    expect(counter).toBe(early);
    expect(logged).toBe(true);
    expect(elapsed).toBeLessThan(80);
    process.kill(first.pid!, "SIGKILL");
    await new Promise((resolve) => first.on("exit", resolve));
  } finally {
    if (first.exitCode == null && first.pid) {
      try { process.kill(first.pid, "SIGKILL"); } catch { /* already dead */ }
    }
  }

  const second = start(cfg);
  try {
    await waitReady(mgmt);
    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
    const ch = await conn.createChannel();
    let msg: amqp.GetMessage | false = false;
    for (let i = 0; i < 40 && !msg; i++) {
      msg = await ch.get("noackq", { noAck: false });
      if (!msg) await Bun.sleep(50);
    }
    if (!msg) throw new Error("noAck body missing after restart");
    expect(msg.content.toString()).toBe(body);
    ch.ack(msg);
    expect(await ch.get("noackq", { noAck: false })).toBe(false);
    console.log("noack every_n_ms survived kill -9");
    await conn.close();
  } finally {
    second.kill("SIGKILL");
  }
});

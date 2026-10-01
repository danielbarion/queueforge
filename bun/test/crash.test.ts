import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
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
    // The confirm returns before the interval fsync. Wait for that flush.
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
    await ch.assertQueue("still", { durable: false });
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

import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

const root = join(import.meta.dir, "..");
const rustBin = join(import.meta.dir, "../../rust/target/debug/queueforge");

function spawnNode(kind: "bun" | "rust", cfg: string): ChildProcess {
  if (kind === "bun") {
    return spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], { cwd: root, stdio: "ignore" });
  }
  return spawn(rustBin, ["--config", cfg, "--dev-bootstrap"], { stdio: "ignore" });
}

async function waitReady(mgmt: number) {
  for (let i = 0; i < 80; i++) {
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

/** Open a channel as soon as AMQP accepts, which can be before `/readyz`. */
async function openChannel(amqpPort: number) {
  for (let i = 0; i < 100; i++) {
    try {
      const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
      const ch = await conn.createChannel();
      return { conn, ch };
    } catch {
      await Bun.sleep(20);
    }
  }
  throw new Error(`amqp not up ${amqpPort}`);
}

async function writeCfg(dir: string, amqpPort: number, mgmt: number, metrics: number, clusterPort: number, node: string, members: string, policy: string) {
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${amqpPort}"
management = "127.0.0.1:${mgmt}"
metrics = "127.0.0.1:${metrics}"
[data]
dir = "${dir}/data"
fsync_policy = "${policy}"
fsync_interval_ms = 400
fsync_every_n_messages = 1
[tls]
enabled = false
[logging]
level = "warn"
[cluster]
node_id = "${node}"
listen = "127.0.0.1:${clusterPort}"
${members}
`,
  );
  return cfg;
}

test("rust-rust-bun and bun-bun-rust keep a confirmed body after kill -9", async () => {
  const shapes: Array<{ label: string; kinds: Array<"rust" | "bun">; base: number }> = [
    { label: "rust-rust-bun", kinds: ["rust", "rust", "bun"], base: 41110 },
    { label: "bun-bun-rust", kinds: ["bun", "bun", "rust"], base: 41510 },
  ];
  for (const shape of shapes) {
    const ids = ["a", "b", "c"];
    const ports = [0, 1, 2].map((i) => ({
      amqp: shape.base + i * 2,
      mgmt: shape.base + 100 + i * 2,
      metrics: shape.base + 200 + i * 2,
      cluster: shape.base + 300 + i * 2,
    }));
    const members = ports.map((p, i) => `{ id = "${ids[i]}", addr = "127.0.0.1:${p.cluster}" }`).join(",\n  ");
    const memberBlock = `members = [\n  ${members}\n]`;
    const kids: ChildProcess[] = [];
    const dirs: string[] = [];
    try {
      for (let i = 0; i < 3; i++) {
        const dir = mkdtempSync(join(tmpdir(), `qf-fo-${shape.label}-${i}-`));
        dirs.push(dir);
        const cfg = await writeCfg(dir, ports[i]!.amqp, ports[i]!.mgmt, ports[i]!.metrics, ports[i]!.cluster, ids[i]!, memberBlock, "every_n_ms");
        kids.push(spawnNode(shape.kinds[i]!, cfg));
      }
      for (const p of ports) await waitReady(p.mgmt);
      await Bun.sleep(400);
      for (const p of ports) {
        const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${p.amqp}/%2f`);
        const ch = await conn.createChannel();
        await ch.assertQueue("qq-durable", { durable: true, arguments: { "x-queue-type": "quorum" } });
        await conn.close();
      }
      await Bun.sleep(200);
      const pub = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${ports[0]!.amqp}/%2f`);
      const pch = await pub.createConfirmChannel();
      for (const body of ["kept-body", "still-body"]) {
        await new Promise<void>((resolve, reject) => {
          pch.sendToQueue("qq-durable", Buffer.from(body), { persistent: true }, (err) => (err ? reject(err) : resolve()));
        });
      }
      await pub.close();
      console.log(`${shape.label} confirm ack`);
      kids[0]!.kill("SIGKILL");
      await new Promise((resolve) => kids[0]!.on("exit", resolve));
      console.log(`${shape.label} leader killed`);
      let got: { body: string; key: string } | null = null;
      for (let n = 0; n < 40 && !got; n++) {
        for (const p of [ports[1]!, ports[2]!]) {
          try {
            const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${p.amqp}/%2f`);
            const ch = await conn.createChannel();
            const msg = await ch.get("qq-durable", { noAck: false });
            if (msg) {
              got = { body: msg.content.toString(), key: msg.fields.routingKey };
              ch.ack(msg);
            }
            // Closing the channel first sends the ack; a bare connection.close
            // drops it, and the body comes back, on RabbitMQ too.
            await ch.close();
            await conn.close();
          } catch {
            /* peer still electing */
          }
          if (got) break;
        }
        if (!got) await Bun.sleep(100);
      }
      expect(got?.body === "kept-body" || got?.body === "still-body").toBe(true);
      expect(got?.key).toBe("qq-durable");
      const acked = got!.body;
      console.log(`${shape.label} survivor delivered ${acked}`);
      const dir = dirs[0]!;
      const cfg = join(dir, "qf.toml");
      kids[0] = spawnNode(shape.kinds[0]!, cfg);
      await waitReady(ports[0]!.mgmt);
      const opened = await openChannel(ports[0]!.amqp);
      const again = await opened.ch.get("qq-durable", { noAck: false });
      expect(again).not.toBe(false);
      const returned = again && again.content.toString();
      expect(returned).not.toBe(acked);
      expect(returned === "kept-body" || returned === "still-body").toBe(true);
      if (again) opened.ch.ack(again);
      const duplicate = await opened.ch.get("qq-durable", { noAck: false });
      expect(duplicate).toBe(false);
      await opened.ch.close();
      await opened.conn.close();
      console.log(`${shape.label} restart returned ${returned} and not ${acked}`);
      kids[0]!.kill("SIGKILL");
      await new Promise((resolve) => kids[0]!.on("exit", resolve));
      kids[0] = spawnNode(shape.kinds[0]!, cfg);
      await waitReady(ports[0]!.mgmt);
      const opened2 = await openChannel(ports[0]!.amqp);
      const third = await opened2.ch.get("qq-durable", { noAck: false });
      expect(third).toBe(false);
      await opened2.conn.close();
      console.log(`${shape.label} second restart did not duplicate`);
    } finally {
      for (const kid of kids) {
        if (kid.exitCode == null && !kid.killed) kid.kill("SIGKILL");
      }
    }
  }
}, 120000);

test("classic confirms wait until the fsync that covers the publish", async () => {
  for (const policy of ["every_n_ms", "always", "every_n_messages"] as const) {
    const dir = mkdtempSync(join(tmpdir(), `qf-classic-${policy}-`));
    const amqpPort = policy === "every_n_ms" ? 41920 : policy === "always" ? 41930 : 41940;
    const mgmt = amqpPort + 100;
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
    const kid = spawnNode("bun", cfg);
    try {
      await waitReady(mgmt);
      const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
      const ch = await conn.createConfirmChannel();
      await ch.assertQueue("classic-q", { durable: true });
      const started = Date.now();
      await new Promise<void>((resolve, reject) => {
        ch.sendToQueue("classic-q", Buffer.from("classic"), { persistent: true }, (err) => (err ? reject(err) : resolve()));
      });
      const elapsed = Date.now() - started;
      console.log(`bun classic ${policy} elapsed_ms=${elapsed}`);
      if (policy === "every_n_ms") expect(elapsed).toBeLessThan(10);
      await conn.close();
      kid.kill("SIGKILL");
      await new Promise((resolve) => kid.on("exit", resolve));
      const again = spawnNode("bun", cfg);
      try {
        await waitReady(mgmt);
        const conn2 = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${amqpPort}/%2f`);
        const ch2 = await conn2.createChannel();
        const msg = await ch2.get("classic-q", { noAck: true });
        expect(msg && msg.content.toString()).toBe("classic");
        console.log(`bun classic ${policy} survived kill`);
        await conn2.close();
      } finally {
        again.kill("SIGKILL");
      }
    } finally {
      if (kid.exitCode == null && !kid.killed) kid.kill("SIGKILL");
    }
  }
}, 60000);

import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

// With one worker per core, a new classic queue goes to the worker with the
// fewest, as `n0` assigns them, instead of by name hash: hashed names left
// one worker with twice its share. Declares racing on several workers, and
// repeated declares of the same name, must agree on one home.
test("classic queues spread evenly over worker processes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-place-"));
  const [portA, portM] = [43380, 43390];
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${portA}"
management = "127.0.0.1:${portM}"
metrics = "127.0.0.1:${portM + 1}"
[data]
dir = "${dir}/data"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10
`,
  );
  const child: ChildProcess = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "ignore", "inherit"],
    env: { ...process.env, QUEUEFORGE_CORES: "4" },
  });
  const url = `amqp://admin:devpassword12@127.0.0.1:${portA}/%2f`;
  const connect = async () => {
    for (let i = 0; i < 150; i++) {
      try {
        return await amqp.connect(url);
      } catch {
        await Bun.sleep(100);
      }
    }
    throw new Error("broker did not start");
  };
  try {
    // Connections land on successive workers, so the declares run on all four.
    const conns = await Promise.all([0, 1, 2, 3].map(() => connect()));
    const chans = await Promise.all(conns.map((c) => c.createChannel()));
    const QUEUES = 16;
    await Promise.all(
      Array.from({ length: QUEUES }, (_, i) => chans[i % 4]!.assertQueue(`spread-${i}`, { durable: true })),
    );
    // The same names again, from other workers: no queue moves.
    await Promise.all(
      Array.from({ length: QUEUES }, (_, i) => chans[(i + 1) % 4]!.assertQueue(`spread-${i}`, { durable: true })),
    );
    const auth = { authorization: "Basic " + btoa("admin:devpassword12") };
    const homes = new Map<string, string>();
    for (let i = 0; i < QUEUES; i++) {
      const info = (await fetch(`http://127.0.0.1:${portM}/api/queues/%2f/spread-${i}`, { headers: auth }).then((r) => r.json())) as { node?: string };
      homes.set(`spread-${i}`, String(info.node));
    }
    const perWorker = new Map<string, number>();
    for (const home of homes.values()) perWorker.set(home, (perWorker.get(home) ?? 0) + 1);
    expect([...perWorker.keys()].sort()).toEqual(["n0", "n1", "n2", "n3"]);
    expect([...perWorker.values()]).toEqual([4, 4, 4, 4]);
    // Every queue still takes and gives its messages, whichever worker the
    // publisher and consumer started on.
    for (let i = 0; i < QUEUES; i++) chans[(i + 2) % 4]!.sendToQueue(`spread-${i}`, Buffer.from(`m${i}`));
    for (let i = 0; i < QUEUES; i++) {
      let m: amqp.GetMessage | false = false;
      for (let t = 0; t < 50 && !m; t++) {
        m = await chans[(i + 3) % 4]!.get(`spread-${i}`, { noAck: true });
        if (!m) await Bun.sleep(50);
      }
      expect(m && m.content.toString()).toBe(`m${i}`);
    }
    // Declares of one new name racing on each worker (connections are dealt
    // to workers in turn, so 8 put two on every one), each followed by a
    // publish: the queue is made once and keeps every message. The placement
    // wait sits between the existence check and the insert.
    const raceConns = await Promise.all(Array.from({ length: 8 }, () => connect()));
    const racers = await Promise.all(raceConns.map((c) => c.createConfirmChannel()));
    await Promise.all(racers.map(async (ch, i) => {
      await ch.assertQueue("raced", { durable: true });
      ch.sendToQueue("raced", Buffer.from(`r${i}`), { persistent: true });
      await ch.waitForConfirms();
    }));
    const seen: string[] = [];
    for (let t = 0; t < 100 && seen.length < 8; t++) {
      const m = await racers[0]!.get("raced", { noAck: true });
      if (m) seen.push(m.content.toString());
      else await Bun.sleep(50);
    }
    expect(seen.sort()).toEqual(Array.from({ length: 8 }, (_, i) => `r${i}`).sort());
    for (const c of raceConns) await c.close();
    for (const c of conns) await c.close();
  } finally {
    const exited = new Promise((resolve) => child.once("exit", resolve));
    child.kill("SIGTERM");
    await exited;
  }
}, 60_000);

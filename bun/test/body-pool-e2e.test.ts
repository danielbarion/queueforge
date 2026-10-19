import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

/**
 * Pooled bodies (src/body-pool.ts) under the cases that share or hold a body:
 * fan-out with consumers at different speeds, requeue, dead-lettering, a
 * consumer that stops reading (partial socket writes), quorum, and mixed sizes.
 * Every body is unique and checked byte for byte; the broker runs with
 * `QUEUEFORGE_BODY_POOL_CHECK=1`, which hashes each pooled body at enqueue and
 * checks it at delivery and before reuse.
 */
async function boot(portA: number, portM: number, cores: number): Promise<{ child: ChildProcess; log: string[] }> {
  const dir = mkdtempSync(join(tmpdir(), "qf-bun-pool-"));
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
fsync_interval_ms = 5
[tls]
enabled = false
`,
  );
  const child = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
    env: { ...process.env, QUEUEFORGE_CORES: String(cores), QUEUEFORGE_CORES_PORT: String(portA + 500), QUEUEFORGE_BODY_POOL_CHECK: "1" },
  });
  const log: string[] = [];
  child.stderr!.on("data", (b: Buffer) => log.push(b.toString()));
  child.stdout!.on("data", (b: Buffer) => log.push(b.toString()));
  for (let i = 0; i < 100; i++) {
    try {
      const res = await fetch(`http://127.0.0.1:${portM}/readyz`);
      if (res.ok && (await res.text()).includes("ready") && (await accepts(portA))) return { child, log };
    } catch {
      /* not up */
    }
    await Bun.sleep(100);
  }
  kill(child);
  throw new Error(`broker did not become ready:\n${log.join("").slice(-2000)}`);
}

/** The AMQP port takes a connection. */
async function accepts(port: number): Promise<boolean> {
  try {
    const s = await Bun.connect({ hostname: "127.0.0.1", port, socket: { data() {} } });
    s.end();
    return true;
  } catch {
    return false;
  }
}

function kill(child: ChildProcess) {
  try {
    process.kill(-child.pid!, "SIGKILL");
  } catch {
    child.kill("SIGKILL");
  }
}

/** A body that names itself: `seq` in the first 8 bytes, then bytes derived from it. */
function body(seq: number, size: number): Buffer {
  const b = Buffer.allocUnsafe(size);
  b.writeUInt32BE(seq, 0);
  b.writeUInt32BE(size, 4);
  let x = (seq * 2654435761) >>> 0;
  for (let i = 8; i < size; i += 4) {
    x = (x ^ (x << 13)) >>> 0;
    x = (x ^ (x >>> 17)) >>> 0;
    x = (x ^ (x << 5)) >>> 0;
    if (i + 4 <= size) b.writeUInt32BE(x, i);
    else for (let j = i; j < size; j++) b[j] = (x >>> ((j - i) * 8)) & 0xff;
  }
  return b;
}

function verify(content: Buffer, seen: Set<number>, where: string) {
  const seq = content.readUInt32BE(0);
  const size = content.readUInt32BE(4);
  if (content.length !== size || !content.equals(body(seq, size))) throw new Error(`${where}: corrupt body for message ${seq}`);
  seen.add(seq);
}

async function metric(portM: number, name: string): Promise<number> {
  const text = await (await fetch(`http://127.0.0.1:${portM + 1}/metrics`)).text();
  const line = text.split("\n").find((l) => l.startsWith(name + " "));
  return Number(line?.split(" ")[1] ?? NaN);
}

async function scenario(portA: number, portM: number) {
  const url = `amqp://admin:devpassword12@127.0.0.1:${portA}/%2f`;
  const pub = await amqp.connect(url);
  const ch = await pub.createConfirmChannel();
  const sizes = [16 * 1024, 20_000, 64 * 1024, 64 * 1024, 200_000, 1024 * 1024];
  const errors: string[] = [];

  // 1. Plain durable queue, two fast consumers: the reuse path.
  await ch.assertQueue("pool-plain", { durable: true });
  // 2. Fan-out to three queues; one consumer is slow.
  await ch.assertExchange("pool-fan", "fanout", { durable: true });
  for (const q of ["pool-fan-a", "pool-fan-b", "pool-fan-c", "pool-fan-late"]) {
    await ch.assertQueue(q, { durable: true });
    await ch.bindQueue(q, "pool-fan", "");
  }
  // pool-fan-late has no consumer until the end: its copies sit in the queue
  // while the other fan-out copies are acked and the pool reuses buffers.
  // 3. Requeue: every third delivery is nacked back once.
  await ch.assertQueue("pool-requeue", { durable: true });
  // 4. Dead-lettering: rejected messages go to pool-dlq.
  await ch.assertQueue("pool-dlq", { durable: true });
  await ch.assertQueue("pool-dlx-src", { durable: true, deadLetterExchange: "", deadLetterRoutingKey: "pool-dlq" });
  // 5. A consumer that stops reading for a while: its writes back up.
  await ch.assertQueue("pool-stalled", { durable: true });
  // 6. Quorum.
  await ch.assertQueue("pool-qq", { durable: true, arguments: { "x-queue-type": "quorum" } });

  const total = 600;
  const want: Record<string, Set<number>> = {};
  const seen: Record<string, Set<number>> = {};
  for (const q of ["pool-plain", "pool-fan-a", "pool-fan-b", "pool-fan-c", "pool-fan-late", "pool-requeue", "pool-dlq", "pool-stalled", "pool-qq"]) {
    want[q] = new Set();
    seen[q] = new Set();
  }

  const consumers: amqp.ChannelModel[] = [];
  async function consume(queue: string, opts: { prefetch: number; delayMs?: number; nackEvery?: number; reject?: boolean }) {
    const c = await amqp.connect(url);
    consumers.push(c);
    const cch = await c.createChannel();
    await cch.prefetch(opts.prefetch);
    const nacked = new Set<number>();
    await cch.consume(queue, async (m) => {
      if (!m) return;
      try {
        const seq = m.content.readUInt32BE(0);
        if (opts.reject) {
          verify(m.content, new Set(), queue);
          cch.reject(m, false);
          return;
        }
        if (opts.nackEvery && seq % opts.nackEvery === 0 && !nacked.has(seq)) {
          nacked.add(seq);
          verify(m.content, new Set(), queue);
          cch.nack(m, false, true);
          return;
        }
        verify(m.content, seen[queue]!, queue);
        if (opts.delayMs) await Bun.sleep(opts.delayMs);
        cch.ack(m);
      } catch (err) {
        errors.push(String(err));
      }
    });
    return c;
  }
  await consume("pool-plain", { prefetch: 20 });
  await consume("pool-plain", { prefetch: 20 });
  await consume("pool-fan-a", { prefetch: 50 });
  await consume("pool-fan-b", { prefetch: 5, delayMs: 2 });
  await consume("pool-fan-c", { prefetch: 200 });
  await consume("pool-requeue", { prefetch: 10, nackEvery: 3 });
  await consume("pool-dlx-src", { prefetch: 10, reject: true });
  await consume("pool-dlq", { prefetch: 50 });
  await consume("pool-qq", { prefetch: 20 });

  // The stalled consumer: a raw socket that reads the handshake, then stops
  // reading while 64 KiB deliveries pile up, then drains them.
  const stalled = await amqp.connect(url);
  consumers.push(stalled);
  const sch = await stalled.createChannel();
  await sch.prefetch(400);
  const socket = (stalled as unknown as { connection: { stream: { pause(): void; resume(): void } } }).connection.stream;
  await sch.consume("pool-stalled", (m) => {
    if (!m) return;
    try {
      verify(m.content, seen["pool-stalled"]!, "pool-stalled");
      sch.ack(m);
    } catch (err) {
      errors.push(String(err));
    }
  });
  socket.pause();

  let seq = 0;
  const waits: Promise<unknown>[] = [];
  for (let i = 0; i < total; i++) {
    const size = sizes[i % sizes.length]!;
    const send = (exchange: string, key: string, queues: string[], persistent = true) => {
      const n = ++seq;
      for (const q of queues) want[q]!.add(n);
      waits.push(new Promise<void>((resolve, reject) => ch.publish(exchange, key, body(n, size), { persistent }, (err) => (err ? reject(err) : resolve()))));
    };
    send("", "pool-plain", ["pool-plain"]);
    send("", "pool-plain", ["pool-plain"], false);
    if (i % 2 === 0) send("pool-fan", "", ["pool-fan-a", "pool-fan-b", "pool-fan-c", "pool-fan-late"]);
    send("", "pool-requeue", ["pool-requeue"]);
    if (i % 4 === 0) send("", "pool-dlx-src", ["pool-dlq"]);
    send("", "pool-stalled", ["pool-stalled"]);
    send("", "pool-qq", ["pool-qq"]);
    if (i % 50 === 0) await Promise.all(waits.splice(0));
    // Let the stalled consumer read again halfway through.
    if (i === 25) socket.resume();
  }
  await Promise.all(waits);
  socket.resume();

  const deadline = Date.now() + 60_000;
  const pending = () => Object.keys(want).filter((q) => q !== "pool-fan-late" && seen[q]!.size < want[q]!.size);
  while (pending().length && Date.now() < deadline && errors.length === 0) await Bun.sleep(100);
  if (pending().length) console.log("pending", pending().map((q) => `${q} ${seen[q]!.size}/${want[q]!.size}`));
  expect(errors).toEqual([]);
  for (const q of Object.keys(want)) if (q !== "pool-fan-late") expect({ q, got: seen[q]!.size }).toEqual({ q, got: want[q]!.size });

  // A second plain round: by now acked bodies have been returned to the pool.
  const before = await metric(portM, "queueforge_body_pool_reused_total");
  for (let i = 0; i < 400; i++) {
    const n = ++seq;
    want["pool-plain"]!.add(n);
    waits.push(new Promise<void>((resolve, reject) => ch.publish("", "pool-plain", body(n, 64 * 1024), { persistent: true }, (err) => (err ? reject(err) : resolve()))));
    if (i % 40 === 0) await Promise.all(waits.splice(0));
  }
  await Promise.all(waits);
  while (seen["pool-plain"]!.size < want["pool-plain"]!.size && Date.now() < deadline + 30_000 && errors.length === 0) await Bun.sleep(100);
  expect(errors).toEqual([]);
  expect(seen["pool-plain"]!.size).toBe(want["pool-plain"]!.size);
  const reused = await metric(portM, "queueforge_body_pool_reused_total");
  // Only now read the fan-out copies that waited through all that reuse.
  await consume("pool-fan-late", { prefetch: 100 });
  while (seen["pool-fan-late"]!.size < want["pool-fan-late"]!.size && Date.now() < deadline + 60_000 && errors.length === 0) await Bun.sleep(100);
  expect(errors).toEqual([]);
  expect(seen["pool-fan-late"]!.size).toBe(want["pool-fan-late"]!.size);
  expect(await metric(portM, "queueforge_body_pool_violations_total")).toBe(0);

  for (const c of consumers) await c.close().catch(() => {});
  await pub.close();
  return { before, reused };
}

for (const [cores, portA] of [[1, 25710], [3, 25730]] as const) {
  test(`pooled bodies stay intact under fan-out, requeue, dead-lettering, stalled sockets and quorum (${cores} core${cores > 1 ? "s" : ""})`, async () => {
    const portM = portA + 10000;
    const { child, log } = await boot(portA, portM, cores);
    try {
      const { before, reused } = await scenario(portA, portM);
      // Reuse happened at all (it is the point), and more of it in the second round.
      expect(reused).toBeGreaterThan(0);
      expect(reused).toBeGreaterThan(before);
      expect(log.join("").includes("body pool")).toBe(false);
    } finally {
      kill(child);
    }
  }, 180_000);
}

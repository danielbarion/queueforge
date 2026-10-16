import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";

/**
 * Large bodies are copied into their own buffer as frames arrive, a body frame
 * that spans reads is streamed, and deliveries go out as views of the stored
 * body. A proxy re-chunks every client write at random points (frame headers,
 * frame ends and payloads split anywhere) and stalls reads from the broker so
 * it sees short writes with views queued.
 */

async function boot(dir: string, portA: number, portM: number): Promise<ChildProcess> {
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${portA}"
management = "127.0.0.1:${portM}"
metrics = "127.0.0.1:${portM + 1}"
[data]
dir = "${dir}/data"
[tls]
enabled = false
`,
  );
  const child = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "ignore", "inherit"],
  });
  for (let i = 0; i < 100; i++) {
    try {
      const res = await fetch(`http://127.0.0.1:${portM}/readyz`);
      if (res.ok && (await res.text()).includes("ready")) return child;
    } catch {
      /* not up */
    }
    await Bun.sleep(100);
  }
  child.kill();
  throw new Error("broker did not become ready");
}

/** A seeded generator, so a failing split is reproducible. */
function rng(seed: number) {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 2 ** 32;
  };
}

/** Forward client bytes in random 1..9000 byte writes; pause broker reads now and then. */
function proxy(target: number, seed: number): Promise<{ server: Server; port: number }> {
  const rand = rng(seed);
  const server = createServer((client: Socket) => {
    const broker = createConnection(target, "127.0.0.1");
    client.pause();
    broker.on("connect", () => client.resume());
    const queue: Buffer[] = [];
    let pumping = false;
    const pump = () => {
      if (pumping) return;
      pumping = true;
      const step = () => {
        const chunk = queue.shift();
        if (!chunk) {
          pumping = false;
          return;
        }
        const n = 1 + Math.floor(rand() * 9000);
        if (chunk.length > n) queue.unshift(chunk.subarray(n));
        broker.write(chunk.subarray(0, n));
        // Most splits go out back to back, some after a turn, so the broker
        // sees both one read holding several pieces and one piece per read.
        if (rand() < 0.3) setTimeout(step, 0);
        else step();
      };
      step();
    };
    client.on("data", (b: Buffer) => {
      queue.push(b);
      pump();
    });
    broker.on("data", (b: Buffer) => {
      if (!client.write(b)) {
        broker.pause();
        client.once("drain", () => broker.resume());
      }
      if (rand() < 0.02) {
        broker.pause();
        setTimeout(() => broker.resume(), 20);
      }
    });
    const close = () => {
      client.destroy();
      broker.destroy();
    };
    client.on("error", close);
    broker.on("error", close);
    client.on("close", close);
    broker.on("close", close);
  });
  return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve({ server, port: (server.address() as { port: number }).port })));
}

const SIZES = [16383, 16384, 20000, 65536, 131065, 131072, 300000, 1048576, 100, 0];

function body(size: number, n: number): Buffer {
  const b = Buffer.allocUnsafe(size);
  for (let i = 0; i < size; i++) b[i] = (i * 31 + n * 7) & 0xff;
  return b;
}

for (const frameMax of [0, 4096]) {
  test(`large bodies survive arbitrary read splits (frame_max ${frameMax || "default"})`, async () => {
    const dir = mkdtempSync(join(tmpdir(), "qf-large-"));
    const portA = 27000 + Math.floor(Math.random() * 2000);
    const portM = portA + 3000;
    const child = await boot(dir, portA, portM);
    const { server, port } = await proxy(portA, 42 + frameMax);
    const url = `amqp://admin:devpassword12@127.0.0.1:${port}/%2f${frameMax ? `?frameMax=${frameMax}` : ""}`;
    const pubConn = await amqp.connect(url);
    const subConn = await amqp.connect(url);
    try {
      const pub = await pubConn.createConfirmChannel();
      const sub = await subConn.createChannel();
      const q = `large-${frameMax}`;
      await pub.assertQueue(q, { durable: true });
      const rounds = 3;
      const expected: Buffer[] = [];
      for (let r = 0; r < rounds; r++) for (const size of SIZES) expected.push(body(size, expected.length));
      const got: Buffer[] = [];
      const done = new Promise<void>((resolve, reject) => {
        const t = setTimeout(() => reject(new Error(`only ${got.length} of ${expected.length} delivered`)), 30000);
        void sub.prefetch(4);
        void sub.consume(q, (m) => {
          if (!m) return;
          got.push(m.content);
          sub.ack(m);
          if (got.length === expected.length) {
            clearTimeout(t);
            resolve();
          }
        });
      });
      // Persistent and transient, several in flight on one channel.
      for (let i = 0; i < expected.length; i++) pub.sendToQueue(q, expected[i]!, { persistent: i % 2 === 0, headers: { n: i } });
      await pub.waitForConfirms();
      await done;
      expect(got.length).toBe(expected.length);
      for (let i = 0; i < expected.length; i++) {
        expect(got[i]!.length).toBe(expected[i]!.length);
        expect(Buffer.compare(got[i]!, expected[i]!)).toBe(0);
      }
    } finally {
      await pubConn.close().catch(() => {});
      await subConn.close().catch(() => {});
      server.close();
      child.kill();
      rmSync(dir, { recursive: true, force: true });
    }
  }, 60000);
}

test("body buffers sized from headers are capped per connection", async () => {
  const { Conn } = await import("../src/amqp/listen.ts");
  await import("../src/amqp/index.ts");
  const conn = new Conn({ write: (b: Uint8Array) => b.length, end() {} }, {} as never);
  const header = (size: number) => {
    const p = new Uint8Array(14);
    p[1] = 60;
    const hi = Math.floor(size / 2 ** 32);
    new DataView(p.buffer).setUint32(4, hi);
    new DataView(p.buffer).setUint32(8, size >>> 0);
    return p;
  };
  // Twenty channels each declare a 60 MiB body and send none of it.
  for (let ch = 1; ch <= 20; ch++) {
    const c = conn.ch(ch);
    c.publish = { exchange: "", routingKey: "q", mandatory: false, immediate: false };
    conn.onHeader(ch, header(60 * 1024 * 1024));
  }
  let reserved = 0;
  for (const c of conn.channels.values()) reserved += c.body?.length ?? 0;
  expect(reserved).toBeLessThanOrEqual(64 * 1024 * 1024);
  expect([...conn.channels.values()].filter((c) => c.body).length).toBe(1);
});

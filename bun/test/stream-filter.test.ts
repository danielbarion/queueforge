import { afterAll, beforeAll, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import amqp from "amqplib";
import { connect as streamConnect, Offset, type Client } from "../../conformance/node_modules/rabbitmq-stream-js-client";

// Stream protocol filtering, as RabbitMQ 3.13+: publish v2 carries a filter
// value per message and subscribe properties `filter.N` / `match-unfiltered`
// select what is delivered. The value is the same one AMQP 0-9-1 publishers
// set with `x-stream-filter-value`, so both sides filter on each other's.

const [portA, portM, portS] = [44110, 44111, 44113];
let child: ChildProcess;
const clients: Client[] = [];

beforeAll(async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-stream-filter-"));
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${portA}"
management = "127.0.0.1:${portM}"
metrics = "127.0.0.1:${portM + 1}"
stream = "127.0.0.1:${portS}"
[data]
dir = "${dir}/data"
`,
  );
  child = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], { cwd: join(import.meta.dir, ".."), stdio: ["ignore", "ignore", "inherit"] });
  for (let i = 0; i < 150; i++) {
    try {
      if ((await fetch(`http://127.0.0.1:${portM}/readyz`)).ok) return;
    } catch {
      /* starting */
    }
    await Bun.sleep(50);
  }
  throw new Error("broker did not start");
});

afterAll(async () => {
  for (const c of clients.splice(0)) await c.close().catch(() => {});
  child?.kill("SIGKILL");
});

async function client(): Promise<Client> {
  const c = await streamConnect({ hostname: "127.0.0.1", port: portS, username: "admin", password: "devpassword12", vhost: "/" });
  clients.push(c);
  return c;
}

/** Bodies a filtering stream consumer gets from the first offset. No client-side post-filter: the server must filter. */
async function readFiltered(c: Client, stream: string, values: string[], matchUnfiltered: boolean): Promise<string[]> {
  const got: string[] = [];
  const consumer = await c.declareConsumer(
    { stream, offset: Offset.first(), filter: { values, matchUnfiltered, postFilterFunc: () => true } },
    (m: { content: Buffer }) => {
      got.push(m.content.toString());
    },
  );
  await Bun.sleep(600);
  await c.closeConsumer(consumer.extendedId);
  return got;
}

test("publish v2 filter values select stream protocol deliveries", async () => {
  const c = await client();
  const stream = `sf-${crypto.randomUUID()}`;
  await c.createStream({ stream, arguments: {} });
  const pub = await c.declarePublisher({ stream }, (m) => m.applicationProperties?.color as string | undefined);
  for (const [body, color] of [["r1", "red"], ["b1", "blue"], ["n1", undefined], ["r2", "red"], ["g1", "green"]] as const) {
    await pub.send(Buffer.from(body), color ? { applicationProperties: { color } } : {});
  }
  await pub.flush();
  await Bun.sleep(200);
  expect(await readFiltered(c, stream, ["red"], false)).toEqual(["r1", "r2"]);
  expect(await readFiltered(c, stream, ["red", "green"], false)).toEqual(["r1", "r2", "g1"]);
  expect(await readFiltered(c, stream, ["blue"], true)).toEqual(["b1", "n1"]);
  await c.deleteStream({ stream });
});

test("filter values interoperate between AMQP 0-9-1 and the stream protocol", async () => {
  const c = await client();
  const stream = `sf-mix-${crypto.randomUUID()}`;
  const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${portA}/%2f`);
  try {
    const ch = await conn.createChannel();
    await ch.assertQueue(stream, { durable: true, arguments: { "x-queue-type": "stream" } });
    // AMQP 0-9-1 publishes, filtered from the stream protocol.
    ch.sendToQueue(stream, Buffer.from("amqp-red"), { headers: { "x-stream-filter-value": "red" } });
    ch.sendToQueue(stream, Buffer.from("amqp-blue"), { headers: { "x-stream-filter-value": "blue" } });
    // Stream protocol v2 publishes, filtered from AMQP 0-9-1.
    const pub = await c.declarePublisher({ stream }, (m) => m.applicationProperties?.color as string | undefined);
    await pub.send(Buffer.from("stream-red"), { applicationProperties: { color: "red" } });
    await pub.send(Buffer.from("stream-blue"), { applicationProperties: { color: "blue" } });
    await pub.flush();
    await Bun.sleep(300);
    expect(await readFiltered(c, stream, ["red"], false)).toEqual(["amqp-red", "stream-red"]);
    const got: string[] = [];
    await ch.prefetch(100);
    await ch.consume(stream, (m) => {
      if (!m) return;
      got.push(m.content.toString());
      ch.ack(m);
    }, { arguments: { "x-stream-offset": "first", "x-stream-filter": "blue" } });
    await Bun.sleep(600);
    expect(got).toEqual(["amqp-blue", "stream-blue"]);
    await ch.deleteQueue(stream);
  } finally {
    await conn.close().catch(() => {});
  }
});

test("command versions advertise publish v2 for filtering", async () => {
  const c = await client();
  // The client gates filtering on the server's publish version and its peer version.
  const conn = (c as unknown as { locatorConnection: { serverVersions: Array<{ key: number; minVersion: number; maxVersion: number }> } }).locatorConnection;
  const publish = conn.serverVersions.find((v) => v.key === 2);
  expect(publish?.minVersion).toBe(1);
  expect(publish?.maxVersion).toBe(2);
  expect(c.rabbitManagementVersion >= "3.13.0").toBe(true);
});

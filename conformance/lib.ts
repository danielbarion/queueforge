/**
 * Client helpers shared by every conformance test.
 *
 * Tests read the broker from QF_* variables set by run.ts, so the same file
 * runs unchanged against RabbitMQ and each QueueForge broker.
 */
import amqp, { type Channel, type ChannelModel, type ConfirmChannel, type ConsumeMessage, type Message } from "amqplib";
import { afterAll } from "bun:test";

export const TARGET = process.env.QF_TARGET ?? "unknown";
export const AMQP_URL = process.env.QF_AMQP ?? "amqp://admin:devpassword12@127.0.0.1:5672/%2f";
export const MGMT = process.env.QF_MGMT ?? "http://127.0.0.1:15672";
export const AUTH = `Basic ${btoa("admin:devpassword12")}`;

const open: ChannelModel[] = [];
/** The reply code of the latest channel or connection close, for closeCode. */
let lastCode: number | null = null;
const noteClose = (err: unknown) => {
  const code = (err as { code?: number })?.code;
  if (typeof code === "number") lastCode = code;
};

afterAll(async () => {
  for (const c of open.splice(0)) await c.close().catch(() => {});
});

let seq = 0;
/** A name no other test in this run uses. */
export function uniq(prefix: string): string {
  seq++;
  return `conf-${prefix}-${process.pid}-${seq}-${Math.random().toString(36).slice(2, 8)}`;
}

/** A connection that is closed when the file ends. Errors are swallowed so a channel close does not crash the run. */
export async function connect(url = AMQP_URL): Promise<ChannelModel> {
  const c = await amqp.connect(url);
  c.on("error", noteClose);
  open.push(c);
  return c;
}

export async function channel(): Promise<Channel> {
  const c = await connect();
  const ch = await c.createChannel();
  ch.on("error", noteClose);
  return ch;
}

export async function confirmChannel(): Promise<ConfirmChannel> {
  const c = await connect();
  const ch = await c.createConfirmChannel();
  ch.on("error", noteClose);
  return ch;
}

/** Resolve with the AMQP reply code a rejected operation closed its channel with. */
export async function closeCode(p: Promise<unknown>): Promise<number | null> {
  lastCode = null;
  try {
    await p;
    return null;
  } catch (err) {
    const code = (err as { code?: number }).code;
    if (typeof code === "number") return code;
    const m = /\b([2-5]\d\d)\b/.exec(String((err as Error).message));
    if (m) return Number(m[1]);
    // "Channel ended": the code came on the close event instead.
    await sleep(20);
    return lastCode ?? -1;
  }
}

/**
 * The next message on `queue`, or null after `ms`. Polls basic.get, so no
 * consumer is left holding a message once the test moves on.
 */
export async function nextMessage(ch: Channel, queue: string, ms = 2000, opts: { noAck?: boolean } = {}): Promise<Message | null> {
  const deadline = Date.now() + ms;
  for (;;) {
    const msg = await ch.get(queue, { noAck: opts.noAck ?? false });
    if (msg) return msg;
    if (Date.now() >= deadline) return null;
    await sleep(20);
  }
}

/** Collect up to `n` messages within `ms`, by basic.get. */
export async function collect(ch: Channel, queue: string, n: number, ms = 2000, opts: { noAck?: boolean } = {}): Promise<Message[]> {
  const got: Message[] = [];
  const deadline = Date.now() + ms;
  while (got.length < n) {
    const msg = await ch.get(queue, { noAck: opts.noAck ?? false });
    if (msg) {
      got.push(msg);
      continue;
    }
    if (Date.now() >= deadline) break;
    await sleep(20);
  }
  return got;
}

/** Every message a consumer receives within `ms`. The consumer is cancelled first, then the list is returned. */
export async function consumeFor(
  ch: Channel,
  queue: string,
  ms: number,
  opts: { noAck?: boolean; arguments?: Record<string, unknown> } = {},
): Promise<ConsumeMessage[]> {
  const got: ConsumeMessage[] = [];
  const ok = await ch.consume(queue, (m) => m && got.push(m), { noAck: opts.noAck ?? true, arguments: opts.arguments });
  await sleep(ms);
  await ch.cancel(ok.consumerTag);
  return got;
}

export async function mgmt(path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(`${MGMT}${path}`, {
    ...init,
    headers: { authorization: AUTH, "content-type": "application/json", ...(init.headers ?? {}) },
  });
}

export const sleep = (ms: number) => Bun.sleep(ms);

// amqplib wraps only the common methods. The rest go through its public rpc.
// defs.js is not in the package exports map, so it is loaded by path.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const defs = require(`${import.meta.dir}/node_modules/amqplib/lib/defs.js`) as Record<string, number>;

/** Send `method` (an amqplib defs name such as "TxSelect") and wait for its -Ok. */
export async function rpc(ch: Channel, method: string, fields: object): Promise<unknown> {
  const call = (ch as unknown as { rpc: (m: number, f: object, e: number) => Promise<unknown> }).rpc;
  return call.call(ch, defs[method]!, fields, defs[`${method}Ok`]!);
}

/** Poll until `fn` returns true or `ms` passes. */
export async function eventually(fn: () => Promise<boolean> | boolean, ms = 3000): Promise<boolean> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await fn()) return true;
    await sleep(50);
  }
  return false;
}

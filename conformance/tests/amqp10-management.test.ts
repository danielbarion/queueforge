/**
 * RabbitMQ 4's AMQP 1.0 management node ("HTTP over AMQP", v2 addresses):
 * the official AMQP 1.0 clients declare topology through it.
 * A paired sender and receiver attach to /management; each request carries
 * an HTTP method as subject, a path as `to`, and the reply comes back with
 * the status code as subject and correlation-id = the request's message-id.
 */
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createPrivateKey, createSign } from "node:crypto";
// eslint-disable-next-line @typescript-eslint/no-require-imports
const rhea = require("rhea");
import { channel, mgmt, nextMessage, uniq } from "../lib.ts";

const PORT = Number(process.env.QF_AMQP_PORT ?? 5672);

type Reply = { status: string; body: any };
type Node = { request: (method: string, path: string, body?: unknown) => Promise<Reply>; close: () => void; raw: any };

/** The AMQP error a refused request ended the session with. */
class Refused extends Error {
  constructor(readonly condition: string) {
    super(condition);
  }
}

function openNode(username = "admin", password = "devpassword12"): Promise<Node> {
  const container = rhea.create_container({ id: uniq("mgmt10") });
  return new Promise((done, fail) => {
    const raw = container.connect({ host: "127.0.0.1", port: PORT, username, password, hostname: "vhost:/", reconnect: false, idle_time_out: 0 });
    raw.on("connection_error", (ctx: any) => fail(new Error(String(ctx.connection.error?.condition ?? "connection error"))));
    raw.on("disconnected", (ctx: any) => fail(new Error(String(ctx.error?.message ?? "disconnected"))));
    raw.on("connection_open", () => {
      // RabbitMQ wants the sender attached first, then the receiver of the pair,
      // and credit on the receiver before the first request.
      const name = "management-link-pair";
      const props = { paired: true };
      const sender = raw.open_sender({ name, target: { address: "/management" }, source: { address: "/management" }, properties: props, snd_settle_mode: 1 });
      const waiting = new Map<string, { ok: (r: Reply) => void; no: (e: Error) => void }>();
      let seq = 0;
      // RabbitMQ answers a request the user may not make by ending the session.
      const refuse = (ctx: any) => {
        const condition = String(ctx.session?.error?.condition ?? ctx.connection?.error?.condition ?? "refused");
        for (const w of waiting.values()) w.no(new Refused(condition));
        waiting.clear();
      };
      raw.on("session_error", refuse);
      raw.on("session_close", refuse);
      container.on("error", () => {});
      sender.once("sender_error", (ctx: any) => fail(new Error(String(ctx.sender.error?.description ?? "sender refused"))));
      sender.once("sender_open", () => {
        const receiver = raw.open_receiver({ name, source: { address: "/management" }, target: { address: "/management" }, properties: props, credit_window: 10, snd_settle_mode: 1 });
        receiver.once("receiver_error", (ctx: any) => fail(new Error(String(ctx.receiver.error?.description ?? "receiver refused"))));
        receiver.on("message", (ctx: any) => {
          const id = String(ctx.message.correlation_id ?? "");
          waiting.get(id)?.ok({ status: String(ctx.message.subject ?? ""), body: ctx.message.body });
          waiting.delete(id);
        });
        receiver.once("receiver_open", () =>
          setTimeout(
            () =>
              done({
                raw,
                close: () => raw.close(),
                request: (method, path, body) =>
                  new Promise((ok, no) => {
                    const message_id = `req-${++seq}`;
                    waiting.set(message_id, { ok, no });
                    setTimeout(() => {
                      if (waiting.delete(message_id)) no(new Error(`no reply to ${method} ${path}`));
                    }, 5000);
                    const msg: Record<string, unknown> = { message_id, to: path, subject: method, reply_to: "$me" };
                    if (body !== undefined) msg.body = body;
                    sender.send(msg);
                  }),
              }),
            200,
          ),
        );
      });
    });
  });
}

const enc = encodeURIComponent;

test("AMQP 1.0 management :: PUT /queues declares a queue and GET reads it back", async () => {
  const node = await openNode();
  const q = uniq("m10-q");
  const put = await node.request("PUT", `/queues/${enc(q)}`, { durable: true, arguments: { "x-queue-type": "quorum" } });
  // 201 for a new queue, 200 for one that already existed.
  expect(put.status).toBe("201");
  expect(put.body?.name).toBe(q);
  expect(put.body?.type).toBe("quorum");
  const get = await node.request("GET", `/queues/${enc(q)}`);
  expect(get.status).toBe("200");
  expect(Boolean(get.body?.durable)).toBe(true);
  expect((await node.request("PUT", `/queues/${enc(q)}`, { durable: true, arguments: { "x-queue-type": "quorum" } })).status).toBe("200");
  const missing = await node.request("GET", `/queues/${enc(uniq("m10-none"))}`);
  expect(missing.status).toBe("404");
  const del = await node.request("DELETE", `/queues/${enc(q)}`);
  expect(del.status).toBe("200");
  expect(Number(del.body?.message_count ?? -1)).toBe(0);
  node.close();
});

test("AMQP 1.0 management :: exchanges and bindings are declared and removed", async () => {
  const node = await openNode();
  const x = uniq("m10-x");
  const q = uniq("m10-bq");
  expect((await node.request("PUT", `/exchanges/${enc(x)}`, { type: "direct", durable: true, auto_delete: false, internal: false, arguments: {} })).status).toBe("204");
  expect((await node.request("PUT", `/queues/${enc(q)}`, { durable: true, arguments: {} })).status).toBe("201");
  const bind = await node.request("POST", "/bindings", { source: x, binding_key: "k1", arguments: {}, destination_queue: q });
  expect(bind.status).toBe("204");
  const ch = await channel();
  ch.publish(x, "k1", Buffer.from("via-m10-binding"));
  expect((await nextMessage(ch, q, 2000, { noAck: true }))?.content.toString()).toBe("via-m10-binding");
  const unbind = await node.request("DELETE", `/bindings/src=${enc(x)};dstq=${enc(q)};key=k1;args=`);
  expect(unbind.status).toBe("204");
  ch.publish(x, "k1", Buffer.from("gone"));
  expect(await nextMessage(ch, q, 400, { noAck: true })).toBeNull();
  expect((await node.request("DELETE", `/exchanges/${enc(x)}`)).status).toBe("204");
  expect((await node.request("DELETE", `/queues/${enc(q)}`)).status).toBe("200");
  node.close();
});

test("AMQP 1.0 management :: DELETE /queues/:q/messages purges and reports the count", async () => {
  const node = await openNode();
  const q = uniq("m10-purge");
  await node.request("PUT", `/queues/${enc(q)}`, { durable: true, arguments: {} });
  const ch = await channel();
  for (let i = 0; i < 3; i++) ch.sendToQueue(q, Buffer.from(String(i)));
  await new Promise((r) => setTimeout(r, 300));
  const purge = await node.request("DELETE", `/queues/${enc(q)}/messages`);
  expect(purge.status).toBe("200");
  expect(Number(purge.body?.message_count)).toBe(3);
  await node.request("DELETE", `/queues/${enc(q)}`);
  node.close();
});

const refusal = (p: Promise<unknown>) => p.then(() => "allowed", (e) => (e instanceof Refused ? e.condition : String(e)));

test("AMQP 1.0 management :: a declare outside the user's permissions ends the session with unauthorized-access", async () => {
  const user = uniq("m10-user");
  await mgmt(`/api/users/${user}`, { method: "PUT", body: JSON.stringify({ password: "m10-secret", tags: "" }) });
  await mgmt(`/api/permissions/%2f/${user}`, { method: "PUT", body: JSON.stringify({ configure: "^allowed-", write: ".*", read: ".*" }) });
  const node = await openNode(user, "m10-secret");
  const ok = await node.request("PUT", `/queues/${enc(`allowed-${uniq("q")}`)}`, { durable: true, arguments: {} });
  expect(ok.status).toBe("201");
  await node.request("DELETE", `/queues/${enc(ok.body.name)}`);
  expect(await refusal(node.request("PUT", `/queues/${enc(uniq("denied"))}`, { durable: true, arguments: {} }))).toBe("amqp:unauthorized-access");
  node.close();
  await mgmt(`/api/users/${user}`, { method: "DELETE" });
});

const b64url = (b: Buffer | string) => Buffer.from(b).toString("base64url");
function token(scope: string): string {
  const header = { alg: "RS256", typ: "JWT", kid: process.env.QF_OAUTH_KID };
  const now = Math.floor(Date.now() / 1000);
  const body = { iat: now, exp: now + 600, aud: ["rabbitmq"], sub: "conf-oauth-client", scope };
  const input = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(body))}`;
  const sig = createSign("RSA-SHA256").update(input).sign(createPrivateKey(readFileSync(process.env.QF_OAUTH_KEY ?? "")));
  return `${input}.${b64url(sig)}`;
}

test("OAuth 2.0 :: AMQP 1.0 PUT /auth/tokens on the management node refreshes the token", async () => {
  const first = uniq("oa10a");
  const second = uniq("oa10b");
  const scope = (p: string) => `rabbitmq.configure:*/${p}* rabbitmq.write:*/${p}* rabbitmq.read:*/${p}*`;
  const node = await openNode("oauth", token(scope(first)));
  expect((await node.request("PUT", `/queues/${enc(`${first}-q`)}`, { durable: true, arguments: {} })).status).toBe("201");
  const refresh = await node.request("PUT", "/auth/tokens", Buffer.from(token(scope(second))));
  expect(refresh.status).toBe("204");
  expect((await node.request("PUT", `/queues/${enc(`${second}-q`)}`, { durable: true, arguments: {} })).status).toBe("201");
  await node.request("DELETE", `/queues/${enc(`${second}-q`)}`);
  expect(await refusal(node.request("PUT", `/queues/${enc(`${first}-again`)}`, { durable: true, arguments: {} }))).toBe("amqp:unauthorized-access");
  node.close();
  const ch = await channel();
  await ch.deleteQueue(`${first}-q`);
});

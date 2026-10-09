import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/index.ts";
import { parseConfig } from "../src/config.ts";
import { Store } from "../src/store.ts";
import { managementApp } from "../src/http/app.ts";
import { sessions } from "../src/http/session.ts";
import { writeProps, type BasicProps } from "../src/amqp10/map.ts";

async function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "qf-http-inspection-"));
  const cfg = parseConfig(""); cfg.dataDir = dir;
  const store = new Store(join(dir, "bun.sqlite"), "always", 10);
  const broker = new Broker(cfg, store); broker.load();
  await broker.putUser("inspector", "password12", ["management"], true);
  await broker.putPerm({ user: "inspector", vhost: "/", configure: ".*", write: ".*", read: ".*" });
  await broker.declareQueue({ vhost: "/", name: "inspect", durable: true, exclusive: false, autoDelete: false, passive: false, args: {} });
  const app = managementApp(broker, dir); app.compile();
  let cookie = "";
  const call = (path: string, body: unknown) => app.handle(new Request(`http://localhost${path}`, { method: "POST", headers: { host: "localhost", cookie, "content-type": "application/json" }, body: JSON.stringify(body) }));
  const login = await call("/api/login", { username: "inspector", password: "password12" });
  expect(login.status).toBe(200); cookie = login.headers.get("set-cookie")!.split(";")[0]!;
  const publish = (body: Uint8Array | string, props: BasicProps = { headers: [] }) => broker.publish({ vhost: "/", exchange: "", routingKey: "inspect", body: typeof body === "string" ? Buffer.from(body) : body, headers: props.headers, propRaw: writeProps(props), persistent: props.deliveryMode === 2, priority: props.priority ?? 0, expiration: "", confirm: false });
  return { broker, publish, get: (body: unknown) => call("/api/queues/%2F/inspect/get", body), queue: () => broker.queues.get(broker.key("/", "inspect"))!, close() { sessions.delete(cookie.split("=")[1]!); store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

test("inspection requeues batches without duplicate reads or unacked leaks", async () => {
  const f = await fixture();
  try {
    for (const body of ["one", "two", "three"]) await f.publish(body);
    for (const ackmode of ["ack_requeue_true", "reject_requeue_true"]) {
      const response = await f.get({ count: 20, ackmode, encoding: "auto" }); expect(response.status).toBe(200);
      const rows = await response.json();
      expect(rows.map((row: { payload: string }) => row.payload)).toEqual(["one", "two", "three"]);
      expect(rows.every((row: { redelivered: boolean }) => row.redelivered === (ackmode === "reject_requeue_true"))).toBe(true);
      expect(f.queue().ready.length).toBe(3); expect(f.queue().unacked.size).toBe(0);
    }
    expect((await (await f.get({ count: 2, ackmode: "ack_requeue_false" })).json()).map((row: { payload: string }) => row.payload)).toEqual(["one", "two"]);
    expect(f.queue().ready.length).toBe(1);
    await f.get({ ackmode: "reject_requeue_false" });
    expect(f.queue().ready.length).toBe(0); expect(f.queue().unacked.size).toBe(0);
  } finally { f.close(); }
});

test("inspection exposes basic properties and recursive x-death headers", async () => {
  const f = await fixture();
  try {
    await f.publish("body", { contentType: "text/plain", contentEncoding: "utf-8", deliveryMode: 2, priority: 3, correlationId: "correlation", replyTo: "reply", expiration: "60000", messageId: "message", timestamp: 1234567, type: "event", userId: "inspector", appId: "app", headers: [
      ["x-death", { t: "A", v: [{ t: "F", v: [["queue", { t: "S", v: "source" }], ["count", { t: "l", v: 2 }], ["time", { t: "T", v: 1234 }], ["routing-keys", { t: "A", v: [{ t: "S", v: "original" }] }]] }] }],
      ["nested", { t: "F", v: [["enabled", { t: "t", v: true }], ["empty", { t: "V" }]] }],
      ["binary", { t: "x", v: new Uint8Array([255, 0]) }],
    ] });
    const [row] = await (await f.get({ ackmode: "ack_requeue_true" })).json();
    expect(row.properties).toEqual({ content_type: "text/plain", content_encoding: "utf-8", delivery_mode: 2, priority: 3, correlation_id: "correlation", reply_to: "reply", expiration: "60000", message_id: "message", timestamp: 1234567, type: "event", user_id: "inspector", app_id: "app", headers: { "x-death": [{ queue: "source", count: 2, time: 1234, "routing-keys": ["original"] }], nested: { enabled: true, empty: null }, binary: { encoding: "base64", value: "/wA=" } } });
    expect(f.queue().ready.length).toBe(1); expect(f.queue().unacked.size).toBe(0);
  } finally { f.close(); }
});

test("inspection preserves binary payloads and omits absent headers", async () => {
  const f = await fixture();
  try {
    await f.publish(new Uint8Array([255, 0, 128]));
    let [row] = await (await f.get({ ackmode: "ack_requeue_true", encoding: "auto" })).json();
    expect(row.payload_encoding).toBe("base64"); expect(Buffer.from(row.payload, "base64")).toEqual(Buffer.from([255, 0, 128]));
    expect(row.payload_bytes).toBe(3); expect(row.properties).toEqual({});
    [row] = await (await f.get({ ackmode: "ack_requeue_true", encoding: "base64" })).json(); expect(row.payload_encoding).toBe("base64");
    await f.get({ ackmode: "ack_requeue_false" }); await f.publish("plain");
    [row] = await (await f.get({ ackmode: "ack_requeue_true", encoding: "auto" })).json(); expect(row.payload_encoding).toBe("string"); expect(row.payload).toBe("plain");
  } finally { f.close(); }
});

test("invalid inspection options do not dequeue messages", async () => {
  const f = await fixture();
  try {
    await f.publish("retained");
    for (const body of [{ ackmode: "invalid" }, { encoding: "invalid" }, { count: -1 }, { count: 1.5 }]) {
      expect((await f.get(body)).status).toBe(400); expect(f.queue().ready.length).toBe(1); expect(f.queue().unacked.size).toBe(0);
    }
  } finally { f.close(); }
});

test("inspection caps large batches at twenty messages", async () => {
  const f = await fixture();
  try {
    for (let i = 0; i < 25; i++) await f.publish(`message-${i}`);
    const rows = await (await f.get({ count: 100, ackmode: "ack_requeue_true" })).json();
    expect(rows.length).toBe(20); expect(new Set(rows.map((row: { payload: string }) => row.payload)).size).toBe(20);
    expect(f.queue().ready.length).toBe(25); expect(f.queue().unacked.size).toBe(0);
  } finally { f.close(); }
});

test("inspection returns held messages if a later batch read fails", async () => {
  const f = await fixture();
  const original = f.broker.get;
  try {
    await f.publish("held");
    let reads = 0;
    f.broker.get = async function (...args) {
      if (++reads === 2) throw new Error("injected read failure");
      return original.apply(this, args);
    };
    expect((await f.get({ count: 2, ackmode: "ack_requeue_true" })).status).toBe(500);
    expect(f.queue().ready.length).toBe(1); expect(f.queue().unacked.size).toBe(0);
  } finally { f.broker.get = original; f.close(); }
});

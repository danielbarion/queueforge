import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/index.ts";
import { parseConfig } from "../src/config.ts";
import { Store } from "../src/store.ts";
import { managementApp } from "../src/http/app.ts";
import { sessions } from "../src/http/session.ts";
import type { Principal } from "../src/auth/backends.ts";

async function fixture() {
  const dir = mkdtempSync(join(tmpdir(), "qf-http-permissions-"));
  const cfg = parseConfig("");
  cfg.dataDir = dir;
  const store = new Store(join(dir, "bun.sqlite"), "always", 10);
  const broker = new Broker(cfg, store);
  broker.load();
  await broker.putUser("admin", "password12", ["administrator"], true);
  await broker.putPerm({ user: "admin", vhost: "/", configure: ".*", write: ".*", read: ".*" });
  await broker.putUser("restricted", "password12", ["management"], true);
  await broker.declareQueue({ vhost: "/", name: "private", durable: true, exclusive: false, autoDelete: false, passive: false, args: {} });
  await broker.publish({ vhost: "/", exchange: "", routingKey: "private", body: new TextEncoder().encode("secret"), headers: [], propRaw: new Uint8Array(), persistent: false, priority: 0, expiration: "", confirm: false });
  const app = managementApp(broker, dir);
  app.compile();
  const tokens: string[] = [];
  const call = (path: string, cookie = "", method = "GET", body?: unknown, authorization?: string) => app.handle(new Request(`http://localhost${path}`, {
    method, headers: { host: "localhost", cookie, "content-type": "application/json", ...(authorization ? { authorization } : {}) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  }));
  const login = async (username: string, password = "password12") => {
    const response = await call("/api/login", "", "POST", { username, password });
    expect(response.status).toBe(200);
    const cookie = response.headers.get("set-cookie")!.split(";")[0]!;
    tokens.push(cookie.split("=")[1]!);
    return cookie;
  };
  return { broker, call, login, close() { for (const t of tokens) sessions.delete(t); store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

test("management access does not grant queue read or exchange write", async () => {
  const f = await fixture();
  try {
    const cookie = await f.login("restricted");
    expect((await f.call("/api/queues/%2F/private/get", cookie, "POST", { ackmode: "ack_requeue_false" })).status).toBe(403);
    expect((await f.call("/api/exchanges/%2F/amq.direct/publish", cookie, "POST", { routing_key: "private", payload: "unauthorized" })).status).toBe(403);
    expect(f.broker.queues.get(f.broker.key("/", "private"))!.ready.length).toBe(1);
    await f.broker.putPerm({ user: "restricted", vhost: "/", configure: "^$", write: "^$", read: "^other$" });
    expect((await f.call("/api/queues/%2F/private/get", cookie, "POST", {})).status).toBe(403);
    expect((await f.call("/api/queues/%2F/private", cookie, "DELETE")).status).toBe(403);
    expect((await f.call("/api/queues/%2F/new", cookie, "PUT", { durable: true })).status).toBe(403);
    await f.broker.putPerm({ user: "restricted", vhost: "/", configure: "^$", write: "^$", read: "^private$" });
    const response = await f.call("/api/queues/%2F/private/get", cookie, "POST", { ackmode: "ack_requeue_false" });
    expect(response.status).toBe(200);
    expect((await response.json())[0].payload).toBe("secret");
  } finally { f.close(); }
});

test("management bindings enforce exchange read and queue write", async () => {
  const f = await fixture();
  try {
    const cookie = await f.login("restricted");
    await f.broker.putPerm({ user: "restricted", vhost: "/", configure: ".*", write: "^$", read: ".*" });
    const body = { source: "amq.direct", destination: "private", routing_key: "key" };
    expect((await f.call("/api/bindings/%2F", cookie, "POST", body)).status).toBe(403);
    await f.broker.putPerm({ user: "restricted", vhost: "/", configure: "^$", write: "^private$", read: "^amq\\.direct$" });
    expect((await f.call("/api/bindings/%2F", cookie, "POST", body)).status).toBe(201);
  } finally { f.close(); }
});

test("cookie sessions reject deletion and password rotation", async () => {
  const f = await fixture();
  try {
    const cookie = await f.login("restricted");
    await f.broker.putUser("restricted", "newpassword12", ["management"], false);
    expect((await f.call("/api/whoami", cookie)).status).toBe(401);
    const next = await f.login("restricted", "newpassword12");
    await f.broker.deleteUser("restricted");
    expect((await f.call("/api/whoami", next)).status).toBe(401);
  } finally { f.close(); }
});

test("cookie and Basic sessions use current tags", async () => {
  const f = await fixture();
  try {
    const cookie = await f.login("admin");
    const basic = `Basic ${btoa("admin:password12")}`;
    expect((await f.call("/api/users", "", "GET", undefined, basic)).status).toBe(200);
    await f.broker.putUser("admin", null, ["management"], false);
    expect((await f.call("/api/users", cookie)).status).toBe(403);
    expect((await f.call("/api/users", "", "GET", undefined, basic)).status).toBe(403);
    await f.broker.putUser("admin", null, [], false);
    expect((await f.call("/api/whoami", cookie)).status).toBe(401);
    expect((await f.call("/api/whoami", "", "GET", undefined, basic)).status).toBe(401);
  } finally { f.close(); }
});

test("OAuth cannot authenticate an existing internal username", async () => {
  const f = await fixture();
  try {
    let calls = 0;
    f.broker.oauth = { login: async () => { calls++; return { sub: "outsider", principal: { source: "oauth", scopes: [], tags: [], expiresAt: Date.now() + 60000 } }; } } as typeof f.broker.oauth;
    expect(await f.broker.verify("admin", "a.b.c")).toBe(false);
    expect(calls).toBe(0);
    expect(await f.broker.verify("outsider", "a.b.c")).toBe(true);
    expect(f.broker.can("outsider", "/", "read", "private")).toBe(false);
  } finally { f.close(); }
});

test("OAuth management cookie and Basic cache expire with their principal", async () => {
  const f = await fixture();
  try {
    const principal: Principal = { source: "oauth", scopes: [], tags: ["administrator"], expiresAt: Date.now() + 60000 };
    f.broker.oauth = { login: async () => principal.expiresAt! > Date.now() ? { sub: "external", principal } : null } as typeof f.broker.oauth;
    const cookie = await f.login("external", "a.b.c");
    const basic = `Basic ${btoa("external:a.b.c")}`;
    expect((await f.call("/api/users", "", "GET", undefined, basic)).status).toBe(200);
    principal.expiresAt = Date.now() - 1;
    expect((await f.call("/api/users", cookie)).status).toBe(401);
    expect((await f.call("/api/users", "", "GET", undefined, basic)).status).toBe(401);
  } finally { f.close(); }
});

test("equivalent external re-logins keep independent cookie and Basic sessions usable", async () => {
  const f = await fixture();
  try {
    const expiresAt = Date.now() + 60000;
    f.broker.oauth = { login: async () => ({ sub: "external", principal: { source: "oauth", tags: ["management"], scopes: [], expiresAt } }) } as typeof f.broker.oauth;
    const first = await f.login("external", "a.b.c");
    const basic = `Basic ${btoa("external:a.b.c")}`;
    expect((await f.call("/api/whoami", "", "GET", undefined, basic)).status).toBe(200);
    expect((await f.call("/api/whoami", first)).status).toBe(200);
    const second = await f.login("external", "a.b.c");
    expect((await f.call("/api/whoami", first)).status).toBe(200);
    expect((await f.call("/api/whoami", second)).status).toBe(200);
  } finally { f.close(); }
});

test("management topic bindings enforce routing-key read restrictions", async () => {
  const f = await fixture();
  try {
    const cookie = await f.login("restricted");
    await f.broker.putPerm({ user: "restricted", vhost: "/", configure: "^$", write: "^private$", read: "^amq\\.topic$" });
    f.broker.putTopicPerm({ user: "restricted", vhost: "/", exchange: "amq.topic", write: ".*", read: "^allowed$" });
    expect((await f.call("/api/bindings/%2F", cookie, "POST", { source: "amq.topic", destination: "private", routing_key: "forbidden" })).status).toBe(403);
    expect((await f.call("/api/bindings/%2F", cookie, "POST", { source: "amq.topic", destination: "private", routing_key: "allowed" })).status).toBe(201);
    expect((await f.call("/api/bindings/%2F/amq.topic/private/forbidden", cookie, "DELETE")).status).toBe(403);
  } finally { f.close(); }
});

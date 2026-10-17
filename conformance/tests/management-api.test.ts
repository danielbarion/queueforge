/**
 * The RabbitMQ management HTTP API surface that tools build on (rabbitmqadmin,
 * the Kubernetes messaging-topology operator, the Terraform provider,
 * monitoring exporters). Each test drives one group of documented /api routes
 * and checks the fields those tools read.
 */
import { expect, test } from "bun:test";
import { channel, confirmChannel, eventually, mgmt, sleep, uniq } from "../lib.ts";

const json = async (path: string, init: RequestInit = {}) => {
  const res = await mgmt(path, init);
  const text = await res.text();
  let body: any = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  return { status: res.status, body };
};
const put = (path: string, body: unknown) => json(path, { method: "PUT", body: JSON.stringify(body) });
const post = (path: string, body: unknown) => json(path, { method: "POST", body: JSON.stringify(body) });
const del = (path: string) => json(path, { method: "DELETE" });
const enc = encodeURIComponent;
const ok = (status: number) => status >= 200 && status < 300;

test("Management HTTP API :: overview, whoami, nodes, extensions and cluster name answer with their fields", async () => {
  const overview = await json("/api/overview");
  expect(overview.status).toBe(200);
  for (const key of ["management_version", "rabbitmq_version", "cluster_name", "node", "object_totals", "queue_totals", "message_stats", "listeners"]) {
    expect(overview.body).toHaveProperty(key);
  }
  for (const key of ["connections", "channels", "exchanges", "queues", "consumers"]) expect(overview.body.object_totals).toHaveProperty(key);
  const who = await json("/api/whoami");
  expect(who.body.name).toBe("admin");
  const nodes = await json("/api/nodes");
  expect(Array.isArray(nodes.body)).toBe(true);
  expect(nodes.body.length).toBeGreaterThan(0);
  // Memory and disk figures arrive with the first stats sample, so only identity is checked.
  for (const key of ["name", "running", "type"]) expect(nodes.body[0]).toHaveProperty(key);
  const node = await json(`/api/nodes/${enc(nodes.body[0].name)}`);
  expect(node.status).toBe(200);
  expect(node.body.name).toBe(nodes.body[0].name);
  expect((await json("/api/extensions")).status).toBe(200);
  const name = await json("/api/cluster-name");
  expect(typeof name.body.name).toBe("string");
});

test("Management HTTP API :: vhosts are created with metadata, listed, read and deleted", async () => {
  const vh = uniq("api-vh");
  expect(ok((await put(`/api/vhosts/${enc(vh)}`, { description: "made by the api test", tags: "conf,api", default_queue_type: "quorum" })).status)).toBe(true);
  const one = await json(`/api/vhosts/${enc(vh)}`);
  expect(one.status).toBe(200);
  expect(one.body.name).toBe(vh);
  expect(one.body.description).toBe("made by the api test");
  expect(one.body.default_queue_type).toBe("quorum");
  const tags = Array.isArray(one.body.tags) ? one.body.tags : String(one.body.tags).split(",");
  expect(tags).toContain("api");
  const all = await json("/api/vhosts");
  expect(all.body.some((v: { name: string }) => v.name === vh)).toBe(true);
  const perms = await json(`/api/vhosts/${enc(vh)}/permissions`);
  expect(Array.isArray(perms.body)).toBe(true);
  expect(ok((await del(`/api/vhosts/${enc(vh)}`)).status)).toBe(true);
  expect((await json(`/api/vhosts/${enc(vh)}`)).status).toBe(404);
});

test("Management HTTP API :: users, bulk delete, permissions and topic permissions round trip", async () => {
  const u1 = uniq("api-u1");
  const u2 = uniq("api-u2");
  expect(ok((await put(`/api/users/${enc(u1)}`, { password: "api-secret-1", tags: "monitoring" })).status)).toBe(true);
  expect(ok((await put(`/api/users/${enc(u2)}`, { password: "api-secret-2", tags: "" })).status)).toBe(true);
  const one = await json(`/api/users/${enc(u1)}`);
  expect(one.body.name).toBe(u1);
  const tags = Array.isArray(one.body.tags) ? one.body.tags : String(one.body.tags).split(",");
  expect(tags).toContain("monitoring");
  expect(one.body).toHaveProperty("password_hash");
  expect(one.body).toHaveProperty("hashing_algorithm");
  const noperm = await json("/api/users/without-permissions");
  expect(noperm.body.some((u: { name: string }) => u.name === u1)).toBe(true);
  expect(ok((await put(`/api/permissions/%2f/${enc(u1)}`, { configure: "^api-", write: ".*", read: ".*" })).status)).toBe(true);
  const p = await json(`/api/permissions/%2f/${enc(u1)}`);
  expect(p.body.configure).toBe("^api-");
  const byUser = await json(`/api/users/${enc(u1)}/permissions`);
  expect(byUser.body.some((x: { vhost: string }) => x.vhost === "/")).toBe(true);
  expect(ok((await put(`/api/topic-permissions/%2f/${enc(u1)}`, { exchange: "amq.topic", write: "^a\\.", read: ".*" })).status)).toBe(true);
  const tp = await json(`/api/topic-permissions/%2f/${enc(u1)}`);
  expect(tp.body.some((x: { exchange: string; write: string }) => x.exchange === "amq.topic" && x.write === "^a\\.")).toBe(true);
  expect((await json("/api/topic-permissions")).status).toBe(200);
  expect((await json("/api/permissions")).body.some((x: { user: string }) => x.user === u1)).toBe(true);
  expect(ok((await del(`/api/topic-permissions/%2f/${enc(u1)}`)).status)).toBe(true);
  expect(ok((await del(`/api/permissions/%2f/${enc(u1)}`)).status)).toBe(true);
  expect(ok((await post("/api/users/bulk-delete", { users: [u1, u2] })).status)).toBe(true);
  expect((await json(`/api/users/${enc(u1)}`)).status).toBe(404);
  expect((await json(`/api/users/${enc(u2)}`)).status).toBe(404);
});

test("Management HTTP API :: a user created from a password hash logs in", async () => {
  // rabbit_password_hashing_sha256 of "hash-login-pw" with a fixed salt.
  const salt = Buffer.from([0x90, 0x8d, 0xc6, 0x0a]);
  const digest = new Bun.CryptoHasher("sha256").update(Buffer.concat([salt, Buffer.from("hash-login-pw")])).digest();
  const hash = Buffer.concat([salt, digest]).toString("base64");
  const u = uniq("api-hash");
  expect(ok((await put(`/api/users/${enc(u)}`, { password_hash: hash, hashing_algorithm: "rabbit_password_hashing_sha256", tags: "" })).status)).toBe(true);
  await put(`/api/permissions/%2f/${enc(u)}`, { configure: ".*", write: ".*", read: ".*" });
  // No management tag, so the login is checked over AMQP.
  const amqp = (await import("amqplib")).default;
  const port = process.env.QF_AMQP_PORT ?? "5672";
  const c = await amqp.connect(`amqp://${u}:hash-login-pw@127.0.0.1:${port}/%2f`);
  await c.close();
  await expect(amqp.connect(`amqp://${u}:wrong-pw@127.0.0.1:${port}/%2f`)).rejects.toBeDefined();
  await del(`/api/users/${enc(u)}`);
});

test("Management HTTP API :: queues are declared, listed with stats, paged, purged and deleted", async () => {
  const q = uniq("api-q");
  expect(ok((await put(`/api/queues/%2f/${enc(q)}`, { durable: true, auto_delete: false, arguments: { "x-max-length": 100 } })).status)).toBe(true);
  const ch = await confirmChannel();
  for (let i = 0; i < 3; i++) ch.sendToQueue(q, Buffer.from(`m${i}`), { persistent: true });
  await ch.waitForConfirms();
  // RabbitMQ refreshes queue stats every few seconds.
  const counted = await eventually(async () => (await json(`/api/queues/%2f/${enc(q)}`)).body?.messages === 3, 12000);
  expect(counted).toBe(true);
  const info = (await json(`/api/queues/%2f/${enc(q)}`)).body;
  for (const key of ["name", "vhost", "durable", "auto_delete", "exclusive", "arguments", "node", "state", "type", "messages", "messages_ready", "messages_unacknowledged", "consumers"]) {
    expect(info).toHaveProperty(key);
  }
  expect(info.arguments["x-max-length"]).toBe(100);
  const list = await json("/api/queues/%2f");
  expect(list.body.some((x: { name: string }) => x.name === q)).toBe(true);
  const page = await json(`/api/queues?page=1&page_size=100&name=${enc(q)}&use_regex=false`);
  expect(page.status).toBe(200);
  for (const key of ["items", "page", "page_size", "page_count", "item_count", "filtered_count", "total_count"]) expect(page.body).toHaveProperty(key);
  expect(page.body.items.some((x: { name: string }) => x.name === q)).toBe(true);
  const cols = await json(`/api/queues/%2f?columns=name,messages`);
  expect(Object.keys(cols.body.find((x: { name: string }) => x.name === q)).sort()).toEqual(["messages", "name"]);
  const got = await post(`/api/queues/%2f/${enc(q)}/get`, { count: 2, ackmode: "ack_requeue_true", encoding: "auto" });
  expect(got.body.length).toBe(2);
  expect(got.body[0].payload).toBe("m0");
  for (const key of ["payload", "payload_bytes", "redelivered", "exchange", "routing_key", "message_count", "properties"]) expect(got.body[0]).toHaveProperty(key);
  expect(ok((await del(`/api/queues/%2f/${enc(q)}/contents`)).status)).toBe(true);
  expect(await eventually(async () => (await json(`/api/queues/%2f/${enc(q)}`)).body?.messages === 0, 12000)).toBe(true);
  expect(ok((await del(`/api/queues/%2f/${enc(q)}?if-empty=true`)).status)).toBe(true);
  expect((await json(`/api/queues/%2f/${enc(q)}`)).status).toBe(404);
}, 40000);

test("Management HTTP API :: exchanges are declared, published to, listed with bindings, and deleted", async () => {
  const x = uniq("api-x");
  const q = uniq("api-xq");
  expect(ok((await put(`/api/exchanges/%2f/${enc(x)}`, { type: "topic", durable: true, auto_delete: false, internal: false, arguments: {} })).status)).toBe(true);
  expect(ok((await put(`/api/queues/%2f/${enc(q)}`, { durable: true })).status)).toBe(true);
  const created = await post(`/api/bindings/%2f/e/${enc(x)}/q/${enc(q)}`, { routing_key: "a.*", arguments: {} });
  expect(created.status).toBe(201);
  const pub = await post(`/api/exchanges/%2f/${enc(x)}/publish`, { properties: { delivery_mode: 2, headers: { h: "v" } }, routing_key: "a.b", payload: "via-api", payload_encoding: "string" });
  expect(pub.body.routed).toBe(true);
  const miss = await post(`/api/exchanges/%2f/${enc(x)}/publish`, { properties: {}, routing_key: "z.z", payload: "nowhere", payload_encoding: "string" });
  expect(miss.body.routed).toBe(false);
  const ex = await json(`/api/exchanges/%2f/${enc(x)}`);
  for (const key of ["name", "vhost", "type", "durable", "auto_delete", "internal", "arguments"]) expect(ex.body).toHaveProperty(key);
  expect(ex.body.type).toBe("topic");
  const src = await json(`/api/exchanges/%2f/${enc(x)}/bindings/source`);
  expect(src.body.some((b: { destination: string; routing_key: string; properties_key: string }) => b.destination === q && b.routing_key === "a.*" && typeof b.properties_key === "string")).toBe(true);
  const qb = await json(`/api/queues/%2f/${enc(q)}/bindings`);
  expect(qb.body.some((b: { source: string }) => b.source === x)).toBe(true);
  const pair = await json(`/api/bindings/%2f/e/${enc(x)}/q/${enc(q)}`);
  expect(pair.body.length).toBe(1);
  const pk = pair.body[0].properties_key;
  expect((await json(`/api/bindings/%2f/e/${enc(x)}/q/${enc(q)}/${enc(pk)}`)).status).toBe(200);
  const got = await post(`/api/queues/%2f/${enc(q)}/get`, { count: 1, ackmode: "ack_requeue_false", encoding: "auto" });
  expect(got.body[0]?.payload).toBe("via-api");
  expect(got.body[0]?.properties?.headers?.h).toBe("v");
  expect(ok((await del(`/api/bindings/%2f/e/${enc(x)}/q/${enc(q)}/${enc(pk)}`)).status)).toBe(true);
  expect((await json(`/api/bindings/%2f/e/${enc(x)}/q/${enc(q)}`)).body.length).toBe(0);
  expect((await json("/api/exchanges/%2f")).body.some((e: { name: string }) => e.name === "amq.direct")).toBe(true);
  expect(ok((await del(`/api/exchanges/%2f/${enc(x)}?if-unused=true`)).status)).toBe(true);
  expect((await json(`/api/exchanges/%2f/${enc(x)}`)).status).toBe(404);
  await del(`/api/queues/%2f/${enc(q)}`);
});

test("Management HTTP API :: exchange-to-exchange bindings are made and listed by destination", async () => {
  const a = uniq("api-e2e-a");
  const b = uniq("api-e2e-b");
  await put(`/api/exchanges/%2f/${enc(a)}`, { type: "fanout", durable: true });
  await put(`/api/exchanges/%2f/${enc(b)}`, { type: "fanout", durable: true });
  expect((await post(`/api/bindings/%2f/e/${enc(a)}/e/${enc(b)}`, { routing_key: "", arguments: {} })).status).toBe(201);
  const dest = await json(`/api/exchanges/%2f/${enc(b)}/bindings/destination`);
  expect(dest.body.some((x: { source: string; destination_type: string }) => x.source === a && x.destination_type === "exchange")).toBe(true);
  expect((await json("/api/bindings/%2f")).body.some((x: { source: string; destination: string }) => x.source === a && x.destination === b)).toBe(true);
  await del(`/api/exchanges/%2f/${enc(a)}`);
  await del(`/api/exchanges/%2f/${enc(b)}`);
});

test("Management HTTP API :: connections, channels and consumers are listed, read and closed", async () => {
  const name = uniq("api-conn");
  const amqp = (await import("amqplib")).default;
  const c = await amqp.connect(process.env.QF_AMQP!, { clientProperties: { connection_name: name } });
  c.on("error", () => {});
  let closed = false;
  c.on("close", () => {
    closed = true;
  });
  const ch = await c.createChannel();
  const q = uniq("api-cq");
  await ch.assertQueue(q, { durable: true });
  await ch.consume(q, () => {}, { consumerTag: `${name}-tag` });
  type Conn = { name: string; user: string; vhost: string; client_properties?: { connection_name?: string }; channels: number };
  let conn: Conn | undefined;
  expect(
    await eventually(async () => {
      conn = ((await json("/api/connections")).body as Conn[]).find((x) => x.client_properties?.connection_name === name);
      return !!conn;
    }, 12000),
  ).toBe(true);
  for (const key of ["name", "user", "vhost", "peer_host", "peer_port", "protocol", "state", "channels", "client_properties", "connected_at"]) expect(conn).toHaveProperty(key);
  expect((await json(`/api/connections/${enc(conn!.name)}`)).body.name).toBe(conn!.name);
  expect((await json("/api/vhosts/%2f/connections")).body.some((x: Conn) => x.name === conn!.name)).toBe(true);
  expect(await eventually(async () => ((await json(`/api/connections/${enc(conn!.name)}/channels`)).body ?? []).length === 1, 12000)).toBe(true);
  const chans = await json("/api/channels");
  const mine = chans.body.find((x: { connection_details?: { name: string } }) => x.connection_details?.name === conn!.name);
  expect(mine).toBeDefined();
  expect((await json(`/api/channels/${enc(mine.name)}`)).status).toBe(200);
  expect(
    await eventually(async () => ((await json("/api/consumers/%2f")).body ?? []).some((x: { consumer_tag: string; queue: { name: string } }) => x.consumer_tag === `${name}-tag` && x.queue.name === q), 12000),
  ).toBe(true);
  const res = await mgmt(`/api/connections/${enc(conn!.name)}`, { method: "DELETE", headers: { "X-Reason": "closed by the api test" } });
  expect(ok(res.status)).toBe(true);
  expect(await eventually(() => closed, 5000)).toBe(true);
  const admin = await channel();
  await admin.deleteQueue(q);
}, 40000);

test("Management HTTP API :: policies, operator policies and global parameters round trip", async () => {
  const p = uniq("api-pol");
  expect(ok((await put(`/api/policies/%2f/${enc(p)}`, { pattern: "^api-pol-", definition: { "max-length": 10 }, priority: 3, "apply-to": "queues" })).status)).toBe(true);
  const one = await json(`/api/policies/%2f/${enc(p)}`);
  expect(one.body.definition["max-length"]).toBe(10);
  expect(one.body["apply-to"]).toBe("queues");
  expect((await json("/api/policies")).body.some((x: { name: string }) => x.name === p)).toBe(true);
  expect(ok((await del(`/api/policies/%2f/${enc(p)}`)).status)).toBe(true);
  expect((await json(`/api/policies/%2f/${enc(p)}`)).status).toBe(404);
  const op = uniq("api-op");
  expect(ok((await put(`/api/operator-policies/%2f/${enc(op)}`, { pattern: "^api-op-", definition: { "max-length": 5 }, priority: 1, "apply-to": "queues" })).status)).toBe(true);
  expect((await json("/api/operator-policies/%2f")).body.some((x: { name: string }) => x.name === op)).toBe(true);
  expect(ok((await del(`/api/operator-policies/%2f/${enc(op)}`)).status)).toBe(true);
  const g = uniq("api-global");
  expect(ok((await put(`/api/global-parameters/${enc(g)}`, { name: g, value: { team: "conf" } })).status)).toBe(true);
  expect((await json(`/api/global-parameters/${enc(g)}`)).body.value.team).toBe("conf");
  expect(ok((await del(`/api/global-parameters/${enc(g)}`)).status)).toBe(true);
});

test("Management HTTP API :: vhost and user limits are set, listed and cleared", async () => {
  const vh = uniq("api-lim");
  await put(`/api/vhosts/${enc(vh)}`, {});
  expect(ok((await put(`/api/vhost-limits/${enc(vh)}/max-queues`, { value: 7 })).status)).toBe(true);
  const vl = await json(`/api/vhost-limits/${enc(vh)}`);
  expect(vl.body.some((x: { value: Record<string, number> }) => x.value["max-queues"] === 7)).toBe(true);
  expect((await json("/api/vhost-limits")).status).toBe(200);
  expect(ok((await del(`/api/vhost-limits/${enc(vh)}/max-queues`)).status)).toBe(true);
  const u = uniq("api-ulim");
  await put(`/api/users/${enc(u)}`, { password: "x-secret", tags: "" });
  expect(ok((await put(`/api/user-limits/${enc(u)}/max-channels`, { value: 9 })).status)).toBe(true);
  const ul = await json(`/api/user-limits/${enc(u)}`);
  expect(ul.body.some((x: { value: Record<string, number> }) => x.value["max-channels"] === 9)).toBe(true);
  expect(ok((await del(`/api/user-limits/${enc(u)}/max-channels`)).status)).toBe(true);
  await del(`/api/users/${enc(u)}`);
  await del(`/api/vhosts/${enc(vh)}`);
});

test("Management HTTP API :: health checks answer ok", async () => {
  for (const path of [
    "/api/health/checks/alarms",
    "/api/health/checks/local-alarms",
    "/api/health/checks/virtual-hosts",
    "/api/health/checks/port-listener/" + (process.env.QF_TARGET === "rabbitmq" ? "5672" : String(process.env.QF_AMQP_PORT)),
    "/api/health/checks/protocol-listener/amqp",
    "/api/health/checks/node-is-quorum-critical",
    "/api/health/checks/is-in-service",
  ]) {
    const r = await json(path);
    expect([path, r.status]).toEqual([path, 200]);
    expect(r.body.status).toBe("ok");
  }
  expect((await json("/api/aliveness-test/%2f")).body.status).toBe("ok");
});

test("Management HTTP API :: per-vhost definitions export and import", async () => {
  const vh = uniq("api-defs");
  await put(`/api/vhosts/${enc(vh)}`, {});
  await put(`/api/permissions/${enc(vh)}/admin`, { configure: ".*", write: ".*", read: ".*" });
  await put(`/api/queues/${enc(vh)}/kept`, { durable: true });
  const exp = await json(`/api/definitions/${enc(vh)}`);
  expect(exp.status).toBe(200);
  expect(exp.body.queues.some((x: { name: string }) => x.name === "kept")).toBe(true);
  const imported = await post(`/api/definitions/${enc(vh)}`, { queues: [{ name: "imported", durable: true, auto_delete: false, arguments: {} }], exchanges: [], bindings: [] });
  expect(ok(imported.status)).toBe(true);
  expect(await eventually(async () => (await json(`/api/queues/${enc(vh)}/imported`)).status === 200, 5000)).toBe(true);
  await del(`/api/vhosts/${enc(vh)}`);
});

test("Management HTTP API :: feature flags list and the queue rebalance endpoint answer", async () => {
  const flags = await json("/api/feature-flags");
  expect(flags.body.some((f: { name: string; state: string }) => f.name === "quorum_queue" && f.state === "enabled")).toBe(true);
  const rebalance = await post("/api/rebalance/queues", {});
  expect(ok(rebalance.status)).toBe(true);
  await sleep(10);
});

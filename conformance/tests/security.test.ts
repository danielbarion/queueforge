import { expect, test } from "bun:test";
import amqp from "amqplib";
import { AMQP_URL, closeCode, mgmt, sleep, uniq } from "../lib.ts";

function urlFor(user: string, password: string, vhost = "/"): string {
  const u = new URL(AMQP_URL);
  u.username = user;
  u.password = password;
  u.pathname = `/${encodeURIComponent(vhost)}`;
  return u.toString();
}

async function makeUser(perms: { configure: string; write: string; read: string }, vhost = "/") {
  const name = uniq("user");
  const password = "pw-" + Math.random().toString(36).slice(2);
  expect((await mgmt(`/api/users/${name}`, { method: "PUT", body: JSON.stringify({ password, tags: "" }) })).ok).toBe(true);
  const res = await mgmt(`/api/permissions/${encodeURIComponent(vhost)}/${name}`, { method: "PUT", body: JSON.stringify(perms) });
  expect(res.ok).toBe(true);
  return { name, password };
}

async function open(url: string) {
  const c = await amqp.connect(url);
  c.on("error", () => {});
  const ch = await c.createChannel();
  ch.on("error", () => {});
  return { c, ch };
}

test("Users, vhosts, permissions :: a configure pattern limits what a user may declare", async () => {
  const prefix = uniq("allowed");
  const user = await makeUser({ configure: `^${prefix}`, write: ".*", read: ".*" });
  const { c, ch } = await open(urlFor(user.name, user.password));
  await ch.assertQueue(`${prefix}-ok`, { durable: true });
  expect(await closeCode(ch.assertQueue(uniq("denied"), { durable: true }))).toBe(403);
  await c.close().catch(() => {});
  await mgmt(`/api/users/${user.name}`, { method: "DELETE" });
});

test("Users, vhosts, permissions :: a read pattern limits what a user may consume", async () => {
  const q = uniq("readable");
  const user = await makeUser({ configure: ".*", write: ".*", read: "^nothing$" });
  const { c, ch } = await open(urlFor(user.name, user.password));
  await ch.assertQueue(q, { durable: true });
  expect(await closeCode(ch.consume(q, () => {}))).toBe(403);
  await c.close().catch(() => {});
  await mgmt(`/api/users/${user.name}`, { method: "DELETE" });
});

test("Users, vhosts, permissions :: a user without access to a vhost cannot open it", async () => {
  const vhost = uniq("vh");
  expect((await mgmt(`/api/vhosts/${encodeURIComponent(vhost)}`, { method: "PUT", body: "{}" })).ok).toBe(true);
  const user = await makeUser({ configure: ".*", write: ".*", read: ".*" });
  let refused = false;
  try {
    const c = await amqp.connect(urlFor(user.name, user.password, vhost));
    await c.close();
  } catch {
    refused = true;
  }
  expect(refused).toBe(true);
  await mgmt(`/api/users/${user.name}`, { method: "DELETE" });
  await mgmt(`/api/vhosts/${encodeURIComponent(vhost)}`, { method: "DELETE" });
});

test("Topic permissions :: a topic write pattern refuses other routing keys", async () => {
  const user = await makeUser({ configure: ".*", write: ".*", read: ".*" });
  const res = await mgmt(`/api/topic-permissions/%2f/${user.name}`, {
    method: "PUT",
    body: JSON.stringify({ exchange: "amq.topic", write: "^ok\\.", read: ".*" }),
  });
  expect(res.ok).toBe(true);
  const { c, ch } = await open(urlFor(user.name, user.password));
  ch.publish("amq.topic", "ok.fine", Buffer.from("1"));
  ch.publish("amq.topic", "bad.key", Buffer.from("2"));
  expect(await closeCode(ch.checkExchange("amq.topic"))).toBe(403);
  await c.close().catch(() => {});
  await mgmt(`/api/users/${user.name}`, { method: "DELETE" });
});

test("Topic permissions :: a topic read pattern refuses binding other keys", async () => {
  const user = await makeUser({ configure: ".*", write: ".*", read: ".*" });
  await mgmt(`/api/topic-permissions/%2f/${user.name}`, {
    method: "PUT",
    body: JSON.stringify({ exchange: "amq.topic", write: ".*", read: "^mine\\." }),
  });
  const { c, ch } = await open(urlFor(user.name, user.password));
  const q = uniq("topic-read");
  await ch.assertQueue(q, { durable: true });
  await ch.bindQueue(q, "amq.topic", "mine.x");
  expect(await closeCode(ch.bindQueue(q, "amq.topic", "theirs.x"))).toBe(403);
  await c.close().catch(() => {});
  await mgmt(`/api/users/${user.name}`, { method: "DELETE" });
});

test("Vhost and user limits :: max-queues on a vhost refuses one queue too many", async () => {
  const vhost = uniq("vh-limit");
  await mgmt(`/api/vhosts/${encodeURIComponent(vhost)}`, { method: "PUT", body: "{}" });
  await mgmt(`/api/permissions/${encodeURIComponent(vhost)}/admin`, {
    method: "PUT",
    body: JSON.stringify({ configure: ".*", write: ".*", read: ".*" }),
  });
  const set = await mgmt(`/api/vhost-limits/${encodeURIComponent(vhost)}/max-queues`, { method: "PUT", body: JSON.stringify({ value: 1 }) });
  expect(set.ok).toBe(true);
  const limits = (await (await mgmt(`/api/vhost-limits/${encodeURIComponent(vhost)}`)).json()) as Array<{ value: Record<string, number> }>;
  expect(limits[0]?.value["max-queues"]).toBe(1);
  const { c, ch } = await open(urlFor("admin", "devpassword12", vhost));
  await ch.assertQueue(uniq("one"), { durable: true });
  expect(await closeCode(ch.assertQueue(uniq("two"), { durable: true }))).not.toBeNull();
  await c.close().catch(() => {});
  await mgmt(`/api/vhosts/${encodeURIComponent(vhost)}`, { method: "DELETE" });
});

test("Vhost and user limits :: max-connections on a user refuses a second connection", async () => {
  const user = await makeUser({ configure: ".*", write: ".*", read: ".*" });
  const set = await mgmt(`/api/user-limits/${user.name}/max-connections`, { method: "PUT", body: JSON.stringify({ value: 1 }) });
  expect(set.ok).toBe(true);
  const first = await amqp.connect(urlFor(user.name, user.password));
  first.on("error", () => {});
  let refused = false;
  try {
    const second = await amqp.connect(urlFor(user.name, user.password));
    second.on("error", () => {});
    await sleep(200);
    await second.createChannel();
    await second.close();
  } catch {
    refused = true;
  }
  expect(refused).toBe(true);
  await first.close().catch(() => {});
  await mgmt(`/api/users/${user.name}`, { method: "DELETE" });
});

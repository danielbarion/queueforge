import { expect, test } from "bun:test";
import amqp from "amqplib";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

async function boot(portA: number, portM: number): Promise<ChildProcess> {
  const dir = mkdtempSync(join(tmpdir(), "qf-bun-admin-"));
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
[tls]
enabled = false
`,
  );
  const child = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], {
    cwd: join(import.meta.dir, ".."),
    stdio: ["ignore", "pipe", "pipe"],
  });
  for (let i = 0; i < 50; i++) {
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

function cookiePair(setCookie: string | null): string {
  const pair = setCookie?.split(";")[0] ?? "";
  if (!pair.includes("=")) throw new Error(`missing cookie: ${setCookie}`);
  return pair;
}

test("admin pages, exchange bind flow, and port-scoped sessions", async () => {
  const portA = 46781;
  const portM = 56781;
  const child = await boot(portA, portM);
  const base = `http://127.0.0.1:${portM}`;
  try {
    const identity = await fetch(`${base}/api/identity`);
    expect(identity.status).toBe(200);
    expect(await identity.json()).toEqual({ product_name: "QueueForge", kind: "bun" });
    async function login(host: string) {
      const res = await fetch(`${base}/api/login`, {
        method: "POST",
        headers: { "content-type": "application/json", host },
        body: JSON.stringify({ username: "admin", password: "devpassword12" }),
      });
      expect(res.status).toBe(200);
      return cookiePair(res.headers.get("set-cookie"));
    }
    async function whoami(host: string, cookie: string) {
      const res = await fetch(`${base}/api/whoami`, { headers: { host, cookie } });
      return res.status;
    }

    const rustCookie = await login("127.0.0.1:36673");
    const bunCookie = await login("127.0.0.1:36674");
    expect(rustCookie.startsWith("queueforge_session_36673=")).toBe(true);
    expect(bunCookie.startsWith("queueforge_session_36674=")).toBe(true);
    expect(await whoami("127.0.0.1:36673", rustCookie)).toBe(200);
    expect(await whoami("127.0.0.1:36674", rustCookie)).toBe(401);
    expect(await whoami("127.0.0.1:36674", bunCookie)).toBe(200);
    expect(await whoami("127.0.0.1:36673", bunCookie)).toBe(401);

    const cookie = bunCookie;
    const host = "127.0.0.1:36674";
    const overview = await fetch(`${base}/api/overview`, { headers: { host, cookie } });
    expect(overview.status).toBe(200);
    const ov = (await overview.json()) as {
      product_name: string;
      object_totals: { queues: number; exchanges: number };
      message_stats: { publish: number; deliver: number; ack: number };
      queue_totals: { messages_ready: number; messages_unacknowledged: number };
    };
    expect(ov.product_name).toBe("QueueForge");
    expect(typeof ov.object_totals.exchanges).toBe("number");
    expect(typeof ov.message_stats.publish).toBe("number");
    expect(typeof ov.queue_totals.messages_ready).toBe("number");

    const exchanges = await fetch(`${base}/api/exchanges/%2F`, { headers: { host, cookie } });
    expect(exchanges.status).toBe(200);
    const exPage = (await exchanges.json()) as { items: { name: string }[] };
    expect(exPage.items.some((e) => e.name === "amq.direct")).toBe(true);

    const created = await fetch(`${base}/api/exchanges/%2F/dash.ex`, {
      method: "PUT",
      headers: { host, cookie, "content-type": "application/json" },
      body: JSON.stringify({ type: "direct", durable: true }),
    });
    expect(created.status).toBe(201);
    const queued = await fetch(`${base}/api/queues/%2F/dash.q`, {
      method: "PUT",
      headers: { host, cookie, "content-type": "application/json" },
      body: JSON.stringify({ durable: true }),
    });
    expect(queued.status).toBe(201);
    const bound = await fetch(`${base}/api/bindings/%2F`, {
      method: "POST",
      headers: { host, cookie, "content-type": "application/json" },
      body: JSON.stringify({ source: "dash.ex", destination: "dash.q", routing_key: "rk", destination_type: "queue" }),
    });
    expect(bound.status).toBe(201);

    const exAfter = (await (await fetch(`${base}/api/exchanges/%2F`, { headers: { host, cookie } })).json()) as {
      items: { name: string }[];
    };
    const bindAfter = (await (await fetch(`${base}/api/bindings/%2F`, { headers: { host, cookie } })).json()) as {
      items: { source: string; destination: string; routing_key: string }[];
    };
    expect(exAfter.items.some((e) => e.name === "dash.ex")).toBe(true);
    expect(bindAfter.items.some((b) => b.source === "dash.ex" && b.destination === "dash.q" && b.routing_key === "rk")).toBe(true);

    const idle = (await (await fetch(`${base}/api/connections`, { headers: { host, cookie } })).json()) as {
      items: { name: string }[];
      total_count: number;
    };
    expect(idle.items).toEqual([]);
    expect(idle.total_count).toBe(0);
    const amqpConn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${portA}/%2f`);
    const channel = await amqpConn.createChannel();
    await channel.assertQueue("dash.q", { durable: true });
    let live: { items: { name: string; user: string; vhost: string; channels: number; peer_port: number }[]; total_count: number } = {
      items: [],
      total_count: 0,
    };
    for (let i = 0; i < 20; i++) {
      live = (await (await fetch(`${base}/api/connections`, { headers: { host, cookie } })).json()) as typeof live;
      if (live.items.length === 1) break;
      await Bun.sleep(50);
    }
    expect(live.total_count).toBe(1);
    expect(live.items).toHaveLength(1);
    expect(live.items[0]?.user).toBe("admin");
    expect(live.items[0]?.vhost).toBe("/");
    expect(live.items[0]?.channels).toBeGreaterThan(0);
    expect(live.items[0]?.peer_port).toBeGreaterThan(0);
    const overviewLive = (await (await fetch(`${base}/api/overview`, { headers: { host, cookie } })).json()) as {
      object_totals: { connections: number };
    };
    expect(overviewLive.object_totals.connections).toBe(live.total_count);
    await amqpConn.close();
    let gone = live;
    for (let i = 0; i < 20; i++) {
      gone = (await (await fetch(`${base}/api/connections`, { headers: { host, cookie } })).json()) as typeof live;
      if (gone.items.length === 0) break;
      await Bun.sleep(50);
    }
    expect(gone.items).toEqual([]);
    expect(gone.total_count).toBe(0);
    const users = await (await fetch(`${base}/api/users`, { headers: { host, cookie } })).json();
    expect(Array.isArray(users)).toBe(true);
    expect((await fetch(`${base}/api/definitions`, { headers: { host, cookie } })).status).toBe(200);

    const published = await fetch(`${base}/api/exchanges/%2F/dash.ex/publish`, {
      method: "POST",
      headers: { host, cookie, "content-type": "application/json" },
      body: JSON.stringify({ routing_key: "rk", payload: "hello-admin", payload_encoding: "string" }),
    });
    expect(published.status).toBe(200);
    expect(((await published.json()) as { routed: boolean }).routed).toBe(true);
    const gotten = (await (
      await fetch(`${base}/api/queues/%2F/dash.q/get`, {
        method: "POST",
        headers: { host, cookie, "content-type": "application/json" },
        body: JSON.stringify({ count: 1, ackmode: "ack_requeue_false", encoding: "auto" }),
      })
    ).json()) as { payload: string }[];
    expect(gotten[0]?.payload).toBe("hello-admin");

    const metrics = await (await fetch(`${base}/metrics`)).text();
    for (const name of [
      "rabbitmq_connections",
      "rabbitmq_channels",
      "rabbitmq_global_messages_received_total",
      "rabbitmq_global_messages_delivered_total",
      "rabbitmq_global_messages_acknowledged_total",
      "rabbitmq_global_messages_confirmed_total",
      "rabbitmq_queue_messages_ready",
      "rabbitmq_consumers",
      "queueforge_confirm_before_fsync_total",
      "queueforge_wal_fsync_seconds_count",
    ]) {
      expect(metrics.includes(name)).toBe(true);
    }
    const readyLines = metrics.split("\n").filter((line) => line.startsWith("rabbitmq_queue_messages_ready{"));
    const unique = new Set(readyLines);
    expect(readyLines.length).toBe(unique.size);

    expect((await fetch(`${base}/api/bindings/%2F/dash.ex/dash.q/rk`, { method: "DELETE", headers: { host, cookie } })).status).toBe(204);
    expect((await fetch(`${base}/api/exchanges/%2F/dash.ex`, { method: "DELETE", headers: { host, cookie } })).status).toBe(204);
    const exGone = (await (await fetch(`${base}/api/exchanges/%2F`, { headers: { host, cookie } })).json()) as {
      items: { name: string }[];
    };
    expect(exGone.items.some((e) => e.name === "dash.ex")).toBe(false);

    const logout = await fetch(`${base}/api/logout`, { method: "POST", headers: { host: "127.0.0.1:36674", cookie: bunCookie } });
    expect(logout.status).toBe(204);
    expect(logout.headers.get("set-cookie") ?? "").toContain("queueforge_session_36674=");
    expect(logout.headers.get("set-cookie") ?? "").not.toContain("queueforge_session_36673");
    expect(await whoami("127.0.0.1:36673", rustCookie)).toBe(200);
    expect(await whoami("127.0.0.1:36674", bunCookie)).toBe(401);
  } finally {
    child.kill();
  }
});

test("channels, policies, limits, flags, and live connections", async () => {
  const portA = 46791;
  const portM = 56791;
  const child = await boot(portA, portM);
  const base = `http://127.0.0.1:${portM}`;
  const host = "127.0.0.1:36674";
  try {
    const login = await fetch(`${base}/api/login`, {
      method: "POST",
      headers: { "content-type": "application/json", host },
      body: JSON.stringify({ username: "admin", password: "devpassword12" }),
    });
    const queuePage = await fetch(`${base}/queues/%2F/rate.q`);
    expect(queuePage.status).toBe(200);
    expect(await queuePage.text()).toContain("QueueForge");
    const exchangePage = await fetch(`${base}/exchanges/%2F/amq.direct`);
    expect(exchangePage.status).toBe(200);
    const cookie = cookiePair(login.headers.get("set-cookie"));
    const headers = { host, cookie, "content-type": "application/json" };
    async function call(method: string, path: string, body?: unknown) {
      const res = await fetch(`${base}${path}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const text = await res.text();
      const json = text ? JSON.parse(text) : null;
      return { status: res.status, json };
    }

    expect((await call("PUT", "/api/vhosts/ops", {})).status).toBe(201);
    const vhosts = await call("GET", "/api/vhosts");
    expect(vhosts.json.items.some((v: { name: string }) => v.name === "ops")).toBe(true);

    expect((await call("PUT", "/api/permissions/admin/%2F", { configure: ".*", write: ".*", read: ".*" })).status).toBe(201);
    const perms = await call("GET", "/api/permissions");
    expect(perms.json.some((p: { user: string; vhost: string }) => p.user === "admin" && p.vhost === "/")).toBe(true);

    expect((await call("PUT", "/api/topic-permissions/admin/%2F", { exchange: "amq.topic", write: "^ok\\..*", read: "^ok\\..*" })).status).toBe(201);
    const topics = await call("GET", "/api/topic-permissions");
    expect(topics.json.items[0].exchange).toBe("amq.topic");

    const definition = {
      pattern: "^pol\\..*",
      "apply-to": "queues",
      priority: 5,
      definition: {
        "message-ttl": 1000,
        "dead-letter-exchange": "amq.direct",
        "dead-letter-routing-key": "dead",
        "max-length": 9,
        "max-length-bytes": 99,
        expires: 60000,
        overflow: "reject-publish",
        "delivery-limit": 3,
        "alternate-exchange": "amq.fanout",
      },
    };
    expect((await call("PUT", "/api/policies/%2F/pol.main", definition)).status).toBe(201);
    const policies = await call("GET", "/api/policies/%2F");
    const def = policies.json.items[0].definition;
    for (const key of ["message-ttl", "dead-letter-exchange", "dead-letter-routing-key", "max-length", "max-length-bytes", "expires", "overflow", "delivery-limit", "alternate-exchange"]) {
      expect(def[key]).toBeDefined();
    }
    expect((await call("PUT", "/api/operator-policies/%2F/pol.op", { ...definition, pattern: "^pol\\.op.*", definition: { ...definition.definition, "message-ttl": 2500 } })).status).toBe(201);
    expect((await call("PUT", "/api/queues/%2F/pol.op.q", { durable: true })).status).toBe(201);
    const queues = await call("GET", "/api/queues/%2F");
    const q = queues.json.items.find((row: { name: string }) => row.name === "pol.op.q");
    // RabbitMQ applies an operator policy as a cap: the lower TTL (the user policy's 1000) wins.
    expect(q.arguments["x-message-ttl"]).toBe(1000);
    expect(q.arguments["x-dead-letter-exchange"]).toBe("amq.direct");
    expect(q.arguments["x-max-length"]).toBe(9);
    const detail = await call("GET", "/api/queues/%2F/pol.op.q");
    expect(detail.json.operator_policy).toBe("pol.op");
    expect(detail.json.policy).toBe("pol.main");
    expect(detail.json.type).toBe("classic");
    expect((await call("PUT", "/api/queues/%2F/pol.quorum.q", { durable: true, arguments: { "x-queue-type": "quorum" } })).status).toBe(201);
    const quorum = await call("GET", "/api/queues/%2F/pol.quorum.q");
    expect(quorum.json.type).toBe("quorum");

    expect((await call("PUT", "/api/queues/%2F/transient.q", { durable: false })).status).toBe(400);
    expect((await call("POST", "/api/feature-flags/transient_nonexcl_queues/enable")).status).toBe(204);
    expect((await call("PUT", "/api/queues/%2F/transient.q", { durable: false })).status).toBe(201);
    expect((await call("POST", "/api/feature-flags/quorum_queues/disable")).status).toBe(400);
    expect((await call("DELETE", "/api/deprecated-features/transient_nonexcl_queues")).status).toBe(204);

    expect((await call("PUT", "/api/user-limits/admin/max-connections", { value: 1 })).status).toBe(204);
    expect((await call("PUT", "/api/user-limits/admin/max-channels", { value: 4 })).status).toBe(204);
    expect((await call("PUT", "/api/vhost-limits/%2F/max-connections", { value: 3 })).status).toBe(204);
    expect((await call("PUT", "/api/vhost-limits/%2F/max-queues", { value: 50 })).status).toBe(204);
    const limits = await call("GET", "/api/limits");
    expect(limits.json.user_limits[0]["max-channels"]).toBe(4);
    expect(limits.json.vhost_limits[0]["max-queues"]).toBe(50);

    const nodes = await call("GET", "/api/nodes");
    const node = nodes.json.items[0];
    expect(typeof node.name).toBe("string");
    expect(node.running).toBe(true);
    expect(typeof node.uptime).toBe("number");
    expect(typeof node.mem_used).toBe("number");
    expect(typeof node.disk_free).toBe("number");
    expect(typeof node.mem_alarm).toBe("boolean");
    expect(typeof node.disk_free_alarm).toBe("boolean");
    expect(Array.isArray(node.listeners)).toBe(true);
    expect(Array.isArray(node.peers)).toBe(true);

    const conn = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${portA}/%2f`);
    const channel = await conn.createChannel();
    const blocked = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${portA}/%2f`).then(() => "opened", () => "refused");
    expect(blocked).toBe("refused");
    await channel.assertQueue("pol.op.q", { durable: true });
    await channel.consume("pol.op.q", () => {}, { noAck: true });
    let channels: { items: { name: string; number: number }[] } = { items: [] };
    for (let i = 0; i < 20; i++) {
      channels = (await call("GET", "/api/channels")).json;
      if (channels.items.length > 0) break;
      await Bun.sleep(50);
    }
    expect(channels.items.length).toBeGreaterThan(0);
    const chName = encodeURIComponent(channels.items[0]!.name);
    expect((await call("GET", `/api/channels/${chName}`)).status).toBe(200);
    const consumers = await call("GET", "/api/consumers/%2F");
    expect(consumers.json.items.some((c: { queue: { name: string } }) => c.queue.name === "pol.op.q")).toBe(true);
    await conn.close();
    let gone = 1;
    for (let i = 0; i < 20; i++) {
      gone = (await call("GET", "/api/connections")).json.total_count;
      if (gone === 0) break;
      await Bun.sleep(50);
    }
    expect(gone).toBe(0);

    expect((await call("POST", "/api/bindings/%2F", { source: "amq.direct", destination: "pol.op.q", routing_key: "pol.op.q", destination_type: "queue" })).status).toBe(201);
    expect((await call("POST", "/api/exchanges/%2F/amq.direct/publish", { routing_key: "pol.op.q", payload: "n", payload_encoding: "string" })).json.routed).toBe(true);
    expect((await call("DELETE", "/api/bindings/%2F/amq.direct/pol.op.q/pol.op.q")).status).toBe(204);
    const got = await call("POST", "/api/queues/%2F/pol.op.q/get", { count: 1, ackmode: "ack_requeue_false" });
    expect(got.status, JSON.stringify(got.json)).toBe(200);
    expect(got.json[0].payload).toBe("n");
    expect((await call("POST", "/api/queues/%2F/pol.op.q/purge")).status).toBe(200);
    expect((await call("DELETE", "/api/queues/%2F/pol.op.q")).status).toBe(204);
    expect((await call("DELETE", "/api/operator-policies/%2F/pol.op")).status).toBe(204);
    expect((await call("DELETE", "/api/policies/%2F/pol.main")).status).toBe(204);
    expect((await call("DELETE", "/api/topic-permissions/admin/%2F/amq.topic")).status).toBe(204);
    expect((await call("DELETE", "/api/vhosts/ops")).status).toBe(204);
  } finally {
    child.kill();
  }
});

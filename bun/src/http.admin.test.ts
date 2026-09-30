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

import { expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Limits, topic permissions and runtime/global parameters survive a restart and
// round-trip through a definitions file, as they do in RabbitMQ.

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
    stdio: ["ignore", "pipe", "pipe"],
  });
  for (let i = 0; i < 80; i++) {
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

async function stop(child: ChildProcess) {
  const done = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  await done;
}

const AUTH = { authorization: `Basic ${btoa("admin:devpassword12")}`, "content-type": "application/json" };

function api(port: number) {
  return async (method: string, path: string, body?: unknown) => {
    const res = await fetch(`http://127.0.0.1:${port}${path}`, {
      method,
      headers: AUTH,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    return { status: res.status, json: text ? JSON.parse(text) : null };
  };
}

test("limits, topic permissions and parameters are stored and exported", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-bun-params-"));
  const [portA, portM] = [41871, 41881];
  let child = await boot(dir, portA, portM);
  const call = api(portM);
  try {
    expect((await call("PUT", "/api/user-limits/admin/max-connections", { value: 7 })).status).toBe(204);
    expect((await call("PUT", "/api/vhost-limits/%2F/max-queues", { value: 50 })).status).toBe(204);
    expect((await call("PUT", "/api/topic-permissions/admin/%2F", { exchange: "amq.topic", write: "^a", read: ".*" })).status).toBe(201);
    expect((await call("PUT", "/api/parameters/my-component/%2F/one", { value: { x: 1 } })).status).toBe(201);
    expect((await call("PUT", "/api/global-parameters/my-global", { value: "hello" })).status).toBe(201);
    expect((await call("PUT", "/api/cluster-name", { name: "forge" })).status).toBe(204);

    await stop(child);
    child = await boot(dir, portA, portM);

    const limits = (await call("GET", "/api/limits")).json;
    expect(limits.user_limits).toEqual([{ user: "admin", "max-connections": 7, "max-channels": null }]);
    expect(limits.vhost_limits).toEqual([{ vhost: "/", "max-connections": null, "max-queues": 50 }]);
    expect((await call("GET", "/api/topic-permissions")).json).toEqual([
      { user: "admin", vhost: "/", exchange: "amq.topic", write: "^a", read: ".*" },
    ]);
    expect((await call("GET", "/api/parameters/my-component/%2F/one")).json).toEqual({
      component: "my-component",
      vhost: "/",
      name: "one",
      value: { x: 1 },
    });
    expect((await call("GET", "/api/parameters/vhost-limits")).json).toEqual([
      { component: "vhost-limits", vhost: "/", name: "limits", value: { "max-queues": 50 } },
    ]);
    expect((await call("GET", "/api/cluster-name")).json).toEqual({ name: "forge" });

    const defs = (await call("GET", "/api/definitions")).json;
    expect(defs.users.find((u: { name: string }) => u.name === "admin").limits).toEqual({ "max-connections": 7 });
    expect(defs.topic_permissions).toHaveLength(1);
    expect(defs.parameters.map((p: { component: string }) => p.component).sort()).toEqual(["my-component", "vhost-limits"]);
    expect(defs.global_parameters).toContainEqual({ name: "my-global", value: "hello" });
    expect(defs.global_parameters).toContainEqual({ name: "cluster_name", value: "forge" });

    // A fresh broker takes the whole document.
    const dir2 = mkdtempSync(join(tmpdir(), "qf-bun-params-"));
    const other = await boot(dir2, portA + 1, portM + 3);
    const call2 = api(portM + 3);
    try {
      expect((await call2("POST", "/api/definitions", defs)).status).toBe(204);
      const again = (await call2("GET", "/api/definitions")).json;
      expect(again.parameters).toEqual(defs.parameters);
      expect(again.topic_permissions).toEqual(defs.topic_permissions);
      expect(again.global_parameters).toEqual(defs.global_parameters);
      expect((await call2("GET", "/api/limits")).json).toEqual(limits);

      expect((await call2("DELETE", "/api/parameters/vhost-limits/%2F/limits")).status).toBe(204);
      expect((await call2("GET", "/api/limits")).json.vhost_limits).toEqual([]);
      expect((await call2("DELETE", "/api/parameters/my-component/%2F/one")).status).toBe(204);
      expect((await call2("DELETE", "/api/parameters/my-component/%2F/one")).status).toBe(404);
    } finally {
      await stop(other);
    }
  } finally {
    await stop(child);
  }
}, 30_000);

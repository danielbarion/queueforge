/**
 * Start one broker process per core the cgroup grants.
 *
 * This process accepts AMQP and hands each socket to a child before any bytes
 * are read. Children do not share the port. A queue named qN is homed on the
 * child that receives its publisher and its consumer.
 */
import { spawn, type Subprocess } from "bun";
import { mkdirSync } from "node:fs";
import net from "node:net";
import { childPlans, grantedCores, memberList, type ChildPlan } from "./cores.ts";
import { splitHost, type Config } from "./config.ts";
import { mergeChildMetrics } from "./metrics-merge.ts";

export async function supervise(configPath: string, dev: boolean, cfg: Config): Promise<void> {
  const cores = grantedCores();
  const plans = childPlans(cores, cfg.dataDir);
  const members = JSON.stringify(memberList(plans));
  let stopping = false;
  let ready = 0;
  const kids = new Map<string, Subprocess>();

  const start = (plan: ChildPlan) => {
    mkdirSync(plan.dataDir, { recursive: true });
    const args = [process.execPath, "src/main.ts", "--config", configPath];
    if (dev) args.push("--dev-bootstrap");
    const child = spawn({
      cmd: args,
      cwd: process.cwd(),
      env: {
        ...process.env,
        QUEUEFORGE_CHILD: "1",
        QUEUEFORGE_NODE_ID: plan.id,
        QUEUEFORGE_DATA_DIR: plan.dataDir,
        QUEUEFORGE_CLUSTER_LISTEN: plan.clusterListen,
        QUEUEFORGE_MEMBERS: members,
        QUEUEFORGE_ADOPT: "1",
        QUEUEFORGE_BIND_MGMT: plan.bindMgmt ? "1" : "0",
        QUEUEFORGE_METRICS_ADDR: plan.metricsListen,
      },
      ipc(message: { type?: string; home?: string; state?: unknown; bytes?: Uint8Array }, _subprocess, handle) {
        if (message?.type === "ready") ready++;
        if (message?.type === "migrate" && handle && message.home && message.state && message.bytes) {
          const dest = kids.get(message.home);
          try {
            (dest as unknown as { send: (body: unknown, sock?: unknown) => void } | undefined)?.send(
              { type: "conn", state: message.state, bytes: message.bytes },
              handle,
            );
          } catch {
            (handle as { destroy?: () => void }).destroy?.();
          }
        }
      },
      serialization: "advanced",
      stdin: "ignore",
      stdout: "inherit",
      stderr: "inherit",
    });
    kids.set(plan.id, child);
    void child.exited.then((code) => {
      if (stopping) return;
      console.error(`queueforge-bun child ${plan.id} exited ${code}, restarting`);
      ready = Math.max(0, ready - 1);
      start(plan);
    });
  };

  for (const plan of plans) start(plan);
  const deadline = Date.now() + 15000;
  while (ready < plans.length && Date.now() < deadline) await Bun.sleep(20);
  if (ready < plans.length) console.error(`queueforge-bun parent ready ${ready}/${plans.length}`);

  const amqp = splitHost(cfg.amqp);
  let next = 0;
  const server = net.createServer((socket) => {
    const plan = plans[next % plans.length]!;
    next++;
    socket.setNoDelay(true);
    socket.pause();
    const child = kids.get(plan.id);
    try {
      (child as unknown as { send: (message: unknown, handle?: unknown) => void } | undefined)?.send({ type: "conn" }, socket);
    } catch {
      socket.destroy();
    }
  });
  server.listen(amqp.port, amqp.host);
  serveMetrics(cfg, plans);
  console.log(`queueforge-bun parent cores=${cores}`);

  const stop = () => {
    if (stopping) return;
    stopping = true;
    server.close();
    for (const child of kids.values()) child.kill("SIGTERM");
  };
  process.on("SIGTERM", () => {
    stop();
    process.exit(0);
  });
  process.on("SIGINT", () => {
    stop();
    process.exit(0);
  });
  await new Promise(() => {});
}

/** Public metrics port. Sums the confirm-before-fsync counter across children. */
function serveMetrics(cfg: Config, plans: ChildPlan[]) {
  const metrics = splitHost(cfg.metrics);
  Bun.serve({
    hostname: metrics.host,
    port: metrics.port,
    async fetch(req) {
      const path = new URL(req.url).pathname;
      if (path === "/healthz") return new Response("ok\n");
      if (path !== "/metrics" && path !== "/readyz") return new Response("not found\n", { status: 404 });
      const bodies = await Promise.all(plans.map((plan) => scrape(plan.metricsListen, path)));
      if (path === "/readyz") {
        const ready = bodies.every((body) => body.startsWith("ready"));
        return new Response(ready ? "ready\n" : "not ready\n", { status: ready ? 200 : 503 });
      }
      const text = mergeChildMetrics(bodies);
      return new Response(text, { headers: { "content-type": "text/plain; version=0.0.4" } });
    },
  });
}

async function scrape(addr: string, path: string): Promise<string> {
  try {
    const res = await fetch(`http://${addr}${path}`);
    return await res.text();
  } catch {
    return "";
  }
}

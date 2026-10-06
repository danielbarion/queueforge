import { startAmqp } from "./amqp/index.ts";
import { startMqtt, startStomp, startStream } from "./protocols/index.ts";
import { Broker } from "./broker/index.ts";
import { Cluster } from "./cluster.ts";
import { parseConfig, splitHost } from "./config.ts";
import { managementApp, metricsText } from "./http/index.ts";
import { Store } from "./store.ts";
import { join } from "node:path";
import { hashRabbitPassword } from "./broker/auth.ts";

const args = process.argv.slice(2);
let configPath = "";
let dev = false;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--config") configPath = args[++i] ?? "";
  if (args[i] === "--dev-bootstrap") dev = true;
}
if (!configPath) {
  console.error("usage: queueforge --config <file> [--dev-bootstrap]");
  process.exit(2);
}
const cfg = parseConfig(await Bun.file(configPath).text());
const store = new Store(join(cfg.dataDir, "bun.sqlite"), cfg.fsync, cfg.fsyncIntervalMs, cfg.fsyncEveryN);
const broker = new Broker(cfg, store);
if (dev && store.listUsers().length === 0) {
  const hash = hashRabbitPassword("devpassword12");
  store.putUser({ name: "admin", hash, tags: ["administrator"] });
  store.putPerm({ user: "admin", vhost: "/", configure: ".*", write: ".*", read: ".*" });
}
broker.load();
const cluster = new Cluster(broker);
broker.cluster = cluster;
if (cfg.members.length) cluster.start();
setInterval(() => broker.sweep(), 200);

const amqp = splitHost(cfg.amqp);
const mgmt = splitHost(cfg.management);
const metrics = splitHost(cfg.metrics);
startAmqp(amqp.host === "0.0.0.0" ? "0.0.0.0" : amqp.host, amqp.port, broker);
if (cfg.mqtt) {
  const mqtt = splitHost(cfg.mqtt);
  startMqtt(mqtt.host, mqtt.port, broker);
}
if (cfg.stomp) {
  const stomp = splitHost(cfg.stomp);
  startStomp(stomp.host, stomp.port, broker);
}
if (cfg.stream) {
  const stream = splitHost(cfg.stream);
  startStream(stream.host, stream.port, broker);
}
const spa = join(import.meta.dir, "../../rust/ui/dist");
managementApp(broker, spa).listen({ hostname: mgmt.host, port: mgmt.port });
Bun.serve({
  hostname: metrics.host,
  port: metrics.port,
  fetch(req) {
    const path = new URL(req.url).pathname;
    if (path === "/metrics") return new Response(metricsText(broker), { headers: { "content-type": "text/plain; version=0.0.4" } });
    if (path === "/healthz") return new Response("ok\n");
    if (path === "/readyz") {
      return broker.ready ? new Response("ready\n") : new Response("not ready\n", { status: 503 });
    }
    return new Response("not found\n", { status: 404 });
  },
});
console.log(`queueforge-bun amqp=${cfg.amqp} management=${cfg.management} metrics=${cfg.metrics}`);

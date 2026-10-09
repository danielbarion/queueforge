import { LdapBackend, OauthBackend } from "./auth/backends.ts";
import { adoptMigrated, adoptNodeSocket, adoptTlsSocket, startAmqp } from "./amqp/index.ts";
import { startMqtt, startStomp, startStream } from "./protocols/index.ts";
import { Broker } from "./broker/index.ts";
import { Cluster } from "./cluster.ts";
import { dnsMembers, parseConfig, splitHost } from "./config.ts";
import { grantedCores } from "./cores.ts";
import { managementApp, metricsText } from "./http/index.ts";
import { Store } from "./store.ts";
import { supervise } from "./supervise.ts";
import { join } from "node:path";
import { existsSync, readFileSync } from "node:fs";
import { createSecureContext } from "node:tls";
import { markFreshRaft } from "./raft/glue.ts";
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
if ((cfg.tls || cfg.amqps || cfg.mqtts || cfg.stomps || cfg.streamTls) && (!cfg.tlsCert || !cfg.tlsKey)) {
  console.error("[tls] enabled or listeners.amqps needs cert_path and key_path (PEM)");
  process.exit(2);
}
const tls = cfg.tls ? { cert: cfg.tlsCert!, key: cfg.tlsKey!, ca: cfg.tlsCa } : null;
// The per-core parent hands each accepted socket, TLS or not, to a child.
if (process.env.QUEUEFORGE_CHILD !== "1" && grantedCores() > 1) {
  await supervise(configPath, dev, cfg);
  process.exit(0);
}
// DNS discovery: the members are the addresses of dns_name (docs/raft.md, section 10).
const dnsSelf = cfg.discovery === "dns" && cfg.clusterListen ? { id: cfg.nodeId || cfg.clusterListen, addr: cfg.clusterListen } : null;
if (dnsSelf && cfg.dnsName) {
  cfg.nodeId = dnsSelf.id;
  cfg.members = await dnsMembers(cfg.dnsName, cfg.dnsPort ?? splitHost(cfg.clusterListen!).port, dnsSelf);
}
const freshData = !existsSync(join(cfg.dataDir, "bun.sqlite"));
const store = new Store(join(cfg.dataDir, "bun.sqlite"), cfg.fsync, cfg.fsyncIntervalMs, cfg.fsyncEveryN);
if (freshData) markFreshRaft(cfg.dataDir);
const broker = new Broker(cfg, store);
if (cfg.oauth?.jwksUrl) broker.oauth = new OauthBackend(cfg.oauth);
if (cfg.ldap?.userDnPattern) broker.ldap = new LdapBackend(cfg.ldap);
if (dev && store.listUsers().length === 0) {
  const hash = hashRabbitPassword("devpassword12");
  store.putUser({ name: "admin", hash, tags: ["administrator"] });
  store.putPerm({ user: "admin", vhost: "/", configure: ".*", write: ".*", read: ".*" });
}
broker.load();
const cluster = new Cluster(broker);
broker.cluster = cluster;
if (cfg.members.length) {
  cluster.start();
  const need = cfg.members.length - 1;
  const deadline = Date.now() + 5000;
  while (cluster.peers.size < need && Date.now() < deadline) {
    await Bun.sleep(50);
  }
}
if (dnsSelf && cfg.dnsName) {
  setInterval(async () => {
    const found = await dnsMembers(cfg.dnsName!, cfg.dnsPort ?? splitHost(cfg.clusterListen!).port, dnsSelf);
    if (JSON.stringify(found) !== JSON.stringify(cfg.members)) cluster.installMembers(found);
  }, 5000);
}
setInterval(() => broker.sweep(), 200);
broker.resumeShovels();
broker.resumeDeadLetters();

const amqp = splitHost(cfg.amqp);
const mgmt = splitHost(cfg.management);
const metrics = splitHost(cfg.metrics);
const bindMgmt = process.env.QUEUEFORGE_BIND_MGMT !== "0";
const adopt = process.env.QUEUEFORGE_ADOPT === "1";
if (!adopt) {
  startAmqp(amqp.host === "0.0.0.0" ? "0.0.0.0" : amqp.host, amqp.port, broker, cfg.reusePort, tls);
  if (cfg.amqps) {
    const amqps = splitHost(cfg.amqps);
    startAmqp(amqps.host, amqps.port, broker, false, { cert: cfg.tlsCert!, key: cfg.tlsKey!, ca: cfg.tlsCa });
  }
} else {
  // The parent does no TLS: a child handed an AMQPS socket runs the handshake itself.
  const secureContext = cfg.tlsCert && cfg.tlsKey
    ? createSecureContext({ cert: readFileSync(cfg.tlsCert), key: readFileSync(cfg.tlsKey), ...(cfg.tlsCa ? { ca: readFileSync(cfg.tlsCa) } : {}) })
    : null;
  process.on("message", (message: { type?: string; tls?: boolean; state?: { user: string; vhost: string; channels: Array<{ id: number; confirm: boolean; prefetch: number }> }; bytes?: Uint8Array }, handle?: unknown) => {
    if (message?.type !== "conn" || !handle || typeof (handle as { on?: unknown }).on !== "function") return;
    const socket = handle as import("node:net").Socket;
    if (message.tls) {
      if (secureContext) adoptTlsSocket(broker, socket, secureContext, !!cfg.tlsCa);
      else socket.destroy();
    } else if (message.state && message.bytes) adoptMigrated(broker, socket, message.state, message.bytes);
    else adoptNodeSocket(broker, socket);
  });
}
// With one child per core, only the first runs the other protocols' listeners;
// the cluster reaches queues on the other children.
if (bindMgmt && cfg.mqtt) {
  const mqtt = splitHost(cfg.mqtt);
  startMqtt(mqtt.host, mqtt.port, broker);
}
if (bindMgmt && cfg.stomp) {
  const stomp = splitHost(cfg.stomp);
  startStomp(stomp.host, stomp.port, broker);
}
if (bindMgmt && cfg.stream) {
  const stream = splitHost(cfg.stream);
  startStream(stream.host, stream.port, broker);
}
// TLS listeners for MQTT, STOMP and streams use the [tls] certificate.
const protocolTls = cfg.tlsCert && cfg.tlsKey ? { cert: cfg.tlsCert, key: cfg.tlsKey } : null;
if (bindMgmt && cfg.mqtts && protocolTls) {
  const at = splitHost(cfg.mqtts);
  startMqtt(at.host, at.port, broker, protocolTls);
}
if (bindMgmt && cfg.stomps && protocolTls) {
  const at = splitHost(cfg.stomps);
  startStomp(at.host, at.port, broker, protocolTls);
}
if (bindMgmt && cfg.streamTls && protocolTls) {
  const at = splitHost(cfg.streamTls);
  startStream(at.host, at.port, broker, protocolTls);
}
const peers = cfg.members.length ? `${cluster.peers.size}/${cfg.members.length - 1}` : "solo";
if (bindMgmt) {
  const spa = join(import.meta.dir, "../../rust/ui/dist");
  managementApp(broker, spa).listen({
    hostname: mgmt.host,
    port: mgmt.port,
    ...(tls ? { tls: { cert: Bun.file(tls.cert), key: Bun.file(tls.key) } } : {}),
  });
}
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
const who = cfg.nodeId || "solo";
console.log(`queueforge-bun child=${who} peers=${peers} amqp=${adopt ? "adopted" : cfg.amqp} management=${bindMgmt ? cfg.management : "off"} metrics=${cfg.metrics}`);
if (adopt) process.send?.({ type: "ready" });

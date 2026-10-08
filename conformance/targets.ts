/**
 * Start one broker for the conformance run and stop it afterwards.
 *
 * RabbitMQ runs from the same image the benchmark uses, with the plugins
 * whose features the matrix scores. QueueForge brokers run from source on
 * free ports, each in a fresh data directory, as admin / devpassword12.
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fixtures, LDAP_ADMIN_GROUP, LDAP_BIND_DN, LDAP_BIND_PASSWORD, LDAP_USER_DN, type Fixtures } from "./fixtures.ts";

export const ROOT = resolve(import.meta.dir, "..");
export const USER = "admin";
export const PASS = "devpassword12";
export const TARGETS = ["rabbitmq", "rust", "bun", "php"] as const;
export type Target = (typeof TARGETS)[number];

export type Ports = { amqp: number; amqps: number; mgmt: number; metrics: number; mqtt: number; stomp: number; stream: number; mqttWs: number; stompWs: number };

export type Running = {
  target: Target;
  ports: Ports;
  env: Record<string, string>;
  stop: () => Promise<void>;
};

const RABBIT_IMAGE = "rabbitmq:4.3-management";
const RABBIT_PLUGINS = [
  "rabbitmq_management",
  "rabbitmq_mqtt",
  "rabbitmq_stomp",
  "rabbitmq_stream",
  "rabbitmq_web_mqtt",
  "rabbitmq_web_stomp",
  "rabbitmq_consistent_hash_exchange",
  "rabbitmq_event_exchange",
  "rabbitmq_shovel",
  "rabbitmq_federation",
  "rabbitmq_auth_mechanism_ssl",
  "rabbitmq_auth_backend_oauth2",
  "rabbitmq_auth_backend_ldap",
];

async function freePort(): Promise<number> {
  return new Promise((done, fail) => {
    const server = createServer();
    server.once("error", fail);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close(() => done(port));
    });
  });
}

async function ports(): Promise<Ports> {
  return {
    amqp: await freePort(),
    amqps: await freePort(),
    mgmt: await freePort(),
    metrics: await freePort(),
    mqtt: await freePort(),
    stomp: await freePort(),
    stream: await freePort(),
    mqttWs: await freePort(),
    stompWs: await freePort(),
  };
}

/** True once the management API answers an authenticated overview. */
async function managementUp(port: number): Promise<boolean> {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/api/overview`, {
      headers: { authorization: `Basic ${btoa(`${USER}:${PASS}`)}` },
      signal: AbortSignal.timeout(1000),
    });
    return res.ok;
  } catch {
    return false;
  }
}

/** True once an AMQP login on `port` succeeds. */
async function amqpUp(port: number): Promise<boolean> {
  try {
    const amqp = (await import("amqplib")).default;
    const c = await amqp.connect(`amqp://${USER}:${PASS}@127.0.0.1:${port}/%2f`);
    c.on("error", () => {});
    await c.close();
    return true;
  } catch {
    return false;
  }
}

async function waitUp(p: Ports, seconds: number, child?: ChildProcess) {
  const deadline = Date.now() + seconds * 1000;
  while (Date.now() < deadline) {
    if (child && child.exitCode !== null) throw new Error(`broker exited with ${child.exitCode}`);
    // Management answers last on RabbitMQ; QueueForge has no Basic auth, so
    // its readiness is the AMQP login alone.
    if ((await amqpUp(p.amqp)) && (child || (await managementUp(p.mgmt)))) return;
    await Bun.sleep(250);
  }
  throw new Error("broker did not come up");
}

function envFor(target: Target, p: Ports, fx: Fixtures): Record<string, string> {
  // RabbitMQ serves MQTT and STOMP over WebSocket on their own ports at /ws.
  // QueueForge serves both on the management port at /ws, chosen by subprotocol.
  const ws =
    target === "rabbitmq"
      ? { mqtt: `ws://127.0.0.1:${p.mqttWs}/ws`, stomp: `ws://127.0.0.1:${p.stompWs}/ws` }
      : { mqtt: `ws://127.0.0.1:${p.mgmt}/ws`, stomp: `ws://127.0.0.1:${p.mgmt}/ws` };
  return {
    QF_MQTT_WS: ws.mqtt,
    QF_STOMP_WS: ws.stomp,
    QF_TARGET: target,
    QF_AMQP: `amqp://${USER}:${PASS}@127.0.0.1:${p.amqp}/%2f`,
    QF_AMQP_PORT: String(p.amqp),
    QF_MGMT: `http://127.0.0.1:${p.mgmt}`,
    QF_MQTT_PORT: String(p.mqtt),
    QF_STOMP_PORT: String(p.stomp),
    QF_STREAM_PORT: String(p.stream),
    QF_AMQPS_PORT: String(p.amqps),
    QF_PKI: fx.pki,
    QF_OAUTH_KEY: fx.oauthKey,
    QF_OAUTH_KID: fx.oauthKid,
  };
}

function queueforgeConfig(dir: string, p: Ports, fx: Fixtures): string {
  const path = join(dir, "queueforge.toml");
  writeFileSync(
    path,
    `[listeners]
amqp = "127.0.0.1:${p.amqp}"
management = "127.0.0.1:${p.mgmt}"
metrics = "127.0.0.1:${p.metrics}"
mqtt = "127.0.0.1:${p.mqtt}"
stomp = "127.0.0.1:${p.stomp}"
stream = "127.0.0.1:${p.stream}"
amqps = "127.0.0.1:${p.amqps}"

[data]
dir = "${join(dir, "data")}"
fsync_policy = "every_n_ms"
fsync_interval_ms = 10

[logging]
level = "warn"

[tls]
enabled = false
cert_path = "${fx.pki}/server.pem"
key_path = "${fx.pki}/server.key"
ca_path = "${fx.pki}/ca.pem"

[auth.oauth2]
resource_server_id = "rabbitmq"
jwks_url = "https://localhost:${fx.jwksPort}/jwks"
jwks_ca_path = "${fx.pki}/ca.pem"

[auth.ldap]
server = "127.0.0.1"
port = ${fx.ldapPort}
user_dn_pattern = "${LDAP_USER_DN}"
admin_group = "${LDAP_ADMIN_GROUP}"
bind_dn = "${LDAP_BIND_DN}"
bind_password = "${LDAP_BIND_PASSWORD}"
`,
  );
  return path;
}

async function startProcess(target: Exclude<Target, "rabbitmq">): Promise<Running> {
  const fx = await fixtures();
  const p = await ports();
  const dir = mkdtempSync(join(tmpdir(), `qf-conf-${target}-`));
  const config = queueforgeConfig(dir, p, fx);
  const env = { ...process.env, QUEUEFORGE_ADMIN_USER: USER, QUEUEFORGE_ADMIN_PASSWORD: PASS, RUST_LOG: "warn" };
  const cmd =
    target === "rust"
      ? { bin: join(ROOT, "rust/target/debug/queueforge"), args: ["--config", config, "--dev-bootstrap"], cwd: join(ROOT, "rust") }
      : target === "bun"
        ? { bin: "bun", args: ["src/main.ts", "--config", config, "--dev-bootstrap"], cwd: join(ROOT, "bun") }
        : { bin: "php", args: ["bin/queueforge", "--config", config, "--dev-bootstrap"], cwd: join(ROOT, "php") };
  const child = spawn(cmd.bin, cmd.args, { cwd: cmd.cwd, env, stdio: ["ignore", "ignore", "pipe"] });
  let stderr = "";
  child.stderr?.on("data", (chunk) => {
    stderr = (stderr + String(chunk)).slice(-4000);
  });
  try {
    await waitUp(p, 30, child);
  } catch (err) {
    child.kill("SIGKILL");
    throw new Error(`${target}: ${(err as Error).message}\n${stderr}`);
  }
  return {
    target,
    ports: p,
    env: envFor(target, p, fx),
    stop: async () => {
      // A broker that died mid-run says why on stderr.
      if (child.exitCode !== null) console.error(`== ${target} stderr (exit ${child.exitCode}):\n${stderr}`);
      child.kill("SIGTERM");
      const exited = new Promise((done) => child.once("exit", done));
      const timeout = Bun.sleep(3000).then(() => child.kill("SIGKILL"));
      await Promise.race([exited, timeout]);
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

/** rabbitmq.conf and advanced.config for the authentication tests. */
function rabbitAuthConfig(fx: Fixtures): string {
  const dir = mkdtempSync(join(tmpdir(), "qf-conf-rabbit-etc-"));
  writeFileSync(
    join(dir, "20-conformance.conf"),
    `listeners.ssl.default = 5671
ssl_options.cacertfile = /pki/ca.pem
ssl_options.certfile = /pki/server.pem
ssl_options.keyfile = /pki/server.key
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = false
auth_mechanisms.1 = PLAIN
auth_mechanisms.2 = AMQPLAIN
auth_mechanisms.3 = EXTERNAL
ssl_cert_login_from = common_name
auth_backends.1 = internal
auth_backends.2 = rabbit_auth_backend_oauth2
auth_backends.3 = ldap
auth_oauth2.resource_server_id = rabbitmq
auth_oauth2.jwks_url = https://host.docker.internal:${fx.jwksPort}/jwks
auth_oauth2.https.cacertfile = /pki/ca.pem
auth_oauth2.https.peer_verification = verify_peer
auth_ldap.servers.1 = host.docker.internal
auth_ldap.port = ${fx.ldapPort}
auth_ldap.user_dn_pattern = ${LDAP_USER_DN}
auth_ldap.other_bind.user_dn = ${LDAP_BIND_DN}
auth_ldap.other_bind.password = ${LDAP_BIND_PASSWORD}
`,
  );
  writeFileSync(
    join(dir, "advanced.config"),
    `[{rabbitmq_auth_backend_ldap, [{tag_queries, [{administrator, {in_group, "${LDAP_ADMIN_GROUP}"}}, {management, {constant, true}}]}]}].\n`,
  );
  spawnSync("chmod", ["-R", "a+rX", dir]);
  return dir;
}

async function startRabbit(): Promise<Running> {
  const fx = await fixtures();
  const etc = rabbitAuthConfig(fx);
  const p = await ports();
  const name = `qf-conf-rabbit-${process.pid}`;
  spawnSync("docker", ["rm", "-f", name], { stdio: "ignore" });
  const enable = `rabbitmq-plugins enable --offline ${RABBIT_PLUGINS.join(" ")} >/dev/null && exec docker-entrypoint.sh rabbitmq-server`;
  const run = spawnSync(
    "docker",
    [
      "run", "-d", "--name", name,
      "-e", `RABBITMQ_DEFAULT_USER=${USER}`,
      "-e", `RABBITMQ_DEFAULT_PASS=${PASS}`,
      // Stream clients reconnect to the advertised address; advertise the mapped port.
      "-e", `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-rabbitmq_stream advertised_host "127.0.0.1" advertised_port ${p.stream}`,
      "-p", `127.0.0.1:${p.amqp}:5672`,
      "-p", `127.0.0.1:${p.amqps}:5671`,
      "-v", `${fx.pki}:/pki:ro`,
      "-v", `${etc}/20-conformance.conf:/etc/rabbitmq/conf.d/20-conformance.conf:ro`,
      "-v", `${etc}/advanced.config:/etc/rabbitmq/advanced.config:ro`,
      "-p", `127.0.0.1:${p.mgmt}:15672`,
      "-p", `127.0.0.1:${p.mqtt}:1883`,
      "-p", `127.0.0.1:${p.stomp}:61613`,
      "-p", `127.0.0.1:${p.stream}:5552`,
      "-p", `127.0.0.1:${p.mqttWs}:15675`,
      "-p", `127.0.0.1:${p.stompWs}:15674`,
      "--entrypoint", "sh",
      RABBIT_IMAGE, "-c", enable,
    ],
    { encoding: "utf8" },
  );
  if (run.status !== 0) throw new Error(`rabbitmq: ${run.stderr}`);
  const stop = async () => {
    spawnSync("docker", ["rm", "-f", name], { stdio: "ignore" });
  };
  try {
    await waitUp(p, 120);
  } catch (err) {
    const logs = spawnSync("docker", ["logs", "--tail", "30", name], { encoding: "utf8" });
    await stop();
    throw new Error(`${(err as Error).message}\n${logs.stdout}${logs.stderr}`);
  }
  return { target: "rabbitmq", ports: p, env: envFor("rabbitmq", p, fx), stop };
}

export async function start(target: Target): Promise<Running> {
  return target === "rabbitmq" ? startRabbit() : startProcess(target);
}

import { expect, test } from "bun:test";
import amqp from "amqplib";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Broker } from "../src/broker/index.ts";
import {
  hashRabbitPassword,
  passwordPolicyError,
  rabbitPasswordHashMatches,
} from "../src/broker/auth.ts";
import type { Config } from "../src/config.ts";
import { Store } from "../src/store.ts";

/** salt 10 22 33 44 and password devpassword12. Same bytes Rust stores. */
const CROSS_SALT = new Uint8Array([0x10, 0x22, 0x33, 0x44]);
const CROSS_HASH = "ECIzRClc/+1u2ev0HwDpqq+CC4ixd405UlwH0cCTvqGs8avG";
const ARGON2_FIXTURE =
  "$argon2id$v=19$m=19456,t=2,p=1$WbkrTzkiA7IX96DqFx4oyqY4ycwq51Qc2ernvzYq2H8$E44EIl5HVbAgkCuygMc0dOWT79GeatS2lEh4vIWdbvQ";

function cfg(dataDir: string): Config {
  return {
    amqp: "127.0.0.1:0",
    management: "127.0.0.1:0",
    metrics: "127.0.0.1:0",
    mqtt: null,
    stomp: null,
    stream: null,
    dataDir,
    fsync: "every_n_ms",
    fsyncIntervalMs: 50,
    fsyncEveryN: 1,
    tls: false,
    nodeId: "bun",
    clusterListen: null,
    members: [],
    defaultQueueType: "classic",
  };
}

test("new hashes are the rabbitmq sha256 form and an argon2id phc does not verify", async () => {
  expect(passwordPolicyError("short")).toContain("8");
  expect(passwordPolicyError("")).toContain("empty");
  expect(passwordPolicyError("1234567")).toContain("8");
  expect(passwordPolicyError("12345678")).toBeNull();

  const hashed = hashRabbitPassword("devpassword12", CROSS_SALT);
  expect(hashed).toBe(CROSS_HASH);
  expect(hashed.startsWith("$argon2")).toBe(false);
  expect(Buffer.from(hashed, "base64").length).toBe(36);
  expect(rabbitPasswordHashMatches("devpassword12", CROSS_HASH)).toBe(true);
  expect(rabbitPasswordHashMatches("wrong-password", CROSS_HASH)).toBe(false);

  const fresh = hashRabbitPassword("exactly12chr");
  expect(fresh.startsWith("$argon2")).toBe(false);
  expect(Buffer.from(fresh, "base64").length).toBe(36);
  expect(rabbitPasswordHashMatches("exactly12chr", fresh)).toBe(true);

  const dir = mkdtempSync(join(tmpdir(), "qf-bun-argon-drop-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50, 1);
  const broker = new Broker(cfg(dir), store);
  broker.load();
  await broker.importDefinitions({
    users: [{ name: "argonuser", password_hash: ARGON2_FIXTURE, tags: "administrator" }],
  });
  expect(await broker.verify("argonuser", "argon-fixture-pw")).toBe(false);
  expect(await broker.verify("argonuser", "wrong-password!")).toBe(false);
});

test("putUser stores sha256 and rejects a short password", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-bun-hash-"));
  const store = new Store(join(dir, "bun.sqlite"), "every_n_ms", 50, 1);
  const broker = new Broker(cfg(dir), store);
  broker.load();
  await expect(broker.putUser("shorty", "short", [], true)).rejects.toThrow(/8/);
  expect(broker.listUsers().some((u) => u.name === "shorty")).toBe(false);

  await broker.putUser("longname", "exactly12chr", ["administrator"], true);
  const doc = broker.exportDefinitions();
  const row = doc.users.find((u) => u.name === "longname");
  if (!row?.password_hash) throw new Error("missing stored hash");
  expect(row.password_hash.startsWith("$argon2")).toBe(false);
  expect(Buffer.from(row.password_hash, "base64").length).toBe(36);
  expect(rabbitPasswordHashMatches("exactly12chr", row.password_hash)).toBe(true);
});

async function boot(portA: number, portM: number): Promise<ChildProcess> {
  const dir = mkdtempSync(join(tmpdir(), "qf-bun-hashboot-"));
  const cfgPath = join(dir, "qf.toml");
  await Bun.write(
    cfgPath,
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
  const child = spawn("bun", ["src/main.ts", "--config", cfgPath, "--dev-bootstrap"], {
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

test("dev bootstrap and a rust-stored hash both log in over amqp", async () => {
  const portA = 47831;
  const portM = 57831;
  const child = await boot(portA, portM);
  const base = `http://127.0.0.1:${portM}`;
  try {
    const login = await fetch(`${base}/api/login`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username: "admin", password: "devpassword12" }),
    });
    expect(login.status).toBe(200);
    const cookie = cookiePair(login.headers.get("set-cookie"));
    const definitions = await fetch(`${base}/api/definitions`, { headers: { cookie } });
    expect(definitions.status).toBe(200);
    const doc = (await definitions.json()) as {
      users: Array<{ name: string; password_hash?: string }>;
    };
    const admin = doc.users.find((u) => u.name === "admin");
    if (!admin?.password_hash) throw new Error(`admin hash missing: ${JSON.stringify(doc.users)}`);
    expect(admin.password_hash.startsWith("$argon2")).toBe(false);
    expect(Buffer.from(admin.password_hash, "base64").length).toBe(36);
    expect(rabbitPasswordHashMatches("devpassword12", admin.password_hash)).toBe(true);

    const short = await fetch(`${base}/api/users/shorty`, {
      method: "PUT",
      headers: { "content-type": "application/json", cookie },
      body: JSON.stringify({ password: "short", tags: ["administrator"] }),
    });
    expect(short.status).toBeGreaterThanOrEqual(400);
    const users = await fetch(`${base}/api/users`, { headers: { cookie } });
    const listed = (await users.json()) as Array<{ name: string }>;
    expect(listed.some((u) => u.name === "shorty")).toBe(false);

    const imported = await fetch(`${base}/api/definitions`, {
      method: "POST",
      headers: { "content-type": "application/json", cookie },
      body: JSON.stringify({
        users: [{ name: "fromrust", password_hash: CROSS_HASH, tags: "administrator" }],
        permissions: [
          { user: "fromrust", vhost: "/", configure: ".*", write: ".*", read: ".*" },
        ],
      }),
    });
    expect(imported.status).toBe(204);

    const conn = await amqp.connect(`amqp://fromrust:devpassword12@127.0.0.1:${portA}/%2f`);
    const channel = await conn.createChannel();
    await channel.close();
    await conn.close();
  } finally {
    child.kill();
  }
});

import { expect, test } from "bun:test";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import tls from "node:tls";

// MQTT, STOMP and the stream protocol listen on TLS ports too, as RabbitMQ's
// mqtt.listeners.ssl, stomp.listeners.ssl and stream.listeners.ssl.

function exchange(port: number, request: Uint8Array): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const socket = tls.connect({ host: "127.0.0.1", port, rejectUnauthorized: false }, () => socket.write(request));
    let got = Buffer.alloc(0);
    socket.on("data", (chunk: Buffer) => {
      got = Buffer.concat([got, chunk]);
      if (got.length >= 4) {
        socket.end();
        resolve(got);
      }
    });
    socket.on("error", reject);
    setTimeout(() => reject(new Error(`no answer on ${port}`)), 5000).unref();
  });
}

function mqttConnect(user: string, pass: string): Uint8Array {
  const str = (s: string) => {
    const b = Buffer.from(s);
    return Buffer.concat([Buffer.from([b.length >> 8, b.length & 0xff]), b]);
  };
  const variable = Buffer.concat([str("MQTT"), Buffer.from([4, 0xc2, 0, 30])]);
  const payload = Buffer.concat([str("tls-client"), str(user), str(pass)]);
  const rest = Buffer.concat([variable, payload]);
  return Buffer.concat([Buffer.from([0x10, rest.length]), rest]);
}

function streamPeerProperties(): Uint8Array {
  const body = Buffer.alloc(12);
  body.writeUInt16BE(0x0011, 0);
  body.writeUInt16BE(1, 2);
  body.writeUInt32BE(1, 4);
  body.writeInt32BE(0, 8);
  const size = Buffer.alloc(4);
  size.writeUInt32BE(body.length, 0);
  return Buffer.concat([size, body]);
}

const RUST = join(import.meta.dir, "..", "..", "rust", "target", "debug", "queueforge");

async function overTls(kind: "bun" | "rust", base: number) {
  const dir = mkdtempSync(join(tmpdir(), "qf-tls-proto-"));
  const made = spawnSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", join(dir, "key.pem"), "-out", join(dir, "cert.pem"), "-days", "2", "-subj", "/CN=localhost"]);
  expect(made.status).toBe(0);
  const cfg = join(dir, "qf.toml");
  await Bun.write(
    cfg,
    `[listeners]
amqp = "127.0.0.1:${base}"
management = "127.0.0.1:${base + 1}"
metrics = "127.0.0.1:${base + 2}"
mqtts = "127.0.0.1:${base + 3}"
stomps = "127.0.0.1:${base + 4}"
stream_tls = "127.0.0.1:${base + 5}"
[data]
dir = "${dir}/data"
[tls]
cert_path = "${dir}/cert.pem"
key_path = "${dir}/key.pem"
`,
  );
  const child: ChildProcess =
    kind === "rust"
      ? spawn(RUST, ["--config", cfg, "--dev-bootstrap"], { stdio: "ignore" })
      : spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], { cwd: join(import.meta.dir, ".."), stdio: "ignore" });
  try {
    for (let i = 0; i < 100; i++) {
      try {
        if ((await fetch(`http://127.0.0.1:${base + 1}/readyz`)).ok) break;
      } catch {
        /* starting */
      }
      await Bun.sleep(50);
    }
    const connack = await exchange(base + 3, mqttConnect("admin", "devpassword12"));
    expect(connack[0]).toBe(0x20);
    expect(connack[3]).toBe(0);
    const connected = await exchange(base + 4, new TextEncoder().encode("CONNECT\naccept-version:1.2\nhost:/\nlogin:admin\npasscode:devpassword12\n\n\0"));
    expect(connected.toString()).toStartWith("CONNECTED");
    const props = await exchange(base + 5, streamPeerProperties());
    expect(props.readUInt16BE(4)).toBe(0x8011);
  } finally {
    child.kill("SIGKILL");
  }
}

test("MQTT, STOMP and stream over TLS", () => overTls("bun", 44010), 30_000);
test.skipIf(!existsSync(RUST))("MQTT, STOMP and stream over TLS (Rust)", () => overTls("rust", 44020), 30_000);

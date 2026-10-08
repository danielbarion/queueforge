/**
 * x.509 client certificates (SASL EXTERNAL), OAuth 2.0 tokens and LDAP.
 *
 * Fixtures come from fixtures.ts via QF_PKI, QF_OAUTH_KEY and QF_OAUTH_KID.
 * Every target trusts the test CA on its AMQPS port, validates tokens for
 * the resource server id `rabbitmq` against the fixture JWKS, and binds
 * LDAP users as uid=<name>,ou=people,dc=qf,dc=test; members of qf-admins
 * get the administrator tag.
 */
import amqp from "amqplib";
import { createPrivateKey, createSign } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { channel, closeCode, MGMT, mgmt, uniq } from "../lib.ts";

const PKI = process.env.QF_PKI ?? "";
const AMQPS_PORT = Number(process.env.QF_AMQPS_PORT ?? 5671);
const AMQP_PORT = Number(process.env.QF_AMQP_PORT ?? 5672);
const ca = () => readFileSync(join(PKI, "ca.pem"));

async function tlsConnect(cert: string | null, mechanism: "EXTERNAL" | "PLAIN") {
  const opts: Record<string, unknown> = { ca: [ca()], servername: "localhost" };
  if (cert) {
    opts.cert = readFileSync(join(PKI, `${cert}.pem`));
    opts.key = readFileSync(join(PKI, `${cert}.key`));
  }
  if (mechanism === "EXTERNAL") opts.credentials = amqp.credentials.external();
  const url = mechanism === "EXTERNAL" ? `amqps://localhost:${AMQPS_PORT}/%2f` : `amqps://admin:devpassword12@localhost:${AMQPS_PORT}/%2f`;
  const c = await amqp.connect(url, opts);
  c.on("error", () => {});
  return c;
}

const outcome = (p: Promise<{ close: () => Promise<void> }>) =>
  p.then(
    async (c) => {
      await c.close();
      return "open";
    },
    () => "refused",
  );

async function ensureUser(name: string) {
  await mgmt(`/api/users/${encodeURIComponent(name)}`, { method: "PUT", body: JSON.stringify({ password: uniq("pw"), tags: "" }) });
  await mgmt(`/api/permissions/%2f/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify({ configure: ".*", write: ".*", read: ".*" }),
  });
}

test("x.509 client certificates :: EXTERNAL logs in as the certificate's common name", async () => {
  await ensureUser("conf-cert-user");
  const c = await tlsConnect("client", "EXTERNAL");
  const ch = await c.createChannel();
  const q = uniq("x509");
  await ch.assertQueue(q, { durable: true });
  await ch.deleteQueue(q);
  await c.close();
});

test("x.509 client certificates :: EXTERNAL without a certificate, or with an untrusted one, is refused", async () => {
  expect(await outcome(tlsConnect(null, "EXTERNAL"))).toBe("refused");
  expect(await outcome(tlsConnect("stranger", "EXTERNAL"))).toBe("refused");
});

test("x.509 client certificates :: PLAIN still works on the TLS port", async () => {
  expect(await outcome(tlsConnect(null, "PLAIN"))).toBe("open");
});

const b64url = (b: Buffer | string) => Buffer.from(b).toString("base64url");

/** An RS256 token signed with the fixture key. */
function token(claims: Record<string, unknown>, opts: { kid?: string; key?: string } = {}): string {
  const header = { alg: "RS256", typ: "JWT", kid: opts.kid ?? process.env.QF_OAUTH_KID };
  const now = Math.floor(Date.now() / 1000);
  const body = { iat: now, exp: now + 600, aud: ["rabbitmq"], sub: "conf-oauth-client", ...claims };
  const input = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(body))}`;
  const key = createPrivateKey(opts.key ? readFileSync(opts.key) : readFileSync(process.env.QF_OAUTH_KEY ?? ""));
  const sig = createSign("RSA-SHA256").update(input).sign(key);
  return `${input}.${b64url(sig)}`;
}

async function oauthConnect(jwt: string) {
  const c = await amqp.connect(`amqp://oauth:${encodeURIComponent(jwt)}@127.0.0.1:${AMQP_PORT}/%2f`);
  c.on("error", () => {});
  return c;
}

test("OAuth 2.0 :: a signed token is accepted as the password and its scopes grant access", async () => {
  const prefix = uniq("oa");
  const jwt = token({ scope: `rabbitmq.configure:*/${prefix}* rabbitmq.write:*/${prefix}* rabbitmq.read:*/${prefix}*` });
  const c = await oauthConnect(jwt);
  const ch = await c.createChannel();
  ch.on("error", () => {});
  await ch.assertQueue(`${prefix}-q`, { durable: true });
  await ch.deleteQueue(`${prefix}-q`);
  // Outside the scopes: configure is refused with 403.
  expect(await closeCode(ch.assertQueue(uniq("other"), { durable: true }))).toBe(403);
  await c.close().catch(() => {});
});

test("OAuth 2.0 :: a read-only token can read but not declare", async () => {
  const ch = await channel();
  const q = uniq("oa-ro");
  await ch.assertQueue(q, { durable: true });
  ch.sendToQueue(q, Buffer.from("for-token"));
  const c = await oauthConnect(token({ scope: "rabbitmq.read:*/*" }));
  const tch = await c.createChannel();
  tch.on("error", () => {});
  await new Promise((r) => setTimeout(r, 100));
  const got = await tch.get(q, { noAck: true });
  expect(got ? got.content.toString() : null).toBe("for-token");
  expect(await closeCode(tch.assertQueue(uniq("oa-ro-new"), { durable: true }))).toBe(403);
  await c.close().catch(() => {});
});

test("OAuth 2.0 :: expired, wrongly signed, or other-audience tokens are refused", async () => {
  const now = Math.floor(Date.now() / 1000);
  const scope = "rabbitmq.read:*/*";
  expect(await outcome(oauthConnect(token({ scope, exp: now - 60 })))).toBe("refused");
  expect(await outcome(oauthConnect(token({ scope, aud: ["someone-else"] })))).toBe("refused");
  expect(await outcome(oauthConnect(token({ scope }, { key: join(PKI, "client.key") })))).toBe("refused");
});

test("LDAP :: a directory user logs in with its directory password", async () => {
  const c = await amqp.connect(`amqp://ldap-plain:ldap-secret@127.0.0.1:${AMQP_PORT}/%2f`);
  c.on("error", () => {});
  const ch = await c.createChannel();
  const q = uniq("ldap");
  await ch.assertQueue(q, { durable: true });
  await ch.deleteQueue(q);
  await c.close();
});

test("LDAP :: a wrong directory password is refused", async () => {
  expect(await outcome(amqp.connect(`amqp://ldap-plain:wrong@127.0.0.1:${AMQP_PORT}/%2f`))).toBe("refused");
});

test("LDAP :: members of the admin group get the administrator tag", async () => {
  const basic = (u: string) => `Basic ${btoa(`${u}:ldap-secret`)}`;
  const admin = await fetch(`${MGMT}/api/whoami`, { headers: { authorization: basic("ldap-admin") } });
  expect(admin.status).toBe(200);
  const tags = (await admin.json()) as { tags: string | string[] };
  expect(String(tags.tags)).toContain("administrator");
  const plain = await fetch(`${MGMT}/api/users`, { headers: { authorization: basic("ldap-plain") } });
  expect(plain.status).not.toBe(200);
});

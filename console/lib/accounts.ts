import { randomBytes, createCipheriv, createDecipheriv, scryptSync } from "node:crypto";
import { databaseConfigured, migrate, sql } from "./db";
import { hashPassword, verifyPassword } from "./passwords";
import type { BrokerCredential } from "./guard";

const KINDS = new Set(["rust", "bun", "php", "rabbitmq"]);

export type SavedBroker = {
  id: string;
  name: string;
  url: string;
  kind: "rust" | "bun" | "php" | "rabbitmq";
  selected: boolean;
};

const SESSION_MS = 12 * 60 * 60 * 1000;

export type OperatorSession = { id: string; operatorId: string };

function newId(): string {
  return randomBytes(24).toString("base64url");
}

function dataKey(): Buffer | null {
  const secret = process.env.CONSOLE_DATA_KEY;
  if (!secret || secret.length < 16) return null;
  return scryptSync(secret, "queueforge-console-cred", 32);
}

function seal(value: string): string {
  const key = dataKey();
  if (!key) return `plain$${Buffer.from(value).toString("base64url")}`;
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const ciphertext = Buffer.concat([cipher.update(value, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `aes$${iv.toString("base64url")}$${tag.toString("base64url")}$${ciphertext.toString("base64url")}`;
}

function open(stored: string): string | null {
  const [kind, a, b, c] = stored.split("$");
  if (kind === "plain" && a) return Buffer.from(a, "base64url").toString("utf8");
  const key = dataKey();
  if (kind !== "aes" || !a || !b || !c || !key) return null;
  try {
    const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(a, "base64url"));
    decipher.setAuthTag(Buffer.from(b, "base64url"));
    return Buffer.concat([decipher.update(Buffer.from(c, "base64url")), decipher.final()]).toString("utf8");
  } catch {
    return null;
  }
}

async function bootstrap(): Promise<void> {
  const user = process.env.CONSOLE_USER ?? "";
  const password = process.env.CONSOLE_PASSWORD ?? "";
  if (!user || password.length < 12) return;
  const existing = await sql()`select id from operators limit 1`;
  if (existing.length > 0) return;
  await sql()`
    insert into operators (id, username, password_hash)
    values (${crypto.randomUUID()}, ${user}, ${hashPassword(password)})
  `;
}

export async function loginOperator(username: string, password: string): Promise<string | null> {
  if (!databaseConfigured()) return null;
  await migrate();
  await bootstrap();
  const rows = await sql()`select id, password_hash from operators where username = ${username} limit 1`;
  const row = rows[0];
  if (!row || !verifyPassword(password, String(row.password_hash))) return null;
  const id = newId();
  const expires = new Date(Date.now() + SESSION_MS);
  await sql()`insert into sessions (id, operator_id, expires_at) values (${id}, ${row.id}, ${expires})`;
  return id;
}

export async function currentSession(cookieHeader: string | null): Promise<OperatorSession | null> {
  if (!databaseConfigured()) return null;
  const token = readCookie(cookieHeader, "qfc_op");
  if (!token || !/^[A-Za-z0-9_-]{32,}$/.test(token)) return null;
  await migrate();
  await sql()`delete from sessions where expires_at <= now()`;
  const rows = await sql()`
    select id, operator_id from sessions where id = ${token} and expires_at > now() limit 1
  `;
  const row = rows[0];
  if (!row) return null;
  return { id: String(row.id), operatorId: String(row.operator_id) };
}

export async function endSession(cookieHeader: string | null): Promise<void> {
  const token = readCookie(cookieHeader, "qfc_op");
  if (!token || !databaseConfigured()) return;
  await migrate();
  await sql()`delete from sessions where id = ${token}`;
}

export async function listBrokers(operatorId: string): Promise<SavedBroker[]> {
  await migrate();
  const rows = await sql()`
    select id, name, url, kind, selected from brokers
    where operator_id = ${operatorId}
    order by created_at
  `;
  return rows.map((row) => ({
    id: String(row.id),
    name: String(row.name),
    url: String(row.url),
    kind: KINDS.has(String(row.kind)) ? (row.kind as SavedBroker["kind"]) : "rust",
    selected: Boolean(row.selected),
  }));
}

export async function saveBroker(operatorId: string, broker: Omit<SavedBroker, "selected">): Promise<SavedBroker> {
  await migrate();
  await sql()`update brokers set selected = false where operator_id = ${operatorId}`;
  await sql()`
    insert into brokers (id, operator_id, name, url, kind, selected)
    values (${broker.id}, ${operatorId}, ${broker.name}, ${broker.url}, ${broker.kind}, true)
  `;
  return { ...broker, selected: true };
}

export async function selectBroker(operatorId: string, id: string): Promise<void> {
  await migrate();
  await sql()`update brokers set selected = false where operator_id = ${operatorId}`;
  await sql()`update brokers set selected = true where operator_id = ${operatorId} and id = ${id}`;
}

export async function deleteBroker(operatorId: string, id: string): Promise<void> {
  await migrate();
  await sql()`delete from brokers where operator_id = ${operatorId} and id = ${id}`;
}

export async function saveCredential(sessionId: string, origin: string, credential: BrokerCredential): Promise<void> {
  await migrate();
  const stored = seal(credential.value);
  await sql()`
    insert into broker_credentials (session_id, broker_url, kind, value)
    values (${sessionId}, ${origin}, ${credential.kind}, ${stored})
    on conflict (session_id, broker_url) do update set kind = excluded.kind, value = excluded.value
  `;
}

export async function loadCredential(sessionId: string, origin: string): Promise<BrokerCredential | null> {
  await migrate();
  const rows = await sql()`
    select kind, value from broker_credentials where session_id = ${sessionId} and broker_url = ${origin} limit 1
  `;
  const row = rows[0];
  if (!row) return null;
  const value = open(String(row.value));
  if (!value) return null;
  const kind = row.kind === "basic" ? "basic" : "cookie";
  return { kind, value };
}

export async function forgetCredential(sessionId: string, origin: string): Promise<void> {
  await migrate();
  await sql()`delete from broker_credentials where session_id = ${sessionId} and broker_url = ${origin}`;
}

function readCookie(header: string | null, name: string): string | null {
  if (!header) return null;
  for (const part of header.split(";")) {
    const [key, ...rest] = part.trim().split("=");
    if (key === name) return rest.join("=") || null;
  }
  return null;
}

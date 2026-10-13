/**
 * Operator login for the console itself.
 *
 * Broker logins are a second step. This gate is the one that decides whether
 * a browser may use the console at all. The session is an HMAC of an expiry,
 * keyed by the operator password, so it does not live in process memory.
 */

export const OPERATOR_COOKIE = "qfc_op";
const MAX_AGE_SECONDS = 60 * 60 * 12;
const encoder = new TextEncoder();

export function operatorConfigured(): boolean {
  const user = process.env.CONSOLE_USER ?? "";
  const password = process.env.CONSOLE_PASSWORD ?? "";
  return user.length > 0 && password.length >= 12;
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/g, "");
}

function base64UrlToBytes(value: string): Uint8Array | null {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) return null;
  const padded = value.replaceAll("-", "+").replaceAll("_", "/") + "=".repeat((4 - (value.length % 4)) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  let diff = 0;
  for (let i = 0; i < left.length; i++) diff |= left[i]! ^ right[i]!;
  return diff === 0;
}

async function keyedMac(value: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode("queueforge-console-compare"),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(value)));
}

async function sessionKey(): Promise<CryptoKey> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    encoder.encode(`queueforge-console\0${process.env.CONSOLE_PASSWORD ?? ""}`),
  );
  return crypto.subtle.importKey("raw", digest, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
}

/** Both sides are hashed before the compare, so a short guess is not faster. */
export async function credentialsMatch(user: string, password: string): Promise<boolean> {
  if (!operatorConfigured()) return false;
  const userOk = equalBytes(await keyedMac(user), await keyedMac(process.env.CONSOLE_USER ?? ""));
  const passOk = equalBytes(await keyedMac(password), await keyedMac(process.env.CONSOLE_PASSWORD ?? ""));
  return userOk && passOk;
}

export async function issueSession(now = Date.now()): Promise<string> {
  const body = bytesToBase64Url(encoder.encode(JSON.stringify({ exp: now + MAX_AGE_SECONDS * 1000 })));
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", await sessionKey(), encoder.encode(body)));
  return `${body}.${bytesToBase64Url(signature)}`;
}

export async function sessionValid(token: string | null | undefined, now = Date.now()): Promise<boolean> {
  if (!token || !operatorConfigured()) return false;
  const dot = token.indexOf(".");
  if (dot <= 0 || token.indexOf(".", dot + 1) !== -1) return false;
  const body = token.slice(0, dot);
  const given = base64UrlToBytes(token.slice(dot + 1));
  const decoded = base64UrlToBytes(body);
  if (!given || !decoded) return false;
  const expected = new Uint8Array(await crypto.subtle.sign("HMAC", await sessionKey(), encoder.encode(body)));
  if (!equalBytes(expected, given)) return false;
  try {
    const payload = JSON.parse(new TextDecoder().decode(decoded)) as { exp?: unknown };
    return typeof payload.exp === "number" && payload.exp > now;
  } catch {
    return false;
  }
}

export function operatorCookie(token: string, secure: boolean): string {
  return `${OPERATOR_COOKIE}=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${MAX_AGE_SECONDS}${secure ? "; Secure" : ""}`;
}

export function clearOperatorCookie(secure: boolean): string {
  return `${OPERATOR_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${secure ? "; Secure" : ""}`;
}

export function requestIsSecure(request: Request): boolean {
  const forwarded = request.headers.get("x-forwarded-proto");
  if (forwarded) return forwarded.split(",")[0]!.trim() === "https";
  return new URL(request.url).protocol === "https:";
}

/** A same-origin path. Anything else becomes the overview, so login cannot redirect off the console. */
export function safeNext(value: string | null): string {
  if (!value || !value.startsWith("/") || value.startsWith("//") || value.includes("\\") || value.includes("://")) return "/";
  return value;
}

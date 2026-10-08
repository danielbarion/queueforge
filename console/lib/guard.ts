import { randomBytes } from "node:crypto";

/**
 * Server-side guards for the broker proxy routes.
 *
 * The console holds broker credentials for the browser, so every route
 * that uses them must be called by this console's own pages (not another
 * site), and each browser only gets the credentials it logged in with.
 */

export const CONSOLE_COOKIE = "qfc_sid";

/** One stored broker login: a session cookie (QueueForge) or Basic auth (RabbitMQ). */
export type BrokerCredential = { kind: "cookie"; value: string } | { kind: "basic"; value: string };

const globalCreds = globalThis as unknown as { __queueforgeConsoleCreds?: Map<string, BrokerCredential> };

function creds(): Map<string, BrokerCredential> {
  globalCreds.__queueforgeConsoleCreds ??= new Map();
  return globalCreds.__queueforgeConsoleCreds;
}

function key(sid: string, origin: string) {
  return `${sid}\0${origin}`;
}

export function getCredential(sid: string | null, origin: string): BrokerCredential | null {
  if (!sid) return null;
  return creds().get(key(sid, origin)) ?? null;
}

export function setCredential(sid: string, origin: string, credential: BrokerCredential) {
  creds().set(key(sid, origin), credential);
}

export function dropCredential(sid: string | null, origin: string) {
  if (sid) creds().delete(key(sid, origin));
}

/** The headers that carry a stored credential to the broker. */
export function credentialHeaders(credential: BrokerCredential | null, headers: Headers) {
  if (!credential) return;
  if (credential.kind === "cookie") headers.set("cookie", credential.value);
  else headers.set("authorization", credential.value);
}

export function consoleSid(request: Request): string | null {
  const header = request.headers.get("cookie") ?? "";
  for (const part of header.split(";")) {
    const [name, ...rest] = part.trim().split("=");
    if (name === CONSOLE_COOKIE) {
      const value = rest.join("=");
      return /^[A-Za-z0-9_-]{32,}$/.test(value) ? value : null;
    }
  }
  return null;
}

export function newSid(): string {
  return randomBytes(24).toString("base64url");
}

/** `Set-Cookie` for a console session: HttpOnly, SameSite=Strict, this path only. */
export function sidCookie(sid: string, secure: boolean): string {
  return `${CONSOLE_COOKIE}=${sid}; Path=/; HttpOnly; SameSite=Strict${secure ? "; Secure" : ""}`;
}

/**
 * Reject a cross-site call. Browsers send `Origin` on POST and
 * `Sec-Fetch-Site` on every fetch; either naming another site is refused,
 * and a POST must be JSON so a plain HTML form cannot reach the route.
 */
export function crossSite(request: Request): string | null {
  const site = request.headers.get("sec-fetch-site");
  if (site && site !== "same-origin" && site !== "none") return "Cross-site requests are refused.";
  const origin = request.headers.get("origin");
  if (origin) {
    let host: string | null = null;
    try {
      host = new URL(origin).host;
    } catch {
      return "Cross-site requests are refused.";
    }
    const own = request.headers.get("x-forwarded-host") ?? request.headers.get("host");
    if (!own || host !== own) return "Cross-site requests are refused.";
  }
  if (request.method === "POST" && !(request.headers.get("content-type") ?? "").includes("application/json")) {
    return "Send JSON.";
  }
  return null;
}

/** Link-local and cloud metadata addresses are never a broker. */
export function forbiddenHost(origin: string): boolean {
  let host: string;
  try {
    host = new URL(origin).hostname.toLowerCase();
  } catch {
    return true;
  }
  if (host === "metadata.google.internal") return true;
  if (/^169\.254\./.test(host)) return true;
  if (host.startsWith("[fe8") || host.startsWith("[fe9") || host.startsWith("[fea") || host.startsWith("[feb")) return true;
  return false;
}

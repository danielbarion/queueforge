/**
 * Management session cookie for the Bun admin.
 *
 * The cookie name stays `queueforge_session`, with the management port
 * appended when the request host includes a non-default port. That keeps
 * the Rust admin on 36673 and the Bun admin on 36674 from sharing one cookie.
 */
/** Base cookie name. A port suffix is added by {@link cookieNameFromHost}. */
export const COOKIE = "queueforge_session";

/** In-memory login tokens. Lost on process restart, matching the previous module. */
export const sessions = new Map<string, { user: string; tags: string[] }>();

/**
 * Pick the session cookie name for this request host.
 *
 * @param host The Host header, or null when the client omitted it. A comma-separated
 * list uses the first value. An IPv6 host in brackets is ignored except for its port.
 * @returns `queueforge_session`, or `queueforge_session_<port>` when the port is
 * present and is not 80 or 443. Callers must use this same name to set and clear
 * the cookie, or the browser will keep a stale login.
 */
export function cookieNameFromHost(host: string | null): string {
  const raw = (host ?? "").split(",")[0]?.trim() ?? "";
  const port = raw.startsWith("[")
    ? raw.slice(raw.indexOf("]") + 1).replace(/^:/, "")
    : (raw.split(":").length > 1 ? raw.slice(raw.lastIndexOf(":") + 1) : "");
  const n = Number(port);
  if (Number.isInteger(n) && n > 0 && n !== 80 && n !== 443) return `${COOKIE}_${n}`;
  return COOKIE;
}

/**
 * Read one cookie value from a Cookie header.
 *
 * @param header The raw Cookie header, or null.
 * @param name Cookie name, including any port suffix.
 * @returns The value, or null when the header is missing or has no such name.
 * The match is prefix-based on `name=`, so `name` must not be a prefix of another cookie.
 */
export function tokenOf(header: string | null, name: string): string | null {
  if (!header) return null;
  for (const part of header.split(";")) {
    const t = part.trim();
    if (t.startsWith(`${name}=`)) return t.slice(name.length + 1);
  }
  return null;
}

/**
 * Resolve the logged-in user for this request.
 *
 * @param header The Cookie header, or null.
 * @param host The Host header, or null. It selects which cookie name is read.
 * @param authorization The Authorization header. HTTP Basic credentials that
 * {@link verifyBasic} already checked are accepted, as RabbitMQ's API does.
 * @returns The session user and tags, or null when neither is valid.
 */
export function requireUser(header: string | null, host: string | null, authorization: string | null = null) {
  const token = tokenOf(header, cookieNameFromHost(host));
  const fromCookie = token ? sessions.get(token) : undefined;
  if (fromCookie) return fromCookie;
  if (!authorization) return null;
  const basic = basicSessions.get(authorization);
  if (!basic || basic.until < Date.now()) return null;
  return basic.who;
}

/** Verified Basic headers. A hash check per request would cost a PBKDF2 round each time. */
const basicSessions = new Map<string, { who: { user: string; tags: string[] }; until: number }>();
const BASIC_TTL_MS = 5_000;
const BASIC_MAX = 1024;

/**
 * Check an `Authorization: Basic` header once and remember it briefly, so the
 * synchronous route checks can accept it. A wrong password is not cached.
 */
export async function verifyBasic(
  authorization: string | null,
  verify: (user: string, password: string) => Promise<boolean>,
  tags: (user: string) => string[],
): Promise<void> {
  if (!authorization || !authorization.startsWith("Basic ")) return;
  const cached = basicSessions.get(authorization);
  if (cached && cached.until >= Date.now()) return;
  let decoded = "";
  try {
    decoded = atob(authorization.slice(6).trim());
  } catch {
    return;
  }
  const at = decoded.indexOf(":");
  if (at <= 0) return;
  const user = decoded.slice(0, at);
  const password = decoded.slice(at + 1);
  if (!(await verify(user, password))) {
    basicSessions.delete(authorization);
    return;
  }
  const userTags = tags(user);
  if (!userTags.includes("administrator") && !userTags.includes("management") && !userTags.includes("monitoring")) return;
  if (basicSessions.size >= BASIC_MAX) basicSessions.clear();
  basicSessions.set(authorization, { who: { user, tags: userTags }, until: Date.now() + BASIC_TTL_MS });
}

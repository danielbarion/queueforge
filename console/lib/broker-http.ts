export function managementOrigin(input: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(input.trim());
  } catch {
    return null;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
  if (parsed.username || parsed.password) return null;
  return parsed.origin;
}

export function cookiePair(header: string): string | null {
  const pair = header.split(";")[0]?.trim() ?? "";
  if (!pair.includes("=") || pair.startsWith("=")) return null;
  return pair;
}

const BLOCKED = new Set(["/api/login", "/api/logout"]);

/**
 * A broker path the proxy may call, or null.
 *
 * The check runs on the path a URL parser produces, the same one `fetch`
 * sends, so `%2e%2e`, a doubled slash or a trailing slash cannot step
 * outside `/api/` or reach the login routes. `%2F` inside a segment (the
 * default vhost) is kept as written.
 */
export function brokerPath(input: string): string | null {
  if (!input.startsWith("/") || input.startsWith("//")) return null;
  if (input.includes("\\") || input.includes("#") || input.includes("://")) return null;
  // Dot segments, plain or percent-encoded, would be resolved away by the parser below.
  if (/(^|\/)\.\.?(\/|\?|$)/.test(input) || /%2e/i.test(input) || /%5c/i.test(input)) return null;
  let parsed: URL;
  try {
    parsed = new URL(input, "http://broker.invalid");
  } catch {
    return null;
  }
  const pathname = parsed.pathname;
  if (pathname.includes("..") || pathname.includes("//")) return null;
  const bare = pathname.replace(/\/+$/, "").toLowerCase();
  if (BLOCKED.has(bare)) return null;
  const open = pathname === "/healthz" || pathname === "/readyz" || pathname === "/metrics" || pathname.startsWith("/api/");
  return open ? pathname + parsed.search : null;
}

export function sumConfirmBeforeFsync(text: string): number {
  let total = 0;
  for (const line of text.split("\n")) {
    const match = /^queueforge_confirm_before_fsync_total(?:\{.*\})?\s+(\d+(?:\.\d+)?)$/.exec(line.trim());
    if (match) total += Number(match[1]);
  }
  return total;
}

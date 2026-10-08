import { NextResponse } from "next/server";
import { cookiePair, managementOrigin } from "@/lib/broker-http";
import { consoleSid, crossSite, forbiddenHost, newSid, setCredential, sidCookie, type BrokerCredential } from "@/lib/guard";

export const dynamic = "force-dynamic";

function sessionCookie(response: Response): string | null {
  const listed = typeof response.headers.getSetCookie === "function" ? response.headers.getSetCookie() : [];
  const pairs = listed.map((header) => cookiePair(header)).filter((pair): pair is string => pair !== null);
  if (pairs.length > 0) return pairs.join("; ");
  const single = response.headers.get("set-cookie");
  return single ? cookiePair(single) : null;
}

/**
 * Log in to a broker. QueueForge answers `POST /api/login` with a session
 * cookie. RabbitMQ has no such route; it takes HTTP Basic on every call, so
 * the console checks the login with `GET /api/whoami` and keeps the header.
 */
export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  let payload: { url?: string; username?: string; password?: string };
  try {
    payload = (await request.json()) as typeof payload;
  } catch {
    return NextResponse.json({ error: "Send the broker and the login." }, { status: 400 });
  }
  const origin = managementOrigin(String(payload.url ?? ""));
  const username = String(payload.username ?? "");
  const password = String(payload.password ?? "");
  if (!origin || !username || !password || forbiddenHost(origin)) {
    return NextResponse.json({ error: "Name the broker, the user, and the password." }, { status: 400 });
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);
  try {
    let credential: BrokerCredential | null = null;
    let name = username;
    const response = await fetch(new URL("/api/login", origin), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username, password }),
      redirect: "manual",
      cache: "no-store",
      signal: controller.signal,
    });
    // RabbitMQ has no session login, so without a cookie the login is
    // checked with Basic auth below. QueueForge takes Basic too, so a wrong
    // password is refused there as well.
    const cookie = response.ok ? sessionCookie(response) : null;
    if (response.status === 403) return NextResponse.json({ error: "That user has no management access." }, { status: 403 });
    if (cookie) {
      credential = { kind: "cookie", value: cookie };
      try {
        name = ((await response.json()) as { name?: string }).name ?? username;
      } catch {
        /* a login body without a name keeps the typed one */
      }
    } else {
      const basic = `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
      const whoami = await fetch(new URL("/api/whoami", origin), {
        headers: { authorization: basic },
        redirect: "manual",
        cache: "no-store",
        signal: controller.signal,
      });
      if (whoami.status === 401) return NextResponse.json({ error: "Those credentials were refused." }, { status: 401 });
      if (!whoami.ok) return NextResponse.json({ error: "The broker refused the login." }, { status: 422 });
      credential = { kind: "basic", value: basic };
    }
    const sid = consoleSid(request) ?? newSid();
    setCredential(sid, origin, credential);
    const out = NextResponse.json({ name });
    out.headers.append("set-cookie", sidCookie(sid, new URL(request.url).protocol === "https:"));
    return out;
  } catch {
    return NextResponse.json({ error: "Could not reach that address." }, { status: 422 });
  } finally {
    clearTimeout(timer);
  }
}

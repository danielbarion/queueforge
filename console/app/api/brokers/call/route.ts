import { NextResponse } from "next/server";
import { brokerPath, managementOrigin } from "@/lib/broker-http";
import { consoleSid, credentialHeaders, crossSite, dropCredential, forbiddenHost, getCredential } from "@/lib/guard";

export const dynamic = "force-dynamic";

const METHODS = new Set(["GET", "POST", "PUT", "DELETE"]);
/** Health probes need no login; everything else only goes to a broker this browser logged into. */
const OPEN = new Set(["/healthz", "/readyz", "/api/identity"]);

export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  let payload: { url?: string; method?: string; path?: string; body?: unknown };
  try {
    payload = (await request.json()) as typeof payload;
  } catch {
    return NextResponse.json({ error: "Send a broker call." }, { status: 400 });
  }
  const origin = managementOrigin(String(payload.url ?? ""));
  const path = brokerPath(String(payload.path ?? ""));
  const method = String(payload.method ?? "").toUpperCase();
  if (!origin || !path || !METHODS.has(method) || forbiddenHost(origin)) {
    return NextResponse.json({ error: "That call is not allowed." }, { status: 400 });
  }
  const sid = consoleSid(request);
  const credential = getCredential(sid, origin);
  const pathname = path.split("?")[0] ?? "";
  if (!credential && !OPEN.has(pathname)) {
    return NextResponse.json({ status: 401, body: null, text: "" });
  }

  const headers = new Headers();
  credentialHeaders(credential, headers);
  let body: string | undefined;
  if ((method === "POST" || method === "PUT") && payload.body !== undefined) {
    headers.set("content-type", "application/json");
    body = JSON.stringify(payload.body);
  }

  // Definitions can be large; everything else should answer quickly.
  const big = pathname.startsWith("/api/definitions");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), big ? 20000 : 5000);
  try {
    const response = await fetch(new URL(path, origin), {
      method,
      headers,
      body,
      redirect: "manual",
      cache: "no-store",
      signal: controller.signal,
    });
    // A session the broker no longer accepts is forgotten, so the pages show
    // the login form once instead of flipping back every poll.
    if (response.status === 401) dropCredential(sid, origin);
    const text = (await response.text()).slice(0, big ? 20_000_000 : 2_000_000);
    let parsed: unknown = null;
    if ((response.headers.get("content-type") ?? "").includes("json") && text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = null;
      }
    }
    return NextResponse.json({ status: response.status, body: parsed, text });
  } catch {
    return NextResponse.json({ error: "Could not reach that address." }, { status: 422 });
  } finally {
    clearTimeout(timer);
  }
}

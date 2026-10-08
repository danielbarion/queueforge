import { NextResponse } from "next/server";
import { managementOrigin } from "@/lib/broker-http";
import { consoleSid, crossSite, dropCredential, getCredential } from "@/lib/guard";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  let payload: { url?: string };
  try {
    payload = (await request.json()) as { url?: string };
  } catch {
    return NextResponse.json({ error: "Send the broker." }, { status: 400 });
  }
  const origin = managementOrigin(String(payload.url ?? ""));
  if (!origin) return NextResponse.json({ error: "That address is not allowed." }, { status: 400 });
  const sid = consoleSid(request);
  const credential = getCredential(sid, origin);
  dropCredential(sid, origin);
  // Basic auth has no server session to end.
  if (credential?.kind === "cookie") {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 3000);
    try {
      await fetch(new URL("/api/logout", origin), {
        method: "POST",
        headers: { cookie: credential.value },
        redirect: "manual",
        cache: "no-store",
        signal: controller.signal,
      });
    } catch {
      // The console session is already gone. The broker may keep its copy until it expires.
    } finally {
      clearTimeout(timer);
    }
  }
  return NextResponse.json({ ok: true });
}

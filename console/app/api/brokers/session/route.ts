import { NextResponse } from "next/server";
import { managementOrigin } from "@/lib/broker-http";
import { currentSession, loadCredential } from "@/lib/accounts";
import { crossSite } from "@/lib/guard";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  const origin = managementOrigin(new URL(request.url).searchParams.get("url") ?? "");
  if (!origin) return NextResponse.json({ error: "That address is not allowed." }, { status: 400 });
  const session = await currentSession(request.headers.get("cookie"));
  if (!session) return NextResponse.json({ error: "Log in." }, { status: 401 });
  return NextResponse.json({ loggedIn: (await loadCredential(session.id, origin)) !== null });
}

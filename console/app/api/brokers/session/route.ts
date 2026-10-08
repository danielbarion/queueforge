import { NextResponse } from "next/server";
import { managementOrigin } from "@/lib/broker-http";
import { consoleSid, crossSite, getCredential } from "@/lib/guard";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  const origin = managementOrigin(new URL(request.url).searchParams.get("url") ?? "");
  if (!origin) return NextResponse.json({ error: "That address is not allowed." }, { status: 400 });
  return NextResponse.json({ loggedIn: getCredential(consoleSid(request), origin) !== null });
}

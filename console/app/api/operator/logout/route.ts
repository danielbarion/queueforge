import { NextResponse } from "next/server";
import { crossSite } from "@/lib/guard";
import { clearOperatorCookie, requestIsSecure } from "@/lib/operator";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  const response = NextResponse.json({ ok: true });
  response.headers.append("set-cookie", clearOperatorCookie(requestIsSecure(request)));
  return response;
}

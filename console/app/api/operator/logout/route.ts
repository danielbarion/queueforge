import { NextResponse } from "next/server";
import { crossSite } from "@/lib/guard";
import { endSession } from "@/lib/accounts";
import { clearOperatorCookie, requestIsSecure } from "@/lib/operator";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  await endSession(request.headers.get("cookie"));
  const response = NextResponse.json({ ok: true });
  response.headers.append("set-cookie", clearOperatorCookie(requestIsSecure(request)));
  return response;
}

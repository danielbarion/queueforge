import { NextResponse } from "next/server";
import { registerOperator } from "@/lib/accounts";
import { crossSite } from "@/lib/guard";
import { operatorCookie, requestIsSecure } from "@/lib/operator";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  let payload: { username?: string; password?: string };
  try {
    payload = (await request.json()) as typeof payload;
  } catch {
    return NextResponse.json({ error: "Send the user and the password." }, { status: 400 });
  }
  const result = await registerOperator(String(payload.username ?? ""), String(payload.password ?? ""));
  if (!result.ok) return NextResponse.json({ error: result.error }, { status: result.status });
  const response = NextResponse.json({ ok: true });
  response.headers.append("set-cookie", operatorCookie(result.sessionId, requestIsSecure(request)));
  return response;
}

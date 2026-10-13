import { NextResponse } from "next/server";
import { crossSite } from "@/lib/guard";
import { loginOperator } from "@/lib/accounts";
import { databaseConfigured, operatorCookie, requestIsSecure } from "@/lib/operator";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  if (!databaseConfigured()) return NextResponse.json({ error: "The console database is not configured." }, { status: 503 });
  let payload: { username?: string; password?: string };
  try {
    payload = (await request.json()) as typeof payload;
  } catch {
    return NextResponse.json({ error: "Send the user and the password." }, { status: 400 });
  }
  const id = await loginOperator(String(payload.username ?? ""), String(payload.password ?? ""));
  if (!id) return NextResponse.json({ error: "Those credentials were refused." }, { status: 401 });
  const response = NextResponse.json({ ok: true });
  response.headers.append("set-cookie", operatorCookie(id, requestIsSecure(request)));
  return response;
}

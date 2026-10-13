import { NextResponse } from "next/server";
import { crossSite } from "@/lib/guard";
import { credentialsMatch, issueSession, operatorConfigured, operatorCookie, requestIsSecure } from "@/lib/operator";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  if (!operatorConfigured()) return NextResponse.json({ error: "Operator login is not configured." }, { status: 503 });
  let payload: { username?: string; password?: string };
  try {
    payload = (await request.json()) as typeof payload;
  } catch {
    return NextResponse.json({ error: "Send the user and the password." }, { status: 400 });
  }
  const username = String(payload.username ?? "");
  const password = String(payload.password ?? "");
  if (!(await credentialsMatch(username, password))) {
    return NextResponse.json({ error: "Those credentials were refused." }, { status: 401 });
  }
  const response = NextResponse.json({ ok: true });
  response.headers.append("set-cookie", operatorCookie(await issueSession(), requestIsSecure(request)));
  return response;
}

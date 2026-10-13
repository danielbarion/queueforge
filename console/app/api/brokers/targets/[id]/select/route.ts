import { NextResponse } from "next/server";
import { currentSession, selectBroker } from "@/lib/accounts";
import { crossSite } from "@/lib/guard";

export const dynamic = "force-dynamic";

export async function POST(request: Request, context: { params: Promise<{ id: string }> }) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  const session = await currentSession(request.headers.get("cookie"));
  if (!session) return NextResponse.json({ error: "Log in." }, { status: 401 });
  await selectBroker(session.operatorId, (await context.params).id);
  return NextResponse.json({ ok: true });
}

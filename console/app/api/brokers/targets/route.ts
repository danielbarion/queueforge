import { NextResponse } from "next/server";
import { currentSession, listBrokers, saveBroker, type SavedBroker } from "@/lib/accounts";
import { crossSite } from "@/lib/guard";

export const dynamic = "force-dynamic";

const KINDS = new Set(["rust", "bun", "php", "rabbitmq"]);

export async function GET(request: Request) {
  const session = await currentSession(request.headers.get("cookie"));
  if (!session) return NextResponse.json({ error: "Log in." }, { status: 401 });
  const brokers = await listBrokers(session.operatorId);
  const selected = brokers.find((broker) => broker.selected);
  return NextResponse.json({ brokers, selectedId: selected?.id ?? "" });
}

export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  const session = await currentSession(request.headers.get("cookie"));
  if (!session) return NextResponse.json({ error: "Log in." }, { status: 401 });
  let payload: Partial<SavedBroker>;
  try {
    payload = (await request.json()) as Partial<SavedBroker>;
  } catch {
    return NextResponse.json({ error: "Send the broker." }, { status: 400 });
  }
  const name = String(payload.name ?? "").trim();
  const url = String(payload.url ?? "").trim();
  const kind = String(payload.kind ?? "");
  const id = String(payload.id ?? "");
  if (!name || !url || !KINDS.has(kind) || !/^[0-9a-f-]{36}$/i.test(id)) {
    return NextResponse.json({ error: "Name the broker." }, { status: 400 });
  }
  const saved = await saveBroker(session.operatorId, { id, name, url, kind: kind as SavedBroker["kind"] });
  return NextResponse.json(saved);
}

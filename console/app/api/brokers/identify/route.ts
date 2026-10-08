import { NextResponse } from "next/server";
import { managementOrigin } from "@/lib/broker-http";
import { crossSite, forbiddenHost } from "@/lib/guard";

const KINDS = new Set(["rust", "bun", "php"]);

/**
 * Tell what answers at a management URL. QueueForge names itself at
 * `/api/identity`. RabbitMQ has no such route; its management API answers
 * `/api/overview` with 401 and a Basic challenge before a login.
 */
export async function POST(request: Request) {
  const refused = crossSite(request);
  if (refused) return NextResponse.json({ error: refused }, { status: 403 });
  let body: { url?: string };
  try {
    body = (await request.json()) as { url?: string };
  } catch {
    return NextResponse.json({ error: "Send a management URL." }, { status: 400 });
  }

  const origin = managementOrigin(String(body.url ?? ""));
  if (!origin || forbiddenHost(origin)) {
    return NextResponse.json({ error: "The management URL has to be a full http or https address." }, { status: 400 });
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 3000);
  try {
    const response = await fetch(new URL("/api/identity", origin), {
      signal: controller.signal,
      redirect: "manual",
      cache: "no-store",
    });
    if (response.ok) {
      const json = (await response.json().catch(() => ({}))) as { product_name?: string; kind?: string };
      if (json.product_name === "QueueForge" && json.kind && KINDS.has(json.kind)) {
        return NextResponse.json({ kind: json.kind, url: origin });
      }
    }
    const overview = await fetch(new URL("/api/overview", origin), {
      signal: controller.signal,
      redirect: "manual",
      cache: "no-store",
    });
    const challenge = overview.headers.get("www-authenticate") ?? "";
    if (overview.status === 401 && /basic/i.test(challenge)) {
      return NextResponse.json({ kind: "rabbitmq", url: origin });
    }
    return NextResponse.json({ error: "That address did not answer as a QueueForge or RabbitMQ broker." }, { status: 422 });
  } catch {
    return NextResponse.json({ error: "Could not reach that address." }, { status: 422 });
  } finally {
    clearTimeout(timer);
  }
}

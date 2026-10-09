import type { BrokerKind } from "../stores/broker";

export type MessageSnapshot = {
  payload: string;
  encoding: "string" | "base64";
  payloadBytes: number;
  exchange: string;
  routingKey: string;
  redelivered: boolean;
  properties: Record<string, unknown>;
  propertiesAvailable: boolean;
  complete: boolean;
};
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);

/** Decode for presentation/download without changing the saved payload. */
export function messageBytes(message: Pick<MessageSnapshot, "payload" | "encoding">): Uint8Array | null {
  if (message.encoding === "string") return new TextEncoder().encode(message.payload);
  try {
    // API base64 must represent bytes, rather than silently ignoring garbage.
    if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(message.payload)) return null;
    const binary = atob(message.payload);
    if (btoa(binary) !== message.payload) return null;
    return Uint8Array.from(binary, character => character.charCodeAt(0));
  } catch { return null; }
}

/** A malformed response is unknown, not an empty queue or fabricated metadata. */
export function parseMessages(body: unknown): MessageSnapshot[] | null {
  if (!Array.isArray(body)) return null;
  const messages: MessageSnapshot[] = [];
  for (const row of body) {
    if (!record(row) || typeof row.payload !== "string" || (row.payload_encoding !== "string" && row.payload_encoding !== "base64") ||
      typeof row.payload_bytes !== "number" || !Number.isSafeInteger(row.payload_bytes) || row.payload_bytes < 0 ||
      typeof row.exchange !== "string" || typeof row.routing_key !== "string" || typeof row.redelivered !== "boolean" ||
      (row.properties !== undefined && !record(row.properties))) return null;
    const encoding = row.payload_encoding as MessageSnapshot["encoding"];
    const bytes = messageBytes({ payload: row.payload, encoding });
    if (!bytes) return null;
    messages.push({ payload: row.payload, encoding, payloadBytes: row.payload_bytes, exchange: row.exchange,
      routingKey: row.routing_key, redelivered: row.redelivered, properties: record(row.properties) ? row.properties : {},
      propertiesAvailable: record(row.properties), complete: bytes.byteLength === row.payload_bytes });
  }
  return messages;
}

/** Requeue explicitly for RabbitMQ-style ack modes and PHP's boolean API. */
export function inspectionBody(_kind: BrokerKind, count: number) {
  return { count: Math.min(20, Math.max(1, Number.isFinite(count) ? Math.trunc(count) : 1)),
    ackmode: "ack_requeue_true", requeue: true, encoding: "base64" };
}

export function payloadText(message: MessageSnapshot): string {
  const bytes = messageBytes(message);
  if (!bytes) return message.payload;
  let text: string;
  try { text = new TextDecoder("utf-8", { fatal: true }).decode(bytes); }
  catch { return message.payload; } // Binary data stays available in its original encoding.
  try { return JSON.stringify(JSON.parse(text), null, 2); }
  catch { return text; }
}

/** Replay publishes a COPY of this snapshot; its queued original is retained. */
export function replayAvailability(kind: BrokerKind, demo: boolean, message: MessageSnapshot): { allowed: boolean; reason: string } {
  if (demo) return { allowed: false, reason: "Demo mode cannot publish messages." };
  if (!message.complete || messageBytes(message)?.byteLength !== message.payloadBytes) return { allowed: false, reason: "The inspected payload is incomplete. A complete snapshot is required to copy it." };
  if (!message.propertiesAvailable) return { allowed: false, reason: "The inspection response omitted message properties; a faithful copy is unavailable." };
  if (kind !== "rabbitmq") {
    const gaps: Record<Exclude<BrokerKind, "rabbitmq">, string> = {
      rust: "Rust's management get/publish APIs omit headers.",
      bun: "Bun's management publish API strips message properties.",
      php: "PHP's management get API omits message properties.",
    };
    return { allowed: false, reason: `${gaps[kind]} Full property preservation is unavailable.` };
  }
  return { allowed: true, reason: "Publish a copy using the saved payload and properties. The original remains queued; no additional get or delete is performed." };
}

export type DeadLetterDetail = { queue: string | null; reason: string | null; count: number | null; exchange: string | null; routingKeys: string[] | null };
/** RabbitMQ's plain JSON x-death tables; omitted/malformed fields stay unknown. */
export function deadLetterDetails(message: MessageSnapshot): DeadLetterDetail[] {
  const headers = message.properties.headers;
  if (!message.propertiesAvailable || !record(headers) || !Array.isArray(headers["x-death"])) return [];
  return headers["x-death"].filter(record).map(death => ({
    queue: typeof death.queue === "string" ? death.queue : null,
    reason: typeof death.reason === "string" ? death.reason : null,
    count: typeof death.count === "number" && Number.isSafeInteger(death.count) && death.count >= 0 ? death.count : null,
    exchange: typeof death.exchange === "string" ? death.exchange : null,
    routingKeys: Array.isArray(death["routing-keys"]) && death["routing-keys"].every(key => typeof key === "string") ? death["routing-keys"] as string[] : null,
  }));
}

import type { Field } from "./codec.ts";

export type QuorumAppend = {
  vhost: string;
  queue: string;
  messageId: string;
  body: Uint8Array;
  exchange: string;
  routingKey: string;
  persistent: boolean;
  priority: number;
  expiration: string;
  headers: Array<[string, Field]>;
  propRaw: Uint8Array;
};

/** Version 1 quorum body. Rust encodes the same fields and does not read this node's files. */
export function encodeQuorumAppend(src: {
  vhost: string;
  queue: string;
  messageId: string;
  body: Uint8Array;
  exchange: string;
  routingKey: string;
  persistent: boolean;
  headers?: Array<[string, unknown]>;
  propRaw?: Uint8Array;
  priority?: number;
  expiration?: string;
}): Record<string, unknown> {
  return {
    v: 1,
    vhost: src.vhost,
    queue: src.queue,
    message_id: src.messageId,
    body_b64: Buffer.from(src.body).toString("base64"),
    persistent: src.persistent,
    routing_key: src.routingKey,
    exchange: src.exchange,
    // Properties, so a follower that becomes leader delivers the message as published.
    // Readers that do not know these fields ignore them.
    ...(src.propRaw && src.propRaw.length ? { propRaw: Buffer.from(src.propRaw).toString("base64") } : {}),
    ...(src.headers && src.headers.length ? { headers: src.headers } : {}),
    ...(src.priority ? { priority: src.priority } : {}),
    ...(src.expiration ? { expiration: src.expiration } : {}),
  };
}

/** Decode a version-1 quorum append, or the older `body` / nested `message` shapes. */
export function decodeQuorumAppend(payload: Record<string, unknown>): QuorumAppend {
  const nested = payload.message as Record<string, unknown> | undefined;
  const source = nested ?? payload;
  const b64 = String(source.body_b64 ?? source.body ?? "");
  const messageId = String(source.message_id ?? source.qid ?? source.id ?? "");
  const routingKey = String(source.routing_key ?? source.routingKey ?? "");
  return {
    vhost: String(payload.vhost ?? ""),
    queue: String(payload.queue ?? ""),
    messageId,
    body: new Uint8Array(Buffer.from(b64, "base64")),
    exchange: String(source.exchange ?? ""),
    routingKey,
    persistent: source.persistent !== false,
    priority: Number(source.priority ?? 0),
    expiration: String(source.expiration ?? ""),
    headers: (source.headers as QuorumAppend["headers"]) ?? [],
    propRaw: new Uint8Array(Buffer.from(String(source.propRaw ?? ""), "base64")),
  };
}

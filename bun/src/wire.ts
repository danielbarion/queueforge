import type { Field } from "./codec.ts";
import { POOL_MIN, taintBody } from "./body-pool.ts";

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

/** The body bytes a locally built append carries next to `body_b64`. JSON never sees it. */
export const QUORUM_BODY: unique symbol = Symbol("quorum body");

/**
 * Version 1 quorum body. Rust encodes the same fields and does not read this node's files.
 *
 * `body_b64` is computed on first read (a peer send, a snapshot, a JSON log line)
 * and kept; a one-voter group never reads it. It is an own enumerable getter, so
 * JSON, `Object.keys` and spreads see the same object as a plain string field.
 */
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
  let b64: string | null = null;
  const body = src.body;
  // Read later, from the log, a snapshot or a peer send.
  if (body.length >= POOL_MIN) taintBody(body);
  return {
    v: 1,
    vhost: src.vhost,
    queue: src.queue,
    message_id: src.messageId,
    get body_b64() {
      return (b64 ??= Buffer.from(body.buffer, body.byteOffset, body.byteLength).toString("base64"));
    },
    persistent: src.persistent,
    routing_key: src.routingKey,
    exchange: src.exchange,
    // Properties, so a follower that becomes leader delivers the message as published.
    // Readers that do not know these fields ignore them.
    ...(src.propRaw && src.propRaw.length ? { propRaw: Buffer.from(src.propRaw).toString("base64") } : {}),
    ...(src.headers && src.headers.length ? { headers: src.headers } : {}),
    ...(src.priority ? { priority: src.priority } : {}),
    ...(src.expiration ? { expiration: src.expiration } : {}),
    [QUORUM_BODY]: body,
  };
}

/** The body of a quorum append: the bytes it was built from, or its base64 decoded. */
export function quorumAppendBody(payload: Record<string, unknown>): Uint8Array {
  const raw = (payload as { [QUORUM_BODY]?: Uint8Array })[QUORUM_BODY];
  if (raw) return raw;
  const source = (payload.message as Record<string, unknown> | undefined) ?? payload;
  return new Uint8Array(Buffer.from(String(source.body_b64 ?? source.body ?? ""), "base64"));
}

/** The raw body of an append built here, or null for one that arrived as JSON. */
export function rawQuorumBody(payload: unknown): Uint8Array | null {
  return (payload as { [QUORUM_BODY]?: Uint8Array } | null)?.[QUORUM_BODY] ?? null;
}

/** The append's fields without `body_b64`, without computing it: what a binary log record stores beside the body. */
export function quorumAppendHead(payload: Record<string, unknown>): Record<string, unknown> {
  const head: Record<string, unknown> = {};
  for (const k of Object.keys(payload)) if (k !== "body_b64") head[k] = payload[k];
  return head;
}

/** Rebuild an append from `quorumAppendHead` and its body, with `body_b64` back in its place. */
export function withQuorumBody(head: Record<string, unknown>, body: Uint8Array): Record<string, unknown> {
  let b64: string | null = null;
  if (body.length >= POOL_MIN) taintBody(body);
  const out: Record<string, unknown> = {};
  const define = () =>
    Object.defineProperty(out, "body_b64", {
      enumerable: true,
      configurable: true,
      get: () => (b64 ??= Buffer.from(body.buffer, body.byteOffset, body.byteLength).toString("base64")),
    });
  let placed = false;
  for (const k of Object.keys(head)) {
    out[k] = head[k];
    if (k === "message_id") {
      define();
      placed = true;
    }
  }
  if (!placed) define();
  (out as { [QUORUM_BODY]?: Uint8Array })[QUORUM_BODY] = body;
  return out;
}

/** `vhost\0queue\0messageId` of a quorum append, as `decodeQuorumAppend` reads them, without decoding the body. */
export function quorumAppendKey(payload: Record<string, unknown>): string {
  const source = (payload.message as Record<string, unknown> | undefined) ?? payload;
  const messageId = String(source.message_id ?? source.qid ?? source.id ?? "");
  return `${String(payload.vhost ?? "")}\0${String(payload.queue ?? "")}\0${messageId}`;
}

/** Decode a version-1 quorum append, or the older `body` / nested `message` shapes. */
export function decodeQuorumAppend(payload: Record<string, unknown>): QuorumAppend {
  const nested = payload.message as Record<string, unknown> | undefined;
  const source = nested ?? payload;
  const messageId = String(source.message_id ?? source.qid ?? source.id ?? "");
  const routingKey = String(source.routing_key ?? source.routingKey ?? "");
  return {
    vhost: String(payload.vhost ?? ""),
    queue: String(payload.queue ?? ""),
    messageId,
    body: quorumAppendBody(payload),
    exchange: String(source.exchange ?? ""),
    routingKey,
    persistent: source.persistent !== false,
    priority: Number(source.priority ?? 0),
    expiration: String(source.expiration ?? ""),
    headers: (source.headers as QuorumAppend["headers"]) ?? [],
    // Bytes when the payload came in a binary cluster frame.
    propRaw: source.propRaw instanceof Uint8Array ? source.propRaw : new Uint8Array(Buffer.from(String(source.propRaw ?? ""), "base64")),
  };
}

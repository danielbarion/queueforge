/**
 * Queue argument parsing and dead-letter header encoding.
 */
import { replaceHeaderTable, writeTable, W, type Field } from "../codec.ts";
import type { LiveMsg, QArgs, QueueLive } from "./model.ts";
import { headerList, overflowOf } from "./routing.ts";

/**
 * Parse queue arguments into the live argument record.
 *
 * @param raw Declare arguments. Missing keys become null. Unknown keys are ignored.
 * @returns The parsed arguments. An absent or unknown `x-overflow` becomes `drop-head`. Only `quorum` selects a quorum queue; every other type is classic.
 */
export function parseArgs(raw: Record<string, string | number>): QArgs {
  const num = (k: string) => (raw[k] == null ? null : Number(raw[k]));
  const str = (k: string) => (raw[k] == null ? null : String(raw[k]));
  const rawOverflow = str("x-overflow");
  const overflow = rawOverflow === "reject-publish" || rawOverflow === "reject-publish-dlx" ? rawOverflow : "drop-head";
  const rawStrategy = str("x-dead-letter-strategy");
  const dlxStrategy = rawStrategy === "at-least-once" ? "at-least-once" : "at-most-once";
  const maxPriority = num("x-max-priority");
  const qtype = str("x-queue-type");
  return {
    messageTtl: num("x-message-ttl"),
    expiresMs: num("x-expires"),
    maxLength: num("x-max-length"),
    maxLengthBytes: num("x-max-length-bytes"),
    overflow,
    dlxStrategy,
    dlx: str("x-dead-letter-exchange"),
    dlxKey: str("x-dead-letter-routing-key"),
    maxPriority: maxPriority && maxPriority > 0 ? maxPriority : null,
    singleActive: str("x-single-active-consumer") === "true" || raw["x-single-active-consumer"] === 1,
    deliveryLimit: num("x-delivery-limit"),
    queueType: qtype === "quorum" ? "quorum" : qtype === "stream" ? "stream" : "classic",
  };
}

/**
 * Record one death in the `x-death` headers, as RabbitMQ does.
 *
 * @param queue Queue the message died from.
 * @param reason Death reason.
 * @param exchange Exchange the message was last published to.
 * @param routingKey Routing key stored as the only item of `routing-keys`.
 * @param prev Previous headers. An `x-death` entry for the same queue and
 * reason has its count raised and moves to the front; other entries stay.
 * The first-death headers are set once and kept; the last-death headers
 * always name this death.
 * @returns A new header list. The caller attaches it; this function does not publish.
 */
export function deathHeaders(
  queue: string,
  reason: string,
  exchange: string,
  routingKey: string,
  prev: Array<[string, Field]>,
): Array<[string, Field]> {
  const old = prev.find(([k]) => k === "x-death")?.[1];
  const entries: Array<Array<[string, Field]>> = [];
  if (old?.t === "A") {
    for (const item of old.v as Field[]) if (item.t === "F") entries.push(item.v as Array<[string, Field]>);
  }
  const text = (e: Array<[string, Field]>, k: string) => {
    const f = e.find(([key]) => key === k)?.[1];
    return f && (f.t === "S" || f.t === "s") ? String(f.v) : "";
  };
  const at = entries.findIndex((e) => text(e, "queue") === queue && text(e, "reason") === reason);
  let count = 1;
  if (at >= 0) {
    const f = entries[at]!.find(([k]) => k === "count")?.[1];
    count = (f && "v" in f ? Number(f.v) : 0) + 1;
    entries.splice(at, 1);
  }
  const entry: Array<[string, Field]> = [
    ["queue", { t: "S", v: queue }],
    ["reason", { t: "S", v: reason }],
    ["count", { t: "l", v: count }],
    ["exchange", { t: "S", v: exchange }],
    ["routing-keys", { t: "A", v: [{ t: "S", v: routingKey }] }],
    ["time", { t: "T", v: Math.floor(Date.now() / 1000) }],
  ];
  const firstReason = prev.find(([k]) => k === "x-first-death-reason")?.[1];
  const firstQueue = prev.find(([k]) => k === "x-first-death-queue")?.[1];
  const firstExchange = prev.find(([k]) => k === "x-first-death-exchange")?.[1];
  const dropped = new Set([
    "x-death",
    "x-first-death-reason",
    "x-first-death-queue",
    "x-first-death-exchange",
    "x-last-death-reason",
    "x-last-death-queue",
    "x-last-death-exchange",
  ]);
  const rest = prev.filter(([k]) => !dropped.has(k));
  return [
    ...rest,
    ["x-death", { t: "A", v: [{ t: "F", v: entry }, ...entries.map((e) => ({ t: "F", v: e }) as Field)] }],
    ["x-first-death-reason", firstReason ?? { t: "S", v: reason }],
    ["x-first-death-queue", firstQueue ?? { t: "S", v: queue }],
    ["x-first-death-exchange", firstExchange ?? { t: "S", v: exchange }],
    ["x-last-death-reason", { t: "S", v: reason }],
    ["x-last-death-queue", { t: "S", v: queue }],
    ["x-last-death-exchange", { t: "S", v: exchange }],
  ];
}

/**
 * Encode basic properties that carry only a headers table.
 *
 * @param headers Header table written after the headers flag. Other property flags stay clear.
 * @returns Property bytes a publish can store as `propRaw`.
 */
export function propsWithDeath(headers: Array<[string, Field]>): Uint8Array {
  const w = new W();
  w.u16(0x2000);
  writeTable(w, headers);
  return w.concat();
}

/**
 * Copy numeric, boolean, and string fields into a plain argument map.
 *
 * @param fields AMQP field table. Arrays, tables, and other types are skipped.
 * @returns A map whose booleans are `1` or `0`. Keys that were skipped are absent, not null.
 */
export function argsFromFields(fields: Array<[string, Field]>): Record<string, string | number> {
  const out: Record<string, string | number> = {};
  for (const [k, v] of fields) {
    if (v.t === "I" || v.t === "l") out[k] = v.v;
    else if (v.t === "t") out[k] = v.v ? 1 : 0;
    else if (v.t === "S" || v.t === "s") out[k] = v.v;
  }
  return out;
}

/** Next id for a message that has no store row id yet. */
let transientSeq = 1;

/**
 * Take the next transient message id suffix.
 *
 * @returns The previous counter value, then increments it. Ids are unique in this process.
 */
export function takeTransientSeq(): number {
  return transientSeq++;
}

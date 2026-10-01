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
    queueType: qtype === "quorum" ? "quorum" : "classic",
  };
}

/**
 * Prepend one `x-death` entry and the first-death headers.
 *
 * @param queue Queue the message died from.
 * @param reason Death reason stored on the new entry and on `x-first-death-reason`.
 * @param exchange Exchange the message was last published to.
 * @param routingKey Routing key stored as the only item of `routing-keys`.
 * @param prev Previous headers. Existing `x-death` and first-death keys are dropped, so earlier deaths are not kept.
 * @returns A new header list. The caller attaches it; this function does not publish.
 */
export function deathHeaders(
  queue: string,
  reason: string,
  exchange: string,
  routingKey: string,
  prev: Array<[string, Field]>,
): Array<[string, Field]> {
  const entry: Array<[string, Field]> = [
    ["queue", { t: "S", v: queue }],
    ["reason", { t: "S", v: reason }],
    ["count", { t: "l", v: 1 }],
    ["exchange", { t: "S", v: exchange }],
    ["routing-keys", { t: "A", v: [{ t: "S", v: routingKey }] }],
  ];
  const rest = prev.filter(([k]) => k !== "x-death" && k !== "x-first-death-reason" && k !== "x-first-death-queue");
  return [
    ...rest,
    ["x-death", { t: "A", v: [{ t: "F", v: entry }] }],
    ["x-first-death-reason", { t: "S", v: reason }],
    ["x-first-death-queue", { t: "S", v: queue }],
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

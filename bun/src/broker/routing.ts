/**
 * Routing-key and header matching, plus the static quorum home hash.
 */
import { fieldEq, fieldStr, type Field } from "../codec.ts";
import type { Consumer, LiveMsg, QueueLive } from "./model.ts";

/**
 * Match an AMQP topic binding pattern against a routing key.
 *
 * @param pattern Words split on `.`. `*` is one word and `#` is zero or more. An empty pattern matches only an empty key.
 * @param key Routing key, split on `.` the same way.
 * @returns True when `pattern` matches `key`. A `#` that is not last still tries every remaining split.
 */
export function topicMatches(pattern: string, key: string): boolean {
  const pat = pattern === "" ? [] : pattern.split(".");
  const words = key === "" ? [] : key.split(".");
  const rec = (p: string[], k: string[]): boolean => {
    if (p.length === 0) return k.length === 0;
    if (p[0] === "#") {
      if (p.length === 1) return true;
      for (let i = 0; i <= k.length; i++) if (rec(p.slice(1), k.slice(i))) return true;
      return false;
    }
    if (k.length === 0) return false;
    if (p[0] === "*" || p[0] === k[0]) return rec(p.slice(1), k.slice(1));
    return false;
  };
  return rec(pat, words);
}

/**
 * Match header-exchange binding arguments against message headers.
 *
 * @param args Binding arguments. `x-match` of `any` means one remaining pair is enough. Any other `x-match`, including a missing one, means every remaining pair must match.
 * @param headers Message headers compared with `fieldEq`.
 * @returns True when the match mode succeeds. No pairs and `x-match` `any` is false. No pairs and any other mode is true. `x-match` itself is not compared as a header.
 */
export function headersMatch(args: Array<[string, Field]>, headers: Array<[string, Field]>): boolean {
  const any = args.some(([k, v]) => k === "x-match" && fieldStr(v) === "any");
  const checks = args.filter(([k]) => k !== "x-match");
  if (checks.length === 0) return !any;
  const hit = (k: string, v: Field) => headers.some(([hk, hv]) => hk === k && fieldEq(hv, v));
  return any ? checks.some(([k, v]) => hit(k, v)) : checks.every(([k, v]) => hit(k, v));
}

/**
 * Hash a string with 32-bit FNV-1a over UTF-16 code units.
 *
 * @param s Text to hash. This is not the Rust cluster hash, which uses bytes and a 64-bit FNV.
 * @returns The hash as an unsigned 32-bit number. Callers that must agree with a Rust home use `queueHome` only on the Bun side.
 */
export function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/**
 * Read one header as a list of strings.
 *
 * @param headers Field table to search.
 * @param name Header key. The first field with this key is used.
 * @returns String items from an array field, or one string when the field itself is a string. A missing field or any other type returns an empty list.
 */
export function headerList(headers: Array<[string, Field]>, name: string): string[] {
  const field = headers.find(([key]) => key === name)?.[1];
  if (!field) return [];
  if (field.t === "A") return field.v.flatMap((item) => (item.t === "S" || item.t === "s" ? [item.v] : []));
  if (field.t === "S" || field.t === "s") return [field.v];
  return [];
}

/**
 * Parse an `x-overflow` value.
 *
 * @param value Argument text, or null when the argument is absent.
 * @returns `drop-head`, `reject-publish`, or `reject-publish-dlx`. Any other string, including null, returns null.
 */
export function overflowOf(value: string | null): "drop-head" | "reject-publish" | "reject-publish-dlx" | null {
  if (value === "drop-head" || value === "reject-publish" || value === "reject-publish-dlx") return value;
  return null;
}

/**
 * Build a live message that is not yet stored.
 *
 * @param q Queue whose name fills the id when `src.id` is omitted. The queue is not modified.
 * @param src Body, routing fields, and optional id. `redelivered` defaults to false.
 * @returns A `LiveMsg` with `rowId` and `expiresAt` null. The caller enqueues it. The default id is `rej-` plus the queue name.
 */
export function liveFrom(q: QueueLive, src: { body: Uint8Array; exchange: string; routingKey: string; headers: Array<[string, Field]>; propRaw: Uint8Array; persistent: boolean; priority: number; redelivered?: boolean; id?: string }): LiveMsg {
  return {
    id: src.id ?? `rej-${q.name}`,
    rowId: null,
    body: src.body,
    exchange: src.exchange,
    routingKey: src.routingKey,
    headers: src.headers,
    propRaw: src.propRaw,
    persistent: src.persistent,
    priority: src.priority,
    expiresAt: null,
    redelivered: !!src.redelivered,
  };
}

/**
 * Choose the next consumer that still wants a delivery.
 *
 * @param q Queue whose `consumers` and `rr` cursor are read. Round-robin advances `q.rr`. Single-active does not.
 * @returns The chosen consumer, or null when none call `want()`. Single-active returns the highest priority. Otherwise the cursor walks consumers of the highest priority.
 */
export function pickConsumer(q: QueueLive): Consumer | null {
  const ready = q.consumers.filter((c) => c.want());
  if (!ready.length) return null;
  if (q.argsParsed.singleActive) {
    return ready.reduce((best, c) => ((c.priority ?? 0) > (best.priority ?? 0) ? c : best));
  }
  const bestPri = Math.max(...ready.map((c) => c.priority ?? 0));
  const n = q.consumers.length;
  for (let i = 0; i < n; i++) {
    const c = q.consumers[(q.rr + i) % n]!;
    if (c.want() && (c.priority ?? 0) === bestPri) {
      q.rr = (q.rr + i + 1) % n;
      return c;
    }
  }
  return null;
}

/**
 * Pick a static quorum home from the member list.
 *
 * @param vhost Vhost joined with `name` by a NUL before hashing.
 * @param name Queue name. Exclusive queues do not use this helper.
 * @param members Cluster members. An empty list returns null. The list is copied and sorted by id; the caller's array is left as it was.
 * @returns The chosen member id, or null when `members` is empty. The index is `fnv1a` of the joined key modulo the sorted length.
 */
export function queueHome(vhost: string, name: string, members: { id: string }[]): string | null {
  if (members.length === 0) return null;
  const sorted = [...members].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return sorted[fnv1a(`${vhost}\0${name}`) % sorted.length]!.id;
}

/**
 * Routing-key and header matching, plus the static quorum home hash.
 */
import { fieldEq, fieldStr, type Field } from "../codec.ts";
import type { Consumer, LiveMsg, QueueLive } from "./model.ts";

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

export function headersMatch(args: Array<[string, Field]>, headers: Array<[string, Field]>): boolean {
  const any = args.some(([k, v]) => k === "x-match" && fieldStr(v) === "any");
  const checks = args.filter(([k]) => k !== "x-match");
  if (checks.length === 0) return !any;
  const hit = (k: string, v: Field) => headers.some(([hk, hv]) => hk === k && fieldEq(hv, v));
  return any ? checks.some(([k, v]) => hit(k, v)) : checks.every(([k, v]) => hit(k, v));
}

export function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

export function headerList(headers: Array<[string, Field]>, name: string): string[] {
  const field = headers.find(([key]) => key === name)?.[1];
  if (!field) return [];
  if (field.t === "A") return field.v.flatMap((item) => (item.t === "S" || item.t === "s" ? [item.v] : []));
  if (field.t === "S" || field.t === "s") return [field.v];
  return [];
}

export function overflowOf(value: string | null): "drop-head" | "reject-publish" | "reject-publish-dlx" | null {
  if (value === "drop-head" || value === "reject-publish" || value === "reject-publish-dlx") return value;
  return null;
}

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

export function queueHome(vhost: string, name: string, members: { id: string }[]): string | null {
  if (members.length === 0) return null;
  const sorted = [...members].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return sorted[fnv1a(`${vhost}\0${name}`) % sorted.length]!.id;
}

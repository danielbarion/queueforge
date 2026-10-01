/**
 * Policy table matching and the management policy request body.
 */
import type { Policy } from "./model.ts";

export function matchOne(rows: Policy[], vhost: string, name: string, entity: "queues" | "exchanges"): Policy | null {
  let best: Policy | null = null;
  for (const p of rows) {
    if (p.vhost !== vhost) continue;
    if (p.applyTo !== "all" && p.applyTo !== entity) continue;
    let ok = false;
    try {
      ok = new RegExp(p.pattern).test(name);
    } catch {
      ok = false;
    }
    if (!ok) continue;
    if (!best || p.priority > best.priority || (p.priority === best.priority && p.name < best.name)) best = p;
  }
  return best;
}

export function policyItem(p: Policy) {
  return {
    vhost: p.vhost,
    name: p.name,
    pattern: p.pattern,
    "apply-to": p.applyTo,
    priority: p.priority,
    definition: {
      ...(p.messageTtl != null ? { "message-ttl": p.messageTtl } : {}),
      ...(p.dlx ? { "dead-letter-exchange": p.dlx } : {}),
      ...(p.dlxKey ? { "dead-letter-routing-key": p.dlxKey } : {}),
      ...(p.maxLength != null ? { "max-length": p.maxLength } : {}),
      ...(p.maxLengthBytes != null ? { "max-length-bytes": p.maxLengthBytes } : {}),
      ...(p.expiresMs != null ? { expires: p.expiresMs } : {}),
      ...(p.overflow ? { overflow: p.overflow } : {}),
      ...(p.dlxStrategy ? { "dead-letter-strategy": p.dlxStrategy } : {}),
      ...(p.deliveryLimit != null ? { "delivery-limit": p.deliveryLimit } : {}),
      ...(p.alternate ? { "alternate-exchange": p.alternate } : {}),
    },
  };
}

export function policyFromBody(vhost: string, name: string, body: {
  pattern?: string;
  "apply-to"?: string;
  priority?: number;
  definition?: Record<string, string | number>;
}): Policy {
  const apply = body["apply-to"] ?? "all";
  if (apply !== "queues" && apply !== "exchanges" && apply !== "all") {
    throw new Error("apply-to must be queues, exchanges, or all");
  }
  const def = body.definition ?? {};
  const num = (k: string) => {
    const v = def[k];
    if (v == null || v === "") return null;
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? n : null;
  };
  const str = (k: string) => {
    const v = def[k];
    return v == null || v === "" ? null : String(v);
  };
  if (!body.pattern) throw new Error("pattern is required");
  const known = new Set([
    "message-ttl", "dead-letter-exchange", "dead-letter-routing-key", "max-length", "max-length-bytes",
    "expires", "overflow", "delivery-limit", "alternate-exchange", "dead-letter-strategy", "federation-upstream-set",
  ]);
  const unknown = Object.keys(def).filter((k) => !known.has(k));
  if (unknown.length) throw new Error(`${JSON.stringify(unknown)} are not recognised policy settings`);
  return {
    vhost,
    name,
    pattern: body.pattern,
    applyTo: apply,
    priority: body.priority ?? 0,
    messageTtl: num("message-ttl"),
    expiresMs: num("expires"),
    dlx: str("dead-letter-exchange"),
    dlxKey: str("dead-letter-routing-key"),
    maxLength: num("max-length"),
    maxLengthBytes: num("max-length-bytes"),
    overflow: (["drop-head", "reject-publish", "reject-publish-dlx"] as const).find((v) => v === str("overflow")) ?? null,
    dlxStrategy: (["at-most-once", "at-least-once"] as const).find((v) => v === str("dead-letter-strategy")) ?? null,
    deliveryLimit: num("delivery-limit"),
    alternate: str("alternate-exchange"),
  };
}

export function fillPolicyArgs(
  declared: Record<string, string | number>,
  pol: Policy | null,
  base?: Record<string, string | number>,
): Record<string, string | number> {
  const out = { ...(base ?? declared) };
  if (!pol) return out;
  const empty = (key: string) => declared[key] == null || declared[key] === "";
  if (empty("x-message-ttl") && pol.messageTtl != null) out["x-message-ttl"] = pol.messageTtl;
  if (empty("x-dead-letter-exchange") && pol.dlx) out["x-dead-letter-exchange"] = pol.dlx;
  if (empty("x-dead-letter-routing-key") && pol.dlxKey) out["x-dead-letter-routing-key"] = pol.dlxKey;
  if (empty("x-max-length") && pol.maxLength != null) out["x-max-length"] = pol.maxLength;
  if (empty("x-max-length-bytes") && pol.maxLengthBytes != null) out["x-max-length-bytes"] = pol.maxLengthBytes;
  if (empty("x-expires") && pol.expiresMs != null) out["x-expires"] = pol.expiresMs;
  if (empty("x-overflow") && pol.overflow) out["x-overflow"] = pol.overflow;
  if (empty("x-dead-letter-strategy") && pol.dlxStrategy) out["x-dead-letter-strategy"] = pol.dlxStrategy;
  if (empty("x-delivery-limit") && pol.deliveryLimit != null) out["x-delivery-limit"] = pol.deliveryLimit;
  return out;
}

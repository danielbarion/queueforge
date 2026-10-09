import { rowsOf } from "./catalog";
export { rowsOf };

export type DeadLetterSource = {
  queue: string;
  exchange: string;
  routingKey: string | null;
  bindingKey: string | null;
  via: "arguments" | "effective policy" | "applied policy";
  policy: string | null;
};
export type DeadLetterCandidate = {
  name: string;
  type: string | null;
  vhost: string;
  ready: number | null;
  unacked: number | null;
  sources: DeadLetterSource[];
};
const record = (value: unknown): Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
const count = (value: unknown): number | null => typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
export function queueCandidate(row: Record<string, unknown>): DeadLetterCandidate | null {
  if (typeof row.name !== "string") return null;
  return { name: row.name, type: typeof row.type === "string" ? row.type : null, vhost: typeof row.vhost === "string" ? row.vhost : "/", ready: count(row.messages_ready), unacked: count(row.messages_unacknowledged), sources: [] };
}

/** Discover configured destination candidates, not the provenance of their messages. */
export function discoverDeadLetterQueues(queues: Record<string, unknown>[], bindings: Record<string, unknown>[], policies: Record<string, unknown>[]): DeadLetterCandidate[] {
  const loaded = queues.flatMap((row) => { const q = queueCandidate(row); return q ? [q] : []; });
  const found = new Map<string, DeadLetterCandidate>();
  for (const source of queues) {
    if (typeof source.name !== "string") continue;
    const vhost = typeof source.vhost === "string" ? source.vhost : "/";
    const args = record(source.arguments);
    const effective = record(source.effective_policy_definition);
    const policyName = typeof source.policy === "string" ? source.policy : null;
    const policy = policyName === null ? undefined : policies.find((p) => p.name === policyName && (p.vhost ?? "/") === vhost);
    const definition = record(policy?.definition);
    const direct = typeof args["x-dead-letter-exchange"] === "string";
    const resolved = typeof effective["dead-letter-exchange"] === "string";
    const exchange = direct ? args["x-dead-letter-exchange"] : resolved ? effective["dead-letter-exchange"] : definition["dead-letter-exchange"];
    if (typeof exchange !== "string") continue;
    const rawKey = args["x-dead-letter-routing-key"] ?? effective["dead-letter-routing-key"] ?? definition["dead-letter-routing-key"];
    const routingKey = typeof rawKey === "string" ? rawKey : null;
    const via: DeadLetterSource["via"] = direct ? "arguments" : resolved ? "effective policy" : "applied policy";
    const routes = exchange === ""
      ? routingKey === null ? [] : [{ destination: routingKey, routing_key: routingKey }]
      : bindings.filter((b) => b.source === exchange && (b.vhost ?? "/") === vhost && b.destination_type === "queue");
    for (const binding of routes) {
      const destination = loaded.find((q) => q.name === binding.destination && q.vhost === vhost);
      if (!destination) continue;
      const identity = `${vhost}\0${destination.name}`;
      let candidate = found.get(identity);
      if (!candidate) { candidate = { ...destination, sources: [] }; found.set(identity, candidate); }
      const note: DeadLetterSource = { queue: source.name, exchange, routingKey, bindingKey: typeof binding.routing_key === "string" ? binding.routing_key : null, via, policy: direct ? null : policyName };
      if (!candidate.sources.some((s) => JSON.stringify(s) === JSON.stringify(note))) candidate.sources.push(note);
    }
  }
  return [...found.values()].sort((a, b) => a.name.localeCompare(b.name));
}

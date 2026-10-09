import { callBroker, type BrokerCall } from "./client";
import { firstGauge } from "./catalog";
import type { BrokerTarget } from "../stores/broker";

export type FleetSnapshot = {
  target: BrokerTarget; sampledAt: number; latencyMs: number; stale: boolean; errors: string[];
  loginNeeded: boolean; health: boolean | null; ready: boolean | null; implementation: string | null;
  version: string | null; connections: number | null; queues: number | null; readyMessages: number | null;
  unackedMessages: number | null; memoryBytes: number | null; diskBytes: number | null;
  resourceScope: string | null; nodes: string[] | null;
};
const record = (value: unknown): Record<string, unknown> | null => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
const number = (value: unknown): number | null => typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
const text = (value: unknown): string | null => typeof value === "string" && value.length > 0 ? value : null;
const paths = { health: "/healthz", ready: "/readyz", identity: "/api/identity", overview: "/api/overview", queues: "/api/queues/%2F?page_size=500", nodes: "/api/nodes", metrics: "/metrics" } as const;

/** Probe each target without changing the selected broker or its live store. */
export async function probeFleet(target: BrokerTarget): Promise<FleetSnapshot> {
  const started = performance.now();
  const errors: string[] = [];
  let loginNeeded = false;
  const entries = await Promise.all(Object.entries(paths).map(async ([key, path]) => {
    let result: BrokerCall | null = null;
    try {
      result = await callBroker(target.url, "GET", path);
      if (result.status === 401) { loginNeeded = true; errors.push(`${path}: login needed (HTTP 401).`); }
      else if (result.status === 403) errors.push(`${path}: permission denied (HTTP 403).`);
      else if (result.status !== 200 && !(result.status === 404 && ["health", "ready", "identity", "nodes", "metrics"].includes(key))) errors.push(`${path}: HTTP ${result.status}.`);
    } catch (error) { errors.push(`${path}: ${error instanceof Error ? error.message : "request failed"}`); }
    return [key, result] as const;
  }));
  const responses = Object.fromEntries(entries) as Record<keyof typeof paths, BrokerCall | null>;
  const body = (key: keyof typeof paths) => responses[key]?.status === 200 ? responses[key]!.body : null;
  const identity = record(body("identity"));
  const overview = record(body("overview"));
  const objectTotals = record(overview?.object_totals);
  const rawQueues = body("queues"), container = record(rawQueues);
  const queues = Array.isArray(rawQueues) ? rawQueues : Array.isArray(container?.items) ? container.items : null;
  if (responses.overview?.status === 200 && !overview) errors.push("/api/overview: malformed response.");
  if (responses.queues?.status === 200 && (!queues || queues.some(item => !record(item) || typeof record(item)?.name !== "string"))) errors.push("/api/queues: malformed response.");
  const validQueues = queues && queues.every(item => record(item) && typeof record(item)?.name === "string") ? queues as Record<string, unknown>[] : null;
  const totalCount = number(container?.total_count);
  const sum = (field: string): number | null => {
    if (!validQueues || (totalCount !== null && totalCount > validQueues.length)) return null;
    const values = validQueues.map(row => number(row[field]));
    return values.some(value => value === null) ? null : (values as number[]).reduce((a, b) => a + b, 0);
  };
  const rawNodes = body("nodes");
  const nodeRows = Array.isArray(rawNodes) ? rawNodes : record(rawNodes)?.items;
  const nodes = Array.isArray(nodeRows) && nodeRows.every(row => record(row) && typeof row.name === "string") ? nodeRows as Record<string, unknown>[] : null;
  const metrics = responses.metrics?.status === 200 ? responses.metrics.text : "";
  const memory = firstGauge(metrics, "rabbitmq_process_resident_memory_bytes");
  const disk = firstGauge(metrics, "rabbitmq_disk_space_available_bytes");
  const firstNode = nodes?.[0];
  const probe = (key: "health" | "ready") => responses[key]?.status === 200 ? true : responses[key] && responses[key]!.status >= 500 ? false : null;
  return {
    target, sampledAt: Date.now(), latencyMs: performance.now() - started, stale: errors.length > 0, errors, loginNeeded,
    health: probe("health"), ready: probe("ready"), implementation: text(identity?.kind),
    version: text(overview?.product_version) ?? text(overview?.rabbitmq_version) ?? text(identity?.version),
    connections: number(objectTotals?.connections), queues: validQueues ? totalCount ?? validQueues.length : null,
    readyMessages: sum("messages_ready"),
    unackedMessages: sum("messages_unacknowledged"),
    memoryBytes: memory ?? number(firstNode?.mem_used), diskBytes: disk ?? number(firstNode?.disk_free),
    resourceScope: memory !== null || disk !== null ? "Local broker metrics; missing gauges use the first reported node" : text(firstNode?.name),
    nodes: nodes ? [...new Set(nodes.flatMap(node => [node.name as string, ...(Array.isArray(node.peers) ? node.peers.flatMap(peer => { const name = text(record(peer)?.name); return name ? [name] : []; }) : [])]))] : null,
  };
}

import { AUDITED, featureGroups, type Broker, type Level } from "../../site/app/features";
import { paced, scale, formatRate, type Bar } from "../../site/app/bench";
import summary from "./reference-conformance.json";
import type { BrokerKind } from "../stores/broker";

export { featureGroups, formatRate };
export const audited = AUDITED;
export const implementations: Array<{ kind: BrokerKind; name: string }> = [
  { kind: "rust", name: "Rust" }, { kind: "bun", name: "Bun" },
  { kind: "php", name: "PHP" }, { kind: "rabbitmq", name: "RabbitMQ 4.3" },
];
const keys: Record<BrokerKind, Broker> = { rust: "rust", bun: "bun", php: "php", rabbitmq: "mq" };
export type Capability = { level: Level | "unknown"; note: string };
/** Source-audited capability, independent of live endpoint availability. */
export function capability(kind: BrokerKind, featureName: string): Capability {
  const feature = featureGroups.flatMap(group => group.rows).find(row => row.name.toLowerCase() === featureName.toLowerCase());
  if (!feature) return { level: "unknown", note: "No entry in the saved capability reference." };
  const cell = feature.cells[keys[kind]];
  return typeof cell === "string" ? { level: cell, note: "" } : { level: cell[0], note: cell[1] };
}
export const savedConformance = summary as Record<string, Partial<Record<BrokerKind, Level>>>;
export const referenceSources = {
  capabilities: "site/app/features.ts", conformance: "console/lib/reference-conformance.json (snapshot of conformance/results/summary.json)",
  benchmarks: "site/app/bench.ts", methodology: "BENCHMARK.md",
};
export type BenchmarkMode = "paced" | "spread";
export const benchmarkModes: Record<BenchmarkMode, { title: string; rows: Bar[]; method: string; date: string }> = {
  paced: {
    title: "Paced · 1 CPU / 512 MiB", rows: paced,
    date: "2026-10-08 from 12:50Z; Rust rerun 13:43Z on a quiet host",
    method: "Durable 256-byte body, 128 confirms in flight. Mac client through published host ports, one container at a time. Score counts acked deliveries over the paced windows; a step is kept when confirms and acks reach 95% of the offered rate.",
  },
  spread: {
    title: "Spread · 4 CPU / 4 GiB", rows: scale,
    date: "2026-10-08 at 13:00:41Z; PHP rerun 13:31Z after its handoff fix",
    method: "Unpaced durable classic queues, 256-byte persistent body, manual acks. Linux qf-loadgen client inside Docker network qf-load, no host port. 16 publishers, 16 consumers, 16 queues; window 512 per publisher, prefetch 1024, 8192 in flight. 2 s warmup / 8 s measure. Rates use the lower of confirms/s and deliveries/s. Broker pinned to CPUs 0–3; client to CPUs 4–6.",
  },
};
export function benchmarkRate(mode: BenchmarkMode, kind: BrokerKind): number | null {
  return benchmarkModes[mode].rows.find(row => row.tone === keys[kind])?.rate ?? null;
}

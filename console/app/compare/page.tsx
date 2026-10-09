"use client";

import { useState } from "react";
import { GitCompareArrows } from "lucide-react";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { audited, benchmarkModes, benchmarkRate, capability, featureGroups, formatRate, implementations, referenceSources, savedConformance, type BenchmarkMode } from "@/lib/reference";
import type { BrokerKind } from "@/stores/broker";

type Filter = "all" | "different" | "gaps";
const labels = { y: "Supported", p: "Partial", n: "Unsupported", unknown: "Unknown" };
function Support({ level, note = "", historical = false }: { level: keyof typeof labels; note?: string; historical?: boolean }) {
  return <div><span className={`badge badge-sm ${level === "y" ? "badge-success badge-outline" : level === "p" ? "badge-warning badge-outline" : "badge-ghost"}`}>{historical ? ({ y: "All tests passed", p: "Some tests passed", n: "No tests passed", unknown: "No saved result" })[level] : labels[level]}</span>{note && <p className="mt-2 max-w-sm text-xs leading-relaxed text-muted">{note}</p>}</div>;
}
function matches(filter: Filter, left: string, right: string) {
  return filter === "all" || (filter === "different" ? left !== right : left !== "y" || right !== "y");
}

export default function ComparePage() {
  const [left, setLeft] = useState<BrokerKind>("rust");
  const [right, setRight] = useState<BrokerKind>("bun");
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<Filter>("all");
  const [mode, setMode] = useState<BenchmarkMode>("paced");
  const name = (kind: BrokerKind) => implementations.find(row => row.kind === kind)!.name;
  const query = search.trim().toLowerCase();
  const features = featureGroups.flatMap(group => group.rows.map(row => ({ ...row, group: group.title }))).filter(row => {
    const a = capability(left, row.name), b = capability(right, row.name);
    return `${row.name} ${row.detail} ${row.group} ${a.note} ${b.note}`.toLowerCase().includes(query) && matches(filter, a.level, b.level);
  });
  const historical = Object.entries(savedConformance).filter(([feature, results]) => feature.toLowerCase().includes(query) && matches(filter, results[left] ?? "unknown", results[right] ?? "unknown"));
  const rates = [left, right].map(kind => ({ kind, rate: benchmarkRate(mode, kind) }));
  const max = Math.max(1, ...rates.map(row => row.rate ?? 0));
  const methodology = benchmarkModes[mode];
  return <div className="space-y-6">
    <PageHeader title="Compare implementations" icon={GitCompareArrows} detail="Explore feature coverage and recorded benchmarks across the four stacks." />
    <Card><CardBody>
      <div className="grid gap-4 sm:grid-cols-2">
        {[{ label: "First implementation", value: left, set: setLeft }, { label: "Second implementation", value: right, set: setRight }].map(control => <label key={control.label} className="flex flex-col gap-2 text-sm font-medium">{control.label}<select className="select select-bordered w-full" value={control.value} onChange={event => control.set(event.target.value as BrokerKind)}>{implementations.map(row => <option key={row.kind} value={row.kind}>{row.name}</option>)}</select></label>)}
      </div>
      <div className="flex flex-wrap gap-3">
        <label className="flex min-w-48 flex-1 flex-col gap-2 text-sm">Search features<input className="input input-bordered w-full" value={search} onChange={event => setSearch(event.target.value)} placeholder="Protocol, queue, permissions…" /></label>
        <label className="flex flex-col gap-2 text-sm">Coverage filter<select className="select select-bordered" value={filter} onChange={event => setFilter(event.target.value as Filter)}><option value="all">All features</option><option value="different">Different coverage</option><option value="gaps">Coverage gaps</option></select></label>
      </div>
    </CardBody></Card>

    <Card><CardBody>
      <div><h2 className="text-lg font-semibold">Capability reference</h2><p className="mt-1 text-sm text-muted">Source audit dated {audited}. Partial support notes describe the recorded limitations. This reference does not probe a connected broker.</p></div>
      <div className="overflow-x-auto"><table className="table"><thead><tr><th>Feature</th><th>{name(left)}</th><th>{name(right)}</th></tr></thead><tbody>{features.map(row => <tr key={row.name}><td><span className="text-xs text-muted">{row.group}</span><p className="font-medium">{row.name}</p><p className="mt-1 max-w-md text-xs text-muted">{row.detail}</p></td><td><Support {...capability(left, row.name)} /></td><td><Support {...capability(right, row.name)} /></td></tr>)}</tbody></table>{!features.length && <p className="py-6 text-sm text-muted">No features match these filters.</p>}</div>
      <p className="text-xs text-muted">Source: <code>{referenceSources.capabilities}</code></p>
    </CardBody></Card>

    <Card><CardBody>
      <div><h2 className="text-lg font-semibold">Saved conformance results</h2><p className="mt-1 text-sm text-muted">Historical test matrix bundled with this console, not live validation. Rows distinguish all, some or no recorded tests passing. They do not prove overall support. The summary has no run timestamp or revision. A current broker may differ.</p></div>
      <div className="overflow-x-auto"><table className="table table-sm"><thead><tr><th>Test feature</th><th>{name(left)}</th><th>{name(right)}</th></tr></thead><tbody>{historical.map(([feature, results]) => <tr key={feature}><td>{feature}</td><td><Support historical level={results[left] ?? "unknown"} /></td><td><Support historical level={results[right] ?? "unknown"} /></td></tr>)}</tbody></table>{!historical.length && <p className="py-6 text-sm text-muted">No saved test rows match these filters.</p>}</div>
      <p className="text-xs text-muted">Source: <code>{referenceSources.conformance}</code>. Missing cells are unknown.</p>
    </CardBody></Card>

    <Card><CardBody>
      <div className="flex flex-wrap items-start justify-between gap-4"><div><h2 className="text-lg font-semibold">Recorded benchmark</h2><p className="mt-1 text-sm text-muted">Historical results for the selected workload. Changing the workload changes the measurement.</p></div><label className="flex flex-col gap-2 text-sm">Workload<select className="select select-bordered" value={mode} onChange={event => setMode(event.target.value as BenchmarkMode)}>{Object.entries(benchmarkModes).map(([key, item]) => <option key={key} value={key}>{item.title}</option>)}</select></label></div>
      <div className="space-y-5" role="img" aria-label={`${methodology.title}, messages per second for ${name(left)} and ${name(right)}`}>
        {rates.map((row, index) => <div key={`${row.kind}-${index}`}><div className="mb-2 flex justify-between gap-4 text-sm"><span className="font-medium">{name(row.kind)}</span><span className="font-mono">{row.rate == null ? "No recorded rate" : `${formatRate(row.rate)} msg/s`}</span></div><div className="h-3 overflow-hidden rounded-full bg-base-300"><div className={`h-full rounded-full ${index === 0 ? "bg-primary" : "bg-secondary"}`} style={{ width: `${100 * (row.rate ?? 0) / max}%` }} /></div></div>)}
      </div>
      <div className="overflow-x-auto"><table className="table table-sm"><thead><tr><th>Implementation</th><th>Recorded msg/s</th></tr></thead><tbody>{rates.map((row, index) => <tr key={`${row.kind}-${index}`}><td>{name(row.kind)}</td><td className="font-mono">{row.rate == null ? "Unknown" : formatRate(row.rate)}</td></tr>)}</tbody></table></div>
      <div className="space-y-3 text-sm leading-relaxed text-muted"><p><strong>Measured:</strong> {methodology.date} (UTC).</p><p>{methodology.method}</p><p>QueueForge durable confirms return after the covering fsync. RabbitMQ 4.3 classic confirms before its flush, so the acknowledgment guarantees differ. These single-node classic-queue figures do not measure mixed-cluster or Raft quorum performance, and do not validate the current build.</p><p>Sources: <code>{referenceSources.benchmarks}</code> and <a className="link" href="https://github.com/danielbarion/queueforge/blob/main/BENCHMARK.md">{referenceSources.methodology}</a>. Paced and spread rates use different clients, network paths and scoring; compare implementations within one workload.</p></div>
    </CardBody></Card>
  </div>;
}

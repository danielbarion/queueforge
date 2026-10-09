"use client";

import { useEffect, useState } from "react";
import { ArchiveX } from "lucide-react";
import { BrokerGate } from "@/components/auth/BrokerGate";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { MessageInspector } from "@/components/messages/MessageInspector";
import { callBroker } from "@/lib/client";
import { discoverDeadLetterQueues, queueCandidate, rowsOf, type DeadLetterCandidate } from "@/lib/dead-letter";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { formatCount } from "@/stores/live";

function Workspace() {
  const broker = useBrokerStore(selectedBroker);
  const [queues, setQueues] = useState<Record<string, unknown>[] | null>(null);
  const [bindings, setBindings] = useState<Record<string, unknown>[] | null>(null);
  const [policies, setPolicies] = useState<Record<string, unknown>[] | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [manual, setManual] = useState("");
  const [inspecting, setInspecting] = useState<DeadLetterCandidate | null>(null);
  const [nonce, setNonce] = useState(0);
  useEffect(() => {
    if (!broker) return;
    let stopped = false;
    setQueues(null); setBindings(null); setPolicies(null); setErrors([]); setInspecting(null); setManual("");
    const paths = ["/api/queues/%2F?page_size=500", "/api/bindings/%2F", "/api/policies/%2F"];
    void Promise.allSettled(paths.map((path) => callBroker(broker.url, "GET", path))).then((results) => {
      if (stopped) return;
      const notes: string[] = [];
      results.forEach((result, index) => {
        const name = ["Queue list", "Bindings", "Policies"][index];
        const response = result.status === "fulfilled" ? result.value : null;
        const rows = response?.status === 200 ? rowsOf(response.body) : null;
        if (rows === null) notes.push(`${name} unavailable${response ? ` (HTTP ${response.status})` : ""}.`);
        [setQueues, setBindings, setPolicies][index]!(rows);
      });
      setErrors(notes);
    });
    return () => { stopped = true; };
  }, [broker, nonce]);
  if (!broker) return null;
  const loaded = (queues ?? []).flatMap((row) => { const q = queueCandidate(row); return q ? [q] : []; }).filter((q) => q.vhost === "/");
  const candidates = discoverDeadLetterQueues(queues ?? [], bindings ?? [], policies ?? []).filter((q) => q.vhost === "/");
  const filtered = candidates.filter((q) => q.name.toLowerCase().includes(search.toLowerCase()));
  const manualOptions = loaded.filter((q) => q.name.toLowerCase().includes(search.toLowerCase())).sort((a, b) => a.name.localeCompare(b.name));
  const picked = manualOptions.find((q) => q.name === manual);
  return <>
    <Card><CardBody>
      <div className="flex flex-wrap items-center justify-between gap-3"><h2 className="text-base font-semibold">Configured destination candidates</h2><button className="btn btn-sm btn-ghost" onClick={() => setNonce((n) => n + 1)}>Refresh metadata</button></div>
      <p className="text-sm leading-6 text-muted">Candidates come from source queue dead-letter configuration and destination bindings. A binding does not prove a message was dead-lettered: inspect its x-death headers for recorded failure history. Destination selection may also depend on exchange type and routing key.</p>
      <label className="qf-field"><span>Search queue names</span><input className="input input-bordered w-full" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Filter destination queues" /></label>
      {errors.length > 0 && <p role="status" className="text-sm text-warning">{errors.join(" ")}</p>}
      {queues === null && errors.length === 0 && <p className="text-sm text-muted">Loading broker metadata…</p>}
      {queues !== null && filtered.length === 0 && <p className="text-sm text-muted">{candidates.length ? "No destination candidates match this search." : "No configured destination candidates could be resolved from the returned metadata. Choose a queue manually below."}</p>}
      {filtered.length > 0 && <div className="qf-table-scroll" tabIndex={0} role="region" aria-label="Dead-letter destination candidates"><table className="qf-table"><thead><tr><th>Queue / configured source</th><th className="num">Ready</th><th className="num">Unacked</th><th>Inspect</th></tr></thead><tbody>{filtered.map((q) => <tr key={q.name}><td><strong>{q.name}</strong><span className="ml-2 text-xs text-muted">{q.type ?? "Type unavailable"}</span>{q.sources.map((source, index) => <p key={index} className="mt-2 text-xs text-muted">{source.queue} → {source.exchange === "" ? "(default exchange)" : source.exchange} · {source.routingKey === null ? "Original message routing key" : `Override key: ${source.routingKey || "(empty)"}`} · binding: {source.bindingKey ?? "unavailable"} · {source.via}{source.policy ? ` (${source.policy})` : ""}</p>)}</td><td className="num">{q.ready === null ? "—" : formatCount(q.ready)}</td><td className="num">{q.unacked === null ? "—" : formatCount(q.unacked)}</td><td><button className="btn btn-sm btn-ghost" onClick={() => setInspecting(q)}>Inspect</button></td></tr>)}</tbody></table></div>}
    </CardBody></Card>
    <Card><CardBody>
      <h2 className="text-base font-semibold">Choose a queue manually</h2><p className="text-sm text-muted">Some PHP and Rust responses omit queue arguments or applied-policy metadata. You can inspect any loaded queue; selecting it does not classify its messages as dead letters. Missing counts appear as —.</p>
      <div className="flex flex-wrap items-end gap-3"><label className="qf-field min-w-48 flex-1"><span>Queue on /</span><select className="select select-bordered w-full" value={manual} onChange={(event) => setManual(event.target.value)}><option value="">Select a queue</option>{manualOptions.map((q) => <option key={q.name} value={q.name}>{q.name}</option>)}</select></label><button className="btn btn-primary" disabled={!picked} onClick={() => picked && setInspecting(picked)}>Inspect queue</button></div>
      {picked && <p className="text-xs text-muted">Ready {picked.ready === null ? "—" : formatCount(picked.ready)} · Unacked {picked.unacked === null ? "—" : formatCount(picked.unacked)}</p>}
    </CardBody></Card>
    {inspecting && <MessageInspector broker={broker} queue={{ name: inspecting.name, ...(inspecting.type ? { type: inspecting.type } : {}) }} onClose={() => setInspecting(null)} deadLetter />}
  </>;
}
export default function DeadLettersPage() {
  return <><PageHeader title="Dead-letter workspace" icon={ArchiveX} detail="Inspect configured dead-letter destinations and recorded failure history on the / vhost." /><BrokerGate><Workspace /></BrokerGate></>;
}

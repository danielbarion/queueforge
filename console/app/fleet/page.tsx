"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Network, RefreshCw } from "lucide-react";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { DEMO_PROFILES } from "@/lib/demo";
import { formatBytes } from "@/lib/catalog";
import { probeFleet, type FleetSnapshot } from "@/lib/fleet";
import { audited, capability } from "@/lib/reference";
import { useBrokerStore, type BrokerTarget } from "@/stores/broker";

const keyOf = (target: BrokerTarget) => `${target.id}:${target.url}`;
const count = (value: number | null | undefined) => value == null ? "Unknown" : value.toLocaleString("en-US");
const bytes = (value: number | null | undefined) => value == null ? "Unknown" : formatBytes(value);
const health = (value: boolean | null | undefined, positive: string) => value == null ? "Unknown / not exposed" : value ? positive : "Unavailable";
const levels = { y: "Supported", p: "Partial", n: "Unsupported", unknown: "Unknown" };

export default function FleetPage() {
  const router = useRouter();
  const saved = useBrokerStore(state => state.targets);
  const demo = useBrokerStore(state => state.demo);
  const targets = demo ? DEMO_PROFILES : saved;
  const [snapshots, setSnapshots] = useState<Record<string, FleetSnapshot & { retainedAt?: number }>>({});
  const [nonce, refresh] = useState(0);
  const inFlight = useRef(new Set<string>());
  useEffect(() => {
    let stopped = false;
    const running = inFlight.current;
    async function tick(target: BrokerTarget) {
      const key = keyOf(target);
      if (stopped || document.hidden || running.has(key)) return;
      running.add(key);
      try {
        const fresh = await probeFleet(target);
        if (stopped) return;
        setSnapshots(current => {
          const previous = current[key];
          if (!previous || !fresh.stale) return { ...current, [key]: fresh };
          // Failed fields retain the last known snapshot and are labeled stale.
          return { ...current, [key]: { ...fresh,
            retainedAt: previous.retainedAt ?? previous.sampledAt,
            version: fresh.version ?? previous.version, implementation: fresh.implementation ?? previous.implementation,
            connections: fresh.connections ?? previous.connections, queues: fresh.queues ?? previous.queues,
            readyMessages: fresh.readyMessages ?? previous.readyMessages, unackedMessages: fresh.unackedMessages ?? previous.unackedMessages,
            memoryBytes: fresh.memoryBytes ?? previous.memoryBytes, diskBytes: fresh.diskBytes ?? previous.diskBytes,
            resourceScope: fresh.resourceScope ?? previous.resourceScope, nodes: fresh.nodes ?? previous.nodes,
          } };
        });
      } finally { running.delete(key); }
    }
    const poll = () => targets.forEach(target => void tick(target));
    poll();
    const interval = setInterval(poll, 5000);
    const visible = () => { if (!document.hidden) poll(); };
    document.addEventListener("visibilitychange", visible);
    return () => { stopped = true; clearInterval(interval); document.removeEventListener("visibilitychange", visible); };
  }, [targets, nonce]);
  function open(target: BrokerTarget) {
    const state = useBrokerStore.getState();
    if (demo) state.setDemo(true, target.kind); else state.select(target.id);
    router.push("/");
  }
  return <div className="space-y-6">
    <PageHeader title="Broker fleet" icon={Network} detail="Independent snapshots of your broker profiles. Each broker refreshes every five seconds while this page is visible." />
    <div className="flex flex-wrap items-center justify-between gap-3"><p className="text-sm text-muted">{demo ? "Demo fixtures · simulated data, no broker connections" : `${targets.length} configured profiles · unknown values are never treated as zero`}</p><button type="button" className="btn btn-sm btn-ghost" onClick={() => refresh(value => value + 1)}><RefreshCw className="size-4" />Refresh</button></div>
    {!targets.length && <Card><CardBody><h2 className="font-semibold">Add a broker to start monitoring</h2><p className="text-sm text-muted">Saved profiles remain independent; fleet probes do not change your selected broker.</p><Link href="/brokers" className="btn btn-primary self-start">Configure brokers</Link></CardBody></Card>}
    <div className="grid items-start gap-6 xl:grid-cols-2">{targets.map(target => {
      const sample = snapshots[keyOf(target)];
      return <Card key={keyOf(target)}><CardBody>
        <div className="flex items-start justify-between gap-3"><div><h2 className="text-lg font-semibold">{target.name}</h2><p className="mt-1 break-all text-xs text-muted">{target.url}</p></div><span className={`badge badge-outline ${sample?.stale ? "badge-warning" : "badge-ghost"}`}>{!sample ? "Probing" : sample.loginNeeded ? "Login needed" : sample.stale ? "Stale / partial" : demo ? "Demo" : "Sampled"}</span></div>
        <p className="text-sm"><strong>{sample?.implementation ?? `Configured: ${target.kind}`}</strong> · {sample?.version ?? "Version unknown"}</p>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-4 text-sm">{[
          ["Health probe", health(sample?.health, "Healthy")], ["Readiness probe", health(sample?.ready, "Ready")],
          ["Queues on /", count(sample?.queues)], ["Connections", count(sample?.connections)],
          ["Ready messages on /", count(sample?.readyMessages)], ["Unacked messages on /", count(sample?.unackedMessages)],
          ["Memory", bytes(sample?.memoryBytes)], ["Disk free", bytes(sample?.diskBytes)],
        ].map(([label, value]) => <div key={label}><dt className="text-xs text-muted">{label}</dt><dd className="mt-1 font-mono">{value}</dd></div>)}</dl>
        <div className="space-y-1 text-xs text-muted"><p>Members: {sample?.nodes === null || !sample ? "Unknown / not exposed" : sample.nodes.length ? sample.nodes.join(", ") : "No reported members"}</p>{sample?.resourceScope && <p>Resource scope: {sample.resourceScope}</p>}<p>{sample ? `Latest probe ${new Date(sample.sampledAt).toLocaleTimeString()} · probe round trip ${sample.latencyMs.toFixed(0)} ms` : "Waiting for first snapshot."}</p>{sample?.stale && <p className="text-warning">Retained values may be from {new Date(sample.retainedAt ?? sample.sampledAt).toLocaleTimeString()}.</p>}</div>
        {!!sample?.errors.length && <ul className="space-y-1 text-xs text-error" aria-label="Probe errors">{sample.errors.map(error => <li key={error}>{error}</li>)}</ul>}
        <div className="border-t border-base-300 pt-4"><p className="mb-3 text-xs text-muted">Capability reference · audit {audited}, not live verification</p><div className="space-y-2">{["Quorum queues", "Stream queues", "Dead-letter exchanges"].map(feature => { const support = capability(target.kind, feature); return <div key={feature} className="text-xs"><span className="mr-2">{feature}</span><span className={`badge badge-xs badge-outline ${support.level === "y" ? "badge-success" : support.level === "p" ? "badge-warning" : "badge-ghost"}`}>{levels[support.level]}</span>{support.note && <p className="mt-1 text-muted">{support.note}</p>}</div>; })}</div></div>
        <button type="button" className="btn btn-primary self-start" onClick={() => open(target)}>{sample?.loginNeeded ? "Select broker to log in" : "View broker"}</button>
      </CardBody></Card>;
    })}</div>
  </div>;
}

"use client";

import { useEffect, useState } from "react";
import { Server } from "lucide-react";
import { BrokerGate } from "@/components/auth/BrokerGate";
import { Stat } from "@/components/ui/Stat";
import { PageHeader } from "@/components/ui/PageHeader";
import { callBroker } from "@/lib/client";
import { firstGauge, formatBytes } from "@/lib/catalog";
import { selectedBroker, useBrokerStore } from "@/stores/broker";

type ClusterSnap = {
  version: string;
  name: string;
  disk: number | null;
};

function Cluster() {
  const broker = useBrokerStore(selectedBroker);
  const [snap, setSnap] = useState<ClusterSnap | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!broker) return;
    let stop = false;
    async function load() {
      if (!broker) return;
      try {
        const [overview, cluster, metrics] = await Promise.all([
          callBroker(broker.url, "GET", "/api/overview"),
          callBroker(broker.url, "GET", "/api/cluster-name"),
          callBroker(broker.url, "GET", "/metrics"),
        ]);
        if (stop) return;
        const overviewBody = overview.body !== null && typeof overview.body === "object" ? (overview.body as { product_version?: unknown }) : null;
        const clusterBody = cluster.body !== null && typeof cluster.body === "object" ? (cluster.body as { name?: unknown }) : null;
        if (overview.status !== 200 || !overviewBody || typeof overviewBody.product_version !== "string") {
          setError("Overview did not answer.");
          setSnap(null);
          return;
        }
        if (cluster.status !== 200 || !clusterBody || typeof clusterBody.name !== "string") {
          setError("The cluster name did not answer.");
          setSnap(null);
          return;
        }
        setError(null);
        setSnap({
          version: overviewBody.product_version,
          name: clusterBody.name,
          disk: metrics.status === 200 ? firstGauge(metrics.text, "rabbitmq_disk_space_available_bytes") : null,
        });
      } catch (caught) {
        if (!stop) {
          setSnap(null);
          setError(caught instanceof Error ? caught.message : "Could not reach that address.");
        }
      }
    }
    void load();
    const timer = setInterval(() => void load(), 3000);
    return () => {
      stop = true;
      clearInterval(timer);
    };
  }, [broker]);

  if (error) return <p className="text-sm text-error">{error}</p>;
  if (!snap) return <p className="text-sm text-muted">Loading cluster…</p>;
  const disk = snap.disk === null ? "—" : `${formatBytes(snap.disk)} (${snap.disk} B)`;
  return (
    <div className="grid gap-3 sm:grid-cols-3">
      <Stat label="Version" value={snap.version} />
      <Stat label="Name" value={snap.name || "—"} />
      <Stat label="Free disk" value={disk} />
    </div>
  );
}

export default function ClusterPage() {
  return (
    <>
      <PageHeader title="Cluster" icon={Server} detail="Version, name, and free disk reported by the broker in use. This page does not add or restart nodes." />
      <BrokerGate>
        <Cluster />
      </BrokerGate>
    </>
  );
}

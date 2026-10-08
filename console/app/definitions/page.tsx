"use client";

import { useEffect, useState } from "react";
import { ScrollText } from "lucide-react";
import { BrokerGate } from "@/components/auth/BrokerGate";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { callBroker } from "@/lib/client";
import { definitionCounts, type DefinitionCounts } from "@/lib/catalog";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { formatCount } from "@/stores/live";

function Definitions() {
  const broker = useBrokerStore(selectedBroker);
  const [text, setText] = useState<string | null>(null);
  const [counts, setCounts] = useState<DefinitionCounts | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!broker) return;
    let stop = false;
    async function load() {
      if (!broker) return;
      try {
        const response = await callBroker(broker.url, "GET", "/api/definitions");
        if (stop) return;
        const parsed = response.status === 200 ? definitionCounts(response.body) : null;
        if (!parsed) {
          setError("Definitions did not answer.");
          return;
        }
        setError(null);
        setCounts(parsed);
        setText(response.text);
      } catch (caught) {
        if (!stop) setError(caught instanceof Error ? caught.message : "Could not reach that address.");
      }
    }
    void load();
    return () => {
      stop = true;
    };
  }, [broker]);

  function download() {
    if (!text) return;
    const blob = new Blob([text], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "queueforge-definitions.json";
    link.click();
    URL.revokeObjectURL(url);
  }

  if (error) return <p className="text-sm text-error">{error}</p>;
  if (!counts) return <p className="text-sm text-muted">Loading definitions…</p>;
  return (
    <Card>
      <CardBody>
        <dl className="grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
          <div>
            <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">Vhosts</dt>
            <dd className="font-mono">{formatCount(counts.vhosts)}</dd>
          </div>
          <div>
            <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">Queues</dt>
            <dd className="font-mono">{formatCount(counts.queues)}</dd>
          </div>
          <div>
            <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">Exchanges</dt>
            <dd className="font-mono">{formatCount(counts.exchanges)}</dd>
          </div>
          <div>
            <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">Bindings</dt>
            <dd className="font-mono">{formatCount(counts.bindings)}</dd>
          </div>
        </dl>
        <button type="button" className="btn btn-primary w-fit" onClick={download}>
          Download
        </button>
      </CardBody>
    </Card>
  );
}

export default function DefinitionsPage() {
  return (
    <>
      <PageHeader title="Definitions" icon={ScrollText} detail="The topology document the broker returned, saved unchanged. This page does not import it." />
      <BrokerGate>
        <Definitions />
      </BrokerGate>
    </>
  );
}

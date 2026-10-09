"use client";

import { useEffect, useState } from "react";
import { ArrowLeftRight } from "lucide-react";
import { BrokerGate } from "@/components/auth/BrokerGate";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { callBroker } from "@/lib/client";
import { parseExchanges, type ExchangeRow } from "@/lib/catalog";
import { selectedBroker, useBrokerStore } from "@/stores/broker";

function ExchangeList() {
  const broker = useBrokerStore(selectedBroker);
  const [rows, setRows] = useState<ExchangeRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!broker) return;
    let stop = false;
    async function load() {
      if (!broker) return;
      try {
        const response = await callBroker(broker.url, "GET", "/api/exchanges/%2F?page_size=500");
        if (stop) return;
        const parsed = response.status === 200 ? parseExchanges(response.body) : null;
        if (!parsed) {
          setRows(null);
          setError("The exchange list did not answer.");
          return;
        }
        setError(null);
        setRows(parsed);
      } catch (caught) {
        if (!stop) setError(caught instanceof Error ? caught.message : "Could not reach that address.");
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
  if (!rows) return <p className="text-sm text-muted">Loading exchanges…</p>;
  if (rows.length === 0) return <p className="text-sm text-muted">No exchanges on /.</p>;
  return (
    <Card>
      <CardBody>
        <div className="qf-table-scroll" tabIndex={0} role="region" aria-label="Data table"><table className="qf-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Durable</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((exchange) => (
              <tr key={exchange.name}>
                <td>{exchange.name === "" ? "(default)" : exchange.name}</td>
                <td>{exchange.type}</td>
                <td>{exchange.durable ? "yes" : "no"}</td>
              </tr>
            ))}
          </tbody>
        </table></div>
      </CardBody>
    </Card>
  );
}

export default function ExchangesPage() {
  return (
    <>
      <PageHeader title="Exchanges" icon={ArrowLeftRight} detail="Exchanges on the / vhost. This page does not declare or delete them." />
      <BrokerGate>
        <ExchangeList />
      </BrokerGate>
    </>
  );
}

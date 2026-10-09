"use client";

import { useEffect, useState } from "react";
import { Cable } from "lucide-react";
import { BrokerGate } from "@/components/auth/BrokerGate";
import { Card, CardBody } from "@/components/ui/Card";
import { ConfirmDialog } from "@/components/ui/ConfirmDialog";
import { PageHeader } from "@/components/ui/PageHeader";
import { callBroker } from "@/lib/client";
import { parseConnections, type ConnectionRow } from "@/lib/catalog";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { formatCount, useLiveStore } from "@/stores/live";

function ConnectionList() {
  const broker = useBrokerStore(selectedBroker);
  const nudge = useLiveStore((state) => state.nudge);
  const [rows, setRows] = useState<ConnectionRow[] | null>(null);
  const [total, setTotal] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<ConnectionRow | null>(null);
  const [working, setWorking] = useState(false);
  const [tick, setTick] = useState(0);

  useEffect(() => {
    if (!broker) return;
    let stop = false;
    async function load() {
      if (!broker) return;
      try {
        const response = await callBroker(broker.url, "GET", "/api/connections?page_size=500");
        if (stop) return;
        const parsed = response.status === 200 ? parseConnections(response.body) : null;
        if (!parsed) {
          setRows(null);
          setError("The connection list did not answer.");
          return;
        }
        setError(null);
        setRows(parsed.rows);
        setTotal(parsed.total);
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
  }, [broker, tick]);

  async function close() {
    if (!broker || !pending) return;
    setWorking(true);
    try {
      const response = await callBroker(broker.url, "DELETE", `/api/connections/${encodeURIComponent(pending.name)}`);
      if (response.status === 404) setError(`${pending.name} was already gone.`);
      else if (response.status >= 400) setError("The broker refused that close.");
      else setError(null);
      setPending(null);
      setTick((value) => value + 1);
      nudge();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Could not reach that address.");
    } finally {
      setWorking(false);
    }
  }

  if (error && !rows) return <p className="text-sm text-error">{error}</p>;
  if (!rows) return <p className="text-sm text-muted">Loading connections…</p>;

  return (
    <>
      {error && <p className="mb-4 text-sm text-error">{error}</p>}
      {rows.length === 0 ? (
        <p className="text-sm text-muted">
          {total && total > 0
            ? `${formatCount(total)} connections. This broker does not name them, so they cannot be closed.`
            : "No connections."}
        </p>
      ) : (
        <Card>
          <CardBody>
            <div className="qf-table-scroll" tabIndex={0} role="region" aria-label="Data table"><table className="qf-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>User</th>
                  <th>Peer</th>
                  <th className="num">Channels</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {rows.map((connection) => (
                  <tr key={connection.name}>
                    <td className="font-mono text-xs">{connection.name}</td>
                    <td>{connection.user}</td>
                    <td className="font-mono text-xs">
                      {connection.peerHost}:{connection.peerPort}
                    </td>
                    <td className="num">{formatCount(connection.channels)}</td>
                    <td className="num">
                      <button type="button" className="btn btn-ghost btn-sm text-error" onClick={() => setPending(connection)}>
                        Close
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table></div>
          </CardBody>
        </Card>
      )}
      {pending && (
        <ConfirmDialog
          title={`Close ${pending.name}?`}
          body={`${pending.user} at ${pending.peerHost}:${pending.peerPort} will be disconnected.`}
          action="Close"
          pending={working}
          onConfirm={() => void close()}
          onClose={() => setPending(null)}
        />
      )}
    </>
  );
}

export default function ConnectionsPage() {
  return (
    <>
      <PageHeader title="Connections" icon={Cable} detail="Open AMQP connections on the broker in use. Close is offered only when the broker names the connection." />
      <BrokerGate>
        <ConnectionList />
      </BrokerGate>
    </>
  );
}

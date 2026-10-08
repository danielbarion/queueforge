"use client";

import { useEffect, useState, type ReactNode } from "react";
import { BrokerGate } from "@/components/auth/BrokerGate";
import { Card, CardBody } from "@/components/ui/Card";
import { callBroker } from "@/lib/client";
import { rowsOf } from "@/lib/catalog";
import { selectedBroker, useBrokerStore } from "@/stores/broker";

export type Column = {
  label: string;
  /** Renders one cell from the raw management API row. */
  cell: (row: Record<string, unknown>) => ReactNode;
};

/**
 * A polled table of one management API list (`/api/channels`,
 * `/api/users`, ...). Accepts RabbitMQ's bare array and QueueForge's
 * `{items: [...]}`. `action` renders an optional last column.
 */
export function ApiList({
  path,
  columns,
  empty,
  rowKey,
  action,
  every = 5000,
}: {
  path: string;
  columns: Column[];
  empty: string;
  rowKey: (row: Record<string, unknown>) => string;
  action?: (row: Record<string, unknown>, reload: () => void) => ReactNode;
  every?: number;
}) {
  const broker = useBrokerStore(selectedBroker);
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    if (!broker) return;
    let stop = false;
    async function load() {
      if (!broker) return;
      try {
        const response = await callBroker(broker.url, "GET", path);
        if (stop) return;
        const parsed = response.status === 200 ? rowsOf(response.body) : null;
        if (!parsed) {
          setError(response.status === 404 ? "This broker does not serve that list." : "The list did not answer.");
          return;
        }
        setError(null);
        setRows(parsed);
      } catch (caught) {
        if (!stop) setError(caught instanceof Error ? caught.message : "Could not reach that address.");
      }
    }
    void load();
    const timer = setInterval(() => void load(), every);
    return () => {
      stop = true;
      clearInterval(timer);
    };
  }, [broker, path, every, nonce]);

  if (error) return <p className="text-sm text-error">{error}</p>;
  if (!rows) return <p className="text-sm text-muted">Loading…</p>;
  if (rows.length === 0) return <p className="text-sm text-muted">{empty}</p>;
  const reload = () => setNonce((n) => n + 1);
  return (
    <Card>
      <CardBody>
        <table className="qf-table">
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.label}>{column.label}</th>
              ))}
              {action ? <th aria-label="Actions" /> : null}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={rowKey(row)}>
                {columns.map((column) => (
                  <td key={column.label}>{column.cell(row)}</td>
                ))}
                {action ? <td className="text-right">{action(row, reload)}</td> : null}
              </tr>
            ))}
          </tbody>
        </table>
      </CardBody>
    </Card>
  );
}

/** `ApiList` behind the login gate. */
export function GatedList(props: Parameters<typeof ApiList>[0]) {
  return (
    <BrokerGate>
      <ApiList {...props} />
    </BrokerGate>
  );
}

export function text(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.map(String).join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

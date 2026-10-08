"use client";

import { useEffect, useState } from "react";
import { Server } from "lucide-react";
import { AddBrokerForm } from "@/components/brokers/AddBrokerForm";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { callBroker, logoutBroker } from "@/lib/client";
import { useBrokerStore, type BrokerKind } from "@/stores/broker";

const KIND_CLASS: Record<BrokerKind, string> = {
  rust: "text-rust",
  bun: "text-bun",
  php: "text-php",
  rabbitmq: "text-warning",
};

type Probe = {
  answering: boolean;
  ready: boolean | null;
  kind: BrokerKind | null;
};

function isKind(value: unknown): value is BrokerKind {
  return value === "rust" || value === "bun" || value === "php" || value === "rabbitmq";
}

export default function BrokersPage() {
  const targets = useBrokerStore((state) => state.targets);
  const selectedId = useBrokerStore((state) => state.selectedId);
  const select = useBrokerStore((state) => state.select);
  const remove = useBrokerStore((state) => state.remove);
  const [probes, setProbes] = useState<Record<string, Probe>>({});

  useEffect(() => {
    let stop = false;
    async function tick() {
      const next: Record<string, Probe> = {};
      await Promise.all(
        targets.map(async (target) => {
          try {
            const [health, ready, identity] = await Promise.all([
              callBroker(target.url, "GET", "/healthz"),
              callBroker(target.url, "GET", "/readyz"),
              callBroker(target.url, "GET", "/api/identity"),
            ]);
            const body = identity.body;
            const reported = body !== null && typeof body === "object" && "kind" in body ? body.kind : null;
            // RabbitMQ answers 404 on /healthz and has no /api/identity.
            const answering = health.status === 200 || health.status === 404;
            next[target.id] = {
              answering,
              ready: answering ? ready.status === 200 || (ready.status === 404 && answering) : null,
              kind: isKind(reported) ? reported : target.kind,
            };
          } catch {
            next[target.id] = { answering: false, ready: null, kind: null };
          }
        }),
      );
      if (!stop) setProbes(next);
    }
    void tick();
    const timer = setInterval(() => void tick(), 5000);
    return () => {
      stop = true;
      clearInterval(timer);
    };
  }, [targets]);

  async function onRemove(id: string, url: string) {
    try {
      await logoutBroker(url);
    } finally {
      remove(id);
    }
  }

  return (
    <>
      <PageHeader title="Brokers" icon={Server} detail="Add the management address of a broker you run. Each row is probed on its own. One is in use." />
      <div className="grid items-start gap-6 lg:grid-cols-2">
        <Card>
          <CardBody>
            <h2 className="text-sm font-semibold">Add</h2>
            <AddBrokerForm />
          </CardBody>
        </Card>
        <Card>
          <CardBody>
            <h2 className="text-sm font-semibold">Added</h2>
            {targets.length === 0 && <p className="text-sm text-muted">None yet.</p>}
            <ul className="flex flex-col gap-3">
              {targets.map((target) => {
                const probe = probes[target.id];
                const kind = probe?.kind ?? target.kind;
                const line = !probe ? "checking…" : !probe.answering ? "not answering" : probe.ready ? "answering · ready" : "answering · not ready";
                return (
                  <li key={target.id} className="flex items-start justify-between gap-3 border-b border-base-300 pb-3 last:border-0">
                    <div>
                      <div className="font-medium">{target.name}</div>
                      <div className={`font-mono text-xs ${KIND_CLASS[kind]}`}>
                        {kind} · {target.url}
                      </div>
                      <div className="text-xs text-muted">{line}</div>
                    </div>
                    <div className="flex gap-2">
                      <button type="button" className="btn btn-ghost btn-sm" disabled={target.id === selectedId} onClick={() => select(target.id)}>
                        {target.id === selectedId ? "In use" : "Use"}
                      </button>
                      <button type="button" className="btn btn-ghost btn-sm text-error" onClick={() => void onRemove(target.id, target.url)}>
                        Remove
                      </button>
                    </div>
                  </li>
                );
              })}
            </ul>
          </CardBody>
        </Card>
      </div>
    </>
  );
}

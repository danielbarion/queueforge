"use client";

import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { Layers } from "lucide-react";
import { LoginForm } from "@/components/auth/LoginForm";
import { Card, CardBody } from "@/components/ui/Card";
import { ConfirmDialog } from "@/components/ui/ConfirmDialog";
import { PageHeader } from "@/components/ui/PageHeader";
import { callBroker } from "@/lib/client";
import { parseBindings, type BindingSnap } from "@/lib/live-diff";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { useAlertStore } from "@/stores/alerts";
import { formatCount, useLiveStore } from "@/stores/live";

export default function QueueDetailPage() {
  const params = useParams<{ name: string }>();
  const name = decodeURIComponent(params.name);
  const router = useRouter();
  const broker = useBrokerStore(selectedBroker);
  const authed = useLiveStore((state) => state.authed);
  const queues = useLiveStore((state) => state.queues);
  const nudge = useLiveStore((state) => state.nudge);
  const push = useAlertStore((state) => state.push);
  const queue = queues.find((item) => item.name === name);
  const [bindings, setBindings] = useState<BindingSnap[]>([]);
  const [pending, setPending] = useState<"purge" | "delete" | null>(null);
  const [working, setWorking] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!broker || !authed) return;
    let stop = false;
    async function load() {
      if (!broker) return;
      const direct = await callBroker(broker.url, "GET", `/api/queues/%2F/${encodeURIComponent(name)}/bindings`);
      if (stop) return;
      if (direct.status === 200 && direct.body !== null) {
        setBindings(parseBindings(direct.body, name));
        return;
      }
      const all = await callBroker(broker.url, "GET", "/api/bindings/%2F");
      if (!stop && all.status === 200) setBindings(parseBindings(all.body, name));
    }
    void load();
    const timer = setInterval(() => void load(), 3000);
    return () => {
      stop = true;
      clearInterval(timer);
    };
  }, [broker, authed, name]);

  async function run() {
    if (!broker || !pending) return;
    setWorking(true);
    setError(null);
    try {
      const path = `/api/queues/%2F/${encodeURIComponent(name)}`;
      // RabbitMQ's purge is DELETE .../contents; an older QueueForge build only had POST .../purge.
      let response = pending === "purge" ? await callBroker(broker.url, "DELETE", `${path}/contents`) : await callBroker(broker.url, "DELETE", path);
      if (pending === "purge" && (response.status === 404 || response.status === 405)) {
        response = await callBroker(broker.url, "POST", `${path}/purge`);
      }
      if (response.status >= 400) {
        setError("The broker refused that.");
        return;
      }
      if (pending === "purge") {
        const count = typeof response.body === "object" && response.body && "message_count" in response.body ? Number(response.body.message_count) : queue?.messages ?? 0;
        push(`Purged ${count} messages from ${name}.`);
        setPending(null);
        nudge();
      } else {
        push(`Deleted ${name}.`);
        router.push("/queues");
      }
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Could not reach that address.");
    } finally {
      setWorking(false);
    }
  }

  const held = queue?.messages ?? 0;

  return (
    <>
      <PageHeader title={name} icon={Layers} detail="One queue on /. Purge and delete name it and wait for a confirm." />
      <Link href="/queues" className="mb-4 inline-block text-sm text-primary">
        All queues
      </Link>
      {!broker && <p className="text-sm text-muted">Add a broker first.</p>}
      {broker && authed === null && <p className="text-sm text-muted">Checking the session…</p>}
      {broker && authed === false && (
        <Card>
          <CardBody>
            <LoginForm url={broker.url} />
          </CardBody>
        </Card>
      )}
      {broker && authed && (
        <div className="grid gap-4 lg:grid-cols-2">
          <Card>
            <CardBody>
              {queue ? (
                <dl className="grid grid-cols-2 gap-3 text-sm">
                  <div>
                    <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">Type</dt>
                    <dd>{queue.type}</dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">Consumers</dt>
                    <dd className="font-mono">{formatCount(queue.consumers)}</dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">Ready</dt>
                    <dd className="font-mono">{formatCount(queue.messagesReady)}</dd>
                  </div>
                  <div>
                    <dt className="text-[11px] font-semibold uppercase tracking-wider text-muted">Unacked</dt>
                    <dd className="font-mono">{formatCount(queue.messagesUnacked)}</dd>
                  </div>
                </dl>
              ) : (
                <p className="text-sm text-muted">This queue is not in the latest list.</p>
              )}
              {error && <p className="text-sm text-error">{error}</p>}
              <div className="flex gap-2">
                <button type="button" className="btn btn-ghost" disabled={!queue} onClick={() => setPending("purge")}>
                  Purge
                </button>
                <button type="button" className="btn btn-ghost text-error" disabled={!queue} onClick={() => setPending("delete")}>
                  Delete
                </button>
              </div>
            </CardBody>
          </Card>
          <Card>
            <CardBody>
              <h2 className="text-sm font-semibold">Bindings</h2>
              {bindings.length === 0 ? (
                <p className="text-sm text-muted">No bindings.</p>
              ) : (
                <table className="qf-table">
                  <thead>
                    <tr>
                      <th>Exchange</th>
                      <th>Routing key</th>
                    </tr>
                  </thead>
                  <tbody>
                    {bindings.map((binding) => (
                      <tr key={`${binding.source}-${binding.routingKey}`}>
                        <td>{binding.source}</td>
                        <td className="font-mono text-xs">{binding.routingKey}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </CardBody>
          </Card>
        </div>
      )}
      {pending && (
        <ConfirmDialog
          title={pending === "purge" ? `Purge ${name}?` : `Delete ${name}?`}
          body={
            pending === "purge"
              ? `This removes ${formatCount(held)} messages from ${name}.`
              : `This deletes ${name}. It holds ${formatCount(held)} messages.`
          }
          action={pending === "purge" ? "Purge" : "Delete"}
          pending={working}
          onConfirm={() => void run()}
          onClose={() => setPending(null)}
        />
      )}
    </>
  );
}

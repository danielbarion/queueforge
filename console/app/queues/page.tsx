"use client";

import Link from "next/link";
import { Layers } from "lucide-react";
import { LoginForm } from "@/components/auth/LoginForm";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { formatCount, useLiveStore } from "@/stores/live";

export default function QueuesPage() {
  const broker = useBrokerStore(selectedBroker);
  const authed = useLiveStore((state) => state.authed);
  const queues = useLiveStore((state) => state.queues);
  const rows = [...queues].sort((a, b) => a.name.localeCompare(b.name));

  return (
    <>
      <PageHeader title="Queues" icon={Layers} detail="Queues on the / vhost of the broker in use." />
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
        <Card>
          <CardBody>
            {rows.length === 0 ? (
              <p className="text-sm text-muted">No queues on /.</p>
            ) : (
              <table className="qf-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Type</th>
                    <th className="num">Ready</th>
                    <th className="num">Unacked</th>
                    <th className="num">Consumers</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((queue) => (
                    <tr key={queue.name}>
                      <td>
                        <Link href={`/queues/${encodeURIComponent(queue.name)}`} className="font-medium hover:text-primary">
                          {queue.name}
                        </Link>
                      </td>
                      <td>{queue.type}</td>
                      <td className="num">{formatCount(queue.messagesReady)}</td>
                      <td className="num">{formatCount(queue.messagesUnacked)}</td>
                      <td className="num">{formatCount(queue.consumers)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </CardBody>
        </Card>
      )}
    </>
  );
}

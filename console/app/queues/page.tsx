"use client";

import { useState } from "react";
import { MessageInspector } from "@/components/messages/MessageInspector";
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
  const [search, setSearch] = useState("");
  const [type, setType] = useState("all");
  const [state, setState] = useState("all");
  const [sort, setSort] = useState("name");
  const [inspect, setInspect] = useState<string | null>(null);
  const rows = queues.filter((q) => q.name.toLowerCase().includes(search.toLowerCase()) && (type === "all" || q.type === type) && (state === "all" || (state === "backlog" ? q.messagesReady > 0 : state === "unacked" ? q.messagesUnacked > 0 : q.messages === 0))).sort((a, b) => sort === "name" ? a.name.localeCompare(b.name) : sort === "ready" ? b.messagesReady - a.messagesReady : sort === "unacked" ? b.messagesUnacked - a.messagesUnacked : b.consumers - a.consumers);
  const inspected = queues.find((q) => q.name === inspect);

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
            <div className="qf-filters"><label className="qf-field flex-1"><span>Search queues</span><input className="input input-bordered w-full" placeholder="Queue name…" value={search} onChange={(e) => setSearch(e.target.value)} /></label><label className="qf-field"><span>Type</span><select className="select select-bordered" value={type} onChange={(e) => setType(e.target.value)}><option value="all">All types</option>{[...new Set(queues.map((q) => q.type))].sort().map((t) => <option key={t}>{t}</option>)}</select></label><label className="qf-field"><span>Messages</span><select className="select select-bordered" value={state} onChange={(e) => setState(e.target.value)}><option value="all">All queues</option><option value="backlog">Ready backlog</option><option value="unacked">Unacknowledged</option><option value="empty">Empty</option></select></label><label className="qf-field"><span>Sort by</span><select className="select select-bordered" value={sort} onChange={(e) => setSort(e.target.value)}><option value="name">Name</option><option value="ready">Ready (descending)</option><option value="unacked">Unacked (descending)</option><option value="consumers">Consumers (descending)</option></select></label></div>
            <p className="text-xs text-muted">{rows.length} matching / {queues.length} loaded queues · list request capped at 500</p>
            {rows.length === 0 ? (
              <p className="text-sm text-muted">No loaded queues match these filters.</p>
            ) : (
              <div className="qf-table-scroll" tabIndex={0} role="region" aria-label="Data table"><table className="qf-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Type</th>
                    <th className="num">Ready</th>
                    <th className="num">Unacked</th>
                    <th className="num">Consumers</th><th>Messages</th>
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
                      <td className="num">{formatCount(queue.consumers)}</td><td><button className="btn btn-ghost btn-sm" onClick={() => setInspect(queue.name)}>Inspect</button></td>
                    </tr>
                  ))}
                </tbody>
              </table></div>
            )}
          </CardBody>
        </Card>
      )}
      {broker && inspected && <MessageInspector key={`${broker.id}:${inspected.name}`} broker={broker} queue={inspected} onClose={() => setInspect(null)} />}
    </>
  );
}

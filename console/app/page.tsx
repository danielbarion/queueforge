"use client";

import Link from "next/link";
import { Activity, Gauge, Layers, Server } from "lucide-react";
import { Throughput } from "@/components/live/Throughput";
import { LoginForm } from "@/components/auth/LoginForm";
import { Card, CardBody } from "@/components/ui/Card";
import { PageHeader } from "@/components/ui/PageHeader";
import { Stat } from "@/components/ui/Stat";
import { logoutBroker } from "@/lib/client";
import { totals } from "@/lib/live-diff";
import { selectedBroker, useBrokerStore } from "@/stores/broker";
import { formatCount, formatRate, useLiveStore } from "@/stores/live";

function probeLabel(value: boolean | null, up: string, down: string): string {
  if (value === null) return "—";
  return value ? up : down;
}

export default function OverviewPage() {
  const broker = useBrokerStore(selectedBroker);
  const setDemo = useBrokerStore((state) => state.setDemo);
  const live = useLiveStore();
  const counts = totals(live.queues);
  const busiest = [...live.queues].sort((a, b) => b.messagesReady - a.messagesReady).slice(0, 5);
  const recent = live.activity.slice(0, 8);

  async function logout() {
    if (!broker) return;
    await logoutBroker(broker.url);
    live.setAuthed(false);
    live.nudge();
  }

  if (!broker) return (
    <>
      <PageHeader title="Your broker workspace" icon={Gauge} detail="Connect a broker to monitor messages, manage queues and keep an eye on your cluster." />
      <Card className="qf-welcome mb-8">
        <CardBody className="py-10 sm:p-10">
          <span className="qf-eyebrow">Get started</span>
          <div className="qf-welcome-icon"><Server className="size-8" aria-hidden="true" /></div>
          <h2 className="max-w-lg text-3xl font-semibold tracking-tight">A clear view of your message flow.</h2>
          <p className="max-w-lg text-sm leading-7 text-muted">Connect QueueForge or RabbitMQ using its management address. Keep your brokers in one workspace and switch between them whenever you need.</p>
          <div className="flex flex-wrap gap-3"><Link href="/brokers" className="btn btn-primary mt-2 w-fit">Connect a broker <span aria-hidden="true">↗</span></Link><button className="btn btn-ghost mt-2" onClick={() => setDemo(true)}>Try interactive demo</button></div>
        </CardBody>
      </Card>
      <div className="grid gap-4 sm:grid-cols-3">
        {[{ icon: Server, title: "Connect", text: "Add the management URL of a broker you run." }, { icon: Layers, title: "Operate", text: "Inspect queues, exchanges and connections." }, { icon: Activity, title: "Monitor", text: "Follow message rates, activity and alerts." }].map(({ icon: Icon, title, text }, index) => <Card key={title}><CardBody><span className="qf-eyebrow">0{index + 1}</span><Icon className="size-5 text-primary" aria-hidden="true" /><h3 className="text-base font-semibold">{title}</h3><p className="text-sm leading-6 text-muted">{text}</p></CardBody></Card>)}
      </div>
    </>
  );

  return (
    <>
      <PageHeader
        title="Overview"
        icon={Gauge}
        detail={broker ? `Watching ${broker.name}.` : "No broker is in use. Add one on the Brokers page."}
      />
      {broker && live.stale && live.overview && (
        <p className="mb-4 text-sm text-warning">
          {live.error ?? "The broker stopped answering."} These figures are from{" "}
          {live.sampledAt ? new Date(live.sampledAt).toLocaleTimeString() : "the last poll"}.
        </p>
      )}
      {broker && live.error && !live.overview && <p className="mb-4 text-sm text-error">{live.error}</p>}
      <div className="mb-6 grid gap-3 sm:grid-cols-3">
        <Card>
          <CardBody>
            <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">Health</span>
            <strong className="font-mono text-lg">{probeLabel(live.health, "up", "down")}</strong>
          </CardBody>
        </Card>
        <Card>
          <CardBody>
            <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">Ready</span>
            <strong className="font-mono text-lg">{probeLabel(live.ready, "ready", "not ready")}</strong>
          </CardBody>
        </Card>
        <Card>
          <CardBody>
            <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">Broker</span>
            <strong>{broker ? broker.name : "None"}</strong>
            <span className="font-mono text-xs text-muted">{broker ? broker.url : "Add one to begin"}</span>
            {broker && live.authed && (
              <button type="button" className="btn btn-ghost btn-sm w-fit px-0" onClick={() => void logout()}>
                Log out
              </button>
            )}
          </CardBody>
        </Card>
      </div>
      {broker && live.authed === false && (
        <Card className="mb-6">
          <CardBody>
            <h2 className="text-sm font-semibold">Log in</h2>
            <p className="text-sm text-muted">The console keeps the session. The browser does not receive the broker cookie.</p>
            <LoginForm url={broker.url} />
          </CardBody>
        </Card>
      )}
      <div className="mb-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        <Stat label="Publish /s" value={formatRate(live.rates.publish)} />
        <Stat label="Deliver /s" value={formatRate(live.rates.deliver)} />
        <Stat label="Ack /s" value={formatRate(live.rates.ack)} />
        <Stat label="Ready" value={live.overview ? formatCount(counts.ready) : "—"} />
        <Stat label="Unacked" value={live.overview ? formatCount(counts.unacked) : "—"} />
        <Stat label="Connections" value={live.overview ? formatCount(live.overview.connections) : "—"} />
      </div>
      <div className="mb-6"><Throughput /></div>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.4fr)_minmax(240px,0.8fr)]">
        <Card>
          <CardBody>
            <h2 className="text-sm font-semibold">Queues</h2>
            {busiest.length === 0 ? (
              <p className="text-sm text-muted">{live.authed ? "No queues on /." : "Log in to read the queues."}</p>
            ) : (
              <div className="qf-table-scroll" tabIndex={0} role="region" aria-label="Data table"><table className="qf-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th className="num">Ready</th>
                    <th className="num">Unacked</th>
                  </tr>
                </thead>
                <tbody>
                  {busiest.map((queue) => (
                    <tr key={queue.name}>
                      <td>
                        <Link href={`/queues/${encodeURIComponent(queue.name)}`} className="font-medium hover:text-primary">
                          {queue.name}
                        </Link>
                      </td>
                      <td className="num">{formatCount(queue.messagesReady)}</td>
                      <td className="num">{formatCount(queue.messagesUnacked)}</td>
                    </tr>
                  ))}
                </tbody>
              </table></div>
            )}
          </CardBody>
        </Card>
        <Card>
          <CardBody>
            <h2 className="flex items-center gap-2 text-sm font-semibold">
              <Activity className="size-4 text-primary" aria-hidden="true" />
              Activity
            </h2>
            {recent.length === 0 ? (
              <p className="text-sm text-muted">Waiting for a queue to change.</p>
            ) : (
              <ul className="flex flex-col gap-2">
                {recent.map((event) => (
                  <li key={event.id} className="text-sm">
                    <span className="mr-2 font-mono text-xs text-faint">{new Date(event.at).toLocaleTimeString()}</span>
                    {event.text}
                  </li>
                ))}
              </ul>
            )}
            <Link href="/activity" className="text-sm text-primary">
              All activity
            </Link>
          </CardBody>
        </Card>
      </div>
    </>
  );
}

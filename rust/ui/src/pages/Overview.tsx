import { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { ApiError, getOverview, listQueues, type Overview, type QueueItem, type Whoami } from "../api";
import Layout from "../components/Layout";
import Spark from "../components/Spark";
import { chartSeries, trafficRates, type ChartPoint, type TrafficSample } from "../rates";

type Props = {
  user: Whoami;
  onLoggedOut: () => void;
};

type Probe = "ok" | "down" | "unknown";

type Activity = { at: number; text: string };

export default function OverviewPage({ user, onLoggedOut }: Props) {
  const [data, setData] = useState<Overview | null>(null);
  const [queues, setQueues] = useState<QueueItem[]>([]);
  const [health, setHealth] = useState<Probe>("unknown");
  const [ready, setReady] = useState<Probe>("unknown");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [rates, setRates] = useState({ publish: 0, deliver: 0, ack: 0, ready: 0, unacked: 0 });
  const [history, setHistory] = useState<ChartPoint[]>([]);
  const [activity, setActivity] = useState<Activity[]>([]);
  const samples = useRef<{ at: number; sample: TrafficSample }[]>([]);
  const previousQueues = useRef<Map<string, QueueItem>>(new Map());

  const load = useCallback(async () => {
    setError(null);
    try {
      const [ov, page, healthRes, readyRes] = await Promise.all([
        getOverview(),
        listQueues("/").catch(() => ({ items: [] as QueueItem[] })),
        fetch("/healthz", { credentials: "same-origin" }),
        fetch("/readyz", { credentials: "same-origin" }),
      ]);
      setData(ov);
      setHealth(healthRes.ok ? "ok" : "down");
      setReady(readyRes.ok ? "ok" : "down");
      const listed = [...(page.items ?? [])].sort((a, b) => b.messages - a.messages);
      setQueues(listed);
      const notes: Activity[] = [];
      const now = Date.now();
      const seen = new Set<string>();
      for (const queue of listed) {
        const key = `${queue.vhost}/${queue.name}`;
        seen.add(key);
        const prior = previousQueues.current.get(key);
        if (!prior) {
          if (previousQueues.current.size > 0) notes.push({ at: now, text: `${queue.name} appeared` });
          continue;
        }
        if (prior.messages_ready !== queue.messages_ready) {
          notes.push({
            at: now,
            text: `${queue.name} ready ${prior.messages_ready.toLocaleString()} → ${queue.messages_ready.toLocaleString()}`,
          });
        }
        if (prior.consumers !== queue.consumers) {
          notes.push({
            at: now,
            text: `${queue.name} consumers ${prior.consumers} → ${queue.consumers}`,
          });
        }
      }
      for (const [key, prior] of previousQueues.current) {
        if (!seen.has(key)) notes.push({ at: now, text: `${prior.name} is gone` });
      }
      previousQueues.current = new Map(listed.map((queue) => [`${queue.vhost}/${queue.name}`, queue]));
      if (notes.length) setActivity((current) => [...notes, ...current].slice(0, 12));

      const sample: TrafficSample = {
        publish: ov.message_stats?.publish ?? 0,
        deliver: ov.message_stats?.deliver ?? 0,
        ack: ov.message_stats?.ack ?? 0,
        confirmed: 0,
        ready: ov.queue_totals.messages_ready,
        unacked: ov.queue_totals.messages_unacknowledged,
        consumers: ov.object_totals.consumers,
        confirmBeforeFsync: 0,
        fsync: 0,
      };
      const series = [...samples.current, { at: now, sample }].slice(-40);
      samples.current = series;
      const prior = series.length > 1 ? series[series.length - 2] : null;
      if (prior) {
        const live = trafficRates(prior.sample, sample, (now - prior.at) / 1000);
        setRates({
          publish: live.publishPerSec,
          deliver: live.deliverPerSec,
          ack: live.ackPerSec,
          ready: live.ready,
          unacked: live.unacked,
        });
      } else {
        setRates((current) => ({ ...current, ready: sample.ready, unacked: sample.unacked }));
      }
      setHistory(chartSeries(series));
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load overview");
    } finally {
      setLoading(false);
    }
  }, [onLoggedOut]);

  useEffect(() => {
    void load();
    const id = window.setInterval(() => void load(), 1000);
    return () => window.clearInterval(id);
  }, [load]);

  return (
    <Layout
      user={user}
      onLoggedOut={onLoggedOut}
      title="Overview"
      actions={
        <button type="button" className="btn-secondary" onClick={() => void load()}>
          Refresh
        </button>
      }
    >
      {error && <div className="error">{error}</div>}
      {loading && !data && <p className="muted">Loading overview…</p>}

      {data && (
        <>
          <section className="status-row">
            <ProbeMark label="Health" state={health} />
            <ProbeMark label="Ready" state={ready} />
            <div className="card status-product">
              <span className="label">Broker</span>
              <strong>
                {data.product_name} {data.product_version}
              </strong>
              <span className="muted">AMQP {data.rabbitmq_version_compat}</span>
            </div>
          </section>

          <h2>Throughput</h2>
          <div className="card spark-card">
            <Spark points={history} magnitudes={rates} />
          </div>
          <div className="stat-grid">
            <Stat label="Publish /s" value={rates.publish} />
            <Stat label="Deliver /s" value={rates.deliver} />
            <Stat label="Ack /s" value={rates.ack} />
            <Stat label="Ready" value={rates.ready} />
            <Stat label="Unacked" value={rates.unacked} />
            <Stat label="Connections" value={data.object_totals.connections} />
            <Stat label="Channels" value={data.object_totals.channels} />
            <Stat label="Consumers" value={data.object_totals.consumers} />
          </div>

          <div className="dash-split">
            <section>
              <h2>Queues</h2>
              <div className="table-wrap card">
                <table>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Ready</th>
                      <th>Unacked</th>
                      <th>Consumers</th>
                      <th>State</th>
                    </tr>
                  </thead>
                  <tbody>
                    {queues.slice(0, 8).map((queue) => (
                      <tr key={`${queue.vhost}/${queue.name}`}>
                        <td>
                          <Link to={`/queues/${encodeURIComponent(queue.vhost)}/${encodeURIComponent(queue.name)}`}>
                            <code>{queue.name}</code>
                          </Link>
                        </td>
                        <td>{queue.messages_ready.toLocaleString()}</td>
                        <td>{queue.messages_unacknowledged.toLocaleString()}</td>
                        <td>{queue.consumers}</td>
                        <td>{queue.state || "running"}</td>
                      </tr>
                    ))}
                    {queues.length === 0 && (
                      <tr>
                        <td colSpan={5} className="muted">
                          No queues in /.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </section>
            <section>
              <h2>Activity</h2>
              <ul className="activity card">
                {activity.length === 0 && <li className="muted">Waiting for a queue to change.</li>}
                {activity.map((line, index) => (
                  <li key={`${line.at}-${index}`}>
                    <time>{new Date(line.at).toLocaleTimeString()}</time>
                    {line.text}
                  </li>
                ))}
              </ul>
            </section>
          </div>
        </>
      )}
    </Layout>
  );
}

function ProbeMark({ label, state }: { label: string; state: Probe }) {
  const text = state === "ok" ? "ok" : state === "down" ? "down" : "…";
  return (
    <div className={`card probe is-${state}`}>
      <span className="label">{label}</span>
      <strong>{text}</strong>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  const shown = Number.isInteger(value) ? value.toLocaleString() : value.toFixed(1);
  return (
    <div className="stat card">
      <span className="label">{label}</span>
      <span className="stat-value">{shown}</span>
    </div>
  );
}

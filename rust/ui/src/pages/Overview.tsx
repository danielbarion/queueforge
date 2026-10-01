import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, getOverview, type Overview, type Whoami } from "../api";
import Layout from "../components/Layout";
import Spark from "../components/Spark";
import { chartSeries, trafficRates, type ChartPoint, type TrafficSample } from "../rates";

type Props = {
  user: Whoami;
  onLoggedOut: () => void;
};

export default function OverviewPage({ user, onLoggedOut }: Props) {
  const [data, setData] = useState<Overview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [rates, setRates] = useState({ publish: 0, deliver: 0, ack: 0, ready: 0, unacked: 0 });
  const [history, setHistory] = useState<ChartPoint[]>([]);
  const samples = useRef<{ at: number; sample: TrafficSample }[]>([]);

  const load = useCallback(async () => {
    setError(null);
    try {
      const ov = await getOverview();
      setData(ov);
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
      const now = Date.now();
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
          <section className="card meta">
            <div>
              <span className="label">Product</span>
              <strong>
                {data.product_name} {data.product_version}
              </strong>
            </div>
            <div>
              <span className="label">Management</span>
              <strong>{data.management_version}</strong>
            </div>
            <div>
              <span className="label">AMQP compat</span>
              <strong>{data.rabbitmq_version_compat}</strong>
            </div>
          </section>

          <h2>Object totals</h2>
          <div className="stat-grid">
            <Stat label="Connections" value={data.object_totals.connections} />
            <Stat label="Channels" value={data.object_totals.channels} />
            <Stat label="Queues" value={data.object_totals.queues} />
            <Stat label="Exchanges" value={data.object_totals.exchanges} />
            <Stat label="Consumers" value={data.object_totals.consumers} />
            <Stat label="Vhosts" value={data.object_totals.vhosts} />
          </div>

          <h2>Message rates</h2>
          <div className="card spark-card">
            <Spark points={history} magnitudes={rates} />
          </div>
          <div className="stat-grid">
            <Stat label="Publish /s" value={rates.publish} />
            <Stat label="Deliver /s" value={rates.deliver} />
            <Stat label="Ack /s" value={rates.ack} />
            <Stat label="Ready" value={rates.ready} />
            <Stat label="Unacked" value={rates.unacked} />
          </div>

          <h2>Queue totals</h2>
          <div className="stat-grid">
            <Stat label="Messages" value={data.queue_totals.messages} />
            <Stat label="Ready" value={data.queue_totals.messages_ready} />
            <Stat
              label="Unacked"
              value={data.queue_totals.messages_unacknowledged}
            />
          </div>
        </>
      )}
    </Layout>
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

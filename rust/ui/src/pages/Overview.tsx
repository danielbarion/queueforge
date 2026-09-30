import { useCallback, useEffect, useState } from "react";
import { ApiError, getOverview, type Overview, type Whoami } from "../api";
import Layout from "../components/Layout";

type Props = {
  user: Whoami;
  onLoggedOut: () => void;
};

export default function OverviewPage({ user, onLoggedOut }: Props) {
  const [data, setData] = useState<Overview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setError(null);
    try {
      const ov = await getOverview();
      setData(ov);
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
    const id = window.setInterval(() => void load(), 10_000);
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
  return (
    <div className="stat card">
      <span className="label">{label}</span>
      <span className="stat-value">{value.toLocaleString()}</span>
    </div>
  );
}

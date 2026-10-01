import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  ApiError,
  forceCloseConnection,
  listConnections,
  type ConnectionItem,
  type Whoami,
} from "../api";
import Layout from "../components/Layout";

type Props = { user: Whoami; onLoggedOut: () => void };

export default function ConnectionsPage({ user, onLoggedOut }: Props) {
  const [items, setItems] = useState<ConnectionItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const page = await listConnections();
      setItems(page.items);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load connections");
    }
  }, [onLoggedOut]);

  useEffect(() => {
    void load();
    const id = window.setInterval(() => void load(), 5_000);
    return () => window.clearInterval(id);
  }, [load]);

  async function onForceClose(id: string) {
    if (!window.confirm(`Force-close connection ${id}?`)) return;
    setBusy(id);
    setError(null);
    try {
      await forceCloseConnection(id);
      await load();
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Force-close failed");
    } finally {
      setBusy(null);
    }
  }

  return (
    <Layout
      user={user}
      onLoggedOut={onLoggedOut}
      title="Connections"
      actions={
        <button type="button" className="btn-secondary" onClick={() => void load()}>
          Refresh
        </button>
      }
    >
      {error && <div className="error">{error}</div>}
      <div className="table-wrap card">
        <table>
          <thead>
            <tr>
              <th>Id</th>
              <th>User</th>
              <th>Vhost</th>
              <th>Peer</th>
              <th>Channels</th>
              <th>Connected</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {items.map((c) => (
              <tr key={c.name}>
                <td>
                  <Link to={`/connections/${encodeURIComponent(c.name)}`}><code>{c.name}</code></Link>
                </td>
                <td>{c.user}</td>
                <td>
                  <code>{c.vhost}</code>
                </td>
                <td>
                  {c.peer_host}:{c.peer_port}
                </td>
                <td>{c.channels}</td>
                <td className="muted">
                  {c.connected_at
                    ? new Date(c.connected_at * 1000).toLocaleString()
                    : "—"}
                </td>
                <td className="row-actions">
                  <button
                    type="button"
                    className="btn-danger"
                    disabled={busy === c.name}
                    onClick={() => void onForceClose(c.name)}
                  >
                    {busy === c.name ? "Closing…" : "Force close"}
                  </button>
                </td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  No live AMQP connections.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Layout>
  );
}

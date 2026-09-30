import { useCallback, useEffect, useState } from "react";
import {
  ApiError,
  createBinding,
  deleteBinding,
  listBindings,
  listVhosts,
  type BindingItem,
  type Whoami,
} from "../api";
import Layout from "../components/Layout";

type Props = { user: Whoami; onLoggedOut: () => void };

export default function BindingsPage({ user, onLoggedOut }: Props) {
  const [vhost, setVhost] = useState("/");
  const [vhosts, setVhosts] = useState<string[]>(["/"]);
  const [items, setItems] = useState<BindingItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [source, setSource] = useState("");
  const [destination, setDestination] = useState("");
  const [routingKey, setRoutingKey] = useState("");

  const load = useCallback(async () => {
    setError(null);
    try {
      const vh = await listVhosts();
      setVhosts(vh.items.map((v) => v.name));
      const page = await listBindings(vhost);
      setItems(page.items);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load bindings");
    }
  }, [vhost, onLoggedOut]);

  useEffect(() => {
    void load();
  }, [load]);

  async function onCreate(e: React.FormEvent) {
    e.preventDefault();
    try {
      await createBinding(vhost, {
        source,
        destination,
        routing_key: routingKey,
      });
      setSource("");
      setDestination("");
      setRoutingKey("");
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Bind failed");
    }
  }

  return (
    <Layout
      user={user}
      onLoggedOut={onLoggedOut}
      title="Bindings"
      actions={
        <button type="button" className="btn-secondary" onClick={() => void load()}>
          Refresh
        </button>
      }
    >
      {error && <div className="error">{error}</div>}
      <div className="toolbar">
        <label>
          Vhost
          <select value={vhost} onChange={(e) => setVhost(e.target.value)}>
            {vhosts.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
        </label>
      </div>

      <form className="inline-form card" onSubmit={(e) => void onCreate(e)}>
        <strong>Bind queue</strong>
        <input
          placeholder="source exchange"
          value={source}
          onChange={(e) => setSource(e.target.value)}
        />
        <input
          placeholder="destination queue"
          value={destination}
          onChange={(e) => setDestination(e.target.value)}
        />
        <input
          placeholder="routing key"
          value={routingKey}
          onChange={(e) => setRoutingKey(e.target.value)}
        />
        <button type="submit" className="btn-primary">
          Bind
        </button>
      </form>

      <div className="table-wrap card">
        <table>
          <thead>
            <tr>
              <th>Source</th>
              <th>Destination</th>
              <th>Routing key</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {items.map((b, i) => (
              <tr key={`${b.source}-${b.destination}-${b.routing_key}-${i}`}>
                <td>
                  <code>{b.source || "(default)"}</code>
                </td>
                <td>
                  <code>{b.destination}</code>
                </td>
                <td>
                  <code>{b.routing_key || "—"}</code>
                </td>
                <td className="row-actions">
                  {b.source !== "" && (
                    <button
                      type="button"
                      className="btn-danger"
                      onClick={() =>
                        void deleteBinding(
                          b.vhost,
                          b.source,
                          b.destination,
                          b.properties_key,
                        )
                          .then(load)
                          .catch((e) => setError(String(e.message ?? e)))
                      }
                    >
                      Unbind
                    </button>
                  )}
                </td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr>
                <td colSpan={4} className="muted">
                  No bindings in this vhost.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Layout>
  );
}

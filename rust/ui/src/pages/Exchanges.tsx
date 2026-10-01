import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  ApiError,
  createExchange,
  deleteExchange,
  listExchanges,
  listVhosts,
  type ExchangeItem,
  type Whoami,
} from "../api";
import Layout from "../components/Layout";

type Props = { user: Whoami; onLoggedOut: () => void };

export default function ExchangesPage({ user, onLoggedOut }: Props) {
  const [vhost, setVhost] = useState("/");
  const [vhosts, setVhosts] = useState<string[]>(["/"]);
  const [items, setItems] = useState<ExchangeItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [kind, setKind] = useState("direct");
  const [durable, setDurable] = useState(true);

  const load = useCallback(async () => {
    setError(null);
    try {
      const vh = await listVhosts();
      setVhosts(vh.items.map((v) => v.name));
      const page = await listExchanges(vhost);
      setItems(page.items);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load exchanges");
    }
  }, [vhost, onLoggedOut]);

  useEffect(() => {
    void load();
  }, [load]);

  async function onCreate(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) return;
    try {
      await createExchange(vhost, name.trim(), { type: kind, durable });
      setName("");
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Create failed");
    }
  }

  return (
    <Layout
      user={user}
      onLoggedOut={onLoggedOut}
      title="Exchanges"
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
        <strong>Declare exchange</strong>
        <input
          placeholder="exchange name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <select value={kind} onChange={(e) => setKind(e.target.value)}>
          <option value="direct">direct</option>
          <option value="fanout">fanout</option>
          <option value="topic">topic</option>
          <option value="headers">headers</option>
        </select>
        <label className="checkbox">
          <input
            type="checkbox"
            checked={durable}
            onChange={(e) => setDurable(e.target.checked)}
          />
          durable
        </label>
        <button type="submit" className="btn-primary">
          Create
        </button>
      </form>

      <div className="table-wrap card">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Durable</th>
              <th>Auto-delete</th>
              <th>Internal</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {items.map((ex) => (
              <tr key={`${ex.vhost}/${ex.name || "(default)"}`}>
                <td>
                  <Link to={`/exchanges/${encodeURIComponent(ex.vhost)}/${encodeURIComponent(ex.name)}`}><code>{ex.name || "(AMQP default)"}</code></Link>
                </td>
                <td>{ex.type}</td>
                <td>{ex.durable ? "yes" : "no"}</td>
                <td>{ex.auto_delete ? "yes" : "no"}</td>
                <td>{ex.internal ? "yes" : "no"}</td>
                <td className="row-actions">
                  {!ex.name.startsWith("amq.") && ex.name !== "" && (
                    <button
                      type="button"
                      className="btn-danger"
                      onClick={() =>
                        void deleteExchange(ex.vhost, ex.name)
                          .then(load)
                          .catch((e) => setError(String(e.message ?? e)))
                      }
                    >
                      Delete
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Layout>
  );
}

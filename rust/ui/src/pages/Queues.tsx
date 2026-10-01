import { useCallback, useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import {
  ApiError,
  createQueue,
  deleteQueue,
  listQueues,
  getOverview,
  listVhosts,
  purgeQueue,
  type QueueArguments,
  type QueueItem,
  type Whoami,
} from "../api";
import Layout from "../components/Layout";
import { trafficRates, type TrafficSample } from "../rates";

type Props = { user: Whoami; onLoggedOut: () => void };

export default function QueuesPage({ user, onLoggedOut }: Props) {
  const [vhost, setVhost] = useState("/");
  const [vhosts, setVhosts] = useState<string[]>(["/"]);
  const [items, setItems] = useState<QueueItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [durable, setDurable] = useState(true);
  const [queueType, setQueueType] = useState<"classic" | "quorum">("classic");
  const [messageTtl, setMessageTtl] = useState("");
  const [maxLength, setMaxLength] = useState("");
  const [maxPriority, setMaxPriority] = useState("");
  const [dlx, setDlx] = useState("");
  const [dlxRk, setDlxRk] = useState("");
  const [overflow, setOverflow] = useState<"drop-head" | "reject-publish">(
    "drop-head",
  );
  const [showArgs, setShowArgs] = useState(false);
  const [rates, setRates] = useState({ publish: 0, deliver: 0, ack: 0, ready: 0, unacked: 0 });
  const previous = useRef<{ at: number; sample: TrafficSample } | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const vh = await listVhosts();
      setVhosts(vh.items.map((v) => v.name));
      const page = await listQueues(vhost);
      setItems(page.items);
      const ov = await getOverview();
      const sample: TrafficSample = {
        publish: ov.message_stats?.publish ?? 0,
        deliver: ov.message_stats?.deliver ?? 0,
        ack: ov.message_stats?.ack ?? 0,
        confirmed: 0,
        ready: page.items.reduce((sum, q) => sum + q.messages_ready, 0),
        unacked: page.items.reduce((sum, q) => sum + q.messages_unacknowledged, 0),
        consumers: page.items.reduce((sum, q) => sum + q.consumers, 0),
        confirmBeforeFsync: 0,
        fsync: 0,
      };
      const prior = previous.current;
      const now = Date.now();
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
      previous.current = { at: now, sample };
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load queues");
    }
  }, [vhost, onLoggedOut]);

  useEffect(() => {
    void load();
    const id = window.setInterval(() => void load(), 1000);
    return () => window.clearInterval(id);
  }, [load]);

  async function onCreate(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) return;
    try {
      const argumentsPayload: QueueArguments = {};
      if (messageTtl.trim()) {
        argumentsPayload["x-message-ttl"] = Number(messageTtl);
      }
      if (maxLength.trim()) {
        argumentsPayload["x-max-length"] = Number(maxLength);
      }
      if (maxPriority.trim()) {
        argumentsPayload["x-max-priority"] = Number(maxPriority);
      }
      if (dlx.trim()) {
        argumentsPayload["x-dead-letter-exchange"] = dlx.trim();
      }
      if (dlxRk.trim()) {
        argumentsPayload["x-dead-letter-routing-key"] = dlxRk.trim();
      }
      if (overflow !== "drop-head") {
        argumentsPayload["x-overflow"] = overflow;
      }
      if (queueType === "quorum") {
        argumentsPayload["x-queue-type"] = "quorum";
      }
      await createQueue(vhost, name.trim(), {
        durable,
        arguments: argumentsPayload,
      });
      setName("");
      setMessageTtl("");
      setMaxLength("");
      setMaxPriority("");
      setDlx("");
      setDlxRk("");
      setOverflow("drop-head");
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Create failed");
    }
  }

  return (
    <Layout
      user={user}
      onLoggedOut={onLoggedOut}
      title="Queues"
      actions={
        <button type="button" className="btn-secondary" onClick={() => void load()}>
          Refresh
        </button>
      }
    >
      {error && <div className="error">{error}</div>}
      <div className="stat-grid">
        <div className="stat card"><span className="label">Publish /s</span><span className="stat-value">{rates.publish.toFixed(1)}</span></div>
        <div className="stat card"><span className="label">Deliver /s</span><span className="stat-value">{rates.deliver.toFixed(1)}</span></div>
        <div className="stat card"><span className="label">Ack /s</span><span className="stat-value">{rates.ack.toFixed(1)}</span></div>
        <div className="stat card"><span className="label">Ready</span><span className="stat-value">{rates.ready}</span></div>
        <div className="stat card"><span className="label">Unacked</span><span className="stat-value">{rates.unacked}</span></div>
      </div>

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

      <form className="stack-form card" onSubmit={(e) => void onCreate(e)}>
        <strong>Declare queue</strong>
        <div className="inline-form">
          <input
            placeholder="queue name"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
          <label className="checkbox">
            <input
              type="checkbox"
              checked={durable}
              onChange={(e) => setDurable(e.target.checked)}
            />
            durable
          </label>
          <label>
            Type
            <select value={queueType} onChange={(e) => setQueueType(e.target.value as "classic" | "quorum")}>
              <option value="classic">classic</option>
              <option value="quorum">quorum</option>
            </select>
          </label>
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setShowArgs((s) => !s)}
          >
            {showArgs ? "Hide args" : "Queue args"}
          </button>
          <button type="submit" className="btn-primary">
            Create
          </button>
        </div>
        {showArgs && (
          <div className="args-grid">
            <label>
              x-message-ttl (ms)
              <input
                type="number"
                min={1}
                placeholder="optional"
                value={messageTtl}
                onChange={(e) => setMessageTtl(e.target.value)}
              />
            </label>
            <label>
              x-max-length
              <input
                type="number"
                min={1}
                placeholder="optional"
                value={maxLength}
                onChange={(e) => setMaxLength(e.target.value)}
              />
            </label>
            <label>
              x-max-priority
              <input
                type="number"
                min={0}
                max={255}
                placeholder="optional"
                value={maxPriority}
                onChange={(e) => setMaxPriority(e.target.value)}
              />
            </label>
            <label>
              x-overflow
              <select
                value={overflow}
                onChange={(e) =>
                  setOverflow(e.target.value as "drop-head" | "reject-publish")
                }
              >
                <option value="drop-head">drop-head</option>
                <option value="reject-publish">reject-publish</option>
              </select>
            </label>
            <label>
              x-dead-letter-exchange
              <input
                placeholder="optional"
                value={dlx}
                onChange={(e) => setDlx(e.target.value)}
              />
            </label>
            <label>
              x-dead-letter-routing-key
              <input
                placeholder="optional"
                value={dlxRk}
                onChange={(e) => setDlxRk(e.target.value)}
              />
            </label>
          </div>
        )}
      </form>

      <div className="table-wrap card">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>State</th>
              <th>Ready</th>
              <th>Unacked</th>
              <th>Consumers</th>
              <th>Flags</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {items.map((q) => (
              <tr key={`${q.vhost}/${q.name}`}>
                <td>
                  <Link to={`/queues/${encodeURIComponent(q.vhost)}/${encodeURIComponent(q.name)}`}><code>{q.name}</code></Link>
                </td>
                <td>{q.state}</td>
                <td>{q.messages_ready}</td>
                <td>{q.messages_unacknowledged}</td>
                <td>{q.consumers}</td>
                <td className="muted">
                  {[q.durable && "D", q.exclusive && "E", q.auto_delete && "AD"]
                    .filter(Boolean)
                    .join(" ") || "—"}
                </td>
                <td className="row-actions">
                  <button
                    type="button"
                    className="btn-secondary"
                    onClick={() => {
                      if (!window.confirm(`Purge queue "${q.name}"?`)) return;
                      void purgeQueue(q.vhost, q.name)
                        .then(load)
                        .catch((e) => setError(String(e.message ?? e)));
                    }}
                  >
                    Purge
                  </button>
                  <button
                    type="button"
                    className="btn-danger"
                    onClick={() => {
                      if (!window.confirm(`Delete queue "${q.name}"?`)) return;
                      void deleteQueue(q.vhost, q.name)
                        .then(load)
                        .catch((e) => setError(String(e.message ?? e)));
                    }}
                  >
                    Delete
                  </button>
                </td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  No queues in this vhost.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </Layout>
  );
}

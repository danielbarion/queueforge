import { useCallback, useEffect, useState } from "react";
import {
  ApiError,
  getMessages,
  listExchanges,
  listQueues,
  listVhosts,
  publishMessage,
  type GetMessage,
  type Whoami,
} from "../api";
import Layout from "../components/Layout";

type Props = { user: Whoami; onLoggedOut: () => void };

export default function PublishGetPage({ user, onLoggedOut }: Props) {
  const [vhost, setVhost] = useState("/");
  const [vhosts, setVhosts] = useState<string[]>(["/"]);
  const [exchanges, setExchanges] = useState<string[]>([""]);
  const [queues, setQueues] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);

  const [exchange, setExchange] = useState("");
  const [routingKey, setRoutingKey] = useState("");
  const [payload, setPayload] = useState("hello");
  const [persistent, setPersistent] = useState(false);

  const [queue, setQueue] = useState("");
  const [gotten, setGotten] = useState<GetMessage[]>([]);
  const [ackmode, setAckmode] = useState("ack_requeue_true");

  const loadMeta = useCallback(async () => {
    setError(null);
    try {
      const vh = await listVhosts();
      setVhosts(vh.items.map((v) => v.name));
      const ex = await listExchanges(vhost);
      setExchanges(ex.items.map((e) => e.name));
      const qs = await listQueues(vhost);
      setQueues(qs.items.map((q) => q.name));
      if (!queue && qs.items[0]) setQueue(qs.items[0].name);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        onLoggedOut();
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load metadata");
    }
  }, [vhost, onLoggedOut, queue]);

  useEffect(() => {
    void loadMeta();
  }, [loadMeta]);

  async function onPublish(e: React.FormEvent) {
    e.preventDefault();
    setStatus(null);
    setError(null);
    try {
      const res = await publishMessage(vhost, exchange, {
        routing_key: routingKey,
        payload,
        properties: {
          delivery_mode: persistent ? 2 : 1,
        },
      });
      setStatus(res.routed ? "Message routed." : "Published but not routed to any queue.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Publish failed");
    }
  }

  async function onGet(e: React.FormEvent) {
    e.preventDefault();
    setStatus(null);
    setError(null);
    try {
      const msgs = await getMessages(vhost, queue, 1, ackmode);
      setGotten(msgs);
      setStatus(msgs.length ? `Fetched ${msgs.length} message(s).` : "Queue empty.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Get failed");
    }
  }

  return (
    <Layout
      user={user}
      onLoggedOut={onLoggedOut}
      title="Publish / Get"
      actions={
        <button type="button" className="btn-secondary" onClick={() => void loadMeta()}>
          Refresh
        </button>
      }
    >
      {error && <div className="error">{error}</div>}
      {status && <div className="success">{status}</div>}

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

      <div className="two-col">
        <form className="card form-col" onSubmit={(e) => void onPublish(e)}>
          <h2>Publish</h2>
          <label>
            Exchange
            <select value={exchange} onChange={(e) => setExchange(e.target.value)}>
              {exchanges.map((ex) => (
                <option key={ex || "default"} value={ex}>
                  {ex || "(AMQP default)"}
                </option>
              ))}
            </select>
          </label>
          <label>
            Routing key
            <input value={routingKey} onChange={(e) => setRoutingKey(e.target.value)} />
          </label>
          <label>
            Payload
            <textarea
              rows={4}
              value={payload}
              onChange={(e) => setPayload(e.target.value)}
            />
          </label>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={persistent}
              onChange={(e) => setPersistent(e.target.checked)}
            />
            persistent (delivery_mode=2)
          </label>
          <button type="submit" className="btn-primary">
            Publish
          </button>
        </form>

        <form className="card form-col" onSubmit={(e) => void onGet(e)}>
          <h2>Get</h2>
          <label>
            Queue
            <select value={queue} onChange={(e) => setQueue(e.target.value)}>
              {queues.map((q) => (
                <option key={q} value={q}>
                  {q}
                </option>
              ))}
            </select>
          </label>
          <label>
            Ack mode
            <select value={ackmode} onChange={(e) => setAckmode(e.target.value)}>
              <option value="ack_requeue_true">Peek (requeue)</option>
              <option value="ack_requeue_false">Consume (ack)</option>
            </select>
          </label>
          <button type="submit" className="btn-primary">
            Get message
          </button>

          {gotten.map((m, i) => (
            <div key={i} className="message-preview">
              <div className="muted">
                {m.exchange || "(default)"} → {m.routing_key} · {m.payload_bytes}B ·{" "}
                {m.payload_encoding}
                {m.redelivered ? " · redelivered" : ""}
              </div>
              <pre>{m.payload}</pre>
            </div>
          ))}
        </form>
      </div>
    </Layout>
  );
}

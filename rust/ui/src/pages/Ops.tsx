import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  ApiError,
  acknowledgeDeprecated,
  createBinding,
  createExchange,
  createQueue,
  createVhost,
  deleteBinding,
  deleteExchange,
  deleteLimit,
  deletePermission,
  deletePolicy,
  deleteQueue,
  deleteTopicPermission,
  deleteUser,
  deleteVhost,
  forceCloseConnection,
  getChannel,
  getConnection,
  getExchange,
  getMessages,
  getQueue,
  listChannels,
  listConsumers,
  listDeprecated,
  listFeatureFlags,
  listLimits,
  listNodes,
  listPermissions,
  listPolicies,
  listQueues,
  listTopicPermissions,
  listUsers,
  listVhosts,
  publishMessage,
  purgeQueue,
  putLimit,
  putPermission,
  putPolicy,
  putTopicPermission,
  setFeatureFlag,
  type ChannelItem,
  type PolicyItem,
  type QueueDetail,
  type Whoami,
} from "../api";
import Layout from "../components/Layout";
import Spark from "../components/Spark";
import { chartSeries, trafficRates, type ChartPoint, type TrafficSample } from "../rates";

type Props = { user: Whoami; onLoggedOut: () => void };

function fail(err: unknown, onLoggedOut: () => void): string | null {
  if (err instanceof ApiError && err.status === 401) {
    onLoggedOut();
    return null;
  }
  return err instanceof Error ? err.message : "Request failed";
}

function useTick(load: () => Promise<void>, ms = 5000) {
  useEffect(() => {
    void load();
    const id = window.setInterval(() => void load(), ms);
    return () => window.clearInterval(id);
  }, [load, ms]);
}

export function ChannelsPage({ user, onLoggedOut }: Props) {
  const [items, setItems] = useState<ChannelItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try {
      setItems((await listChannels()).items);
    } catch (err) {
      setError(fail(err, onLoggedOut));
    }
  }, [onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title="Channels">
      {error && <div className="error">{error}</div>}
      <div className="table-wrap card">
        <table>
          <thead><tr><th>Name</th><th>Connection</th><th>User</th><th>Vhost</th><th>Number</th><th>Peer</th></tr></thead>
          <tbody>
            {items.map((c) => (
              <tr key={c.name}>
                <td><Link to={`/channels/${encodeURIComponent(c.name)}`}><code>{c.name}</code></Link></td>
                <td><Link to={`/connections/${encodeURIComponent(c.connection_details.name)}`}>{c.connection_details.name}</Link></td>
                <td>{c.user}</td><td>{c.vhost}</td><td>{c.number}</td>
                <td>{c.peer_host}:{c.peer_port}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Layout>
  );
}

export function ChannelDetailPage({ user, onLoggedOut }: Props) {
  const { name = "" } = useParams();
  const channelName = decodeURIComponent(name);
  const [row, setRow] = useState<ChannelItem | null>(null);
  const [consumers, setConsumers] = useState<{ consumer_tag: string; queue: { name: string } }[]>([]);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try {
      const channel = await getChannel(channelName);
      setRow(channel);
      const page = await listConsumers(channel.vhost || "/");
      setConsumers(page.items.filter((c) => c.channel_details.name === channel.name));
    } catch (err) {
      setError(fail(err, onLoggedOut));
    }
  }, [channelName, onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title={channelName || "Channel"}>
      {error && <div className="error">{error}</div>}
      {row && (
        <section className="card meta">
          <div><span className="label">Connection</span><strong>{row.connection_details.name}</strong></div>
          <div><span className="label">User</span><strong>{row.user}</strong></div>
          <div><span className="label">Vhost</span><strong>{row.vhost}</strong></div>
          <div><span className="label">Peer</span><strong>{row.peer_host}:{row.peer_port}</strong></div>
        </section>
      )}
      <h2>Consumers</h2>
      <div className="table-wrap card">
        <table>
          <thead><tr><th>Tag</th><th>Queue</th></tr></thead>
          <tbody>{consumers.map((c) => <tr key={c.consumer_tag}><td><code>{c.consumer_tag}</code></td><td>{c.queue.name}</td></tr>)}</tbody>
        </table>
      </div>
    </Layout>
  );
}

export function ConnectionDetailPage({ user, onLoggedOut }: Props) {
  const { name = "" } = useParams();
  const id = decodeURIComponent(name);
  const [row, setRow] = useState<Awaited<ReturnType<typeof getConnection>> | null>(null);
  const [channels, setChannels] = useState<ChannelItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try {
      setRow(await getConnection(id));
      setChannels((await listChannels()).items.filter((c) => c.connection_details.name === id));
    } catch (err) {
      setError(fail(err, onLoggedOut));
    }
  }, [id, onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title={id || "Connection"} actions={
      <button type="button" className="btn-danger" onClick={() => void forceCloseConnection(id).then(load).catch((err) => setError(fail(err, onLoggedOut)))}>Force close</button>
    }>
      {error && <div className="error">{error}</div>}
      {row && <section className="card meta"><div><span className="label">User</span><strong>{row.user}</strong></div><div><span className="label">Vhost</span><strong>{row.vhost}</strong></div><div><span className="label">Peer</span><strong>{row.peer_host}:{row.peer_port}</strong></div><div><span className="label">Channels</span><strong>{row.channels}</strong></div></section>}
      <div className="table-wrap card"><table><thead><tr><th>Channel</th><th>Number</th></tr></thead><tbody>{channels.map((c) => <tr key={c.name}><td><Link to={`/channels/${encodeURIComponent(c.name)}`}><code>{c.name}</code></Link></td><td>{c.number}</td></tr>)}</tbody></table></div>
    </Layout>
  );
}

export function QueueDetailPage({ user, onLoggedOut }: Props) {
  const params = useParams();
  const vhost = decodeURIComponent(params.vhost ?? "/");
  const name = decodeURIComponent(params.name ?? "");
  const [queue, setQueue] = useState<QueueDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [rates, setRates] = useState({ publish: 0, deliver: 0, ack: 0, ready: 0, unacked: 0 });
  const [history, setHistory] = useState<ChartPoint[]>([]);
  const [payload, setPayload] = useState("hello");
  const [routingKey, setRoutingKey] = useState("");
  const [exchange, setExchange] = useState("");
  const [bindKey, setBindKey] = useState("");
  const [got, setGot] = useState("");
  const samples = useRef<{ at: number; sample: TrafficSample }[]>([]);
  const load = useCallback(async () => {
    try {
      const row = await getQueue(vhost, name);
      setQueue(row);
      const sample: TrafficSample = {
        publish: row.message_stats?.publish ?? 0,
        deliver: row.message_stats?.deliver ?? 0,
        ack: row.message_stats?.ack ?? 0,
        confirmed: 0,
        ready: row.messages_ready,
        unacked: row.messages_unacknowledged,
        consumers: row.consumers,
        confirmBeforeFsync: 0,
        fsync: 0,
      };
      const now = Date.now();
      const series = [...samples.current, { at: now, sample }].slice(-40);
      samples.current = series;
      const prior = series.length > 1 ? series[series.length - 2] : null;
      if (prior) {
        const live = trafficRates(prior.sample, sample, (now - prior.at) / 1000);
        setRates({ publish: live.publishPerSec, deliver: live.deliverPerSec, ack: live.ackPerSec, ready: live.ready, unacked: live.unacked });
      } else {
        setRates((current) => ({ ...current, ready: sample.ready, unacked: sample.unacked }));
      }
      setHistory(chartSeries(series));
    } catch (err) {
      setError(fail(err, onLoggedOut));
    }
  }, [vhost, name, onLoggedOut]);
  useTick(load, 1000);

  async function run(action: () => Promise<void>) {
    try {
      await action();
      await load();
    } catch (err) {
      setError(fail(err, onLoggedOut));
    }
  }

  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title={name || "Queue"} actions={
      <>
        <button type="button" className="btn-secondary" onClick={() => void run(() => purgeQueue(vhost, name).then(() => undefined))}>Purge</button>
        <button type="button" className="btn-danger" onClick={() => void run(() => deleteQueue(vhost, name))}>Delete</button>
      </>
    }>
      {error && <div className="error">{error}</div>}
      {queue && (
        <>
          <section className="card meta">
            <div><span className="label">Vhost</span><strong>{queue.vhost}</strong></div>
            <div><span className="label">State</span><strong>{queue.state}</strong></div>
            <div><span className="label">Policy</span><strong>{queue.policy || "—"}</strong></div>
            <div><span className="label">Operator policy</span><strong>{queue.operator_policy || "—"}</strong></div>
            <div><span className="label">Type</span><strong>{queue.type || "classic"}</strong></div>
          </section>
          <div className="card spark-card"><Spark points={history} /></div>
          <div className="stat-grid">
            <div className="stat card"><span className="label">Publish /s</span><span className="stat-value">{rates.publish.toFixed(1)}</span></div>
            <div className="stat card"><span className="label">Deliver /s</span><span className="stat-value">{rates.deliver.toFixed(1)}</span></div>
            <div className="stat card"><span className="label">Ack /s</span><span className="stat-value">{rates.ack.toFixed(1)}</span></div>
            <div className="stat card"><span className="label">Ready</span><span className="stat-value">{rates.ready}</span></div>
            <div className="stat card"><span className="label">Unacked</span><span className="stat-value">{rates.unacked}</span></div>
          </div>
          <form className="stack-form card" onSubmit={(e) => { e.preventDefault(); void run(async () => { await createBinding(vhost, { source: exchange, destination: name, routing_key: bindKey }); }); }}>
            <strong>Bind</strong>
            <div className="inline-form">
              <input placeholder="exchange" value={exchange} onChange={(e) => setExchange(e.target.value)} />
              <input placeholder="routing key" value={bindKey} onChange={(e) => setBindKey(e.target.value)} />
              <button type="submit">Bind</button>
            </div>
          </form>
          <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void run(() => deleteBinding(vhost, exchange, name, bindKey)); }}>
            <button type="submit" className="btn-secondary">Unbind same exchange and key</button>
          </form>
          <form className="stack-form card" onSubmit={(e) => { e.preventDefault(); void run(async () => { await publishMessage(vhost, exchange || "amq.direct", { routing_key: routingKey || name, payload }); }); }}>
            <strong>Publish</strong>
            <div className="inline-form">
              <input placeholder="exchange (blank = amq.direct)" value={exchange} onChange={(e) => setExchange(e.target.value)} />
              <input placeholder="routing key" value={routingKey} onChange={(e) => setRoutingKey(e.target.value)} />
              <input placeholder="payload" value={payload} onChange={(e) => setPayload(e.target.value)} />
              <button type="submit">Publish</button>
            </div>
          </form>
          <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void run(async () => { const rows = await getMessages(vhost, name, 1); setGot(rows[0]?.payload ?? "(empty)"); }); }}>
            <button type="submit" className="btn-secondary">Get</button>
            <code>{got}</code>
          </form>
          <h2>Consumers</h2>
          <div className="table-wrap card">
            <table>
              <thead><tr><th>Tag</th><th>Channel</th></tr></thead>
              <tbody>{(queue.consumer_details ?? []).map((c) => <tr key={c.consumer_tag}><td><code>{c.consumer_tag}</code></td><td>{c.channel_details.name}</td></tr>)}</tbody>
            </table>
          </div>
          <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void run(() => createQueue(vhost, name, { durable: true })); }}>
            <button type="submit" className="btn-secondary">Re-declare durable</button>
          </form>
        </>
      )}
    </Layout>
  );
}

export function ExchangeDetailPage({ user, onLoggedOut }: Props) {
  const params = useParams();
  const vhost = decodeURIComponent(params.vhost ?? "/");
  const name = decodeURIComponent(params.name ?? "");
  const [row, setRow] = useState<Awaited<ReturnType<typeof getExchange>> | null>(null);
  const [queues, setQueues] = useState<string[]>([]);
  const [destination, setDestination] = useState("");
  const [routingKey, setRoutingKey] = useState("");
  const [payload, setPayload] = useState("hello");
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try {
      setRow(await getExchange(vhost, name));
      setQueues((await listQueues(vhost)).items.map((q) => q.name));
    } catch (err) {
      setError(fail(err, onLoggedOut));
    }
  }, [vhost, name, onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title={name || "(AMQP default)"} actions={name ? <button type="button" className="btn-danger" onClick={() => void deleteExchange(vhost, name).catch((err) => setError(fail(err, onLoggedOut)))}>Delete</button> : undefined}>
      {error && <div className="error">{error}</div>}
      {row && <section className="card meta"><div><span className="label">Type</span><strong>{row.type}</strong></div><div><span className="label">Policy</span><strong>{row.policy || "—"}</strong></div><div><span className="label">Operator policy</span><strong>{row.operator_policy || "—"}</strong></div></section>}
      <form className="stack-form card" onSubmit={(e) => { e.preventDefault(); void createBinding(vhost, { source: name, destination, routing_key: routingKey }).then(load).catch((err) => setError(fail(err, onLoggedOut))); }}>
        <strong>Bind to queue</strong>
        <div className="inline-form">
          <select value={destination} onChange={(e) => setDestination(e.target.value)}><option value="">queue</option>{queues.map((q) => <option key={q}>{q}</option>)}</select>
          <input placeholder="routing key" value={routingKey} onChange={(e) => setRoutingKey(e.target.value)} />
          <button type="submit">Bind</button>
          <button type="button" className="btn-secondary" onClick={() => void deleteBinding(vhost, name, destination, routingKey).then(load)}>Unbind</button>
        </div>
      </form>
      <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void publishMessage(vhost, name, { routing_key: routingKey, payload }).catch((err) => setError(fail(err, onLoggedOut))); }}>
        <input value={payload} onChange={(e) => setPayload(e.target.value)} />
        <button type="submit">Publish</button>
      </form>
      <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void createExchange(vhost, name, { type: row?.type, durable: true }).then(load); }}>
        <button type="submit" className="btn-secondary">Re-declare</button>
      </form>
    </Layout>
  );
}

const POLICY_KEYS = ["message-ttl", "dead-letter-exchange", "dead-letter-routing-key", "max-length", "max-length-bytes", "expires", "overflow", "delivery-limit", "alternate-exchange"] as const;

export function PoliciesPage({ user, onLoggedOut }: Props) {
  const [items, setItems] = useState<PolicyItem[]>([]);
  const [operator, setOperator] = useState(false);
  const [vhost, setVhost] = useState("/");
  const [name, setName] = useState("");
  const [pattern, setPattern] = useState(".*");
  const [applyTo, setApplyTo] = useState("all");
  const [priority, setPriority] = useState("0");
  const [defs, setDefs] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try {
      const page = await listPolicies();
      const ops = await fetch("/api/operator-policies", { credentials: "same-origin" }).then((r) => r.json()) as { items: PolicyItem[] };
      setItems([...(operator ? [] : page.items), ...(operator ? ops.items : [])]);
      if (!operator) setItems(page.items);
    } catch (err) {
      setError(fail(err, onLoggedOut));
    }
  }, [onLoggedOut, operator]);
  useTick(load);
  async function show(op: boolean) {
    setOperator(op);
    try {
      if (op) {
        const res = await fetch("/api/operator-policies", { credentials: "same-origin" });
        setItems(((await res.json()) as { items: PolicyItem[] }).items);
      } else {
        setItems((await listPolicies()).items);
      }
    } catch (err) {
      setError(fail(err, onLoggedOut));
    }
  }
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title="Policies" actions={
      <>
        <button type="button" className={operator ? "btn-secondary" : "btn-primary"} onClick={() => void show(false)}>User</button>
        <button type="button" className={operator ? "btn-primary" : "btn-secondary"} onClick={() => void show(true)}>Operator</button>
      </>
    }>
      {error && <div className="error">{error}</div>}
      <form className="stack-form card" onSubmit={(e) => {
        e.preventDefault();
        const definition: Record<string, string | number> = {};
        for (const key of POLICY_KEYS) {
          const value = defs[key]?.trim();
          if (!value) continue;
          definition[key] = Number.isFinite(Number(value)) && !["dead-letter-exchange", "dead-letter-routing-key", "overflow", "alternate-exchange"].includes(key) ? Number(value) : value;
        }
        void putPolicy(vhost, name, { pattern, "apply-to": applyTo, priority: Number(priority) || 0, definition }, operator).then(() => show(operator)).catch((err) => setError(fail(err, onLoggedOut)));
      }}>
        <strong>{operator ? "Operator policy" : "Policy"}</strong>
        <div className="inline-form">
          <input placeholder="vhost" value={vhost} onChange={(e) => setVhost(e.target.value)} />
          <input placeholder="name" value={name} onChange={(e) => setName(e.target.value)} />
          <input placeholder="pattern" value={pattern} onChange={(e) => setPattern(e.target.value)} />
          <select value={applyTo} onChange={(e) => setApplyTo(e.target.value)}><option>all</option><option>queues</option><option>exchanges</option></select>
          <input placeholder="priority" value={priority} onChange={(e) => setPriority(e.target.value)} />
          <button type="submit">Save</button>
        </div>
        <div className="args-grid">
          {POLICY_KEYS.map((key) => (
            <label key={key}>{key}<input value={defs[key] ?? ""} onChange={(e) => setDefs({ ...defs, [key]: e.target.value })} /></label>
          ))}
        </div>
      </form>
      <div className="table-wrap card">
        <table>
          <thead><tr><th>Vhost</th><th>Name</th><th>Pattern</th><th>Apply</th><th>Priority</th><th>Definition</th><th /></tr></thead>
          <tbody>
            {items.map((p) => (
              <tr key={`${p.vhost}/${p.name}`}>
                <td>{p.vhost}</td><td><code>{p.name}</code></td><td>{p.pattern}</td><td>{p["apply-to"]}</td><td>{p.priority}</td>
                <td><code>{JSON.stringify(p.definition)}</code></td>
                <td><button type="button" className="btn-danger" onClick={() => void deletePolicy(p.vhost, p.name, operator).then(() => show(operator))}>Delete</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Layout>
  );
}

export function VhostsPage({ user, onLoggedOut }: Props) {
  const [items, setItems] = useState<string[]>([]);
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setItems((await listVhosts()).items.map((v) => v.name)); } catch (err) { setError(fail(err, onLoggedOut)); }
  }, [onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title="Virtual Hosts">
      {error && <div className="error">{error}</div>}
      <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void createVhost(name).then(load).catch((err) => setError(fail(err, onLoggedOut))); }}>
        <input placeholder="vhost" value={name} onChange={(e) => setName(e.target.value)} />
        <button type="submit">Create</button>
      </form>
      <div className="table-wrap card"><table><thead><tr><th>Name</th><th /></tr></thead><tbody>{items.map((v) => <tr key={v}><td><Link to={`/vhosts/${encodeURIComponent(v)}`}><code>{v}</code></Link></td><td>{v !== "/" && <button type="button" className="btn-danger" onClick={() => void deleteVhost(v).then(load)}>Delete</button>}</td></tr>)}</tbody></table></div>
    </Layout>
  );
}

export function VhostDetailPage({ user, onLoggedOut }: Props) {
  const { name = "" } = useParams();
  const vhost = decodeURIComponent(name);
  const [perms, setPerms] = useState<{ user: string; configure: string; write: string; read: string }[]>([]);
  const [userName, setUserName] = useState("admin");
  const [configure, setConfigure] = useState(".*");
  const [write, setWrite] = useState(".*");
  const [read, setRead] = useState(".*");
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setPerms((await listPermissions()).filter((p) => p.vhost === vhost)); } catch (err) { setError(fail(err, onLoggedOut)); }
  }, [vhost, onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title={vhost || "Vhost"} actions={vhost !== "/" ? <button type="button" className="btn-danger" onClick={() => void deleteVhost(vhost)}>Delete</button> : undefined}>
      {error && <div className="error">{error}</div>}
      <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void putPermission(userName, vhost, { configure, write, read }).then(load).catch((err) => setError(fail(err, onLoggedOut))); }}>
        <input value={userName} onChange={(e) => setUserName(e.target.value)} />
        <input value={configure} onChange={(e) => setConfigure(e.target.value)} />
        <input value={write} onChange={(e) => setWrite(e.target.value)} />
        <input value={read} onChange={(e) => setRead(e.target.value)} />
        <button type="submit">Set permissions</button>
      </form>
      <div className="table-wrap card"><table><thead><tr><th>User</th><th>Configure</th><th>Write</th><th>Read</th><th /></tr></thead><tbody>{perms.map((p) => <tr key={p.user}><td>{p.user}</td><td>{p.configure}</td><td>{p.write}</td><td>{p.read}</td><td><button type="button" className="btn-danger" onClick={() => void deletePermission(p.user, vhost).then(load)}>Clear</button></td></tr>)}</tbody></table></div>
    </Layout>
  );
}

export function UserDetailPage({ user, onLoggedOut }: Props) {
  const { name = "" } = useParams();
  const userName = decodeURIComponent(name);
  const [vhost, setVhost] = useState("/");
  const [exchange, setExchange] = useState("amq.topic");
  const [configure, setConfigure] = useState(".*");
  const [write, setWrite] = useState(".*");
  const [read, setRead] = useState(".*");
  const [topics, setTopics] = useState<{ vhost: string; exchange: string; write: string; read: string }[]>([]);
  const [tags, setTags] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try {
      const users = await listUsers();
      setTags(users.find((u) => u.name === userName)?.tags ?? []);
      setTopics((await listTopicPermissions()).items.filter((p) => p.user === userName));
    } catch (err) { setError(fail(err, onLoggedOut)); }
  }, [userName, onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title={userName} actions={<button type="button" className="btn-danger" onClick={() => void deleteUser(userName)}>Delete</button>}>
      {error && <div className="error">{error}</div>}
      <p className="muted">Tags {tags.join(", ") || "—"}</p>
      <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void putPermission(userName, vhost, { configure, write, read }).then(load).catch((err) => setError(fail(err, onLoggedOut))); }}>
        <strong>Permissions</strong>
        <input value={vhost} onChange={(e) => setVhost(e.target.value)} />
        <input value={configure} onChange={(e) => setConfigure(e.target.value)} />
        <input value={write} onChange={(e) => setWrite(e.target.value)} />
        <input value={read} onChange={(e) => setRead(e.target.value)} />
        <button type="submit">Set</button>
      </form>
      <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void putTopicPermission(userName, vhost, { exchange, write, read }).then(load).catch((err) => setError(fail(err, onLoggedOut))); }}>
        <strong>Topic permissions</strong>
        <input value={exchange} onChange={(e) => setExchange(e.target.value)} />
        <button type="submit">Set topic</button>
      </form>
      <div className="table-wrap card"><table><thead><tr><th>Vhost</th><th>Exchange</th><th>Write</th><th>Read</th><th /></tr></thead><tbody>{topics.map((t) => <tr key={`${t.vhost}/${t.exchange}`}><td>{t.vhost}</td><td>{t.exchange}</td><td>{t.write}</td><td>{t.read}</td><td><button type="button" className="btn-danger" onClick={() => void deleteTopicPermission(userName, t.vhost, t.exchange).then(load)}>Delete</button></td></tr>)}</tbody></table></div>
    </Layout>
  );
}

export function LimitsPage({ user, onLoggedOut }: Props) {
  const [data, setData] = useState<Awaited<ReturnType<typeof listLimits>> | null>(null);
  const [scope, setScope] = useState<"user" | "vhost">("user");
  const [name, setName] = useState("admin");
  const [kind, setKind] = useState("max-connections");
  const [value, setValue] = useState("10");
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setData(await listLimits()); } catch (err) { setError(fail(err, onLoggedOut)); }
  }, [onLoggedOut]);
  useTick(load);
  const kinds = scope === "user" ? ["max-connections", "max-channels"] : ["max-connections", "max-queues"];
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title="Limits">
      {error && <div className="error">{error}</div>}
      <form className="inline-form card" onSubmit={(e) => { e.preventDefault(); void putLimit(scope, name, kind, Number(value)).then(load).catch((err) => setError(fail(err, onLoggedOut))); }}>
        <select value={scope} onChange={(e) => { const next = e.target.value as "user" | "vhost"; setScope(next); setKind(next === "user" ? "max-connections" : "max-connections"); }}><option value="user">user</option><option value="vhost">vhost</option></select>
        <input value={name} onChange={(e) => setName(e.target.value)} />
        <select value={kind} onChange={(e) => setKind(e.target.value)}>{kinds.map((k) => <option key={k}>{k}</option>)}</select>
        <input value={value} onChange={(e) => setValue(e.target.value)} />
        <button type="submit">Set</button>
        <button type="button" className="btn-danger" onClick={() => void deleteLimit(scope, name, kind).then(load)}>Clear</button>
      </form>
      <h2>Users</h2>
      <div className="table-wrap card"><table><thead><tr><th>User</th><th>Max connections</th><th>Max channels</th></tr></thead><tbody>{(data?.user_limits ?? []).map((row) => <tr key={row.user}><td>{row.user}</td><td>{row["max-connections"] ?? "—"}</td><td>{row["max-channels"] ?? "—"}</td></tr>)}</tbody></table></div>
      <h2>Virtual hosts</h2>
      <div className="table-wrap card"><table><thead><tr><th>Vhost</th><th>Max connections</th><th>Max queues</th></tr></thead><tbody>{(data?.vhost_limits ?? []).map((row) => <tr key={row.vhost}><td>{row.vhost}</td><td>{row["max-connections"] ?? "—"}</td><td>{row["max-queues"] ?? "—"}</td></tr>)}</tbody></table></div>
    </Layout>
  );
}

export function FeatureFlagsPage({ user, onLoggedOut }: Props) {
  const [items, setItems] = useState<{ name: string; state: string; stability: string }[]>([]);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setItems((await listFeatureFlags()).items); } catch (err) { setError(fail(err, onLoggedOut)); }
  }, [onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title="Feature Flags">
      {error && <div className="error">{error}</div>}
      <div className="table-wrap card"><table><thead><tr><th>Name</th><th>State</th><th>Stability</th><th /></tr></thead><tbody>{items.map((f) => <tr key={f.name}><td><code>{f.name}</code></td><td><span className={f.state === "enabled" ? "pill on" : "pill"}>{f.state}</span></td><td>{f.stability}</td><td><button type="button" className="btn-secondary" onClick={() => void setFeatureFlag(f.name, f.state !== "enabled").then(load).catch((err) => setError(fail(err, onLoggedOut)))}>{f.state === "enabled" ? "Disable" : "Enable"}</button></td></tr>)}</tbody></table></div>
    </Layout>
  );
}

export function DeprecatedPage({ user, onLoggedOut }: Props) {
  const [items, setItems] = useState<{ name: string; deprecation_phase: string; acknowledged: boolean; description: string }[]>([]);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setItems((await listDeprecated()).items); } catch (err) { setError(fail(err, onLoggedOut)); }
  }, [onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title="Deprecated Features">
      {error && <div className="error">{error}</div>}
      <div className="table-wrap card"><table><thead><tr><th>Name</th><th>Phase</th><th>Acknowledged</th><th>Description</th><th /></tr></thead><tbody>{items.map((f) => <tr key={f.name}><td><code>{f.name}</code></td><td>{f.deprecation_phase}</td><td>{f.acknowledged ? "yes" : "no"}</td><td>{f.description}</td><td>{!f.acknowledged && <button type="button" onClick={() => void acknowledgeDeprecated(f.name).then(load).catch((err) => setError(fail(err, onLoggedOut)))}>Acknowledge</button>}</td></tr>)}</tbody></table></div>
    </Layout>
  );
}

export function ClusterPage({ user, onLoggedOut }: Props) {
  const [nodes, setNodes] = useState<Awaited<ReturnType<typeof listNodes>>["items"]>([]);
  const [error, setError] = useState<string | null>(null);
  const load = useCallback(async () => {
    try { setNodes((await listNodes()).items); } catch (err) { setError(fail(err, onLoggedOut)); }
  }, [onLoggedOut]);
  useTick(load);
  return (
    <Layout user={user} onLoggedOut={onLoggedOut} title="Cluster">
      {error && <div className="error">{error}</div>}
      {nodes.map((node) => (
        <section key={node.name} className="card" style={{ marginBottom: 16 }}>
          <h2 style={{ marginTop: 0 }}>{node.name}</h2>
          <div className="stat-grid">
            <div className="stat"><span className="label">Running</span><span className="stat-value">{node.running ? "yes" : "no"}</span></div>
            <div className="stat"><span className="label">Uptime</span><span className="stat-value">{Math.floor(node.uptime / 1000)}s</span></div>
            <div className="stat"><span className="label">Memory</span><span className="stat-value">{Math.round(node.mem_used / 1024 / 1024)} MB</span></div>
            <div className="stat"><span className="label">Disk free</span><span className="stat-value">{Math.round(node.disk_free / 1024 / 1024)} MB</span></div>
            <div className="stat"><span className="label">Memory alarm</span><span className={node.mem_alarm ? "pill warn" : "pill"}>{node.mem_alarm ? "alarm" : "clear"}</span></div>
            <div className="stat"><span className="label">Disk alarm</span><span className={node.disk_free_alarm ? "pill warn" : "pill"}>{node.disk_free_alarm ? "alarm" : "clear"}</span></div>
          </div>
          <p>Listeners {node.listeners.map((l) => `${l.protocol} ${l.ip_address}:${l.port}`).join(", ") || "—"}</p>
          <p>Peers {node.peers.map((p) => p.name).join(", ") || "this node only"}</p>
        </section>
      ))}
    </Layout>
  );
}

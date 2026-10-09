export type Member = { id: string; addr: string };

export type Config = {
  amqp: string;
  management: string;
  metrics: string;
  mqtt: string | null;
  stomp: string | null;
  stream: string | null;
  /** TLS listeners for the other protocols, as RabbitMQ's `*.listeners.ssl` (8883, 61614, 5551). */
  mqtts?: string | null;
  stomps?: string | null;
  streamTls?: string | null;
  /** A TLS AMQP listener next to the plain one; uses the [tls] certificate. */
  amqps?: string | null;
  dataDir: string;
  fsync: "never" | "every_n_ms" | "always" | "every_n_messages";
  fsyncIntervalMs: number;
  fsyncEveryN: number;
  tls: boolean;
  /** PEM certificate and key paths. Required when `tls` is on, as in the Rust broker. */
  tlsCert?: string;
  tlsKey?: string;
  /** CA that client certificates must chain to. Set, it enables SASL EXTERNAL. */
  tlsCa?: string;
  oauth?: { resourceServerId: string; jwksUrl: string; jwksCaPath: string | null } | null;
  ldap?: { server: string; port: number; userDnPattern: string; adminGroup: string | null; bindDn: string | null; bindPassword: string | null } | null;
  nodeId: string;
  clusterListen: string | null;
  members: Member[];
  /** `"dns"` builds `members` from the addresses of `dnsName` (docs/raft.md, section 10). */
  discovery?: string | null;
  dnsName?: string | null;
  dnsPort?: number | null;
  reusePort?: boolean;
  defaultQueueType: "classic" | "quorum";
};

function unquote(s: string): string {
  const t = s.trim();
  if ((t.startsWith('"') && t.endsWith('"')) || (t.startsWith("'") && t.endsWith("'"))) {
    return t.slice(1, -1);
  }
  return t;
}

/** Subset TOML reader for the QueueForge example config. */
export function parseConfig(text: string): Config {
  const cfg: Config = {
    amqp: "0.0.0.0:5672",
    management: "0.0.0.0:15672",
    metrics: "127.0.0.1:15692",
    mqtt: null,
    stomp: null,
    stream: null,
    mqtts: null,
    stomps: null,
    streamTls: null,
    amqps: null,
    dataDir: "./data",
    fsync: "every_n_ms",
    fsyncIntervalMs: 100,
    fsyncEveryN: 1,
    tls: false,
    oauth: null,
    ldap: null,
    nodeId: "",
    clusterListen: null,
    members: [],
    discovery: null,
    dnsName: null,
    dnsPort: null,
    reusePort: false,
    defaultQueueType: "classic",
  };
  let section = "";
  const lines = text.split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i] ?? "";
    const line = raw.replace(/#.*$/, "").trim();
    if (!line) continue;
    const sec = line.match(/^\[([^\]]+)\]$/);
    if (sec) {
      section = sec[1] ?? "";
      continue;
    }
    if (section === "cluster" && line.startsWith("members")) {
      const buf = [line];
      while (!buf.join(" ").includes("]") && i + 1 < lines.length) {
        i++;
        buf.push((lines[i] ?? "").replace(/#.*$/, ""));
      }
      const body = buf.join(" ");
      const re = /\{\s*id\s*=\s*"([^"]+)"\s*,\s*addr\s*=\s*"([^"]+)"\s*\}/g;
      let m: RegExpExecArray | null;
      while ((m = re.exec(body))) {
        cfg.members.push({ id: m[1] ?? "", addr: m[2] ?? "" });
      }
      continue;
    }
    const kv = line.match(/^([A-Za-z0-9_]+)\s*=\s*(.+)$/);
    if (!kv) continue;
    const key = kv[1] ?? "";
    const val = unquote(kv[2] ?? "");
    if (section === "listeners" && key === "amqp") cfg.amqp = val;
    if (section === "listeners" && key === "management") cfg.management = val;
    if (section === "listeners" && key === "metrics") cfg.metrics = val;
    if (section === "listeners" && key === "mqtt") cfg.mqtt = val;
    if (section === "listeners" && key === "stomp") cfg.stomp = val;
    if (section === "listeners" && key === "stream") cfg.stream = val;
    if (section === "listeners" && key === "mqtts") cfg.mqtts = val;
    if (section === "listeners" && key === "stomps") cfg.stomps = val;
    if (section === "listeners" && key === "stream_tls") cfg.streamTls = val;
    if (section === "listeners" && key === "amqps") cfg.amqps = val;
    if (section === "tls" && key === "ca_path") cfg.tlsCa = val;
    if (section === "auth.oauth2") {
      cfg.oauth ??= { resourceServerId: "rabbitmq", jwksUrl: "", jwksCaPath: null };
      if (key === "resource_server_id") cfg.oauth.resourceServerId = val;
      if (key === "jwks_url") cfg.oauth.jwksUrl = val;
      if (key === "jwks_ca_path") cfg.oauth.jwksCaPath = val;
    }
    if (section === "auth.ldap") {
      cfg.ldap ??= { server: "127.0.0.1", port: 389, userDnPattern: "", adminGroup: null, bindDn: null, bindPassword: null };
      if (key === "server") cfg.ldap.server = val;
      if (key === "port") cfg.ldap.port = Number(val) || 389;
      if (key === "user_dn_pattern") cfg.ldap.userDnPattern = val;
      if (key === "admin_group") cfg.ldap.adminGroup = val;
      if (key === "bind_dn") cfg.ldap.bindDn = val;
      if (key === "bind_password") cfg.ldap.bindPassword = val;
    }
    if (section === "data" && key === "dir") cfg.dataDir = val;
    if (section === "data" && key === "fsync_policy") {
      if (val === "never" || val === "always" || val === "every_n_ms" || val === "every_n_messages") cfg.fsync = val;
    }
    if (section === "data" && key === "fsync_interval_ms") cfg.fsyncIntervalMs = Number(val) || 100;
    if (section === "data" && key === "fsync_every_n_messages") cfg.fsyncEveryN = Number(val) || 1;
    if (section === "tls" && key === "enabled") cfg.tls = val === "true";
    if (section === "tls" && key === "cert_path") cfg.tlsCert = val;
    if (section === "tls" && key === "key_path") cfg.tlsKey = val;
    if (section === "cluster" && key === "node_id") cfg.nodeId = val;
    if (section === "cluster" && key === "discovery") cfg.discovery = val;
    if (section === "cluster" && key === "dns_name") cfg.dnsName = val;
    if (section === "cluster" && key === "dns_port") cfg.dnsPort = Number(val) || null;
    if (section === "cluster" && key === "listen") cfg.clusterListen = val;
    if (section === "limits" && key === "default_queue_type" && (val === "classic" || val === "quorum")) {
      cfg.defaultQueueType = val;
    }
  }
  if (process.env.QUEUEFORGE_DATA_DIR) cfg.dataDir = process.env.QUEUEFORGE_DATA_DIR;
  if (process.env.QUEUEFORGE_AMQP_ADDR) cfg.amqp = process.env.QUEUEFORGE_AMQP_ADDR;
  if (process.env.QUEUEFORGE_MGMT_ADDR) cfg.management = process.env.QUEUEFORGE_MGMT_ADDR;
  if (process.env.QUEUEFORGE_METRICS_ADDR) cfg.metrics = process.env.QUEUEFORGE_METRICS_ADDR;
  if (process.env.QUEUEFORGE_NODE_ID) cfg.nodeId = process.env.QUEUEFORGE_NODE_ID;
  if (process.env.QUEUEFORGE_CLUSTER_LISTEN) cfg.clusterListen = process.env.QUEUEFORGE_CLUSTER_LISTEN;
  if (process.env.QUEUEFORGE_REUSE_PORT === "1") cfg.reusePort = true;
  if (process.env.QUEUEFORGE_TLS_ENABLED) cfg.tls = process.env.QUEUEFORGE_TLS_ENABLED === "true" || process.env.QUEUEFORGE_TLS_ENABLED === "1";
  if (process.env.QUEUEFORGE_TLS_CERT) cfg.tlsCert = process.env.QUEUEFORGE_TLS_CERT;
  if (process.env.QUEUEFORGE_TLS_KEY) cfg.tlsKey = process.env.QUEUEFORGE_TLS_KEY;
  if (process.env.QUEUEFORGE_MEMBERS) {
    const parsed = JSON.parse(process.env.QUEUEFORGE_MEMBERS) as Member[];
    if (Array.isArray(parsed)) cfg.members = parsed;
  }
  return cfg;
}

export function splitHost(addr: string): { host: string; port: number } {
  const i = addr.lastIndexOf(":");
  return { host: addr.slice(0, i), port: Number(addr.slice(i + 1)) };
}

/**
 * Members from the addresses `name` resolves to, each `<ip>:<port>`, plus
 * this node under its `listen` address. A failed lookup gives just this node.
 */
export async function dnsMembers(name: string, port: number, self: Member): Promise<Member[]> {
  const { lookup } = await import("node:dns/promises");
  let found: Member[] = [];
  try {
    const rows = await lookup(name, { all: true });
    found = rows.map((r) => {
      const addr = r.family === 6 ? `[${r.address}]:${port}` : `${r.address}:${port}`;
      return { id: addr, addr };
    });
  } catch {
    found = [];
  }
  if (!found.some((m) => m.id === self.id)) found.push(self);
  const seen = new Set<string>();
  return found.filter((m) => !seen.has(m.id) && seen.add(m.id)).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

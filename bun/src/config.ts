export type Member = { id: string; addr: string };

export type Config = {
  amqp: string;
  management: string;
  metrics: string;
  mqtt: string | null;
  stomp: string | null;
  stream: string | null;
  dataDir: string;
  fsync: "never" | "every_n_ms" | "always" | "every_n_messages";
  fsyncIntervalMs: number;
  fsyncEveryN: number;
  tls: boolean;
  nodeId: string;
  clusterListen: string | null;
  members: Member[];
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
    dataDir: "./data",
    fsync: "every_n_ms",
    fsyncIntervalMs: 100,
    fsyncEveryN: 1,
    tls: false,
    nodeId: "",
    clusterListen: null,
    members: [],
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
    if (section === "data" && key === "dir") cfg.dataDir = val;
    if (section === "data" && key === "fsync_policy") {
      if (val === "never" || val === "always" || val === "every_n_ms" || val === "every_n_messages") cfg.fsync = val;
    }
    if (section === "data" && key === "fsync_interval_ms") cfg.fsyncIntervalMs = Number(val) || 100;
    if (section === "data" && key === "fsync_every_n_messages") cfg.fsyncEveryN = Number(val) || 1;
    if (section === "tls" && key === "enabled") cfg.tls = val === "true";
    if (section === "cluster" && key === "node_id") cfg.nodeId = val;
    if (section === "cluster" && key === "listen") cfg.clusterListen = val;
    if (section === "limits" && key === "default_queue_type" && (val === "classic" || val === "quorum")) {
      cfg.defaultQueueType = val;
    }
  }
  if (process.env.QUEUEFORGE_DATA_DIR) cfg.dataDir = process.env.QUEUEFORGE_DATA_DIR;
  if (process.env.QUEUEFORGE_AMQP_ADDR) cfg.amqp = process.env.QUEUEFORGE_AMQP_ADDR;
  if (process.env.QUEUEFORGE_MGMT_ADDR) cfg.management = process.env.QUEUEFORGE_MGMT_ADDR;
  if (process.env.QUEUEFORGE_METRICS_ADDR) cfg.metrics = process.env.QUEUEFORGE_METRICS_ADDR;
  return cfg;
}

export function splitHost(addr: string): { host: string; port: number } {
  const i = addr.lastIndexOf(":");
  return { host: addr.slice(0, i), port: Number(addr.slice(i + 1)) };
}

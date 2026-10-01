import { createHash, timingSafeEqual } from "node:crypto";
import { fieldEq, fieldStr, replaceHeaderTable, writeTable, W, type Field } from "./codec.ts";
import type { Config } from "./config.ts";
import { ChanError } from "./errors.ts";
import { Store, type BindRow, type ExRow, type QueueRow } from "./store.ts";
import { encodeQuorumAppend } from "./wire.ts";

/** RabbitMQ `password_hash`: base64(salt[4] || SHA-256 or SHA-512 of salt || password). */
function rabbitPasswordHashMatches(password: string, encoded: string): boolean {
  const raw = Buffer.from(encoded, "base64");
  if (raw.length !== 36 && raw.length !== 68) return false;
  const salt = raw.subarray(0, 4);
  const digest = raw.subarray(4);
  const algo = digest.length === 32 ? "sha256" : "sha512";
  const computed = createHash(algo).update(salt).update(password, "utf8").digest();
  return timingSafeEqual(computed, digest);
}

export { ChanError };

type FedLink = { upstream: string; downstream: string; pattern: string };
const fedLinks: FedLink[] = [];
const fedUpstreams: { downstream: string; upstream: string }[] = [];

export function addFederationUpstream(downstream: string, upstream: string) {
  fedUpstreams.push({ downstream, upstream });
}

export function addFederationPolicy(downstream: string, pattern: string) {
  for (const up of fedUpstreams) {
    if (up.downstream === downstream) fedLinks.push({ upstream: up.upstream, downstream, pattern });
  }
}

export type LiveMsg = {
  id: string;
  rowId: number | null;
  body: Uint8Array;
  exchange: string;
  routingKey: string;
  headers: Array<[string, Field]>;
  propRaw: Uint8Array;
  persistent: boolean;
  priority: number;
  expiresAt: number | null;
  redelivered: boolean;
  deliveries?: number;
};

export type Consumer = {
  tag: string;
  session: number;
  noAck: boolean;
  exclusive: boolean;
  priority?: number;
  want: () => boolean;
  deliver: (msg: LiveMsg) => void;
  onCancel?: () => void;
};

type QArgs = {
  messageTtl: number | null;
  expiresMs: number | null;
  maxLength: number | null;
  maxLengthBytes: number | null;
  overflow: "drop-head" | "reject-publish" | "reject-publish-dlx";
  dlxStrategy: "at-most-once" | "at-least-once";
  dlx: string | null;
  dlxKey: string | null;
  maxPriority: number | null;
  singleActive: boolean;
  deliveryLimit: number | null;
  queueType: "classic" | "quorum";
};

export type QueueLive = QueueRow & {
  argsParsed: QArgs;
  declaredArgs: Record<string, string | number>;
  ready: LiveMsg[];
  replicas: LiveMsg[];
  unacked: Map<string, LiveMsg>;
  consumers: Consumer[];
  rr: number;
  lastUsed: number;
};

export type Policy = {
  vhost: string;
  name: string;
  pattern: string;
  applyTo: "queues" | "exchanges" | "all";
  priority: number;
  messageTtl: number | null;
  expiresMs: number | null;
  dlx: string | null;
  dlxKey: string | null;
  maxLength: number | null;
  maxLengthBytes: number | null;
  overflow: "drop-head" | "reject-publish" | "reject-publish-dlx" | null;
  dlxStrategy: "at-most-once" | "at-least-once" | null;
  deliveryLimit: number | null;
  alternate: string | null;
};

const BUILTIN: Array<[string, string, boolean]> = [
  ["", "default", true],
  ["amq.direct", "direct", false],
  ["amq.fanout", "fanout", false],
  ["amq.topic", "topic", false],
];

export function topicMatches(pattern: string, key: string): boolean {
  const pat = pattern === "" ? [] : pattern.split(".");
  const words = key === "" ? [] : key.split(".");
  const rec = (p: string[], k: string[]): boolean => {
    if (p.length === 0) return k.length === 0;
    if (p[0] === "#") {
      if (p.length === 1) return true;
      for (let i = 0; i <= k.length; i++) if (rec(p.slice(1), k.slice(i))) return true;
      return false;
    }
    if (k.length === 0) return false;
    if (p[0] === "*" || p[0] === k[0]) return rec(p.slice(1), k.slice(1));
    return false;
  };
  return rec(pat, words);
}

export function headersMatch(args: Array<[string, Field]>, headers: Array<[string, Field]>): boolean {
  const any = args.some(([k, v]) => k === "x-match" && fieldStr(v) === "any");
  const checks = args.filter(([k]) => k !== "x-match");
  if (checks.length === 0) return !any;
  const hit = (k: string, v: Field) => headers.some(([hk, hv]) => hk === k && fieldEq(hv, v));
  return any ? checks.some(([k, v]) => hit(k, v)) : checks.every(([k, v]) => hit(k, v));
}

function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function headerList(headers: Array<[string, Field]>, name: string): string[] {
  const field = headers.find(([key]) => key === name)?.[1];
  if (!field) return [];
  if (field.t === "A") return field.v.flatMap((item) => (item.t === "S" || item.t === "s" ? [item.v] : []));
  if (field.t === "S" || field.t === "s") return [field.v];
  return [];
}

function overflowOf(value: string | null): "drop-head" | "reject-publish" | "reject-publish-dlx" | null {
  if (value === "drop-head" || value === "reject-publish" || value === "reject-publish-dlx") return value;
  return null;
}

function liveFrom(q: QueueLive, src: { body: Uint8Array; exchange: string; routingKey: string; headers: Array<[string, Field]>; propRaw: Uint8Array; persistent: boolean; priority: number; redelivered?: boolean; id?: string }): LiveMsg {
  return {
    id: src.id ?? `rej-${q.name}`,
    rowId: null,
    body: src.body,
    exchange: src.exchange,
    routingKey: src.routingKey,
    headers: src.headers,
    propRaw: src.propRaw,
    persistent: src.persistent,
    priority: src.priority,
    expiresAt: null,
    redelivered: !!src.redelivered,
  };
}

function pickConsumer(q: QueueLive): Consumer | null {
  const ready = q.consumers.filter((c) => c.want());
  if (!ready.length) return null;
  if (q.argsParsed.singleActive) {
    return ready.reduce((best, c) => ((c.priority ?? 0) > (best.priority ?? 0) ? c : best));
  }
  const bestPri = Math.max(...ready.map((c) => c.priority ?? 0));
  const n = q.consumers.length;
  for (let i = 0; i < n; i++) {
    const c = q.consumers[(q.rr + i) % n]!;
    if (c.want() && (c.priority ?? 0) === bestPri) {
      q.rr = (q.rr + i + 1) % n;
      return c;
    }
  }
  return null;
}

export function queueHome(vhost: string, name: string, members: { id: string }[]): string | null {
  if (members.length === 0) return null;
  const sorted = [...members].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  return sorted[fnv1a(`${vhost}\0${name}`) % sorted.length]!.id;
}

function parseArgs(raw: Record<string, string | number>): QArgs {
  const num = (k: string) => (raw[k] == null ? null : Number(raw[k]));
  const str = (k: string) => (raw[k] == null ? null : String(raw[k]));
  const rawOverflow = str("x-overflow");
  const overflow = rawOverflow === "reject-publish" || rawOverflow === "reject-publish-dlx" ? rawOverflow : "drop-head";
  const rawStrategy = str("x-dead-letter-strategy");
  const dlxStrategy = rawStrategy === "at-least-once" ? "at-least-once" : "at-most-once";
  const maxPriority = num("x-max-priority");
  const qtype = str("x-queue-type");
  return {
    messageTtl: num("x-message-ttl"),
    expiresMs: num("x-expires"),
    maxLength: num("x-max-length"),
    maxLengthBytes: num("x-max-length-bytes"),
    overflow,
    dlxStrategy,
    dlx: str("x-dead-letter-exchange"),
    dlxKey: str("x-dead-letter-routing-key"),
    maxPriority: maxPriority && maxPriority > 0 ? maxPriority : null,
    singleActive: str("x-single-active-consumer") === "true" || raw["x-single-active-consumer"] === 1,
    deliveryLimit: num("x-delivery-limit"),
    queueType: qtype === "quorum" ? "quorum" : "classic",
  };
}

function deathHeaders(
  queue: string,
  reason: string,
  exchange: string,
  routingKey: string,
  prev: Array<[string, Field]>,
): Array<[string, Field]> {
  const entry: Array<[string, Field]> = [
    ["queue", { t: "S", v: queue }],
    ["reason", { t: "S", v: reason }],
    ["count", { t: "l", v: 1 }],
    ["exchange", { t: "S", v: exchange }],
    ["routing-keys", { t: "A", v: [{ t: "S", v: routingKey }] }],
  ];
  const rest = prev.filter(([k]) => k !== "x-death" && k !== "x-first-death-reason" && k !== "x-first-death-queue");
  return [
    ...rest,
    ["x-death", { t: "A", v: [{ t: "F", v: entry }] }],
    ["x-first-death-reason", { t: "S", v: reason }],
    ["x-first-death-queue", { t: "S", v: queue }],
  ];
}

export function propsWithDeath(headers: Array<[string, Field]>): Uint8Array {
  const w = new W();
  w.u16(0x2000);
  writeTable(w, headers);
  return w.concat();
}

export function argsFromFields(fields: Array<[string, Field]>): Record<string, string | number> {
  const out: Record<string, string | number> = {};
  for (const [k, v] of fields) {
    if (v.t === "I" || v.t === "l") out[k] = v.v;
    else if (v.t === "t") out[k] = v.v ? 1 : 0;
    else if (v.t === "S" || v.t === "s") out[k] = v.v;
  }
  return out;
}

let transientSeq = 1;

export type Prom = {
  connections: number;
  connectionsOpened: number;
  connectionsClosed: number;
  channels: number;
  channelsOpened: number;
  channelsClosed: number;
  queuesDeclared: number;
  queuesCreated: number;
  queuesDeleted: number;
  consumers: number;
  received: number;
  receivedConfirm: number;
  confirmed: number;
  routed: number;
  unroutableDropped: number;
  unroutableReturned: number;
  delivered: number;
  deliveredConsumeManual: number;
  deliveredConsumeAuto: number;
  deliveredGetManual: number;
  deliveredGetAuto: number;
  getEmpty: number;
  acknowledged: number;
  redelivered: number;
  dlxExpired: number;
  dlxRejected: number;
  dlxMaxlen: number;
  dlxDeliveryLimit: number;
};

function emptyProm(): Prom {
  return {
    connections: 0,
    connectionsOpened: 0,
    connectionsClosed: 0,
    channels: 0,
    channelsOpened: 0,
    channelsClosed: 0,
    queuesDeclared: 0,
    queuesCreated: 0,
    queuesDeleted: 0,
    consumers: 0,
    received: 0,
    receivedConfirm: 0,
    confirmed: 0,
    routed: 0,
    unroutableDropped: 0,
    unroutableReturned: 0,
    delivered: 0,
    deliveredConsumeManual: 0,
    deliveredConsumeAuto: 0,
    deliveredGetManual: 0,
    deliveredGetAuto: 0,
    getEmpty: 0,
    acknowledged: 0,
    redelivered: 0,
    dlxExpired: 0,
    dlxRejected: 0,
    dlxMaxlen: 0,
    dlxDeliveryLimit: 0,
  };
}

export type MgmtConnection = {
  name: string;
  user: string;
  vhost: string;
  peer_host: string;
  peer_port: number;
  channels: number;
  connected_at: number;
  close: () => void;
};

export type MgmtChannel = {
  name: string;
  connection: string;
  user: string;
  vhost: string;
  number: number;
  peer_host: string;
  peer_port: number;
};

export type MgmtConsumer = {
  consumer_tag: string;
  connection: string;
  channel: number;
  queue: string;
  vhost: string;
};

export type TopicPerm = {
  user: string;
  vhost: string;
  exchange: string;
  write: string;
  read: string;
};

function matchOne(rows: Policy[], vhost: string, name: string, entity: "queues" | "exchanges"): Policy | null {
  let best: Policy | null = null;
  for (const p of rows) {
    if (p.vhost !== vhost) continue;
    if (p.applyTo !== "all" && p.applyTo !== entity) continue;
    let ok = false;
    try {
      ok = new RegExp(p.pattern).test(name);
    } catch {
      ok = false;
    }
    if (!ok) continue;
    if (!best || p.priority > best.priority || (p.priority === best.priority && p.name < best.name)) best = p;
  }
  return best;
}

export function policyItem(p: Policy) {
  return {
    vhost: p.vhost,
    name: p.name,
    pattern: p.pattern,
    "apply-to": p.applyTo,
    priority: p.priority,
    definition: {
      ...(p.messageTtl != null ? { "message-ttl": p.messageTtl } : {}),
      ...(p.dlx ? { "dead-letter-exchange": p.dlx } : {}),
      ...(p.dlxKey ? { "dead-letter-routing-key": p.dlxKey } : {}),
      ...(p.maxLength != null ? { "max-length": p.maxLength } : {}),
      ...(p.maxLengthBytes != null ? { "max-length-bytes": p.maxLengthBytes } : {}),
      ...(p.expiresMs != null ? { expires: p.expiresMs } : {}),
      ...(p.overflow ? { overflow: p.overflow } : {}),
      ...(p.dlxStrategy ? { "dead-letter-strategy": p.dlxStrategy } : {}),
      ...(p.deliveryLimit != null ? { "delivery-limit": p.deliveryLimit } : {}),
      ...(p.alternate ? { "alternate-exchange": p.alternate } : {}),
    },
  };
}

export function policyFromBody(vhost: string, name: string, body: {
  pattern?: string;
  "apply-to"?: string;
  priority?: number;
  definition?: Record<string, string | number>;
}): Policy {
  const apply = body["apply-to"] ?? "all";
  if (apply !== "queues" && apply !== "exchanges" && apply !== "all") {
    throw new Error("apply-to must be queues, exchanges, or all");
  }
  const def = body.definition ?? {};
  const num = (k: string) => {
    const v = def[k];
    if (v == null || v === "") return null;
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? n : null;
  };
  const str = (k: string) => {
    const v = def[k];
    return v == null || v === "" ? null : String(v);
  };
  if (!body.pattern) throw new Error("pattern is required");
  const known = new Set([
    "message-ttl", "dead-letter-exchange", "dead-letter-routing-key", "max-length", "max-length-bytes",
    "expires", "overflow", "delivery-limit", "alternate-exchange", "dead-letter-strategy", "federation-upstream-set",
  ]);
  const unknown = Object.keys(def).filter((k) => !known.has(k));
  if (unknown.length) throw new Error(`${JSON.stringify(unknown)} are not recognised policy settings`);
  return {
    vhost,
    name,
    pattern: body.pattern,
    applyTo: apply,
    priority: body.priority ?? 0,
    messageTtl: num("message-ttl"),
    expiresMs: num("expires"),
    dlx: str("dead-letter-exchange"),
    dlxKey: str("dead-letter-routing-key"),
    maxLength: num("max-length"),
    maxLengthBytes: num("max-length-bytes"),
    overflow: (["drop-head", "reject-publish", "reject-publish-dlx"] as const).find((v) => v === str("overflow")) ?? null,
    dlxStrategy: (["at-most-once", "at-least-once"] as const).find((v) => v === str("dead-letter-strategy")) ?? null,
    deliveryLimit: num("delivery-limit"),
    alternate: str("alternate-exchange"),
  };
}

function fillPolicyArgs(
  declared: Record<string, string | number>,
  pol: Policy | null,
  base?: Record<string, string | number>,
): Record<string, string | number> {
  const out = { ...(base ?? declared) };
  if (!pol) return out;
  const empty = (key: string) => declared[key] == null || declared[key] === "";
  if (empty("x-message-ttl") && pol.messageTtl != null) out["x-message-ttl"] = pol.messageTtl;
  if (empty("x-dead-letter-exchange") && pol.dlx) out["x-dead-letter-exchange"] = pol.dlx;
  if (empty("x-dead-letter-routing-key") && pol.dlxKey) out["x-dead-letter-routing-key"] = pol.dlxKey;
  if (empty("x-max-length") && pol.maxLength != null) out["x-max-length"] = pol.maxLength;
  if (empty("x-max-length-bytes") && pol.maxLengthBytes != null) out["x-max-length-bytes"] = pol.maxLengthBytes;
  if (empty("x-expires") && pol.expiresMs != null) out["x-expires"] = pol.expiresMs;
  if (empty("x-overflow") && pol.overflow) out["x-overflow"] = pol.overflow;
  if (empty("x-dead-letter-strategy") && pol.dlxStrategy) out["x-dead-letter-strategy"] = pol.dlxStrategy;
  if (empty("x-delivery-limit") && pol.deliveryLimit != null) out["x-delivery-limit"] = pol.deliveryLimit;
  return out;
}

export class Broker {
  ready = false;
  prom = emptyProm();
  exchanges = new Map<string, ExRow>();
  queues = new Map<string, QueueLive>();
  bindings: BindRow[] = [];
  policies: Policy[] = [];
  e2e: Array<{ vhost: string; source: string; destination: string; routingKey: string }> = [];
  sessions = new Map<number, { vhost: string; queue: string }>();
  remoteQuorum = new Map<string, number>();
  cluster: {
    replicate: (kind: string, payload: unknown) => Promise<void>;
    call: (home: string, op: string, payload: unknown) => Promise<unknown>;
    subscribe: (home: string, payload: unknown, onDeliver: (msg: LiveMsg) => void) => Promise<number>;
    peerIds: () => string[];
  } | null = null;
  private users = new Map<string, { hash: string; tags: string[] }>();
  private perms: Array<{ user: string; vhost: string; configure: string; write: string; read: string }> = [];
  private vhosts = new Set<string>();
  private sessionsNext = 1;
  private connSeq = 0;
  private mgmtConnections = new Map<string, MgmtConnection>();
  private mgmtChannels = new Map<string, MgmtChannel>();
  private mgmtConsumers: MgmtConsumer[] = [];
  private topicPerms: TopicPerm[] = [];
  private userConnLimit = new Map<string, number>();
  private userChanLimit = new Map<string, number>();
  private vhostConnLimit = new Map<string, number>();
  private vhostQueueLimit = new Map<string, number>();
  operatorPolicies: Policy[] = [];
  transientNonexcl = false;
  readonly startedAt = Date.now();

  constructor(
    public cfg: Config,
    public store: Store,
  ) {}

  key(vhost: string, name: string) {
    return `${vhost}\0${name}`;
  }

  /** Register a connection that finished connection.open. Returns its management id. */
  openMgmtConnection(input: {
    user: string;
    vhost: string;
    peerHost: string;
    peerPort: number;
    close: () => void;
  }): string {
    const name = `conn-${++this.connSeq}`;
    this.mgmtConnections.set(name, {
      name,
      user: input.user,
      vhost: input.vhost,
      peer_host: input.peerHost,
      peer_port: input.peerPort,
      channels: 0,
      connected_at: Math.floor(Date.now() / 1000),
      close: input.close,
    });
    return name;
  }

  setMgmtChannels(name: string, channels: number) {
    const row = this.mgmtConnections.get(name);
    if (row) row.channels = channels;
  }

  listMgmtConnections() {
    return [...this.mgmtConnections.values()]
      .map(({ close: _close, ...row }) => row)
      .sort((a, b) => a.name.localeCompare(b.name));
  }

  closeMgmtConnection(name: string): boolean {
    const row = this.mgmtConnections.get(name);
    if (!row) return false;
    row.close();
    return true;
  }

  forgetMgmtConnection(name: string) {
    this.mgmtConnections.delete(name);
    this.clearMgmtChildren(name);
  }

  syncMgmtChannels(conn: string, user: string, vhost: string, peerHost: string, peerPort: number, numbers: number[]) {
    for (const [name, row] of this.mgmtChannels) {
      if (row.connection === conn) this.mgmtChannels.delete(name);
    }
    for (const number of numbers) {
      const name = `${conn}:${number}`;
      this.mgmtChannels.set(name, { name, connection: conn, user, vhost, number, peer_host: peerHost, peer_port: peerPort });
    }
    const live = this.mgmtConnections.get(conn);
    if (live) live.channels = numbers.length;
  }

  clearMgmtChildren(conn: string) {
    for (const [name, row] of this.mgmtChannels) {
      if (row.connection === conn) this.mgmtChannels.delete(name);
    }
    this.mgmtConsumers = this.mgmtConsumers.filter((c) => c.connection !== conn);
  }

  noteMgmtConsumer(row: MgmtConsumer) {
    this.mgmtConsumers = this.mgmtConsumers.filter(
      (c) => !(c.connection === row.connection && c.channel === row.channel && c.consumer_tag === row.consumer_tag),
    );
    this.mgmtConsumers.push(row);
  }

  forgetMgmtConsumer(conn: string, channel: number, tag: string) {
    this.mgmtConsumers = this.mgmtConsumers.filter(
      (c) => !(c.connection === conn && c.channel === channel && c.consumer_tag === tag),
    );
  }

  forgetMgmtChannelConsumers(conn: string, channel: number) {
    this.mgmtConsumers = this.mgmtConsumers.filter((c) => !(c.connection === conn && c.channel === channel));
  }

  listMgmtChannels() {
    return [...this.mgmtChannels.values()].sort((a, b) => a.name.localeCompare(b.name));
  }

  getMgmtChannel(name: string) {
    return this.mgmtChannels.get(name) ?? null;
  }

  listMgmtConsumers(vhost?: string, queue?: string) {
    return this.mgmtConsumers.filter((c) => (vhost == null || c.vhost === vhost) && (queue == null || c.queue === queue));
  }

  connectionAllowed(user: string, vhost: string): boolean {
    const userMax = this.userConnLimit.get(user);
    if (userMax != null && [...this.mgmtConnections.values()].filter((c) => c.user === user).length >= userMax) return false;
    const vhostMax = this.vhostConnLimit.get(vhost);
    if (vhostMax != null && [...this.mgmtConnections.values()].filter((c) => c.vhost === vhost).length >= vhostMax) return false;
    return true;
  }

  channelAllowed(user: string): boolean {
    const max = this.userChanLimit.get(user);
    if (max == null) return true;
    return [...this.mgmtChannels.values()].filter((c) => c.user === user).length < max;
  }

  queueAllowed(vhost: string, current: number): boolean {
    const max = this.vhostQueueLimit.get(vhost);
    return max == null || current < max;
  }

  setUserLimit(user: string, connections: number | null, channels: number | null) {
    if (connections == null) this.userConnLimit.delete(user);
    else this.userConnLimit.set(user, connections);
    if (channels == null) this.userChanLimit.delete(user);
    else this.userChanLimit.set(user, channels);
  }

  setVhostLimit(vhost: string, connections: number | null, queues: number | null) {
    if (connections == null) this.vhostConnLimit.delete(vhost);
    else this.vhostConnLimit.set(vhost, connections);
    if (queues == null) this.vhostQueueLimit.delete(vhost);
    else this.vhostQueueLimit.set(vhost, queues);
  }

  listUserLimits() {
    const users = new Set([...this.userConnLimit.keys(), ...this.userChanLimit.keys()]);
    return [...users].sort().map((user) => ({
      user,
      "max-connections": this.userConnLimit.get(user) ?? null,
      "max-channels": this.userChanLimit.get(user) ?? null,
    }));
  }

  listVhostLimits() {
    const vhosts = new Set([...this.vhostConnLimit.keys(), ...this.vhostQueueLimit.keys()]);
    return [...vhosts].sort().map((vhost) => ({
      vhost,
      "max-connections": this.vhostConnLimit.get(vhost) ?? null,
      "max-queues": this.vhostQueueLimit.get(vhost) ?? null,
    }));
  }

  putTopicPerm(perm: TopicPerm) {
    try {
      new RegExp(perm.write);
      new RegExp(perm.read);
    } catch {
      throw new Error("write and read must be valid patterns");
    }
    this.topicPerms = this.topicPerms.filter(
      (p) => !(p.user === perm.user && p.vhost === perm.vhost && p.exchange === perm.exchange),
    );
    this.topicPerms.push(perm);
  }

  deleteTopicPerm(user: string, vhost: string, exchange: string): boolean {
    const before = this.topicPerms.length;
    this.topicPerms = this.topicPerms.filter((p) => !(p.user === user && p.vhost === vhost && p.exchange === exchange));
    return this.topicPerms.length !== before;
  }

  listTopicPerms(user?: string) {
    return this.topicPerms
      .filter((p) => user == null || p.user === user)
      .sort((a, b) => a.user.localeCompare(b.user) || a.vhost.localeCompare(b.vhost) || a.exchange.localeCompare(b.exchange));
  }

  topicWriteAllowed(user: string, vhost: string, exchange: string, routingKey: string): boolean {
    const perm = this.topicPerms.find((p) => p.user === user && p.vhost === vhost && p.exchange === exchange);
    if (!perm) return true;
    try {
      return new RegExp(perm.write).test(routingKey);
    } catch {
      return false;
    }
  }

  topicReadAllowed(user: string, vhost: string, exchange: string, routingKey: string): boolean {
    const perm = this.topicPerms.find((p) => p.user === user && p.vhost === vhost && p.exchange === exchange);
    if (!perm) return true;
    try {
      return new RegExp(perm.read).test(routingKey);
    } catch {
      return false;
    }
  }

  matchPolicy(vhost: string, name: string, entity: "queues" | "exchanges"): Policy | null {
    return matchOne(this.policies, vhost, name, entity);
  }

  argsWithPolicy(vhost: string, name: string, args: Record<string, string | number>): Record<string, string | number> {
    const user = this.matchPolicy(vhost, name, "queues");
    const operator = this.matchOperatorPolicy(vhost, name, "queues");
    let out = fillPolicyArgs(args, user);
    out = fillPolicyArgs(args, operator, out);
    return out;
  }

  matchOperatorPolicy(vhost: string, name: string, entity: "queues" | "exchanges"): Policy | null {
    return matchOne(this.operatorPolicies, vhost, name, entity);
  }

  policyNames(vhost: string, name: string): { policy: string | null; operator_policy: string | null } {
    return {
      policy: this.matchPolicy(vhost, name, "queues")?.name ?? null,
      operator_policy: this.matchOperatorPolicy(vhost, name, "queues")?.name ?? null,
    };
  }

  upsertOperatorPolicy(p: Policy) {
    try {
      new RegExp(p.pattern);
    } catch {
      throw new Error("invalid policy pattern");
    }
    this.operatorPolicies = this.operatorPolicies.filter((x) => !(x.vhost === p.vhost && x.name === p.name));
    this.operatorPolicies.push(p);
    this.applyPolicies();
  }

  deleteOperatorPolicy(vhost: string, name: string): boolean {
    const before = this.operatorPolicies.length;
    this.operatorPolicies = this.operatorPolicies.filter((p) => !(p.vhost === vhost && p.name === name));
    if (this.operatorPolicies.length === before) return false;
    this.applyPolicies();
    return true;
  }

  deletePolicy(vhost: string, name: string): boolean {
    const before = this.policies.length;
    this.policies = this.policies.filter((p) => !(p.vhost === vhost && p.name === name));
    if (this.policies.length === before) return false;
    this.store.deletePolicy(vhost, name);
    this.applyPolicies();
    return true;
  }

  listPerms() {
    return [...this.perms];
  }

  upsertPolicy(p: Policy) {
    try {
      new RegExp(p.pattern);
    } catch {
      throw new Error("invalid policy pattern");
    }
    this.policies = this.policies.filter((x) => !(x.vhost === p.vhost && x.name === p.name));
    this.policies.push(p);
    this.store.putPolicy(p);
    this.applyPolicies();
  }

  applyPolicies() {
    for (const q of this.queues.values()) {
      const merged = this.argsWithPolicy(q.vhost, q.name, q.declaredArgs);
      if (merged["x-queue-type"] == null && q.argsParsed.queueType === "quorum") merged["x-queue-type"] = "quorum";
      q.args = merged;
      q.argsParsed = parseArgs(merged);
      if (q.argsParsed.queueType === "quorum" && q.argsParsed.deliveryLimit == null) q.argsParsed.deliveryLimit = 20;
      if (q.durable) this.store.putQueue({ ...q, args: merged });
    }
  }

  load() {
    this.store.ensureVhost("/");
    for (const name of this.store.listVhosts()) this.vhosts.add(name);
    if (!this.vhosts.has("/")) this.vhosts.add("/");
    for (const u of this.store.listUsers()) this.users.set(u.name, { hash: u.hash, tags: u.tags });
    this.perms = this.store.listPerms();
    for (const e of this.store.listExchanges()) this.exchanges.set(this.key(e.vhost, e.name), e);
    for (const vh of this.vhosts) this.ensureBuiltins(vh);
    for (const q of this.store.listQueues()) {
      if (q.exclusive) continue;
      const quorum = parseArgs(q.args).queueType === "quorum";
      if (!quorum && q.home && this.cfg.nodeId && q.home !== this.cfg.nodeId) {
        this.queues.set(this.key(q.vhost, q.name), this.makeQueue(q, true));
        continue;
      }
      this.queues.set(this.key(q.vhost, q.name), this.makeQueue(q, false));
    }
    this.bindings = this.store.listBindings();
    for (const p of this.store.listPolicies() as Policy[]) this.policies.push(p);
    this.applyPolicies();
    for (const row of this.store.listMessages()) {
      const q = this.queues.get(this.key(row.vhost, row.queue));
      if (!q || (q.argsParsed.queueType !== "quorum" && q.home && this.cfg.nodeId && q.home !== this.cfg.nodeId)) continue;
      const meta = JSON.parse(row.meta) as {
        exchange: string;
        routingKey: string;
        headers: Array<[string, Field]>;
        propRaw: string;
        persistent: boolean;
        priority: number;
        expiresAt: number | null;
        redelivered: boolean;
      };
      q.ready.push({
        id: `d-${row.id}`,
        rowId: row.id,
        body: row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body as ArrayBuffer),
        exchange: meta.exchange,
        routingKey: meta.routingKey,
        headers: meta.headers ?? [],
        propRaw: new Uint8Array(Buffer.from(meta.propRaw, "base64")),
        persistent: meta.persistent,
        priority: meta.priority ?? 0,
        expiresAt: meta.expiresAt,
        redelivered: !!meta.redelivered,
      });
    }
    this.ready = true;
  }

  private makeQueue(q: QueueRow, proxy: boolean): QueueLive {
    return {
      ...q,
      home: proxy ? q.home : q.home,
      argsParsed: parseArgs(q.args),
      declaredArgs: { ...q.args },
      ready: [],
      replicas: [],
      unacked: new Map(),
      consumers: [],
      rr: 0,
      lastUsed: Date.now(),
    };
  }

  ensureBuiltins(vhost: string) {
    this.vhosts.add(vhost);
    this.store.ensureVhost(vhost);
    for (const [name, kind, internal] of BUILTIN) {
      const row: ExRow = { vhost, name, kind, durable: true, autoDelete: false, internal, alternate: null };
      const k = this.key(vhost, name);
      if (!this.exchanges.has(k)) {
        this.exchanges.set(k, row);
        this.store.putExchange(row);
      }
    }
  }

  async verify(user: string, password: string): Promise<boolean> {
    const row = this.users.get(user);
    if (!row) return false;
    if (row.hash.startsWith("$")) return Bun.password.verify(password, row.hash);
    return rabbitPasswordHashMatches(password, row.hash);
  }

  can(user: string, vhost: string, kind: "configure" | "write" | "read", resource = ".*"): boolean {
    const perm = this.perms.find((p) => p.user === user && p.vhost === vhost);
    if (!perm) return false;
    try {
      return new RegExp(perm[kind]).test(resource);
    } catch {
      return false;
    }
  }

  hasVhostAccess(user: string, vhost: string): boolean {
    return this.perms.some((p) => p.user === user && p.vhost === vhost);
  }

  homeOf(vhost: string, name: string, exclusive: boolean): string | null {
    if (this.cfg.members.length === 0) return null;
    if (exclusive) return this.cfg.nodeId || null;
    return queueHome(vhost, name, this.cfg.members);
  }

  isLocalHome(home: string | null): boolean {
    return !home || !this.cfg.nodeId || home === this.cfg.nodeId;
  }

  async declareExchange(vhost: string, name: string, kind: string, durable: boolean, autoDelete: boolean, internal: boolean, alternate: string | null) {
    if (name.startsWith("amq.")) throw new ChanError(403, `ACCESS_REFUSED - exchange ${vhost}/${name}`);
    const row: ExRow = { vhost, name, kind, durable, autoDelete, internal, alternate };
    this.exchanges.set(this.key(vhost, name), row);
    if (durable) this.store.putExchange(row);
    await this.cluster?.replicate("exchange", row);
  }

  async deleteExchange(vhost: string, name: string) {
    if (name === "" || name.startsWith("amq.")) throw new ChanError(403, `ACCESS_REFUSED - exchange ${vhost}/${name}`);
    if (!this.exchanges.has(this.key(vhost, name))) throw new ChanError(404, `NOT_FOUND - exchange ${vhost}/${name}`);
    this.exchanges.delete(this.key(vhost, name));
    this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.exchange === name));
    this.store.deleteExchange(vhost, name);
    await this.cluster?.replicate("delete_exchange", { vhost, name });
  }

  async declareQueue(opts: {
    vhost: string;
    name: string;
    durable: boolean;
    exclusive: boolean;
    autoDelete: boolean;
    passive: boolean;
    args: Record<string, string | number>;
  }): Promise<{ name: string; messages: number; consumers: number }> {
    let name = opts.name;
    if (!name) name = `amq.gen-${crypto.randomUUID()}`;
    const home = this.homeOf(opts.vhost, name, opts.exclusive);
    const existing = this.queues.get(this.key(opts.vhost, name));
    if (opts.passive) {
      if (!existing) throw new ChanError(404, `NOT_FOUND - queue ${opts.vhost}/${name}`);
      this.prom.queuesDeclared++;
      return { name, messages: existing.ready.length, consumers: existing.consumers.length };
    }
    if (existing) {
      if (existing.durable !== opts.durable) throw new ChanError(406, `PRECONDITION_FAILED - inequivalent arg durable for queue ${name}`);
      this.prom.queuesDeclared++;
      return { name, messages: existing.ready.length, consumers: existing.consumers.length };
    }
    const qtype = String(opts.args["x-queue-type"] ?? "");
    if (!opts.durable && !opts.exclusive && qtype !== "quorum" && !this.transientNonexcl) {
      throw new ChanError(541, "INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated. By default, this feature is not permitted anymore.");
    }
    const here = [...this.queues.values()].filter((q) => q.vhost === opts.vhost).length;
    if (!this.queueAllowed(opts.vhost, here)) throw new ChanError(403, "ACCESS_REFUSED - queue limit");
    const requested = String(opts.args["x-queue-type"] ?? "");
    if (requested && requested !== "classic" && requested !== "quorum") {
      throw new ChanError(406, `PRECONDITION_FAILED - unsupported x-queue-type '${requested}'`);
    }
    let args = this.argsWithPolicy(opts.vhost, name, opts.args);
    let queueType = requested || (this.cfg.defaultQueueType === "quorum" && opts.durable && !opts.exclusive ? "quorum" : "classic");
    if (queueType === "quorum" && (!opts.durable || opts.exclusive)) {
      throw new ChanError(406, "PRECONDITION_FAILED - quorum queue must be durable and non-exclusive");
    }
    if (queueType === "quorum") args = { ...args, "x-queue-type": "quorum" };
    if (queueType === "quorum" && args["x-delivery-limit"] == null) args["x-delivery-limit"] = 20;
    const row: QueueRow = {
      vhost: opts.vhost,
      name,
      durable: opts.durable,
      exclusive: opts.exclusive,
      autoDelete: opts.autoDelete,
      args,
      home,
    };
    if (!this.isLocalHome(home)) {
      try {
        await this.cluster!.call(home!, "declare_queue", row);
      } catch (err) {
        const text = err instanceof Error ? err.message : String(err);
        if (!text.includes("exists")) throw new ChanError(541, `INTERNAL_ERROR - declare ${home}: ${text}`);
      }
    }
    const live = this.makeQueue(row, !this.isLocalHome(home));
    live.declaredArgs = { ...opts.args };
    this.queues.set(this.key(opts.vhost, name), live);
    if (opts.durable) this.store.putQueue(row);
    await this.cluster?.replicate("queue", row);
    this.prom.queuesDeclared++;
    this.prom.queuesCreated++;
    return { name, messages: 0, consumers: 0 };
  }

  async deleteQueue(vhost: string, name: string): Promise<number> {
    const q = this.queues.get(this.key(vhost, name));
    if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${name}`);
    if (!this.isLocalHome(q.home)) await this.cluster!.call(q.home!, "delete_queue", { vhost, name });
    const n = q.ready.length + q.unacked.size;
    this.prom.consumers = Math.max(0, this.prom.consumers - q.consumers.length);
    this.prom.queuesDeleted++;
    for (const c of q.consumers) c.onCancel?.();
    q.consumers = [];
    this.queues.delete(this.key(vhost, name));
    this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.queue === name));
    this.store.deleteQueue(vhost, name);
    await this.cluster?.replicate("delete_queue", { vhost, name });
    return n;
  }

  async bind(vhost: string, exchange: string, queue: string, routingKey: string, args: Array<[string, Field]>) {
    if (!this.exchanges.has(this.key(vhost, exchange))) throw new ChanError(404, `NOT_FOUND - exchange ${vhost}/${exchange}`);
    if (!this.queues.has(this.key(vhost, queue))) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${queue}`);
    const row: BindRow = { vhost, exchange, queue, routingKey, args };
    const argsJson = JSON.stringify(args);
    this.bindings = this.bindings.filter(
      (b) => !(b.vhost === vhost && b.exchange === exchange && b.queue === queue && b.routingKey === routingKey && JSON.stringify(b.args) === argsJson),
    );
    this.bindings.push(row);
    const q = this.queues.get(this.key(vhost, queue))!;
    if (q.durable || this.exchanges.get(this.key(vhost, exchange))?.durable) this.store.putBinding(row);
    await this.cluster?.replicate("binding", row);
  }

  async unbind(vhost: string, exchange: string, queue: string, routingKey: string, args: Array<[string, Field]> | null) {
    const argsJson = args ? JSON.stringify(args) : null;
    this.bindings = this.bindings.filter((b) => {
      if (b.vhost !== vhost || b.exchange !== exchange || b.queue !== queue || b.routingKey !== routingKey) return true;
      if (argsJson == null) return false;
      return JSON.stringify(b.args) !== argsJson;
    });
    this.store.deleteBinding(vhost, exchange, queue, routingKey, argsJson);
    await this.cluster?.replicate("unbind", { vhost, exchange, queue, routingKey, args });
  }

  route(vhost: string, exchange: string, routingKey: string, headers: Array<[string, Field]>): string[] {
    const seen = new Set<string>();
    const pending = [exchange];
    const out: string[] = [];
    while (pending.length) {
      const current = pending.pop()!;
      if (seen.has(current)) continue;
      seen.add(current);
      const dest = this.routeOne(vhost, current, routingKey, headers);
      out.push(...dest);
      const ex = this.exchanges.get(this.key(vhost, current));
      for (const edge of this.e2e) {
        if (edge.vhost !== vhost || edge.source !== current) continue;
        const ok = ex?.kind === "fanout"
          || (ex?.kind === "topic" ? topicMatches(edge.routingKey, routingKey) : edge.routingKey === routingKey);
        if (ok) pending.push(edge.destination);
      }
      if (!dest.length) {
        const userPol = this.matchPolicy(vhost, current, "exchanges");
        const opPol = this.matchOperatorPolicy(vhost, current, "exchanges");
        const alt = ex?.alternate || opPol?.alternate || userPol?.alternate || null;
        if (alt) pending.push(alt);
      }
    }
    return [...new Set(out)];
  }

  async bindExchange(vhost: string, source: string, destination: string, routingKey: string) {
    if (source === "" || destination === "") throw new ChanError(406, "PRECONDITION_FAILED - cannot bind the default exchange");
    if (!this.exchanges.has(this.key(vhost, source)) || !this.exchanges.has(this.key(vhost, destination))) {
      throw new ChanError(404, `NOT_FOUND - no exchange '${source}' or '${destination}' in vhost '${vhost}'`);
    }
    this.e2e = this.e2e.filter((e) => !(e.vhost === vhost && e.source === source && e.destination === destination && e.routingKey === routingKey));
    this.e2e.push({ vhost, source, destination, routingKey });
  }

  async unbindExchange(vhost: string, source: string, destination: string, routingKey: string) {
    this.e2e = this.e2e.filter((e) => !(e.vhost === vhost && e.source === source && e.destination === destination && e.routingKey === routingKey));
  }

  private routeOne(vhost: string, exchange: string, routingKey: string, headers: Array<[string, Field]>): string[] {
    if (exchange === "") {
      return this.queues.has(this.key(vhost, routingKey)) ? [routingKey] : [];
    }
    const ex = this.exchanges.get(this.key(vhost, exchange));
    if (!ex) throw new ChanError(404, `NOT_FOUND - exchange ${vhost}/${exchange}`);
    if (ex.internal && exchange !== "") throw new ChanError(403, `ACCESS_REFUSED - internal exchange ${exchange}`);
    const rows = this.bindings.filter((b) => b.vhost === vhost && b.exchange === exchange);
    if (ex.kind === "fanout") return [...new Set(rows.map((b) => b.queue))];
    if (ex.kind === "topic") return [...new Set(rows.filter((b) => topicMatches(b.routingKey, routingKey)).map((b) => b.queue))];
    if (ex.kind === "headers") return [...new Set(rows.filter((b) => headersMatch(b.args, headers)).map((b) => b.queue))];
    return [...new Set(rows.filter((b) => b.routingKey === routingKey).map((b) => b.queue))];
  }

  async publish(input: {
    vhost: string;
    exchange: string;
    routingKey: string;
    body: Uint8Array;
    headers: Array<[string, Field]>;
    propRaw: Uint8Array;
    persistent: boolean;
    priority: number;
    expiration: string;
    confirm?: boolean;
    mandatory?: boolean;
  }): Promise<"ack" | "nack" | "return"> {
    this.prom.received++;
    if (input.confirm) this.prom.receivedConfirm++;
    if (input.exchange === "") {
      if (!this.queues.has(this.key(input.vhost, input.routingKey))) {
        throw new ChanError(404, `NOT_FOUND - no queue '${input.routingKey}' in vhost '${input.vhost}'`);
      }
    }
    const dests = this.route(input.vhost, input.exchange, input.routingKey, input.headers);
    for (const key of [...headerList(input.headers, "CC"), ...headerList(input.headers, "BCC")]) {
      if (key === input.routingKey) continue;
      for (const name of this.route(input.vhost, input.exchange, key, input.headers)) {
        if (!dests.includes(name)) dests.push(name);
      }
    }
    if (input.headers.some(([key]) => key === "BCC")) {
      input.headers = input.headers.filter(([key]) => key !== "BCC");
      if (input.propRaw.length) input.propRaw = replaceHeaderTable(input.propRaw, input.headers);
    }
    const copies = fedLinks.filter((link) => link.upstream === input.vhost && new RegExp(link.pattern).test(input.exchange));
    if (!dests.length && copies.length === 0) {
      if (input.mandatory) this.prom.unroutableReturned++;
      else this.prom.unroutableDropped++;
      return "return";
    }
    this.prom.routed += dests.length;
    let rejected = false;
    for (const name of dests) {
      const ok = await this.enqueue(input.vhost, name, {
        body: input.body,
        exchange: input.exchange,
        routingKey: input.routingKey,
        headers: input.headers,
        propRaw: input.propRaw,
        persistent: input.persistent,
        priority: input.priority,
        expiration: input.expiration,
      });
      if (!ok) rejected = true;
    }
    for (const link of copies) {
      for (const name of this.route(link.downstream, input.exchange, input.routingKey, input.headers)) {
        const ok = await this.enqueue(link.downstream, name, {
          body: input.body,
          exchange: input.exchange,
          routingKey: input.routingKey,
          headers: input.headers,
          propRaw: input.propRaw,
          persistent: input.persistent,
          priority: input.priority,
          expiration: input.expiration,
        });
        if (!ok) rejected = true;
      }
    }
    // The interval timer still fsyncs. A publisher confirm does not wait for it.
    if (!rejected && input.confirm) this.prom.confirmed++;
    return rejected ? "nack" : "ack";
  }

  async enqueue(
    vhost: string,
    name: string,
    src: {
      body: Uint8Array;
      exchange: string;
      routingKey: string;
      headers: Array<[string, Field]>;
      propRaw: Uint8Array;
      persistent: boolean;
      priority: number;
      expiration: string;
    },
  ): Promise<boolean> {
    const q = this.queues.get(this.key(vhost, name));
    if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${name}`);
    if (q.argsParsed.queueType === "quorum") return this.enqueueQuorum(q, src);
    if (!this.isLocalHome(q.home)) {
      try {
        await this.cluster!.call(q.home!, "enqueue", {
          vhost,
          queue: name,
          body: Buffer.from(src.body).toString("base64"),
          exchange: src.exchange,
          routingKey: src.routingKey,
          headers: src.headers,
          propRaw: Buffer.from(src.propRaw).toString("base64"),
          persistent: src.persistent,
          priority: src.priority,
          expiration: src.expiration,
        });
        return true;
      } catch (err) {
        throw new ChanError(541, `INTERNAL_ERROR - queue home is unavailable`);
      }
    }
    return this.enqueueLocal(q, src, 0);
  }

  async enqueueQuorum(
    q: QueueLive,
    src: {
      body: Uint8Array;
      exchange: string;
      routingKey: string;
      headers: Array<[string, Field]>;
      propRaw: Uint8Array;
      persistent: boolean;
      priority: number;
      expiration: string;
    },
  ): Promise<boolean> {
    const members = this.cfg.members.length || 1;
    const majority = Math.floor(members / 2) + 1;
    const peers = this.cluster ? this.cluster.peerIds().filter((id) => id !== this.cfg.nodeId) : [];
    if (peers.length + 1 < majority) return false;
    const qid = `q-${this.cfg.nodeId}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const payload = encodeQuorumAppend({
      vhost: q.vhost,
      queue: q.name,
      messageId: qid,
      body: src.body,
      exchange: src.exchange,
      routingKey: src.routingKey,
      persistent: src.persistent,
    });
    const acked: string[] = [];
    if (this.cluster) {
      for (const id of peers) {
        try {
          await this.cluster.call(id, "quorum_append", payload);
          acked.push(id);
        } catch {
          /* a down peer does not count toward the majority */
        }
      }
    }
    if (acked.length + 1 < majority) {
      await Promise.all(acked.map((id) => this.cluster!.call(id, "quorum_drop", { vhost: q.vhost, queue: q.name, id: qid }).catch(() => null)));
      return false;
    }
    const ok = this.enqueueLocal(q, { ...src, id: qid }, 0);
    if (ok) this.store.noteQuorumConfirm();
    if (!ok) {
      await Promise.all(acked.map((id) => this.cluster!.call(id, "quorum_drop", { vhost: q.vhost, queue: q.name, id: qid }).catch(() => null)));
    }
    return ok;
  }

  enqueueLocal(
    q: QueueLive,
    src: {
      body: Uint8Array;
      exchange: string;
      routingKey: string;
      headers: Array<[string, Field]>;
      propRaw: Uint8Array;
      persistent: boolean;
      priority: number;
      expiration: string;
      redelivered?: boolean;
      id?: string;
    },
    depth: number,
  ): boolean {
    this.expire(q);
    const bytes = q.ready.reduce((a, m) => a + m.body.length, 0);
    const overCount = q.argsParsed.maxLength != null && q.ready.length >= q.argsParsed.maxLength;
    const overBytes = q.argsParsed.maxLengthBytes != null && bytes + src.body.length > q.argsParsed.maxLengthBytes;
    if (overCount || overBytes) {
      if (q.argsParsed.overflow === "reject-publish" || q.argsParsed.overflow === "reject-publish-dlx") {
        if (q.argsParsed.overflow === "reject-publish-dlx") {
          this.deadLetter(q, liveFrom(q, src), depth, "maxlen");
          this.prom.dlxMaxlen++;
        }
        return false;
      }
      while (
        q.ready.length &&
        ((q.argsParsed.maxLength != null && q.ready.length >= q.argsParsed.maxLength) ||
          (q.argsParsed.maxLengthBytes != null && q.ready.reduce((a, m) => a + m.body.length, 0) + src.body.length > q.argsParsed.maxLengthBytes))
      ) {
        const dropped = q.ready.shift()!;
        if (dropped.rowId != null) this.store.deleteMessage(dropped.rowId);
        const accepted = this.deadLetter(q, dropped, depth, "maxlen");
        if (!accepted && q.argsParsed.dlxStrategy === "at-least-once") {
          q.ready.unshift(dropped);
          return false;
        }
        this.prom.dlxMaxlen++;
      }
    }
    let expiresAt: number | null = null;
    if (q.argsParsed.messageTtl != null) expiresAt = Date.now() + q.argsParsed.messageTtl;
    if (src.expiration) {
      const ms = Number(src.expiration);
      if (Number.isFinite(ms)) {
        const at = Date.now() + ms;
        expiresAt = expiresAt == null ? at : Math.min(expiresAt, at);
      }
    }
    let rowId: number | null = null;
    const meta = {
      exchange: src.exchange,
      routingKey: src.routingKey,
      headers: src.headers,
      propRaw: Buffer.from(src.propRaw).toString("base64"),
      persistent: src.persistent,
      priority: src.priority,
      expiresAt,
      redelivered: !!src.redelivered,
    };
    if (q.durable && src.persistent) {
      rowId = this.store.insertMessage(q.vhost, q.name, src.body, JSON.stringify(meta));
    }
    const msg: LiveMsg = {
      id: src.id ?? (rowId != null ? `d-${rowId}` : `t-${transientSeq++}`),
      rowId,
      body: src.body,
      exchange: src.exchange,
      routingKey: src.routingKey,
      headers: src.headers,
      propRaw: src.propRaw,
      persistent: src.persistent,
      priority: src.priority,
      expiresAt,
      redelivered: !!src.redelivered,
    };
    if (q.argsParsed.queueType === "quorum" && !this.isQuorumLeader()) {
      q.replicas.push(msg);
      return true;
    }
    if (q.argsParsed.maxPriority != null) {
      const p = Math.min(src.priority, q.argsParsed.maxPriority);
      msg.priority = p;
      let i = 0;
      while (i < q.ready.length && q.ready[i]!.priority >= p) i++;
      q.ready.splice(i, 0, msg);
    } else {
      q.ready.push(msg);
    }
    if (q.argsParsed.queueType !== "quorum" || this.isQuorumLeader()) this.pump(q);
    return true;
  }

  quorumLeader(): string {
    const ids = this.cfg.members.map((member) => member.id);
    if (!ids.length) return this.cfg.nodeId;
    ids.sort();
    const up = new Set<string>([this.cfg.nodeId]);
    for (const id of this.cluster?.peerIds() ?? []) up.add(id);
    const live = ids.filter((id) => up.has(id));
    const majority = Math.floor(ids.length / 2) + 1;
    if (live.length < majority) return ids[0]!;
    return live[0]!;
  }

  isQuorumLeader(): boolean {
    return !this.cfg.members.length || this.quorumLeader() === this.cfg.nodeId;
  }

  /** Move follower copies into the ready queue once this process is the live leader. */
  promoteIfLeader() {
    if (!this.isQuorumLeader()) return;
    for (const q of this.queues.values()) {
      if (q.argsParsed.queueType !== "quorum" || q.replicas.length === 0) continue;
      q.ready.push(...q.replicas);
      q.replicas = [];
      this.pump(q);
    }
  }

  private expire(q: QueueLive) {
    const now = Date.now();
    const keep: LiveMsg[] = [];
    for (const m of q.ready) {
      if (m.expiresAt != null && m.expiresAt <= now) {
        if (m.rowId != null) this.store.deleteMessage(m.rowId);
        const accepted = this.deadLetter(q, m, 0, "expired");
        if (!accepted && q.argsParsed.dlxStrategy === "at-least-once") {
          m.expiresAt = null;
          keep.push(m);
        } else {
          this.prom.dlxExpired++;
        }
      } else keep.push(m);
    }
    q.ready = keep;
  }

  private deadLetter(q: QueueLive, msg: LiveMsg, depth: number, reason: "expired" | "rejected" | "maxlen"): boolean {
    if (!q.argsParsed.dlx || depth > 8) return true;
    const headers = deathHeaders(q.name, reason, msg.exchange, msg.routingKey, msg.headers);
    const rk = q.argsParsed.dlxKey ?? msg.routingKey;
    let dests: string[] = [];
    try {
      dests = this.route(q.vhost, q.argsParsed.dlx, rk, headers);
    } catch {
      return q.argsParsed.dlxStrategy !== "at-least-once";
    }
    if (!dests.length) return q.argsParsed.dlxStrategy !== "at-least-once";
    let accepted = false;
    for (const name of dests) {
      const dest = this.queues.get(this.key(q.vhost, name));
      if (!dest || dest === q) continue;
      if (!this.isLocalHome(dest.home)) {
        accepted = true;
        void this.enqueue(q.vhost, name, {
          body: msg.body,
          exchange: q.argsParsed.dlx,
          routingKey: rk,
          headers,
          propRaw: propsWithDeath(headers),
          persistent: msg.persistent,
          priority: msg.priority,
          expiration: "",
        }).catch(() => {});
      } else if (
        this.enqueueLocal(
          dest,
          {
            body: msg.body,
            exchange: q.argsParsed.dlx,
            routingKey: rk,
            headers,
            propRaw: propsWithDeath(headers),
            persistent: msg.persistent,
            priority: msg.priority,
            expiration: "",
          },
          depth + 1,
        )
      ) {
        accepted = true;
      }
    }
    return accepted || q.argsParsed.dlxStrategy !== "at-least-once";
  }

  pump(q: QueueLive) {
    this.expire(q);
    let guard = 0;
    while (q.ready.length && guard++ < 100000) {
      const chosen = pickConsumer(q);
      if (!chosen) return;
      const msg = q.ready.shift()!;
      if (!chosen.noAck) q.unacked.set(msg.id, msg);
      else if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
      if (q.argsParsed.queueType === "quorum") {
        const target = chosen;
        void this.claimThenDeliver(q, target, msg);
        return;
      }
      this.noteDeliver(chosen.noAck, msg.redelivered);
      chosen.deliver(msg);
    }
  }

  private noteDeliver(autoAck: boolean, redelivered: boolean) {
    this.prom.delivered++;
    if (autoAck) this.prom.deliveredConsumeAuto++;
    else this.prom.deliveredConsumeManual++;
    if (redelivered) this.prom.redelivered++;
  }

  private async claimThenDeliver(q: QueueLive, chosen: Consumer, msg: LiveMsg) {
    await this.quorumDrop(q, msg.id);
    this.noteDeliver(chosen.noAck, msg.redelivered);
    chosen.deliver(msg);
    this.pump(q);
  }

  nextSession(): number {
    const n = this.sessionsNext++;
    const slot = this.slot();
    if (!slot) return n;
    return slot * 0x100000000 + (n & 0xffffffff);
  }

  private slot(): number {
    if (!this.cfg.members.length || !this.cfg.nodeId) return 0;
    const sorted = [...this.cfg.members].sort((a, b) => (a.id < b.id ? -1 : 1));
    const i = sorted.findIndex((m) => m.id === this.cfg.nodeId);
    return i < 0 ? 1 : i + 1;
  }

  async consume(vhost: string, queue: string, consumer: Consumer): Promise<void> {
    const q = this.queues.get(this.key(vhost, queue));
    if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${queue}`);
    if (q.consumers.some((c) => c.exclusive) || (consumer.exclusive && q.consumers.length)) {
      throw new ChanError(403, `ACCESS_REFUSED - exclusive consumer on ${queue}`);
    }
    q.consumers.push(consumer);
    this.prom.consumers++;
    q.lastUsed = Date.now();
    this.sessions.set(consumer.session, { vhost, queue });
  }

  async kick(vhost: string, queue: string, session: number) {
    const q = this.queues.get(this.key(vhost, queue));
    if (!q) return;
    const consumer = q.consumers.find((c) => c.session === session);
    if (q.argsParsed.queueType === "quorum" && !this.isQuorumLeader()) {
      if (!consumer) return;
      q.consumers = q.consumers.filter((c) => c.session !== session);
      this.remoteQuorum.set(`${vhost}\0${queue}\0${consumer.tag}`, session);
      await this.cluster!.subscribe(
        this.quorumLeader(),
        { vhost, queue, session, noAck: consumer.noAck, exclusive: consumer.exclusive },
        (msg) => consumer.deliver(msg),
      );
      return;
    }
    if (q.argsParsed.queueType !== "quorum" && !this.isLocalHome(q.home)) {
      if (!consumer) return;
      await this.cluster!.subscribe(
        q.home!,
        { vhost, queue, session, noAck: consumer.noAck, exclusive: consumer.exclusive },
        (msg) => consumer.deliver(msg),
      );
      return;
    }
    this.pump(q);
  }

  async cancel(vhost: string, queue: string, tag: string) {
    const q = this.queues.get(this.key(vhost, queue));
    if (!q) return;
    const consumer = q.consumers.find((c) => c.tag === tag);
    if (consumer) this.prom.consumers = Math.max(0, this.prom.consumers - 1);
    q.consumers = q.consumers.filter((c) => c.tag !== tag);
    const session = consumer?.session ?? this.remoteQuorum.get(`${vhost}\0${queue}\0${tag}`);
    this.remoteQuorum.delete(`${vhost}\0${queue}\0${tag}`);
    if (session != null && q.argsParsed.queueType === "quorum" && !this.isQuorumLeader()) {
      await this.cluster!.call(this.quorumLeader(), "unsub", { vhost, queue, session });
    }
  }

  async ack(vhost: string, queue: string, id: string) {
    const q = this.queues.get(this.key(vhost, queue));
    if (!q) return;
    if (q.argsParsed.queueType !== "quorum" && !this.isLocalHome(q.home)) {
      await this.cluster!.call(q.home!, "ack", { vhost, queue, id });
      return;
    }
    if (q.argsParsed.queueType === "quorum" && !q.unacked.has(id) && !this.isQuorumLeader()) {
      await this.cluster!.call(this.quorumLeader(), "ack", { vhost, queue, id });
      return;
    }
    const msg = q.unacked.get(id);
    if (!msg) return;
    q.unacked.delete(id);
    if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
    this.prom.acknowledged++;
    if (q.argsParsed.queueType === "quorum") await this.quorumDrop(q, id);
    this.pump(q);
  }

  async quorumDrop(q: QueueLive, id: string) {
    const peers = this.cluster?.peerIds().filter((peer) => peer !== this.cfg.nodeId) ?? [];
    await Promise.all(peers.map((peer) => this.cluster!.call(peer, "quorum_drop", { vhost: q.vhost, queue: q.name, id }).catch(() => null)));
  }

  dropLocal(vhost: string, queue: string, id: string) {
    const q = this.queues.get(this.key(vhost, queue));
    if (!q) return;
    const ready = q.ready.filter((m) => m.id === id);
    q.ready = q.ready.filter((m) => m.id !== id);
    const replicas = q.replicas.filter((m) => m.id === id);
    q.replicas = q.replicas.filter((m) => m.id !== id);
    for (const m of ready.concat(replicas)) if (m.rowId != null) this.store.deleteMessage(m.rowId);
    const held = q.unacked.get(id);
    if (held) {
      q.unacked.delete(id);
      if (held.rowId != null) this.store.deleteMessage(held.rowId);
    }
  }

  async nack(vhost: string, queue: string, id: string, requeue: boolean) {
    const q = this.queues.get(this.key(vhost, queue));
    if (!q) return;
    if (q.argsParsed.queueType !== "quorum" && !this.isLocalHome(q.home)) {
      await this.cluster!.call(q.home!, "nack", { vhost, queue, id, requeue });
      return;
    }
    if (q.argsParsed.queueType === "quorum" && !q.unacked.has(id) && !this.isQuorumLeader()) {
      await this.cluster!.call(this.quorumLeader(), "nack", { vhost, queue, id, requeue });
      return;
    }
    const msg = q.unacked.get(id);
    if (!msg) return;
    q.unacked.delete(id);
    if (requeue) {
      msg.deliveries = (msg.deliveries ?? 0) + 1;
      if (q.argsParsed.deliveryLimit != null && msg.deliveries >= q.argsParsed.deliveryLimit) {
        const accepted = this.deadLetter(q, msg, 0, "rejected");
        if (!accepted && q.argsParsed.dlxStrategy === "at-least-once") {
          q.unacked.set(id, msg);
          return;
        }
        if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
        this.prom.dlxDeliveryLimit++;
        if (q.argsParsed.queueType === "quorum") await this.quorumDrop(q, id);
      } else {
        msg.redelivered = true;
        q.ready.unshift(msg);
        this.pump(q);
      }
    } else {
      const accepted = this.deadLetter(q, msg, 0, "rejected");
      if (!accepted && q.argsParsed.dlxStrategy === "at-least-once") {
        msg.redelivered = true;
        q.ready.unshift(msg);
        this.pump(q);
        return;
      }
      if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
      this.prom.dlxRejected++;
      if (q.argsParsed.queueType === "quorum") await this.quorumDrop(q, id);
    }
  }

  async get(vhost: string, queue: string, noAck: boolean): Promise<LiveMsg | null> {
    const q = this.queues.get(this.key(vhost, queue));
    if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${queue}`);
    if (q.argsParsed.queueType === "quorum" && !this.isQuorumLeader()) {
      try {
        const raw = (await this.cluster!.call(this.quorumLeader(), "get", { vhost, queue, noAck, no_ack: noAck })) as {
          empty?: boolean;
          msg?: { id?: string; body?: string; propRaw?: string; exchange?: string; routingKey?: string; persistent?: boolean; priority?: number; redelivered?: boolean };
          message?: { message_id?: string; body_b64?: string; exchange?: string; routing_key?: string; persistent?: boolean; redelivered?: boolean };
        } | null;
        if (!raw || raw.empty) return null;
        const bunMsg = raw.msg;
        const rustMsg = raw.message;
        const bodyB64 = bunMsg?.body ?? rustMsg?.body_b64;
        if (!bodyB64) return null;
        const id = String(bunMsg?.id ?? rustMsg?.message_id ?? "");
        this.dropLocal(vhost, queue, id);
        const leader = this.quorumLeader();
        const peers = this.cluster?.peerIds().filter((peer) => peer !== this.cfg.nodeId && peer !== leader) ?? [];
        await Promise.all(peers.map((peer) => this.cluster!.call(peer, "quorum_drop", { vhost, queue, id }).catch(() => null)));
        const propRawB64 = bunMsg?.propRaw ?? "";
        return {
          id,
          rowId: null,
          body: new Uint8Array(Buffer.from(bodyB64, "base64")),
          exchange: String(bunMsg?.exchange ?? rustMsg?.exchange ?? ""),
          routingKey: String(bunMsg?.routingKey ?? rustMsg?.routing_key ?? ""),
          headers: [],
          propRaw: propRawB64 ? new Uint8Array(Buffer.from(propRawB64, "base64")) : new Uint8Array(),
          persistent: (bunMsg?.persistent ?? rustMsg?.persistent) !== false,
          priority: Number(bunMsg?.priority ?? 0),
          expiresAt: null,
          redelivered: !!(bunMsg?.redelivered ?? rustMsg?.redelivered),
        };
      } catch {
        throw new ChanError(541, "INTERNAL_ERROR - queue home is unavailable");
      }
    }
    if (q.argsParsed.queueType !== "quorum" && !this.isLocalHome(q.home)) {
      try {
        const raw = (await this.cluster!.call(q.home!, "get", { vhost, queue, noAck })) as {
          empty?: boolean;
          msg?: LiveMsg & { body: string; propRaw: string };
        };
        if (!raw || raw.empty || !raw.msg) return null;
        const m = raw.msg;
        return {
          ...m,
          body: new Uint8Array(Buffer.from(m.body, "base64")),
          propRaw: new Uint8Array(Buffer.from(m.propRaw, "base64")),
        };
      } catch {
        throw new ChanError(541, "INTERNAL_ERROR - queue home is unavailable");
      }
    }
    this.expire(q);
    const msg = q.ready.shift();
    if (!msg) {
      this.prom.getEmpty++;
      return null;
    }
    this.prom.delivered++;
    if (noAck) this.prom.deliveredGetAuto++;
    else this.prom.deliveredGetManual++;
    if (msg.redelivered) this.prom.redelivered++;
    if (!noAck) q.unacked.set(msg.id, msg);
    else if (msg.rowId != null) this.store.deleteMessage(msg.rowId);
    if (q.argsParsed.queueType === "quorum") await this.quorumDrop(q, msg.id);
    return msg;
  }

  async purge(vhost: string, queue: string): Promise<number> {
    const q = this.queues.get(this.key(vhost, queue));
    if (!q) throw new ChanError(404, `NOT_FOUND - queue ${vhost}/${queue}`);
    if (!this.isLocalHome(q.home)) {
      const n = (await this.cluster!.call(q.home!, "purge", { vhost, queue })) as number;
      return Number(n) || 0;
    }
    const n = q.ready.length;
    for (const m of q.ready) if (m.rowId != null) this.store.deleteMessage(m.rowId);
    q.ready = [];
    return n;
  }

  applyRemote(kind: string, payload: Record<string, unknown>) {
    if (kind === "exchange") {
      const e = payload as ExRow;
      this.exchanges.set(this.key(e.vhost, e.name), e);
      if (e.durable) this.store.putExchange(e);
    } else if (kind === "delete_exchange") {
      const vhost = String(payload.vhost);
      const name = String(payload.name);
      this.exchanges.delete(this.key(vhost, name));
      this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.exchange === name));
      this.store.deleteExchange(vhost, name);
    } else if (kind === "queue" || kind === "declare_queue") {
      const row = payload as QueueRow;
      if (!this.queues.has(this.key(row.vhost, row.name))) {
        this.queues.set(this.key(row.vhost, row.name), this.makeQueue(row, !this.isLocalHome(row.home)));
      }
      if (row.durable) this.store.putQueue(row);
    } else if (kind === "delete_queue") {
      const vhost = String(payload.vhost);
      const name = String(payload.name ?? payload.queue);
      this.queues.delete(this.key(vhost, name));
      this.bindings = this.bindings.filter((b) => !(b.vhost === vhost && b.queue === name));
      this.store.deleteQueue(vhost, name);
    } else if (kind === "binding") {
      const b = payload as BindRow;
      this.bindings.push(b);
      this.store.putBinding(b);
    } else if (kind === "unbind") {
      const vhost = String(payload.vhost);
      const exchange = String(payload.exchange);
      const queue = String(payload.queue);
      const routingKey = String(payload.routingKey);
      const argsJson = payload.args ? JSON.stringify(payload.args) : null;
      this.bindings = this.bindings.filter((b) => {
        if (b.vhost !== vhost || b.exchange !== exchange || b.queue !== queue || b.routingKey !== routingKey) return true;
        if (argsJson == null) return false;
        return JSON.stringify(b.args) !== argsJson;
      });
      this.store.deleteBinding(vhost, exchange, queue, routingKey, argsJson);
    } else if (kind === "user") {
      const name = String(payload.name);
      const hash = String(payload.hash);
      const tags = (payload.tags as string[]) ?? [];
      this.users.set(name, { hash, tags });
      this.store.putUser({ name, hash, tags });
    } else if (kind === "delete_user") {
      const name = String(payload.name);
      this.users.delete(name);
      this.perms = this.perms.filter((p) => p.user !== name);
      this.store.deleteUser(name);
    } else if (kind === "permission") {
      const p = payload as { user: string; vhost: string; configure: string; write: string; read: string };
      this.perms = this.perms.filter((x) => !(x.user === p.user && x.vhost === p.vhost));
      this.perms.push(p);
      this.store.putPerm(p);
    } else if (kind === "delete_permission") {
      this.perms = this.perms.filter((p) => !(p.user === payload.user && p.vhost === payload.vhost));
      this.store.deletePerm(String(payload.user), String(payload.vhost));
    } else if (kind === "vhost") {
      this.ensureBuiltins(String(payload.name));
    } else if (kind === "delete_vhost") {
      this.vhosts.delete(String(payload.name));
      this.store.deleteVhost(String(payload.name));
    }
  }

  snapshot() {
    return {
      users: [...this.users.entries()].map(([name, u]) => ({ name, hash: u.hash, tags: u.tags })),
      vhosts: [...this.vhosts],
      permissions: this.perms,
      exchanges: [...this.exchanges.values()],
      queues: [...this.queues.values()].map((q) => ({
        vhost: q.vhost,
        name: q.name,
        durable: q.durable,
        exclusive: q.exclusive,
        autoDelete: q.autoDelete,
        args: q.args,
        home: q.home,
      })),
      bindings: this.bindings,
    };
  }

  applySnapshot(snap: ReturnType<Broker["snapshot"]> | null | undefined) {
    if (!snap) return;
    for (const u of snap.users ?? []) if (!this.users.has(u.name)) {
      this.users.set(u.name, { hash: u.hash, tags: u.tags });
      this.store.putUser(u);
    }
    for (const name of snap.vhosts ?? []) this.ensureBuiltins(name);
    for (const p of snap.permissions ?? []) {
      if (!this.perms.some((x) => x.user === p.user && x.vhost === p.vhost)) {
        this.perms.push(p);
        this.store.putPerm(p);
      }
    }
    for (const e of snap.exchanges ?? []) if (!this.exchanges.has(this.key(e.vhost, e.name))) {
      this.exchanges.set(this.key(e.vhost, e.name), e);
      if (e.durable) this.store.putExchange(e);
    }
    for (const q of snap.queues ?? []) if (!this.queues.has(this.key(q.vhost, q.name))) {
      this.queues.set(this.key(q.vhost, q.name), this.makeQueue(q, !this.isLocalHome(q.home)));
      if (q.durable) this.store.putQueue(q);
    }
    for (const b of snap.bindings ?? []) {
      this.bindings.push(b);
      this.store.putBinding(b);
    }
  }

  async putUser(name: string, password: string | null, tags: string[], create: boolean) {
    const existing = this.users.get(name);
    if (!existing && !password) throw new Error("password required");
    const hash = password ? await Bun.password.hash(password, { algorithm: "argon2id" }) : existing!.hash;
    this.users.set(name, { hash, tags: tags.length ? tags : existing?.tags ?? [] });
    this.store.putUser({ name, hash, tags: this.users.get(name)!.tags });
    await this.cluster?.replicate("user", { name, hash, tags: this.users.get(name)!.tags });
    return create;
  }

  async deleteUser(name: string) {
    this.users.delete(name);
    this.perms = this.perms.filter((p) => p.user !== name);
    this.store.deleteUser(name);
    await this.cluster?.replicate("delete_user", { name });
  }

  async putPerm(p: { user: string; vhost: string; configure: string; write: string; read: string }) {
    this.perms = this.perms.filter((x) => !(x.user === p.user && x.vhost === p.vhost));
    this.perms.push(p);
    this.store.putPerm(p);
    await this.cluster?.replicate("permission", p);
  }

  async deletePerm(user: string, vhost: string) {
    this.perms = this.perms.filter((p) => !(p.user === user && p.vhost === vhost));
    this.store.deletePerm(user, vhost);
    await this.cluster?.replicate("delete_permission", { user, vhost });
  }

  exportDefinitions() {
    const argMap = (args: Array<[string, Field]>) => {
      const out: Record<string, string | number> = {};
      for (const [k, v] of args) {
        if (v.t === "I" || v.t === "l") out[k] = v.v;
        else if (v.t === "S" || v.t === "s") out[k] = v.v;
      }
      return out;
    };
    return {
      rabbit_version: "3.13.0",
      queueforge_version: "0.1.0",
      users: [...this.users.entries()].map(([name, u]) => ({
        name,
        password_hash: u.hash,
        tags: u.tags.join(","),
      })),
      vhosts: [...this.vhosts].map((name) => ({ name })),
      permissions: this.perms.map((p) => ({ ...p })),
      exchanges: [...this.exchanges.values()]
        .filter((e) => e.name !== "")
        .map((e) => ({
          name: e.name,
          vhost: e.vhost,
          type: e.kind === "default" ? "direct" : e.kind,
          durable: e.durable,
          auto_delete: e.autoDelete,
          internal: e.internal,
        })),
      queues: [...this.queues.values()]
        .filter((q) => !q.exclusive)
        .map((q) => ({
          name: q.name,
          vhost: q.vhost,
          durable: q.durable,
          exclusive: false,
          auto_delete: q.autoDelete,
          type: q.argsParsed.queueType,
          arguments: q.args,
        })),
      bindings: this.bindings.map((b) => ({
        source: b.exchange,
        vhost: b.vhost,
        destination: b.queue,
        destination_type: "queue",
        routing_key: b.routingKey,
        arguments: argMap(b.args),
      })),
      policies: this.policies.map((p) => ({
        vhost: p.vhost,
        name: p.name,
        pattern: p.pattern,
        "apply-to": p.applyTo,
        priority: p.priority,
        definition: {
          ...(p.messageTtl != null ? { "message-ttl": p.messageTtl } : {}),
          ...(p.dlx ? { "dead-letter-exchange": p.dlx } : {}),
          ...(p.dlxKey ? { "dead-letter-routing-key": p.dlxKey } : {}),
          ...(p.maxLength != null ? { "max-length": p.maxLength } : {}),
          ...(p.maxLengthBytes != null ? { "max-length-bytes": p.maxLengthBytes } : {}),
          ...(p.expiresMs != null ? { expires: p.expiresMs } : {}),
          ...(p.overflow ? { overflow: p.overflow } : {}),
          ...(p.dlxStrategy ? { "dead-letter-strategy": p.dlxStrategy } : {}),
          ...(p.deliveryLimit != null ? { "delivery-limit": p.deliveryLimit } : {}),
          ...(p.alternate ? { "alternate-exchange": p.alternate } : {}),
        },
      })),
    };
  }

  async importDefinitions(body: {
    users?: Array<{ name: string; password?: string; password_hash?: string; tags?: string | string[] }>;
    vhosts?: Array<{ name: string }>;
    permissions?: Array<{ user: string; vhost: string; configure: string; write: string; read: string }>;
    exchanges?: Array<{ name: string; vhost: string; type?: string; durable?: boolean; auto_delete?: boolean; internal?: boolean }>;
    queues?: Array<{ name: string; vhost: string; durable?: boolean; exclusive?: boolean; auto_delete?: boolean; type?: string; arguments?: Record<string, string | number> }>;
    bindings?: Array<{ source: string; vhost: string; destination: string; destination_type?: string; routing_key?: string; arguments?: Record<string, string | number> }>;
    policies?: Array<{ vhost: string; name: string; pattern: string; "apply-to"?: string; priority?: number; definition?: Record<string, string | number> }>;
  }) {
    for (const vh of body.vhosts ?? []) this.ensureBuiltins(vh.name);
    for (const u of body.users ?? []) {
      const rawTags = u.tags ?? "";
      const tags = Array.isArray(rawTags) ? rawTags : rawTags.split(/[,\s]+/).filter(Boolean);
      if (u.password) await this.putUser(u.name, u.password, tags, true);
      else if (u.password_hash) {
        this.users.set(u.name, { hash: u.password_hash, tags });
        this.store.putUser({ name: u.name, hash: u.password_hash, tags });
      }
    }
    for (const p of body.permissions ?? []) await this.putPerm(p);
    for (const ex of body.exchanges ?? []) {
      if (!ex.name || ex.name.startsWith("amq.")) continue;
      await this.declareExchange(ex.vhost, ex.name, ex.type ?? "direct", !!ex.durable, !!ex.auto_delete, !!ex.internal, null);
    }
    for (const p of body.policies ?? []) {
      const apply = p["apply-to"] ?? "all";
      if (apply !== "queues" && apply !== "exchanges" && apply !== "all") continue;
      const def = p.definition ?? {};
      const num = (k: string) => {
        const v = def[k];
        if (v == null || v === "") return null;
        const n = Number(v);
        return Number.isFinite(n) && n > 0 ? n : null;
      };
      const str = (k: string) => {
        const v = def[k];
        return v == null || v === "" ? null : String(v);
      };
      this.upsertPolicy({
        vhost: p.vhost,
        name: p.name,
        pattern: p.pattern,
        applyTo: apply,
        priority: p.priority ?? 0,
        messageTtl: num("message-ttl"),
        expiresMs: num("expires"),
        dlx: str("dead-letter-exchange"),
        dlxKey: str("dead-letter-routing-key"),
        maxLength: num("max-length"),
        maxLengthBytes: num("max-length-bytes"),
        overflow: overflowOf(str("overflow")),
        dlxStrategy: str("dead-letter-strategy") === "at-least-once" ? "at-least-once" : str("dead-letter-strategy") === "at-most-once" ? "at-most-once" : null,
        deliveryLimit: num("delivery-limit"),
        alternate: str("alternate-exchange"),
      });
    }
    for (const q of body.queues ?? []) {
      if (q.exclusive) continue;
      const args = { ...(q.arguments ?? {}) };
      if (q.type && args["x-queue-type"] == null) args["x-queue-type"] = q.type;
      await this.declareQueue({
        vhost: q.vhost,
        name: q.name,
        durable: !!q.durable,
        exclusive: false,
        autoDelete: !!q.auto_delete,
        passive: false,
        args,
      });
    }
    for (const b of body.bindings ?? []) {
      if (b.destination_type && b.destination_type !== "queue") continue;
      if (!b.source) continue;
      const args = Object.entries(b.arguments ?? {}).map(([k, v]) =>
        [k, typeof v === "number" ? { t: "I" as const, v } : { t: "S" as const, v: String(v) }] as [string, Field],
      );
      await this.bind(b.vhost, b.source, b.destination, b.routing_key ?? "", args);
    }
  }

  listUsers() {
    return [...this.users.entries()].map(([name, u]) => ({ name, tags: u.tags }));
  }

  userTags(name: string) {
    return this.users.get(name)?.tags ?? [];
  }

  sweep() {
    const now = Date.now();
    for (const q of [...this.queues.values()]) {
      if (!this.isLocalHome(q.home)) continue;
      this.pump(q);
      const expires = q.argsParsed.expiresMs;
      if (expires != null && q.consumers.length === 0 && now - q.lastUsed >= expires) {
        void this.deleteQueue(q.vhost, q.name).catch(() => {});
      }
    }
  }
}

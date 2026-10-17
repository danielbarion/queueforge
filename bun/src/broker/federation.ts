/**
 * Federation of exchanges and queues, as RabbitMQ's federation plugin.
 *
 * Upstreams are the `federation-upstream` runtime parameter, sets the
 * `federation-upstream-set` parameter (plus the implicit set `all`), and a
 * policy with `federation-upstream` or `federation-upstream-set` names what a
 * matching exchange or queue federates from. Everything is stored like any
 * other parameter and policy, so links come back after a restart:
 * {@link syncFederation} derives the links from that state and starts or
 * stops them whenever it changes.
 *
 * An exchange link consumes the upstream exchange through an internal queue
 * bound with the downstream exchange's binding keys, kept in sync on bind and
 * unbind, and republishes into the downstream exchange with an
 * `x-received-from` hop list; `max-hops` stops loops.
 *
 * A queue link moves messages from the upstream queue into the downstream
 * queue only while the downstream queue has a consumer with spare credit and
 * nothing ready, and acks upstream after the downstream enqueue.
 *
 * A URI with no host (`amqp://`, `amqp:///vhost`) is this broker and runs in
 * process; any other URI is dialed with amqplib. None of this runs on the
 * publish or deliver path of an unfederated resource.
 */
import amqp from "amqplib";
import { R, readTable, replaceHeaderTable, W, writeTable, type Field } from "../codec.ts";
import { Broker } from "./class.ts";
import type { Consumer, LiveMsg, Policy } from "./model.ts";
import { queueHome } from "./routing.ts";
import { localVhost } from "./shovel.ts";

export type Upstream = {
  name: string;
  uris: string[];
  exchange: string | null;
  queue: string | null;
  maxHops: number;
  prefetch: number;
  reconnectDelay: number;
  ackMode: "on-confirm" | "on-publish" | "no-ack";
  trustUserId: boolean;
  expires: number | null;
  messageTtl: number | null;
};

export type FedLinkStatus = {
  node: string;
  vhost: string;
  id: string;
  upstream: string;
  type: "exchange" | "queue";
  exchange?: string;
  upstream_exchange?: string;
  queue?: string;
  upstream_queue?: string;
  uri: string;
  status: "starting" | "running" | "error";
  error?: string;
  local_connection?: string;
  timestamp: string;
};

/** One running link. `resync` brings an exchange link's bindings up to date. */
export type FedLink = {
  key: string;
  status: FedLinkStatus;
  /** Upstream definition the link was started with, to restart it on change. */
  spec: string;
  stop: () => Promise<void>;
  resync?: () => Promise<void>;
};

/** `amqp://user:secret@host` without the secret. */
export function stripPassword(uri: string): string {
  return uri.replace(/^(amqps?:\/\/[^:@/]*):[^@/]*@/, "$1@");
}

function num(v: unknown, d: number): number {
  const n = Number(v);
  return v != null && v !== "" && Number.isFinite(n) ? n : d;
}

/** Read one upstream definition. Unknown fields are ignored. */
export function upstreamFrom(name: string, value: unknown): Upstream | null {
  if (!value || typeof value !== "object") return null;
  const v = value as Record<string, unknown>;
  const uris = (Array.isArray(v.uri) ? v.uri : [v.uri]).filter((u): u is string => typeof u === "string" && u !== "");
  if (!uris.length) return null;
  const ackMode = v["ack-mode"] === "on-publish" || v["ack-mode"] === "no-ack" ? v["ack-mode"] : "on-confirm";
  return {
    name,
    uris,
    exchange: typeof v.exchange === "string" && v.exchange ? v.exchange : null,
    queue: typeof v.queue === "string" && v.queue ? v.queue : null,
    maxHops: Math.max(1, num(v["max-hops"], 1)),
    prefetch: Math.max(1, num(v["prefetch-count"], 1000)),
    reconnectDelay: Math.max(0, num(v["reconnect-delay"], 5)),
    ackMode,
    trustUserId: v["trust-user-id"] === true,
    expires: v.expires == null ? null : num(v.expires, 0) || null,
    messageTtl: v["message-ttl"] == null ? null : num(v["message-ttl"], 0) || null,
  };
}

/** Properties with `headers` as the header table, adding the table when there was none. */
export function withHeaders(propRaw: Uint8Array, headers: Array<[string, Field]>): Uint8Array {
  if (propRaw.length < 2) {
    const w = new W();
    w.u16(0x2000);
    writeTable(w, headers);
    return w.concat();
  }
  const flags = (propRaw[0]! << 8) | propRaw[1]!;
  if (flags & 0x2000) return replaceHeaderTable(propRaw, headers);
  const r = new R(propRaw);
  r.u16();
  if (flags & 0x8000) r.shortstr();
  if (flags & 0x4000) r.shortstr();
  const w = new W();
  w.u16(flags | 0x2000);
  w.bytes(propRaw.subarray(2, r.o));
  writeTable(w, headers);
  w.bytes(propRaw.subarray(r.o));
  return w.concat();
}

function readHeaders(propRaw: Uint8Array): Array<[string, Field]> {
  if (propRaw.length < 2) return [];
  const flags = (propRaw[0]! << 8) | propRaw[1]!;
  if (!(flags & 0x2000)) return [];
  const r = new R(propRaw);
  r.u16();
  if (flags & 0x8000) r.shortstr();
  if (flags & 0x4000) r.shortstr();
  return readTable(r);
}

/**
 * The hop list a federated copy carries: the previous `x-received-from` with
 * this hop appended. Null when the copy has already made `maxHops` hops or
 * has passed through `downstream` on this cluster before.
 */
export function receivedFrom(
  headers: Array<[string, Field]>,
  hop: { uri: string; exchange: string; vhost: string; cluster: string; redelivered: boolean },
  maxHops: number,
  downstream: { exchange: string; vhost: string },
): Array<[string, Field]> | null {
  const prev = headers.find(([k]) => k === "x-received-from")?.[1];
  const list: Field[] = prev?.t === "A" ? [...prev.v] : [];
  if (list.length >= maxHops) return null;
  for (const item of list) {
    if (item.t !== "F") continue;
    const get = (k: string) => {
      const f = item.v.find(([key]) => key === k)?.[1];
      return f && (f.t === "S" || f.t === "s") ? String(f.v) : "";
    };
    if (get("cluster-name") === hop.cluster && get("exchange") === downstream.exchange && (get("vhost") || downstream.vhost) === downstream.vhost) return null;
  }
  list.push({
    t: "F",
    v: [
      ["uri", { t: "S", v: stripPassword(hop.uri) }],
      ["exchange", { t: "S", v: hop.exchange }],
      ["redelivered", { t: "t", v: hop.redelivered }],
      ["cluster-name", { t: "S", v: hop.cluster }],
      ["vhost", { t: "S", v: hop.vhost }],
    ],
  });
  return [...headers.filter(([k]) => k !== "x-received-from"), ["x-received-from", { t: "A", v: list }]];
}

/** Upstreams a policy names, in the policy's vhost. */
function upstreamsFor(this: Broker, policy: Policy): Upstream[] {
  const vhost = policy.vhost;
  const all = this.listRuntimeParams("federation-upstream", vhost);
  const byName = (name: string) => {
    const row = all.find((r) => r.name === name);
    return row ? upstreamFrom(row.name, row.value) : null;
  };
  if (policy.federationUpstream) {
    const one = byName(policy.federationUpstream);
    return one ? [one] : [];
  }
  const setName = policy.federationUpstreamSet;
  if (!setName) return [];
  if (setName === "all") return all.map((r) => upstreamFrom(r.name, r.value)).filter((u): u is Upstream => !!u);
  const set = this.listRuntimeParams("federation-upstream-set", vhost).find((r) => r.name === setName);
  const out: Upstream[] = [];
  for (const item of Array.isArray(set?.value) ? (set.value as Array<Record<string, unknown>>) : []) {
    const name = typeof item?.upstream === "string" ? item.upstream : "";
    const row = all.find((r) => r.name === name);
    // Set entries may override the upstream's own fields.
    const merged = row ? upstreamFrom(name, { ...(row.value as Record<string, unknown>), ...item }) : null;
    if (merged) out.push(merged);
  }
  return out;
}

function federates(p: Policy | null): p is Policy {
  return !!p && !!(p.federationUpstream || p.federationUpstreamSet);
}

/**
 * Whether this process runs the link of one downstream exchange or queue.
 *
 * With one worker per core every worker sees the same topology, policies and
 * upstreams, and each link runs on exactly one of them: a queue link where
 * the downstream queue's consumers and ready messages are (its home, or its
 * quorum leader), an exchange link on the worker the exchange name hashes
 * to. Elsewhere (one process, or a cluster of separate nodes) every process
 * runs its links, as before.
 */
function ownsLink(this: Broker, type: "exchange" | "queue", vhost: string, name: string): boolean {
  if (!this.workerPool()) return true;
  if (type === "exchange") return queueHome(vhost, `federation-exchange:${name}`, this.cfg.members) === this.cfg.nodeId;
  const q = this.queues.get(this.key(vhost, name));
  if (!q) return false;
  if (q.argsParsed.queueType === "quorum") return this.isQuorumLeader(q);
  return this.isLocalHome(q.home);
}

/**
 * Start the links the current policies and upstreams call for and stop the
 * rest. Cheap when nothing is federated: one scan of the policy list.
 */
export function syncFederation(this: Broker): void {
  if (!this.federationOn) return;
  if (this.fedLinks.size === 0 && !this.policies.some(federates)) return;
  const want = new Map<string, { type: "exchange" | "queue"; vhost: string; name: string; up: Upstream }>();
  for (const ex of this.exchanges.values()) {
    if (ex.name === "" || ex.name.startsWith("amq.")) continue;
    const p = this.matchPolicy(ex.vhost, ex.name, "exchanges");
    if (!federates(p) || !ownsLink.call(this, "exchange", ex.vhost, ex.name)) continue;
    for (const up of upstreamsFor.call(this, p)) want.set(`exchange\0${ex.vhost}\0${ex.name}\0${up.name}`, { type: "exchange", vhost: ex.vhost, name: ex.name, up });
  }
  for (const q of this.queues.values()) {
    if (q.exclusive || q.name.startsWith("federation: ")) continue;
    const p = this.matchPolicy(q.vhost, q.name, "queues");
    if (!federates(p) || !ownsLink.call(this, "queue", q.vhost, q.name)) continue;
    for (const up of upstreamsFor.call(this, p)) want.set(`queue\0${q.vhost}\0${q.name}\0${up.name}`, { type: "queue", vhost: q.vhost, name: q.name, up });
  }
  for (const [key, link] of this.fedLinks) {
    const w = want.get(key);
    if (w && JSON.stringify(w.up) === link.spec) continue;
    this.fedLinks.delete(key);
    void link.stop().catch(() => {});
  }
  for (const [key, w] of want) {
    if (this.fedLinks.has(key)) continue;
    const link = w.type === "exchange" ? exchangeLink.call(this, key, w.vhost, w.name, w.up) : queueLink.call(this, key, w.vhost, w.name, w.up);
    this.fedLinks.set(key, link);
  }
}

/** A downstream exchange's bindings changed: re-bind its links' internal queues. */
export function federationBindingsChanged(this: Broker, vhost: string, exchange: string): void {
  for (const link of this.fedLinks.values()) {
    if (link.status.type === "exchange" && link.status.vhost === vhost && link.status.exchange === exchange) void link.resync?.().catch(() => {});
  }
}

/** Link status rows of the links this process runs. */
export function federationLinks(this: Broker, vhost?: string): FedLinkStatus[] {
  return [...this.fedLinks.values()].map((l) => l.status).filter((s) => vhost == null || s.vhost === vhost);
}

/**
 * Link status rows for `/api/federation-links`: this process's links and,
 * with one worker per core, every other worker's. A worker that does not
 * answer contributes none.
 */
export async function allFederationLinks(this: Broker, vhost?: string): Promise<FedLinkStatus[]> {
  const out = this.federationLinks(vhost);
  if (!this.workerPool() || !this.cluster) return out;
  const peers = this.cfg.members.filter((m) => m.id !== this.cfg.nodeId && this.cluster!.peers.has(m.id));
  const rows = await Promise.all(
    peers.map((m) =>
      Promise.race([
        this.cluster!.call(m.id, "federation_links", {}).catch(() => []),
        new Promise<unknown>((done) => setTimeout(() => done([]), 2000).unref?.()),
      ]),
    ),
  );
  for (const list of rows) {
    for (const s of Array.isArray(list) ? (list as FedLinkStatus[]) : []) if (vhost == null || s.vhost === vhost) out.push(s);
  }
  return out;
}

function newStatus(this: Broker, type: "exchange" | "queue", vhost: string, name: string, up: Upstream, upName: string): FedLinkStatus {
  return {
    node: this.cfg.nodeId || "queueforge",
    vhost,
    id: `${type}:${vhost}:${name}:${up.name}`,
    upstream: up.name,
    type,
    ...(type === "exchange" ? { exchange: name, upstream_exchange: upName } : { queue: name, upstream_queue: upName }),
    uri: stripPassword(up.uris[0]!),
    status: "starting",
    timestamp: new Date().toISOString(),
  };
}

function setStatus(s: FedLinkStatus, status: FedLinkStatus["status"], error?: string) {
  s.status = status;
  s.timestamp = new Date().toISOString();
  if (error) s.error = error;
  else delete s.error;
}

/** The binding keys (with arguments) the downstream exchange routes on. */
function downstreamKeys(this: Broker, vhost: string, exchange: string): Map<string, { key: string; args: Array<[string, Field]> }> {
  const out = new Map<string, { key: string; args: Array<[string, Field]> }>();
  for (const b of this.bindings) {
    if (b.vhost !== vhost || b.exchange !== exchange) continue;
    out.set(`${b.routingKey}\0${JSON.stringify(b.args ?? [])}`, { key: b.routingKey, args: b.args ?? [] });
  }
  for (const e of this.e2e) {
    if (e.vhost === vhost && e.source === exchange) out.set(`${e.routingKey}\0[]`, { key: e.routingKey, args: [] });
  }
  return out;
}

/** Republish one federated copy into the downstream exchange or queue. */
async function republish(
  this: Broker,
  up: Upstream,
  to: { vhost: string; exchange: string; routingKey: string; hopExchange: string },
  msg: { body: Uint8Array; routingKey: string; propRaw: Uint8Array; headers: Array<[string, Field]>; persistent: boolean; priority: number; redelivered: boolean },
  uri: string,
): Promise<boolean> {
  const headers = receivedFrom(
    msg.headers,
    { uri, exchange: to.hopExchange, vhost: to.vhost, cluster: this.clusterName(), redelivered: msg.redelivered },
    up.maxHops,
    { exchange: to.exchange, vhost: to.vhost },
  );
  // Past max-hops or a loop: the copy is settled upstream and dropped.
  if (!headers) return true;
  const result = await this.publish({
    vhost: to.vhost,
    exchange: to.exchange,
    routingKey: to.routingKey,
    body: msg.body,
    headers,
    propRaw: withHeaders(msg.propRaw, headers),
    persistent: msg.persistent,
    priority: msg.priority,
    expiration: "",
    confirm: up.ackMode === "on-confirm",
  });
  return result !== "nack";
}

/** Retry `start` after the reconnect delay until `stopped()`. */
function retry(up: Upstream, status: FedLinkStatus, stopped: () => boolean, start: () => Promise<void>) {
  const run = async () => {
    if (stopped()) return;
    try {
      await start();
    } catch (err) {
      if (stopped()) return;
      setStatus(status, "error", err instanceof Error ? err.message : String(err));
      const t = setTimeout(run, Math.max(250, up.reconnectDelay * 1000));
      t.unref?.();
    }
  };
  void run();
}

function exchangeLink(this: Broker, key: string, vhost: string, exchange: string, up: Upstream): FedLink {
  const upExchange = up.exchange ?? exchange;
  const status = newStatus.call(this, "exchange", vhost, exchange, up, upExchange);
  const internal = `federation: ${upExchange} -> ${this.cfg.nodeId || "queueforge"} ${vhost}:${exchange}`;
  const queueArgs: Record<string, number> = {};
  if (up.expires) queueArgs["x-expires"] = up.expires;
  if (up.messageTtl) queueArgs["x-message-ttl"] = up.messageTtl;
  let stopped = false;
  let bound = new Map<string, { key: string; args: Array<[string, Field]> }>();
  let stopLink: (() => Promise<void>) | null = null;
  let bindRemote: ((k: string, args: Array<[string, Field]>, add: boolean) => Promise<void>) | null = null;
  const uri = up.uris[0]!;
  const upVhost = localVhost(uri, vhost);

  const resync = async () => {
    if (stopped || !bindRemote) return;
    const want = downstreamKeys.call(this, vhost, exchange);
    for (const [k, b] of want) if (!bound.has(k)) await bindRemote(b.key, b.args, true);
    for (const [k, b] of bound) if (!want.has(k)) await bindRemote(b.key, b.args, false);
    bound = want;
  };

  const startLocal = async (from: string) => {
    if (!this.exchanges.has(this.key(from, upExchange))) throw new Error(`no exchange '${upExchange}' in vhost '${from}' upstream`);
    await this.declareQueue({ vhost: from, name: internal, durable: true, exclusive: false, autoDelete: false, passive: false, args: queueArgs });
    bindRemote = async (k, args, add) => {
      if (add) await this.bind(from, upExchange, internal, k, args);
      else await this.unbind(from, upExchange, internal, k, args);
    };
    bound = new Map();
    await resync();
    let inflight = 0;
    const tag = `federation-link-${status.id}`;
    const consumer: Consumer = {
      tag,
      session: this.nextSession(),
      noAck: up.ackMode === "no-ack",
      exclusive: false,
      prefetch: up.prefetch,
      want: () => !stopped && inflight < up.prefetch,
      deliver: (msg: LiveMsg) => {
        inflight++;
        void republish
          .call(this, up, { vhost, exchange, routingKey: msg.routingKey, hopExchange: upExchange }, msg, uri)
          .then(
            (ok) => {
              if (consumer.noAck) return;
              return ok ? this.ack(from, internal, msg.id) : this.nack(from, internal, msg.id, true);
            },
            () => (consumer.noAck ? undefined : this.nack(from, internal, msg.id, true)),
          )
          .catch(() => {})
          .finally(() => {
            inflight--;
            const q = this.queues.get(this.key(from, internal));
            if (q && !stopped) void this.kick(from, internal, consumer.session);
            void q;
          });
      },
    };
    await this.consume(from, internal, consumer);
    stopLink = async () => {
      await this.cancel(from, internal, tag).catch(() => {});
      // The link is gone for good: so is its queue, as RabbitMQ removes it.
      await this.deleteQueue(from, internal).catch(() => 0);
    };
    setStatus(status, "running");
    await this.kick(from, internal, consumer.session);
  };

  const startRemote = async () => {
    const conn = await amqp.connect(uri);
    const ch = await conn.createChannel();
    const closed = () => {
      bindRemote = null;
      if (stopped) return;
      setStatus(status, "error", "upstream connection closed");
      const t = setTimeout(() => retry(up, status, () => stopped, startRemote), Math.max(250, up.reconnectDelay * 1000));
      t.unref?.();
    };
    conn.on("error", () => {});
    conn.on("close", closed);
    await ch.assertQueue(internal, { durable: true, arguments: queueArgs });
    await ch.prefetch(up.prefetch);
    bindRemote = async (k, args, add) => {
      const table = Object.fromEntries(args.map(([name, f]) => [name, "v" in f ? f.v : null]));
      if (add) await ch.bindQueue(internal, upExchange, k, table);
      else await ch.unbindQueue(internal, upExchange, k, table);
    };
    bound = new Map();
    await resync();
    await ch.consume(internal, (m) => {
      if (!m) return;
      const propRaw = encodeAmqplibProps(m.properties);
      const msg = {
        body: new Uint8Array(m.content),
        routingKey: m.fields.routingKey,
        propRaw,
        headers: readHeaders(propRaw),
        persistent: m.properties.deliveryMode === 2,
        priority: Number(m.properties.priority ?? 0),
        redelivered: m.fields.redelivered,
      };
      void republish
        .call(this, up, { vhost, exchange, routingKey: msg.routingKey, hopExchange: upExchange }, msg, uri)
        .then((ok) => {
          if (up.ackMode === "no-ack") return;
          if (ok) ch.ack(m);
          else ch.nack(m, false, true);
        })
        .catch(() => ch.nack(m, false, true));
    }, { noAck: up.ackMode === "no-ack" });
    stopLink = async () => {
      conn.off("close", closed);
      await ch.deleteQueue(internal).catch(() => {});
      await conn.close().catch(() => {});
    };
    setStatus(status, "running");
  };

  retry(up, status, () => stopped, () => (upVhost != null ? startLocal(upVhost) : startRemote()));
  return {
    key,
    status,
    spec: JSON.stringify(up),
    resync,
    stop: async () => {
      stopped = true;
      bindRemote = null;
      await stopLink?.();
    },
  };
}

function queueLink(this: Broker, key: string, vhost: string, queue: string, up: Upstream): FedLink {
  const upQueue = up.queue ?? queue;
  const status = newStatus.call(this, "queue", vhost, queue, up, upQueue);
  const uri = up.uris[0]!;
  const upVhost = localVhost(uri, vhost);
  let stopped = false;
  let timer: Timer | null = null;
  let busy = false;
  let stopLink: (() => Promise<void>) | null = null;

  /** The downstream queue has a consumer with spare credit and nothing ready. */
  const hungry = (): boolean => {
    const dq = this.queues.get(this.key(vhost, queue));
    if (!dq || dq.ready.length > 0) return false;
    return dq.consumers.some((c) => c.want());
  };
  const into = { vhost, exchange: "", routingKey: queue, hopExchange: "" };

  const startLocal = async (from: string) => {
    if (!this.queues.has(this.key(from, upQueue))) throw new Error(`no queue '${upQueue}' in vhost '${from}' upstream`);
    const pull = async () => {
      if (busy || stopped) return;
      busy = true;
      try {
        for (let i = 0; i < up.prefetch && hungry(); i++) {
          const msg = await this.get(from, upQueue, false);
          if (!msg) break;
          const ok = await republish.call(this, up, into, msg, uri).catch(() => false);
          if (ok) await this.ack(from, upQueue, msg.id);
          else {
            await this.nack(from, upQueue, msg.id, true);
            break;
          }
        }
        setStatus(status, "running");
      } catch (err) {
        setStatus(status, "error", err instanceof Error ? err.message : String(err));
      } finally {
        busy = false;
      }
    };
    timer = setInterval(() => void pull(), 50);
    timer.unref?.();
    stopLink = async () => {
      if (timer) clearInterval(timer);
    };
    setStatus(status, "running");
  };

  const startRemote = async () => {
    const conn = await amqp.connect(uri);
    const ch = await conn.createChannel();
    conn.on("error", () => {});
    const closed = () => {
      if (timer) clearInterval(timer);
      if (stopped) return;
      setStatus(status, "error", "upstream connection closed");
      const t = setTimeout(() => retry(up, status, () => stopped, startRemote), Math.max(250, up.reconnectDelay * 1000));
      t.unref?.();
    };
    conn.on("close", closed);
    await ch.checkQueue(upQueue);
    const pull = async () => {
      if (busy || stopped) return;
      busy = true;
      try {
        for (let i = 0; i < up.prefetch && hungry(); i++) {
          const m = await ch.get(upQueue, { noAck: false });
          if (!m) break;
          const propRaw = encodeAmqplibProps(m.properties);
          const ok = await republish
            .call(this, up, into, {
              body: new Uint8Array(m.content),
              routingKey: m.fields.routingKey,
              propRaw,
              headers: readHeaders(propRaw),
              persistent: m.properties.deliveryMode === 2,
              priority: Number(m.properties.priority ?? 0),
              redelivered: m.fields.redelivered,
            }, uri)
            .catch(() => false);
          if (ok) ch.ack(m);
          else {
            ch.nack(m, false, true);
            break;
          }
        }
      } catch {
        /* the close handler reports the error */
      } finally {
        busy = false;
      }
    };
    timer = setInterval(() => void pull(), 50);
    timer.unref?.();
    stopLink = async () => {
      if (timer) clearInterval(timer);
      conn.off("close", closed);
      await conn.close().catch(() => {});
    };
    setStatus(status, "running");
  };

  retry(up, status, () => stopped, () => (upVhost != null ? startLocal(upVhost) : startRemote()));
  return {
    key,
    status,
    spec: JSON.stringify(up),
    stop: async () => {
      stopped = true;
      if (timer) clearInterval(timer);
      await stopLink?.();
    },
  };
}

/** AMQP basic properties from amqplib's decoded form, as raw property bytes. */
function encodeAmqplibProps(p: amqp.MessageProperties): Uint8Array {
  const w = new W();
  let flags = 0;
  const parts: Array<(w: W) => void> = [];
  const short = (bit: number, v: unknown) => {
    if (v == null || v === "") return;
    flags |= bit;
    parts.push((x) => x.shortstr(String(v)));
  };
  short(0x8000, p.contentType);
  short(0x4000, p.contentEncoding);
  const headers = p.headers ? Object.entries(p.headers).map(([k, v]) => [k, toField(v)] as [string, Field]) : [];
  if (headers.length) {
    flags |= 0x2000;
    parts.push((x) => writeTable(x, headers));
  }
  if (p.deliveryMode != null) {
    flags |= 0x1000;
    parts.push((x) => x.u8(Number(p.deliveryMode)));
  }
  if (p.priority != null) {
    flags |= 0x0800;
    parts.push((x) => x.u8(Number(p.priority)));
  }
  short(0x0400, p.correlationId);
  short(0x0200, p.replyTo);
  short(0x0100, p.expiration);
  short(0x0080, p.messageId);
  if (p.timestamp != null) {
    flags |= 0x0040;
    parts.push((x) => x.u64(Number(p.timestamp)));
  }
  short(0x0020, p.type);
  short(0x0010, p.userId);
  short(0x0008, p.appId);
  w.u16(flags);
  for (const part of parts) part(w);
  return w.concat();
}

function toField(v: unknown): Field {
  if (typeof v === "boolean") return { t: "t", v };
  if (typeof v === "number") return Number.isInteger(v) ? { t: "l", v } : { t: "S", v: String(v) };
  if (Array.isArray(v)) return { t: "A", v: v.map(toField) };
  if (v instanceof Uint8Array) return { t: "x", v };
  if (v && typeof v === "object") return { t: "F", v: Object.entries(v).map(([k, x]) => [k, toField(x)] as [string, Field]) };
  return { t: "S", v: v == null ? "" : String(v) };
}

Broker.prototype.syncFederation = syncFederation;
Broker.prototype.federationBindingsChanged = federationBindingsChanged;
Broker.prototype.federationLinks = federationLinks;
Broker.prototype.allFederationLinks = allFederationLinks;

declare module "./class.ts" {
  interface Broker {
    syncFederation: typeof syncFederation;
    federationBindingsChanged: typeof federationBindingsChanged;
    federationLinks: typeof federationLinks;
    allFederationLinks: typeof allFederationLinks;
  }
}

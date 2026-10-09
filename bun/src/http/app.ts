/**
 * Bun management HTTP routes.
 *
 * `managementApp` is one route table. It stays in this file because splitting
 * the function would change how the handlers close over the broker.
 */
import { Elysia } from "elysia";
import { join } from "node:path";
import { addFederationPolicy, addFederationUpstream, addFederationUri, fedUris, policyFromBody, policyItem, type Broker } from "../broker/index.ts";
import type { Policy } from "../broker/model.ts";
import { policyToWire } from "../broker/snapshot.ts";
import { exchangeFromPattern, runFederation } from "../bridge.ts";
import { metricsText } from "./metrics.ts";
import { MqttSession } from "../protocols/mqtt.ts";
import { StompSession } from "../protocols/stomp.ts";
import { availableBytes } from "../disk.ts";
import { cookieNameFromHost, requireUser as requireSession, sessions, tokenOf, verifyBasic, SESSION_LIFETIME_MS, type Session } from "./session.ts";

/** Sessions behind each management WebSocket. */
const wsSessions = new WeakMap<object, MqttSession | StompSession>();
const textDecoder = new TextDecoder();
/** STOMP frames go out as text WebSocket messages, as RabbitMQ sends them. */
function textOf(frame: Uint8Array): string {
  return textDecoder.decode(frame);
}

/**
 * Permission routes take user/vhost, the order the admin UI sends, or
 * vhost/user, RabbitMQ's order. Whichever name is a known vhost is the vhost.
 */
function permTarget(broker: Broker, first: string, second: string): { user: string; vhost: string } {
  const a = decodeURIComponent(first);
  const b = decodeURIComponent(second);
  const vhosts = broker["vhosts"] as Set<string>;
  if (vhosts.has(a) && !vhosts.has(b)) return { vhost: a, user: b };
  return { user: a, vhost: b };
}

/**
 * Build the management server.
 *
 * @param broker Broker whose state the routes read and update.
 * @param spaDir Directory of the admin UI. Unknown paths fall back to its index.html.
 * @returns An Elysia app. The caller listens on the management host and port.
 * Cookie names come from the request Host header. Login tokens live in the
 * process-local session map.
 */
/** RabbitMQ's `leader` and `members` of a quorum queue or replicated stream row. */
function quorumMembers(broker: Broker, q: Parameters<Broker["quorumLeader"]>[0]) {
  if (!q || (q.argsParsed.queueType !== "quorum" && !(q.argsParsed.queueType === "stream" && q.raftGroup))) return {};
  const node = broker.cluster?.consensus?.node;
  const members = node ? node.members(q.raftGroup || "quorum") : broker.cfg.members.map((m) => m.id);
  return {
    leader: broker.quorumLeader(q) || null,
    members: members.length ? members : [broker.cfg.nodeId || "queueforge"],
    raft_group: q.raftGroup || "quorum",
  };
}

/** RabbitMQ 4.3 flags whose behaviour this broker has, plus QueueForge's raft. */
const REQUIRED_FLAGS: Array<{ name: string; desc: string }> = [
  { name: "quorum_queue", desc: "Support queues of type `quorum`" },
  { name: "stream_queue", desc: "Support queues of type `stream`" },
  { name: "implicit_default_bindings", desc: "Default bindings are now implicit, instead of being stored in the database" },
  { name: "user_limits", desc: "Configure connection and channel limits for a user" },
];

function featureFlags(broker: Broker) {
  const flag = (name: string, state: string, stability: string, desc: string, provider = "rabbit") => ({
    name,
    desc,
    doc_url: "",
    state,
    stability,
    require_level: stability === "required" ? "hard" : "none",
    experiment_level: "supported",
    callbacks: [],
    provided_by: provider,
  });
  const items = REQUIRED_FLAGS.map((f) => flag(f.name, "enabled", "required", f.desc));
  // Raft is a feature flag as RabbitMQ's khepri_db: on by itself in a new
  // cluster, enabled by an operator after an upgrade (docs/raft.md, section 8).
  const raft = broker.cluster?.consensus?.flag() ?? "unsupported";
  if (raft !== "unsupported") items.push(flag("raft", raft, "stable", "Raft consensus for metadata and quorum queues", "queueforge"));
  items.push(flag("transient_nonexcl_queues", broker.transientNonexcl ? "enabled" : "disabled", "experimental", "Allow transient non-exclusive queues (a deprecated feature)", "queueforge"));
  return items.sort((a, b) => a.name.localeCompare(b.name));
}

export function managementApp(broker: Broker, spaDir: string) {
  const hasManagement = (tags: string[]) => tags.some((tag) => ["administrator", "management", "monitoring"].includes(tag));
  const identityOf = (user: string): string | null => {
    const row = broker.users.get(user);
    if (row) return row.hash;
    const principal = broker.principalOf(user);
    if (!principal) return null;
    // Equivalent re-logins keep existing sessions; a change in scopes or expiry invalidates them.
    return JSON.stringify({ ...principal, scopes: principal.scopes?.map((scope) => ({
      ...scope, vhost: scope.vhost.toString(), resource: scope.resource.toString(), routingKey: scope.routingKey?.toString() ?? null,
    })) ?? null });
  };
  const sessionOf = (user: string): Session | null => {
    const row = broker.users.get(user);
    const principal = row ? null : broker.principalOf(user);
    const tags = broker.userTags(user);
    if ((!row && !principal) || !hasManagement(tags)) return null;
    const now = Date.now();
    const expiresAt = Math.min(now + SESSION_LIFETIME_MS, principal?.expiresAt ?? Infinity);
    if (expiresAt <= now) return null;
    return { user, tags, identity: identityOf(user), expiresAt, lastSeen: now };
  };
  const validSession = (session: Session): boolean => {
    const row = broker.users.get(session.user);
    const principal = row ? null : broker.principalOf(session.user);
    if (session.identity !== identityOf(session.user) || (!row && !principal)) return false;
    if (principal?.expiresAt != null && principal.expiresAt <= Date.now()) return false;
    session.tags = broker.userTags(session.user);
    return hasManagement(session.tags);
  };
  const requireUser = (cookie: string | null, host: string | null, authorization: string | null = null) =>
    requireSession(cookie, host, authorization, validSession);
  const access = (request: Request, set: { status?: number | string }, vhost: string, checks: Array<["configure" | "write" | "read", string]>) => {
    const session = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
    if (!session) {
      set.status = 401;
      return { error: "unauthorized" };
    }
    if (!broker.hasVhostAccess(session.user, vhost) || checks.some(([kind, resource]) => !broker.can(session.user, vhost, kind, resource))) {
      set.status = 403;
      return { error: "forbidden" };
    }
    return null;
  };
  const enableFlag = (name: string, request: Request, set: { status?: number | string }) => {
    const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
    if (!s?.tags.includes("administrator")) {
      set.status = s ? 403 : 401;
      return { error: "forbidden" };
    }
    if (name === "transient_nonexcl_queues") {
      broker.transientNonexcl = true;
      set.status = 204;
      return "";
    }
    // quorum_queues is the name QueueForge listed before it used RabbitMQ's.
    if (name === "quorum_queues" || REQUIRED_FLAGS.some((f) => f.name === name)) {
      set.status = 204;
      return "";
    }
    if (name === "raft" && broker.cluster) {
      if (!broker.cluster.consensus.enableNow()) {
        set.status = 400;
        return { error: "bad_request", reason: "unsupported" };
      }
      broker.cluster.broadcastFeature("raft");
      set.status = 204;
      return "";
    }
    set.status = 400;
    return { error: "bad_request", reason: "unsupported" };
  };
  return new Elysia()
    // RabbitMQ tools authenticate with HTTP Basic. Check it before the route runs.
    .onRequest(async ({ request }) => {
      await verifyBasic(request.headers.get("authorization"), (u, p) => broker.verify(u, p), sessionOf, validSession);
    })
    .onBeforeHandle(({ request, params, set }) => {
      if (!/^\/api\/(queues|exchanges|bindings|consumers)\//.test(new URL(request.url).pathname)) return;
      const vhost = (params as Record<string, string>).vhost;
      if (vhost == null) return;
      const session = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!session) { set.status = 401; return { error: "unauthorized" }; }
      if (request.method === "GET" && (session.tags.includes("administrator") || session.tags.includes("monitoring"))) return;
      if (!broker.hasVhostAccess(session.user, decodeURIComponent(vhost))) { set.status = 403; return { error: "forbidden" }; }
    })
    // The admin UI reads `{ items }`. RabbitMQ's API answers lists as bare
    // arrays, so a Basic-auth caller gets the array.
    .onAfterHandle(({ request, response }) => {
      if (!request.headers.get("authorization")?.startsWith("Basic ")) return;
      const items = (response as { items?: unknown } | null)?.items;
      if (response && typeof response === "object" && !Array.isArray(response) && Array.isArray(items)) return items;
    })
    .get("/api/vhost-limits/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      return broker
        .listVhostLimits()
        .filter((row) => row.vhost === vhost)
        .map(({ vhost: v, ...value }) => ({ vhost: v, value: Object.fromEntries(Object.entries(value).filter(([, n]) => n != null)) }));
    })
    .get("/api/user-limits/:user", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const user = decodeURIComponent(params.user);
      return broker
        .listUserLimits()
        .filter((row) => row.user === user)
        .map(({ user: u, ...value }) => ({ user: u, value: Object.fromEntries(Object.entries(value).filter(([, n]) => n != null)) }));
    })
    // MQTT and STOMP over WebSocket on /ws, picked by subprotocol, as clients
    // ask for "mqtt" or "v12.stomp". RabbitMQ's plugins serve the same frames.
    .ws("/ws", {
      upgrade({ request }) {
        const offered = (request.headers.get("sec-websocket-protocol") ?? "").split(",").map((p) => p.trim());
        const chosen = offered.find((p) => p === "mqtt" || p === "mqttv3.1") ?? offered.find((p) => /^v1[0-2]\.stomp$|^stomp$/.test(p));
        return chosen ? { "sec-websocket-protocol": chosen } : {};
      },
      open(ws) {
        const offered = (ws.data.request.headers.get("sec-websocket-protocol") ?? "").split(",").map((p) => p.trim());
        const mqtt = offered.some((p) => p === "mqtt" || p === "mqttv3.1");
        const stomp = offered.some((p) => /^v1[0-2]\.stomp$|^stomp$/.test(p));
        if (!mqtt && !stomp) {
          ws.close();
          return;
        }
        const raw = ws.raw;
        const session = mqtt
          ? new MqttSession(broker, (frame) => void raw.send(frame), () => raw.close())
          : new StompSession(broker, (frame) => void raw.send(textOf(frame)), () => raw.close());
        wsSessions.set(raw, session);
      },
      message(ws, message) {
        const session = wsSessions.get(ws.raw);
        if (!session) return;
        if (typeof message === "string") session.feed(new TextEncoder().encode(message));
        else if (message instanceof Uint8Array) session.feed(message);
        else if (message instanceof ArrayBuffer) session.feed(new Uint8Array(message));
        else session.feed(new TextEncoder().encode(JSON.stringify(message)));
      },
      close(ws) {
        const session = wsSessions.get(ws.raw);
        wsSessions.delete(ws.raw);
        session?.closedByPeer();
      },
    })
    .get("/healthz", () => "ok\n")
    .get("/api/identity", () => ({ product_name: "QueueForge", kind: "bun" }))
    .get("/readyz", ({ set }) => {
      if (!broker.ready) {
        set.status = 503;
        return "not ready\n";
      }
      return "ready\n";
    })
    .post("/api/login", async ({ body, request, set }) => {
      const b = body as { username?: string; password?: string };
      const username = b.username ?? "";
      const password = b.password ?? "";
      if (!username || !password || !(await broker.verify(username, password))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const tags = broker.userTags(username);
      if (!tags.includes("administrator") && !tags.includes("management") && !tags.includes("monitoring")) {
        set.status = 403;
        return { error: "forbidden" };
      }
      const token = crypto.randomUUID().replaceAll("-", "");
      const session = sessionOf(username);
      if (!session) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      sessions.set(token, session);
      const name = cookieNameFromHost(request.headers.get("host"));
      set.headers["set-cookie"] = `${name}=${token}; HttpOnly; SameSite=Lax; Path=/`;
      return { name: username, tags };
    })
    .post("/api/logout", ({ request, set }) => {
      const name = cookieNameFromHost(request.headers.get("host"));
      const token = tokenOf(request.headers.get("cookie"), name);
      if (token) sessions.delete(token);
      set.status = 204;
      set.headers["set-cookie"] = `${name}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0`;
      return "";
    })
    .get("/api/whoami", ({ request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { name: s.user, tags: s.tags };
    })
    .get("/api/overview", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      let messages_ready = 0;
      let messages_unacked = 0;
      let consumers = 0;
      for (const q of broker.queues.values()) {
        messages_ready += q.ready.length;
        messages_unacked += q.unacked.size;
        consumers += q.consumers.length;
      }
      return {
        product_name: "QueueForge",
        product_version: "0.1.0",
        management_version: "0.1.0",
        rabbitmq_version_compat: "0.9.1",
        object_totals: {
          connections: broker.prom.connections,
          channels: broker.prom.channels,
          queues: broker.queues.size,
          exchanges: broker.exchanges.size,
          consumers,
          vhosts: broker["vhosts"].size,
        },
        queue_totals: {
          messages: messages_ready + messages_unacked,
          messages_ready,
          messages_unacknowledged: messages_unacked,
        },
        message_stats: {
          publish: broker.prom.received,
          deliver: broker.prom.delivered,
          ack: broker.prom.acknowledged,
        },
      };
    })
    .get("/api/vhosts", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { items: [...broker["vhosts"]].map((name) => ({ name, tracing: broker.tracedVhosts.has(name) })) };
    })
    .put("/api/vhosts/:vhost", async ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const name = decodeURIComponent(params.vhost);
      broker.ensureBuiltins(name);
      // RabbitMQ turns firehose tracing on and off with the vhost's `tracing` field.
      const tracing = (body as { tracing?: unknown } | null)?.tracing;
      if (tracing === true) broker.tracedVhosts.add(name);
      else if (tracing === false) broker.tracedVhosts.delete(name);
      await broker.cluster?.replicate("vhost", { name });
      set.status = 201;
      return { name };
    })
    .delete("/api/vhosts/:vhost", async ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const name = decodeURIComponent(params.vhost);
      broker["vhosts"].delete(name);
      broker.store.deleteVhost(name);
      await broker.cluster?.replicate("delete_vhost", { name });
      set.status = 204;
      return "";
    })
    .get("/api/users", ({ request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      return broker.listUsers();
    })
    .put("/api/users/:name", async ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const b = body as { password?: string; tags?: string[] };
      const existing = broker.userTags(params.name);
      await broker.putUser(params.name, b.password ?? null, b.tags ?? existing, existing.length === 0);
      set.status = existing.length ? 204 : 201;
      return existing.length ? "" : { name: params.name, tags: b.tags ?? [] };
    })
    .delete("/api/users/:name", async ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      await broker.deleteUser(params.name);
      set.status = 204;
      return "";
    })
    .put("/api/permissions/:user/:vhost", async ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const b = body as { configure: string; write: string; read: string };
      const { user, vhost } = permTarget(broker, params.user, params.vhost);
      await broker.putPerm({ user, vhost, configure: b.configure, write: b.write, read: b.read });
      set.status = 201;
      return { user, vhost, ...b };
    })
    .delete("/api/permissions/:user/:vhost", async ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const target = permTarget(broker, params.user, params.vhost);
      await broker.deletePerm(target.user, target.vhost);
      set.status = 204;
      return "";
    })
    .get("/api/queues/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const items = [...broker.queues.values()].filter((q) => q.vhost === vhost).map((q) => ({
        name: q.name,
        vhost,
        durable: q.durable,
        exclusive: q.exclusive,
        auto_delete: q.autoDelete,
        node: q.home || broker.cfg.nodeId || "queueforge",
        ...quorumMembers(broker, q),
        state: "running",
        messages: q.ready.length + q.unacked.size,
        messages_ready: q.ready.length,
        messages_unacknowledged: q.unacked.size,
        consumers: q.consumers.length,
        arguments: q.args,
        type: q.argsParsed.queueType,
      }));
      return { items, total_count: items.length };
    })
    .put("/api/queues/:vhost/:name", async ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const b = (body ?? {}) as { durable?: boolean; exclusive?: boolean; auto_delete?: boolean; arguments?: Record<string, string | number> };
      if (!b.durable && !b.exclusive && !broker.transientNonexcl) {
        set.status = 400;
        return { error: "bad_request", reason: "Feature `transient_nonexcl_queues` is deprecated. By default, this feature is not permitted anymore." };
      }
      const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["configure", params.name] ]);
      if (denied) return denied;
      const res = await broker.declareQueue({
        vhost,
        name: params.name,
        durable: !!b.durable,
        exclusive: !!b.exclusive,
        autoDelete: !!b.auto_delete,
        passive: false,
        args: b.arguments ?? {},
      });
      set.status = 201;
      return { name: res.name };
    })
    .delete("/api/queues/:vhost/:name", async ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["configure", params.name] ]);
      if (denied) return denied;
      await broker.deleteQueue(decodeURIComponent(params.vhost), params.name);
      set.status = 204;
      return "";
    })
    .get("/api/exchanges/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const items = [...broker.exchanges.values()].filter((e) => e.vhost === vhost).map((e) => ({
        name: e.name,
        vhost,
        type: e.kind,
        durable: e.durable,
        auto_delete: e.autoDelete,
        internal: e.internal,
      }));
      items.sort((a, b) => a.name.localeCompare(b.name));
      return { items, total_count: items.length };
    })
    .get("/api/bindings/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const items = broker.bindings.filter((b) => b.vhost === vhost).map((b) => ({
        source: b.exchange,
        destination: b.queue,
        destination_type: "queue",
        routing_key: b.routingKey,
        vhost,
        properties_key: b.routingKey,
      }));
      return { items, total_count: items.length };
    })
    .get("/api/connections", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const items = broker.listMgmtConnections();
      return { items, total_count: items.length };
    })
    .delete("/api/connections/:name", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      if (!broker.closeMgmtConnection(params.name)) {
        set.status = 404;
        return { error: "not found" };
      }
      set.status = 204;
      return "";
    })
    .post("/api/exchanges/:vhost/:name/publish", async ({ params, body, request, set }) => {
      const session = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!session) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const b = (body ?? {}) as { routing_key?: string; payload?: string; payload_encoding?: string };
      const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["write", params.name] ]);
      if (denied) return denied;
      if (!broker.topicWriteAllowed(session.user, vhost, params.name, b.routing_key ?? "")) {
        set.status = 403;
        return { error: "forbidden", reason: "write access to topic refused" };
      }
      const payload = b.payload ?? "";
      const bytes = b.payload_encoding === "base64" ? Buffer.from(payload, "base64") : Buffer.from(payload);
      const result = await broker.publish({
        vhost,
        exchange: params.name,
        routingKey: b.routing_key ?? "",
        body: new Uint8Array(bytes),
        headers: [],
        propRaw: new Uint8Array(),
        persistent: false,
        priority: 0,
        expiration: "",
        confirm: true,
      });
      return { routed: result !== "return" && result !== "nack" };
    })
    .post("/api/queues/:vhost/:name/get", async ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const b = (body ?? {}) as { count?: number; ackmode?: string };
      const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["read", params.name] ]);
      if (denied) return denied;
      const count = Math.max(1, Math.min(b.count ?? 1, 20));
      const noAck = b.ackmode === "ack_requeue_false" || b.ackmode === "reject_requeue_false";
      const out = [];
      for (let i = 0; i < count; i++) {
        const msg = await broker.get(vhost, params.name, noAck || b.ackmode !== "ack_requeue_true");
        if (!msg) break;
        const payload = Buffer.from(msg.body).toString("utf8");
        out.push({
          payload,
          payload_encoding: "string",
          payload_bytes: msg.body.byteLength,
          redelivered: msg.redelivered,
          exchange: msg.exchange,
          routing_key: msg.routingKey,
          properties: {},
        });
      }
      return out;
    })
    .put("/api/exchanges/:vhost/:name", async ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const b = (body ?? {}) as { type?: string; durable?: boolean; auto_delete?: boolean; internal?: boolean; arguments?: Record<string, string> };
      const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["configure", params.name] ]);
      if (denied) return denied;
      await broker.declareExchange(vhost, params.name, b.type ?? "direct", !!b.durable, !!b.auto_delete, !!b.internal, b.arguments?.["alternate-exchange"] ?? null, b.arguments?.["x-delayed-type"] ?? null);
      set.status = 201;
      return { name: params.name, type: b.type ?? "direct" };
    })
    .delete("/api/exchanges/:vhost/:name", async ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["configure", params.name] ]);
      if (denied) return denied;
      await broker.deleteExchange(decodeURIComponent(params.vhost), params.name);
      set.status = 204;
      return "";
    })
    .delete("/api/bindings/:vhost/:exchange/:queue/:key", async ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["read", params.exchange], ["write", params.queue] ]);
      if (denied) return denied;
      const user = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))!.user;
      if (!broker.topicReadAllowed(user, vhost, params.exchange, decodeURIComponent(params.key))) {
        set.status = 403;
        return { error: "forbidden", reason: "read access to topic refused" };
      }
      await broker.unbind(vhost, params.exchange, params.queue, decodeURIComponent(params.key), null);
      set.status = 204;
      return "";
    })
    .post("/api/bindings/:vhost", async ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const b = body as { source: string; destination: string; routing_key?: string; arguments?: Record<string, string | number> };
      const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["read", b.source], ["write", b.destination] ]);
      if (denied) return denied;
      const user = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))!.user;
      if (!broker.topicReadAllowed(user, vhost, b.source, b.routing_key ?? "")) {
        set.status = 403;
        return { error: "forbidden", reason: "read access to topic refused" };
      }
      const args = Object.entries(b.arguments ?? {}).map(([k, v]) => [k, typeof v === "number" ? { t: "I" as const, v } : { t: "S" as const, v: String(v) }] as [string, { t: "I"; v: number } | { t: "S"; v: string }]);
      await broker.bind(vhost, b.source, b.destination, b.routing_key ?? "", args);
      set.status = 201;
      return { source: b.source, destination: b.destination };
    })
    .get("/api/definitions", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return broker.exportDefinitions();
    })
    .post("/api/definitions", async ({ body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      await broker.importDefinitions(body as Parameters<Broker["importDefinitions"]>[0]);
      set.status = 204;
      return "";
    })
    .put("/api/policies/:vhost/:name", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const b = (body ?? {}) as {
        pattern?: string;
        "apply-to"?: string;
        priority?: number;
        definition?: Record<string, string | number>;
      };
      const apply = b["apply-to"] ?? "all";
      if (apply !== "queues" && apply !== "exchanges" && apply !== "all") {
        set.status = 400;
        return { error: "apply-to must be queues, exchanges, or all" };
      }
      const def = b.definition ?? {};
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
      if (!b.pattern) {
        set.status = 400;
        return { error: "pattern is required" };
      }
      const known = new Set([
        "message-ttl",
        "dead-letter-exchange",
        "dead-letter-routing-key",
        "max-length",
        "max-length-bytes",
        "expires",
        "overflow",
        "delivery-limit",
        "alternate-exchange",
        "dead-letter-strategy",
        "federation-upstream-set",
      ]);
      const unknown = Object.keys(def).filter((k) => !known.has(k));
      if (unknown.length) {
        set.status = 400;
        return { error: "bad_request", reason: `${JSON.stringify(unknown)} are not recognised policy settings` };
      }
      if (str("federation-upstream-set")) {
        const downstream = decodeURIComponent(params.vhost);
        addFederationPolicy(downstream, b.pattern);
        const exchange = exchangeFromPattern(b.pattern);
        if (exchange) {
          for (const link of fedUris) {
            if (link.downstream === downstream) void runFederation(link.uri, downstream, exchange, broker).catch((err) => console.error("federation", err));
          }
        }
      }
      try {
        const policy: Policy = {
          vhost: decodeURIComponent(params.vhost),
          name: params.name,
          pattern: b.pattern,
          applyTo: apply,
          priority: b.priority ?? 0,
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
        broker.upsertPolicy(policy);
        void broker.cluster?.replicate("policy", policyToWire(policy));
      } catch (err) {
        set.status = 400;
        return { error: err instanceof Error ? err.message : "bad policy" };
      }
      set.status = 201;
      return { name: params.name };
    })
    .put("/api/parameters/shovel/:vhost/:name", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator") && !s?.tags.includes("management")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const value = ((body ?? {}) as { value?: Record<string, string> }).value ?? {};
      const srcQueue = value["src-queue"];
      const destQueue = value["dest-queue"];
      if (!srcQueue || !destQueue) {
        set.status = 400;
        return { error: "src-queue and dest-queue are required" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const name = decodeURIComponent(params.name);
      // A missing URI means this broker, as `amqp://` does.
      broker.putShovel({ vhost, name, srcUri: value["src-uri"] || "amqp://", srcQueue, destUri: value["dest-uri"] || "amqp://", destQueue });
      set.status = 201;
      return { name };
    })
    .delete("/api/parameters/shovel/:vhost/:name", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator") && !s?.tags.includes("management")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      if (!broker.deleteShovel(decodeURIComponent(params.vhost), decodeURIComponent(params.name))) {
        set.status = 404;
        return { error: "not found" };
      }
      set.status = 204;
      return "";
    })
    .get("/api/shovels", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return {
        items: [...broker.shovels.values()].map(({ def }) => ({ name: def.name, vhost: def.vhost, type: "dynamic", state: "running", src_queue: def.srcQueue, dest_queue: def.destQueue })),
      };
    })
    .put("/api/parameters/federation-upstream/:vhost/:name", ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const uri = ((body ?? {}) as { value?: { uri?: string } }).value?.uri ?? "";
      const slash = uri.indexOf("/", uri.indexOf("://") + 3);
      let upstream = slash >= 0 ? uri.slice(slash + 1) : "/";
      if (upstream === "" || upstream === "%2F" || upstream === "%2f") upstream = "/";
      const downstream = decodeURIComponent(params.vhost);
      addFederationUpstream(downstream, upstream);
      if (uri.includes("://")) addFederationUri(downstream, uri);
      set.status = 201;
      return { uri };
    })
    .get("/api/policies/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      return {
        items: broker.policies
          .filter((p) => p.vhost === vhost)
          .map((p) => ({
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
    })
    .get("/api/policies", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return {
        items: broker.policies.map((p) => ({
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
            ...(p.deliveryLimit != null ? { "delivery-limit": p.deliveryLimit } : {}),
            ...(p.alternate ? { "alternate-exchange": p.alternate } : {}),
          },
        })),
      };
    })
    .delete("/api/policies/:vhost/:name", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      if (!broker.deletePolicy(decodeURIComponent(params.vhost), params.name)) {
        set.status = 404;
        return { error: "not found" };
      }
      void broker.cluster?.replicate("delete_policy", { vhost: decodeURIComponent(params.vhost), name: params.name });
      set.status = 204;
      return "";
    })
    .get("/api/permissions", ({ request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      return broker.listPerms();
    })
    .post("/api/queues/:vhost/:name/purge", async ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      try {
        const n = await broker.purge(decodeURIComponent(params.vhost), params.name);
        return { message_count: n };
      } catch (err) {
        set.status = 404;
        return { error: err instanceof Error ? err.message : "not found" };
      }
    })
    // RabbitMQ's purge route.
    .delete("/api/queues/:vhost/:name/contents", async ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      try {
        const vhost = decodeURIComponent(params.vhost);
      const denied = access(request, set, vhost, [ ["read", params.name] ]);
      if (denied) return denied;
      await broker.purge(decodeURIComponent(params.vhost), decodeURIComponent(params.name));
        set.status = 204;
        return "";
      } catch (err) {
        set.status = 404;
        return { error: err instanceof Error ? err.message : "not found" };
      }
    })
    .get("/api/queues/:vhost/:name", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const q = broker.queues.get(broker.key(vhost, params.name));
      if (!q) {
        set.status = 404;
        return { error: "not found" };
      }
      const names = broker.policyNames(vhost, q.name);
      const consumers = broker.listMgmtConsumers(vhost, q.name);
      return {
        name: q.name,
        vhost,
        durable: q.durable,
        exclusive: q.exclusive,
        auto_delete: q.autoDelete,
        node: q.home || broker.cfg.nodeId || "queueforge",
        ...quorumMembers(broker, q),
        state: "running",
        messages: q.ready.length + q.unacked.size,
        messages_ready: q.ready.length,
        messages_unacknowledged: q.unacked.size,
        consumers: q.consumers.length,
        consumer_details: consumers.map((c) => ({
          consumer_tag: c.consumer_tag,
          channel_details: { name: `${c.connection}:${c.channel}`, connection_name: c.connection, number: c.channel },
          queue: { name: c.queue, vhost: c.vhost },
          ack_required: true,
        })),
        arguments: q.args,
        type: q.argsParsed.queueType,
        policy: names.policy,
        operator_policy: names.operator_policy,
        message_stats: { publish: broker.prom.received, deliver: broker.prom.delivered, ack: broker.prom.acknowledged },
      };
    })
    .get("/api/exchanges/:vhost/:name", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const e = broker.exchanges.get(broker.key(vhost, params.name));
      if (!e) {
        set.status = 404;
        return { error: "not found" };
      }
      const userPol = broker.matchPolicy(vhost, e.name, "exchanges");
      const opPol = broker.matchOperatorPolicy(vhost, e.name, "exchanges");
      return {
        name: e.name,
        vhost,
        type: e.kind,
        durable: e.durable,
        auto_delete: e.autoDelete,
        internal: e.internal,
        arguments: e.alternate ? { "alternate-exchange": e.alternate } : {},
        policy: userPol?.name ?? null,
        operator_policy: opPol?.name ?? null,
      };
    })
    .get("/api/connections/:name", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const row = broker.listMgmtConnections().find((c) => c.name === params.name);
      if (!row) {
        set.status = 404;
        return { error: "not found" };
      }
      return row;
    })
    .get("/api/channels", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const items = broker.listMgmtChannels().map((c) => ({
        name: c.name,
        connection_details: { name: c.connection },
        user: c.user,
        vhost: c.vhost,
        number: c.number,
        peer_host: c.peer_host,
        peer_port: c.peer_port,
      }));
      return { items, total_count: items.length };
    })
    .get("/api/channels/:name", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const c = broker.getMgmtChannel(decodeURIComponent(params.name));
      if (!c) {
        set.status = 404;
        return { error: "not found" };
      }
      return {
        name: c.name,
        connection_details: { name: c.connection },
        user: c.user,
        vhost: c.vhost,
        number: c.number,
        peer_host: c.peer_host,
        peer_port: c.peer_port,
      };
    })
    .get("/api/consumers/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const items = broker.listMgmtConsumers(vhost).map((c) => ({
        consumer_tag: c.consumer_tag,
        channel_details: { name: `${c.connection}:${c.channel}`, connection_name: c.connection, number: c.channel },
        queue: { name: c.queue, vhost: c.vhost },
      }));
      return { items, total_count: items.length };
    })
    .get("/api/topic-permissions", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { items: broker.listTopicPerms() };
    })
    .put("/api/topic-permissions/:user/:vhost", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const b = (body ?? {}) as { exchange?: string; write?: string; read?: string };
      if (!b.exchange) {
        set.status = 400;
        return { error: "exchange and valid write/read patterns are required" };
      }
      try {
        const target = permTarget(broker, params.user, params.vhost);
        broker.putTopicPerm({
          user: target.user,
          vhost: target.vhost,
          exchange: b.exchange,
          write: b.write ?? "",
          read: b.read ?? "",
        });
        const row = broker.listTopicPerms(target.user).find((r) => r.vhost === target.vhost && r.exchange === b.exchange);
        if (row) void broker.cluster?.replicate("topic_permission", row);
      } catch (err) {
        set.status = 400;
        return { error: err instanceof Error ? err.message : "bad pattern" };
      }
      set.status = 201;
      return "";
    })
    .delete("/api/topic-permissions/:user/:vhost/:exchange", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const target = permTarget(broker, params.user, params.vhost);
      if (!broker.deleteTopicPerm(target.user, target.vhost, decodeURIComponent(params.exchange))) {
        set.status = 404;
        return { error: "not found" };
      }
      void broker.cluster?.replicate("delete_topic_permission", { user: target.user, vhost: target.vhost, exchange: decodeURIComponent(params.exchange) });
      set.status = 204;
      return "";
    })
    .get("/api/limits", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { user_limits: broker.listUserLimits(), vhost_limits: broker.listVhostLimits() };
    })
    .put("/api/user-limits/:user/:kind", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const value = Number((body as { value?: number } | null)?.value);
      if (!Number.isFinite(value) || value < 0) {
        set.status = 400;
        return { error: "value is required" };
      }
      const current = broker.listUserLimits().find((row) => row.user === params.user);
      const connections = current?.["max-connections"] ?? null;
      const channels = current?.["max-channels"] ?? null;
      if (params.kind === "max-connections") broker.setUserLimit(params.user, value, channels);
      else if (params.kind === "max-channels") broker.setUserLimit(params.user, connections, value);
      else {
        set.status = 400;
        return { error: "limit must be max-connections or max-channels" };
      }
      void broker.cluster?.replicate("user_limits", broker.listUserLimits().find((row) => row.user === params.user) ?? { user: params.user });
      set.status = 204;
      return "";
    })
    .delete("/api/user-limits/:user/:kind", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const current = broker.listUserLimits().find((row) => row.user === params.user);
      const connections = current?.["max-connections"] ?? null;
      const channels = current?.["max-channels"] ?? null;
      if (params.kind === "max-connections") broker.setUserLimit(params.user, null, channels);
      else if (params.kind === "max-channels") broker.setUserLimit(params.user, connections, null);
      else {
        set.status = 400;
        return { error: "limit must be max-connections or max-channels" };
      }
      void broker.cluster?.replicate("user_limits", broker.listUserLimits().find((row) => row.user === params.user) ?? { user: params.user });
      set.status = 204;
      return "";
    })
    .put("/api/vhost-limits/:vhost/:kind", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const value = Number((body as { value?: number } | null)?.value);
      const vhost = decodeURIComponent(params.vhost);
      const current = broker.listVhostLimits().find((row) => row.vhost === vhost);
      const connections = current?.["max-connections"] ?? null;
      const queues = current?.["max-queues"] ?? null;
      if (params.kind === "max-connections") broker.setVhostLimit(vhost, value, queues);
      else if (params.kind === "max-queues") broker.setVhostLimit(vhost, connections, value);
      else {
        set.status = 400;
        return { error: "limit must be max-connections or max-queues" };
      }
      void broker.cluster?.replicate("vhost_limits", broker.listVhostLimits().find((row) => row.vhost === vhost) ?? { vhost });
      set.status = 204;
      return "";
    })
    .delete("/api/vhost-limits/:vhost/:kind", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const current = broker.listVhostLimits().find((row) => row.vhost === vhost);
      const connections = current?.["max-connections"] ?? null;
      const queues = current?.["max-queues"] ?? null;
      if (params.kind === "max-connections") broker.setVhostLimit(vhost, null, queues);
      else if (params.kind === "max-queues") broker.setVhostLimit(vhost, connections, null);
      else {
        set.status = 400;
        return { error: "limit must be max-connections or max-queues" };
      }
      void broker.cluster?.replicate("vhost_limits", broker.listVhostLimits().find((row) => row.vhost === vhost) ?? { vhost });
      set.status = 204;
      return "";
    })
    .get("/api/feature-flags", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { items: featureFlags(broker) };
    })
    .put("/api/feature-flags/:name/enable", ({ params, request, set }) => enableFlag(params.name, request, set))
    .post("/api/feature-flags/:name/enable", ({ params, request, set }) => enableFlag(params.name, request, set))
    .post("/api/feature-flags/:name/disable", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      // transient_nonexcl_queues is the deprecated-feature toggle, kept here as QueueForge always had it.
      if (params.name === "transient_nonexcl_queues") {
        broker.transientNonexcl = false;
        set.status = 204;
        return "";
      }
      // A RabbitMQ feature flag cannot be turned off once it is enabled.
      set.status = 400;
      return { error: "bad_request", reason: "a feature flag cannot be disabled" };
    })
    .get("/api/deprecated-features", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return {
        items: [{
          name: "transient_nonexcl_queues",
          deprecation_phase: "denied_by_default",
          acknowledged: broker.transientNonexcl,
          description: "Non-exclusive transient queues are refused unless this deprecated behavior is acknowledged.",
        }],
      };
    })
    .delete("/api/deprecated-features/:name", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      if (params.name !== "transient_nonexcl_queues") {
        set.status = 404;
        return { error: "not found" };
      }
      broker.transientNonexcl = true;
      set.status = 204;
      return "";
    })
    .get("/api/nodes", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const mem = process.memoryUsage().rss;
      const disk = availableBytes(broker.cfg.dataDir || ".");
      const amqp = broker.cfg.amqp.split(":");
      const port = Number(amqp.pop());
      const host = amqp.join(":") || "0.0.0.0";
      return {
        items: [{
          name: broker.cfg.nodeId || "queueforge",
          running: true,
          uptime: Date.now() - broker.startedAt,
          mem_used: mem,
          disk_free: disk,
          mem_alarm: broker.memAlarm,
          disk_free_alarm: broker.diskAlarm,
          listeners: [{ protocol: "amqp", ip_address: host, port }],
          peers: broker.cfg.members.map((m) => ({ name: m.id })),
        }],
      };
    })
    .post("/api/nodes", async ({ body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const row = (body ?? {}) as { id?: string; addr?: string };
      const id = String(row.id ?? "").trim();
      const addr = String(row.addr ?? "").trim();
      if (!id || !addr.includes(":")) {
        set.status = 400;
        return { error: "id and addr are required" };
      }
      if (broker.cfg.members.length === 0 || !broker.cluster) {
        set.status = 400;
        return { error: "cluster membership is not configured" };
      }
      const members = broker.cfg.members.filter((member) => member.id !== id);
      members.push({ id, addr });
      try {
        await broker.cluster.changeMembers(members);
      } catch (err) {
        set.status = 503;
        return { error: "no majority to commit the member change", reason: err instanceof Error ? err.message : String(err) };
      }
      set.status = 201;
      return { members: broker.cfg.members };
    })
    .delete("/api/nodes/:name", async ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const name = decodeURIComponent(params.name);
      if (name === broker.cfg.nodeId) {
        set.status = 400;
        return { error: "a node cannot forget itself" };
      }
      if ([...broker.queues.values()].some((queue) => queue.home === name)) {
        set.status = 400;
        return { error: `member ${name} still homes a classic queue` };
      }
      if (!broker.cluster) {
        set.status = 400;
        return { error: "cluster membership is not configured" };
      }
      const members = broker.cfg.members.filter((member) => member.id !== name);
      if (members.length === 0) {
        set.status = 400;
        return { error: "the member list cannot become empty" };
      }
      try {
        await broker.cluster.changeMembers(members);
      } catch (err) {
        set.status = 503;
        return { error: "no majority to commit the member change", reason: err instanceof Error ? err.message : String(err) };
      }
      set.status = 204;
      return "";
    })
    .get("/api/cluster-name", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { name: broker.clusterName() };
    })
    .put("/api/cluster-name", ({ body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const name = (body as { name?: unknown } | null)?.name;
      if (typeof name !== "string" || !name) {
        set.status = 400;
        return { error: "name is required" };
      }
      broker.putGlobalParam("cluster_name", name);
      void broker.cluster?.replicate("global_parameter", { name: "cluster_name", value: name });
      set.status = 204;
      return "";
    })
    .get("/api/parameters", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return broker.listRuntimeParams();
    })
    .get("/api/parameters/:component", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return broker.listRuntimeParams(decodeURIComponent(params.component));
    })
    .get("/api/parameters/:component/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return broker.listRuntimeParams(decodeURIComponent(params.component), decodeURIComponent(params.vhost));
    })
    .get("/api/parameters/:component/:vhost/:name", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const name = decodeURIComponent(params.name);
      const row = broker.listRuntimeParams(decodeURIComponent(params.component), decodeURIComponent(params.vhost)).find((r) => r.name === name);
      if (!row) {
        set.status = 404;
        return { error: "Object Not Found", reason: "Not Found" };
      }
      return row;
    })
    .put("/api/parameters/:component/:vhost/:name", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator") && !s?.tags.includes("policymaker")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const row = {
        component: decodeURIComponent(params.component),
        vhost: decodeURIComponent(params.vhost),
        name: decodeURIComponent(params.name),
        value: (body as { value?: unknown } | null)?.value ?? null,
      };
      if (!broker.vhosts.has(row.vhost)) {
        set.status = 400;
        return { error: "bad_request", reason: `vhost ${row.vhost} does not exist` };
      }
      try {
        broker.putRuntimeParam(row);
      } catch (err) {
        set.status = 400;
        return { error: "bad_request", reason: err instanceof Error ? err.message : "bad value" };
      }
      // A shovel runs on the node it was declared on, as a RabbitMQ dynamic shovel does.
      if (row.component !== "shovel") void broker.cluster?.replicate("parameter", row);
      set.status = 201;
      return "";
    })
    .delete("/api/parameters/:component/:vhost/:name", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator") && !s?.tags.includes("policymaker")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const row = {
        component: decodeURIComponent(params.component),
        vhost: decodeURIComponent(params.vhost),
        name: decodeURIComponent(params.name),
      };
      if (!broker.deleteRuntimeParam(row.component, row.vhost, row.name)) {
        set.status = 404;
        return { error: "Object Not Found", reason: "Not Found" };
      }
      if (row.component !== "shovel") void broker.cluster?.replicate("delete_parameter", row);
      set.status = 204;
      return "";
    })
    .get("/api/global-parameters", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return broker.listGlobalParams();
    })
    .get("/api/global-parameters/:name", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const name = decodeURIComponent(params.name);
      const row = broker.listGlobalParams().find((r) => r.name === name);
      if (!row) {
        set.status = 404;
        return { error: "Object Not Found", reason: "Not Found" };
      }
      return row;
    })
    .put("/api/global-parameters/:name", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const name = decodeURIComponent(params.name);
      const value = (body as { value?: unknown } | null)?.value ?? null;
      broker.putGlobalParam(name, value);
      void broker.cluster?.replicate("global_parameter", { name, value });
      set.status = 201;
      return "";
    })
    .delete("/api/global-parameters/:name", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const name = decodeURIComponent(params.name);
      if (!broker.deleteGlobalParam(name)) {
        set.status = 404;
        return { error: "Object Not Found", reason: "Not Found" };
      }
      void broker.cluster?.replicate("delete_global_parameter", { name });
      set.status = 204;
      return "";
    })
    .get("/api/operator-policies", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { items: broker.operatorPolicies.map(policyItem) };
    })
    .get("/api/operator-policies/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      return { items: broker.operatorPolicies.filter((p) => p.vhost === vhost).map(policyItem) };
    })
    .put("/api/operator-policies/:vhost/:name", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const existed = broker.operatorPolicies.some((p) => p.vhost === vhost && p.name === params.name);
      try {
        broker.upsertOperatorPolicy(policyFromBody(vhost, params.name, (body ?? {}) as Parameters<typeof policyFromBody>[2]));
      } catch (err) {
        set.status = 400;
        return { error: err instanceof Error ? err.message : "bad policy" };
      }
      set.status = existed ? 204 : 201;
      return existed ? "" : { name: params.name };
    })
    .delete("/api/operator-policies/:vhost/:name", ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"), request.headers.get("host"), request.headers.get("authorization"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      if (!broker.deleteOperatorPolicy(decodeURIComponent(params.vhost), params.name)) {
        set.status = 404;
        return { error: "not found" };
      }
      set.status = 204;
      return "";
    })
    .get("/metrics", () => new Response(metricsText(broker), { headers: { "content-type": "text/plain; version=0.0.4" } }))
    .get("/*", async ({ request, set }) => {
      const url = new URL(request.url);
      const rel = url.pathname === "/" ? "index.html" : url.pathname.replace(/^\//, "");
      if (rel === "api" || rel.startsWith("api/") || rel === "healthz" || rel === "readyz" || rel === "metrics") {
        set.status = 404;
        return "not found\n";
      }
      const file = Bun.file(join(spaDir, rel));
      if (await file.exists()) return file;
      const segment = rel.split("/").pop() ?? "";
      const ext = segment.includes(".") ? segment.slice(segment.lastIndexOf(".") + 1) : "";
      const staticExt = new Set(["js", "css", "map", "svg", "png", "ico", "woff", "woff2", "json", "txt", "html", "webp", "gif"]);
      if (staticExt.has(ext)) {
        set.status = 404;
        return "not found\n";
      }
      const index = Bun.file(join(spaDir, "index.html"));
      if (!(await index.exists())) {
        set.status = 404;
        return "not found\n";
      }
      return index;
    });
}

import { Elysia } from "elysia";
import { join } from "node:path";
import { addFederationPolicy, addFederationUpstream, type Broker } from "./broker.ts";

const COOKIE = "queueforge_session";
const sessions = new Map<string, { user: string; tags: string[] }>();

function tokenOf(header: string | null): string | null {
  if (!header) return null;
  for (const part of header.split(";")) {
    const t = part.trim();
    if (t.startsWith(`${COOKIE}=`)) return t.slice(COOKIE.length + 1);
  }
  return null;
}

function requireUser(header: string | null) {
  const token = tokenOf(header);
  if (!token) return null;
  return sessions.get(token) ?? null;
}

export function managementApp(broker: Broker, spaDir: string) {
  return new Elysia()
    .get("/healthz", () => "ok\n")
    .get("/readyz", ({ set }) => {
      if (!broker.ready) {
        set.status = 503;
        return "not ready\n";
      }
      return "ready\n";
    })
    .post("/api/login", async ({ body, set }) => {
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
      sessions.set(token, { user: username, tags });
      set.headers["set-cookie"] = `${COOKIE}=${token}; HttpOnly; SameSite=Lax; Path=/`;
      return { name: username, tags };
    })
    .post("/api/logout", ({ request, set }) => {
      const token = tokenOf(request.headers.get("cookie"));
      if (token) sessions.delete(token);
      set.status = 204;
      set.headers["set-cookie"] = `${COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0`;
      return "";
    })
    .get("/api/whoami", ({ request, set }) => {
      const s = requireUser(request.headers.get("cookie"));
      if (!s) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { name: s.user, tags: s.tags };
    })
    .get("/api/overview", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { product: "queueforge-bun", queue_count: broker.queues.size, exchange_count: broker.exchanges.size };
    })
    .get("/api/vhosts", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return { items: [...broker["vhosts"]].map((name) => ({ name })) };
    })
    .put("/api/vhosts/:vhost", async ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const name = decodeURIComponent(params.vhost);
      broker.ensureBuiltins(name);
      await broker.cluster?.replicate("vhost", { name });
      set.status = 201;
      return { name };
    })
    .delete("/api/vhosts/:vhost", async ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"));
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
      const s = requireUser(request.headers.get("cookie"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      return broker.listUsers();
    })
    .put("/api/users/:name", async ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"));
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
      const s = requireUser(request.headers.get("cookie"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      await broker.deleteUser(params.name);
      set.status = 204;
      return "";
    })
    .put("/api/permissions/:user/:vhost", async ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      const b = body as { configure: string; write: string; read: string };
      const vhost = decodeURIComponent(params.vhost);
      await broker.putPerm({ user: params.user, vhost, configure: b.configure, write: b.write, read: b.read });
      set.status = 201;
      return { user: params.user, vhost, ...b };
    })
    .delete("/api/permissions/:user/:vhost", async ({ params, request, set }) => {
      const s = requireUser(request.headers.get("cookie"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      await broker.deletePerm(params.user, decodeURIComponent(params.vhost));
      set.status = 204;
      return "";
    })
    .get("/api/queues/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      const items = [...broker.queues.values()].filter((q) => q.vhost === vhost).map((q) => ({
        name: q.name,
        vhost,
        durable: q.durable,
        auto_delete: q.autoDelete,
        arguments: q.args,
        type: q.argsParsed.queueType,
      }));
      return { items };
    })
    .put("/api/queues/:vhost/:name", async ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const b = (body ?? {}) as { durable?: boolean; exclusive?: boolean; auto_delete?: boolean; arguments?: Record<string, string | number> };
      if (!b.durable && !b.exclusive) {
        set.status = 400;
        return { error: "bad_request", reason: "Feature `transient_nonexcl_queues` is deprecated. By default, this feature is not permitted anymore." };
      }
      const vhost = decodeURIComponent(params.vhost);
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
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      await broker.deleteQueue(decodeURIComponent(params.vhost), params.name);
      set.status = 204;
      return "";
    })
    .put("/api/exchanges/:vhost/:name", async ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const b = (body ?? {}) as { type?: string; durable?: boolean; auto_delete?: boolean; internal?: boolean; arguments?: Record<string, string> };
      const vhost = decodeURIComponent(params.vhost);
      await broker.declareExchange(vhost, params.name, b.type ?? "direct", !!b.durable, !!b.auto_delete, !!b.internal, b.arguments?.["alternate-exchange"] ?? null);
      set.status = 201;
      return { name: params.name, type: b.type ?? "direct" };
    })
    .delete("/api/exchanges/:vhost/:name", async ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      await broker.deleteExchange(decodeURIComponent(params.vhost), params.name);
      set.status = 204;
      return "";
    })
    .delete("/api/bindings/:vhost/:exchange/:queue/:key", async ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const vhost = decodeURIComponent(params.vhost);
      await broker.unbind(vhost, params.exchange, params.queue, decodeURIComponent(params.key), null);
      set.status = 204;
      return "";
    })
    .post("/api/bindings/:vhost", async ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const b = body as { source: string; destination: string; routing_key?: string; arguments?: Record<string, string | number> };
      const vhost = decodeURIComponent(params.vhost);
      const args = Object.entries(b.arguments ?? {}).map(([k, v]) => [k, typeof v === "number" ? { t: "I" as const, v } : { t: "S" as const, v: String(v) }] as [string, { t: "I"; v: number } | { t: "S"; v: string }]);
      await broker.bind(vhost, b.source, b.destination, b.routing_key ?? "", args);
      set.status = 201;
      return { source: b.source, destination: b.destination };
    })
    .get("/api/definitions", ({ request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      return broker.exportDefinitions();
    })
    .post("/api/definitions", async ({ body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"));
      if (!s?.tags.includes("administrator")) {
        set.status = s ? 403 : 401;
        return { error: "forbidden" };
      }
      await broker.importDefinitions(body as Parameters<Broker["importDefinitions"]>[0]);
      set.status = 204;
      return "";
    })
    .put("/api/policies/:vhost/:name", ({ params, body, request, set }) => {
      const s = requireUser(request.headers.get("cookie"));
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
      if (str("federation-upstream-set")) addFederationPolicy(decodeURIComponent(params.vhost), b.pattern);
      try {
        broker.upsertPolicy({
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
        });
      } catch (err) {
        set.status = 400;
        return { error: err instanceof Error ? err.message : "bad policy" };
      }
      set.status = 201;
      return { name: params.name };
    })
    .put("/api/parameters/shovel/:vhost/:name", async ({ body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const value = ((body ?? {}) as { value?: Record<string, string> }).value ?? {};
      const src = value["src-queue"];
      const dest = value["dest-queue"];
      if (!src || !dest) {
        set.status = 400;
        return { error: "src-queue and dest-queue are required" };
      }
      setInterval(async () => {
        try {
          const msg = await broker.get("/", src, true);
          if (!msg) return;
          await broker.publish({
            vhost: "/",
            exchange: "",
            routingKey: dest,
            body: msg.body,
            headers: msg.headers,
            propRaw: msg.propRaw,
            persistent: false,
            priority: 0,
            expiration: "",
          });
        } catch {
          /* source queue is not ready yet */
        }
      }, 30);
      set.status = 201;
      return { name: src };
    })
    .put("/api/parameters/federation-upstream/:vhost/:name", ({ params, body, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
        set.status = 401;
        return { error: "unauthorized" };
      }
      const uri = ((body ?? {}) as { value?: { uri?: string } }).value?.uri ?? "";
      const slash = uri.indexOf("/", uri.indexOf("://") + 3);
      let upstream = slash >= 0 ? uri.slice(slash + 1) : "/";
      if (upstream === "" || upstream === "%2F" || upstream === "%2f") upstream = "/";
      addFederationUpstream(decodeURIComponent(params.vhost), upstream);
      set.status = 201;
      return { uri };
    })
    .get("/api/policies/:vhost", ({ params, request, set }) => {
      if (!requireUser(request.headers.get("cookie"))) {
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
      if (!requireUser(request.headers.get("cookie"))) {
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
    .get("/*", async ({ request, set }) => {
      const url = new URL(request.url);
      const rel = url.pathname === "/" ? "index.html" : url.pathname.replace(/^\//, "");
      const file = Bun.file(join(spaDir, rel));
      if (!(await file.exists())) {
        set.status = 404;
        return "not found\n";
      }
      return file;
    });
}

function promLabel(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/"/g, '\\"');
}

export function metricsText(broker: Broker): string {
  const p = broker.prom;
  const node = promLabel(broker.cfg.nodeId || "queueforge");
  const lines = [
    "# TYPE rabbitmq_up gauge",
    "rabbitmq_up 1",
    "# TYPE rabbitmq_ready gauge",
    `rabbitmq_ready ${broker.ready ? 1 : 0}`,
    "# TYPE rabbitmq_connections gauge",
    `rabbitmq_connections ${p.connections}`,
    "# TYPE rabbitmq_connections_opened_total counter",
    `rabbitmq_connections_opened_total ${p.connectionsOpened}`,
    "# TYPE rabbitmq_connections_closed_total counter",
    `rabbitmq_connections_closed_total ${p.connectionsClosed}`,
    "# TYPE rabbitmq_channels gauge",
    `rabbitmq_channels ${p.channels}`,
    "# TYPE rabbitmq_channels_opened_total counter",
    `rabbitmq_channels_opened_total ${p.channelsOpened}`,
    "# TYPE rabbitmq_channels_closed_total counter",
    `rabbitmq_channels_closed_total ${p.channelsClosed}`,
    "# TYPE rabbitmq_queues gauge",
    `rabbitmq_queues ${broker.queues.size}`,
    "# TYPE rabbitmq_queues_declared_total counter",
    `rabbitmq_queues_declared_total ${p.queuesDeclared}`,
    "# TYPE rabbitmq_queues_created_total counter",
    `rabbitmq_queues_created_total ${p.queuesCreated}`,
    "# TYPE rabbitmq_queues_deleted_total counter",
    `rabbitmq_queues_deleted_total ${p.queuesDeleted}`,
    "# TYPE rabbitmq_consumers gauge",
    `rabbitmq_consumers ${p.consumers}`,
    "# TYPE rabbitmq_global_consumers gauge",
    `rabbitmq_global_consumers ${p.consumers}`,
    "# TYPE rabbitmq_global_publishers gauge",
    "rabbitmq_global_publishers 0",
    "# TYPE rabbitmq_global_messages_received_total counter",
    `rabbitmq_global_messages_received_total ${p.received}`,
    "# TYPE rabbitmq_global_messages_received_confirm_total counter",
    `rabbitmq_global_messages_received_confirm_total ${p.receivedConfirm}`,
    "# TYPE rabbitmq_global_messages_confirmed_total counter",
    `rabbitmq_global_messages_confirmed_total ${p.confirmed}`,
    "# TYPE rabbitmq_global_messages_routed_total counter",
    `rabbitmq_global_messages_routed_total ${p.routed}`,
    "# TYPE rabbitmq_global_messages_unroutable_dropped_total counter",
    `rabbitmq_global_messages_unroutable_dropped_total ${p.unroutableDropped}`,
    "# TYPE rabbitmq_global_messages_unroutable_returned_total counter",
    `rabbitmq_global_messages_unroutable_returned_total ${p.unroutableReturned}`,
    "# TYPE rabbitmq_global_messages_delivered_total counter",
    `rabbitmq_global_messages_delivered_total ${p.delivered}`,
    "# TYPE rabbitmq_global_messages_delivered_consume_manual_ack_total counter",
    `rabbitmq_global_messages_delivered_consume_manual_ack_total ${p.deliveredConsumeManual}`,
    "# TYPE rabbitmq_global_messages_delivered_consume_auto_ack_total counter",
    `rabbitmq_global_messages_delivered_consume_auto_ack_total ${p.deliveredConsumeAuto}`,
    "# TYPE rabbitmq_global_messages_delivered_get_manual_ack_total counter",
    `rabbitmq_global_messages_delivered_get_manual_ack_total ${p.deliveredGetManual}`,
    "# TYPE rabbitmq_global_messages_delivered_get_auto_ack_total counter",
    `rabbitmq_global_messages_delivered_get_auto_ack_total ${p.deliveredGetAuto}`,
    "# TYPE rabbitmq_global_messages_get_empty_total counter",
    `rabbitmq_global_messages_get_empty_total ${p.getEmpty}`,
    "# TYPE rabbitmq_global_messages_acknowledged_total counter",
    `rabbitmq_global_messages_acknowledged_total ${p.acknowledged}`,
    "# TYPE rabbitmq_global_messages_redelivered_total counter",
    `rabbitmq_global_messages_redelivered_total ${p.redelivered}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_expired_total counter",
    `rabbitmq_global_messages_dead_lettered_expired_total ${p.dlxExpired}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_rejected_total counter",
    `rabbitmq_global_messages_dead_lettered_rejected_total ${p.dlxRejected}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_maxlen_total counter",
    `rabbitmq_global_messages_dead_lettered_maxlen_total ${p.dlxMaxlen}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_delivery_limit_total counter",
    `rabbitmq_global_messages_dead_lettered_delivery_limit_total ${p.dlxDeliveryLimit}`,
    "# TYPE rabbitmq_global_messages_dead_lettered_confirmed_total counter",
    "rabbitmq_global_messages_dead_lettered_confirmed_total 0",
    "# TYPE rabbitmq_alarms_memory_used_watermark gauge",
    "rabbitmq_alarms_memory_used_watermark 0",
    "# TYPE rabbitmq_alarms_free_disk_space_watermark gauge",
    "rabbitmq_alarms_free_disk_space_watermark 0",
    "# TYPE rabbitmq_disk_space_available_bytes gauge",
    "rabbitmq_disk_space_available_bytes 0",
    "# TYPE rabbitmq_unreachable_cluster_peers_count gauge",
    "rabbitmq_unreachable_cluster_peers_count 0",
    "# TYPE queueforge_confirm_before_fsync_total counter",
    `queueforge_confirm_before_fsync_total ${broker.store.confirmsBeforeFsync}`,
    "# TYPE queueforge_full_flush_total counter",
    `queueforge_full_flush_total ${broker.store.fullFlushCount}`,
    "# TYPE rabbitmq_identity_info gauge",
    `rabbitmq_identity_info{rabbitmq_node="${node}",rabbitmq_cluster="queueforge"} 1`,
    "# TYPE rabbitmq_build_info gauge",
    'rabbitmq_build_info{rabbitmq_version="0.1.0"} 1',
  ];
  for (const q of broker.queues.values()) {
    const labels = `vhost="${promLabel(q.vhost)}",queue="${promLabel(q.name)}"`;
    lines.push(`rabbitmq_queue_messages_ready{${labels}} ${q.ready.length}`);
    lines.push(`rabbitmq_queue_messages_unacked{${labels}} ${q.unacked.size}`);
    lines.push(`rabbitmq_queue_messages{${labels}} ${q.ready.length + q.unacked.size}`);
    lines.push(`rabbitmq_queue_consumers{${labels}} ${q.consumers.length}`);
  }
  lines.push("");
  return lines.join("\n");
}

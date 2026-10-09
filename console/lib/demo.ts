import type { BrokerCall } from "./client";
import type { BrokerKind, BrokerTarget } from "@/stores/broker";

export const DEMO_PROFILES: BrokerTarget[] = (["rust", "bun", "php", "rabbitmq"] as BrokerKind[]).map((kind) => ({ id: `demo-${kind}`, name: `${kind === "rabbitmq" ? "RabbitMQ" : kind[0].toUpperCase() + kind.slice(1)} · demo`, kind, url: `demo://${kind}` }));
export const isDemo = (url: string) => url.startsWith("demo://");
export const DEMO_QUEUES = [
  { name: "orders", type: "classic", messages_ready: 128, messages_unacknowledged: 12, consumers: 4, arguments: { "x-dead-letter-exchange": "orders.failures", "x-dead-letter-routing-key": "failed" } },
  { name: "orders.dead-letter", type: "classic", messages_ready: 7, messages_unacknowledged: 0, consumers: 0, arguments: {} },
  { name: "events", type: "classic", messages_ready: 42, messages_unacknowledged: 6, consumers: 2, arguments: {} },
  { name: "payments", type: "quorum", messages_ready: 16, messages_unacknowledged: 2, consumers: 2, arguments: {} },
];

export function demoMessage(queue: string) {
  const dead = queue === "orders.dead-letter";
  const payload = JSON.stringify({ orderId: "demo-1042", event: dead ? "payment.failed" : "order.created", amount: 49.9, currency: "USD" });
  return { payload, payload_encoding: "string", payload_bytes: new TextEncoder().encode(payload).byteLength, routing_key: dead ? "failed" : "created", exchange: dead ? "orders.failures" : "orders", redelivered: dead, properties: { content_type: "application/json", delivery_mode: 2, correlation_id: "demo-order-1042", expiration: "30000", headers: dead ? { "x-death": [{ reason: "rejected", queue: "orders", exchange: "orders", count: 3, "routing-keys": ["created"] }] } : { source: "demo-storefront" } } };
}

/** An isolated read-only fixture adapter. It never opens a network connection. */
export function demoCall(url: string, method: string, path: string, body?: unknown): BrokerCall {
  const kind = url.slice("demo://".length);
  const parts = path.split("?")[0].split("/").map(decodeURIComponent);
  const queue = parts[4] ?? "";
  const reply = (status: number, value: unknown): BrokerCall => ({ status, body: value, text: JSON.stringify(value) });
  if (method === "POST" && parts[2] === "queues" && parts[5] === "get") {
    if (!DEMO_QUEUES.some((q) => q.name === queue)) return reply(404, { error: "Queue not found" });
    const options = body as { encoding?: string } | undefined;
    const message = demoMessage(queue);
    if (options?.encoding === "base64") { message.payload = btoa(message.payload); message.payload_encoding = "base64"; }
    return reply(200, [message]);
  }
  if (method !== "GET") return reply(403, { error: "Demo mode is read-only. Connect a broker to perform this action." });
  if (path === "/healthz" || path === "/readyz") return reply(200, { status: "demo" });
  if (path === "/api/identity") return reply(200, { kind, product_name: "QueueForge demo" });
  if (path === "/api/overview") {
    const ticks = Math.floor(Date.now() / 1000);
    return reply(200, { product_version: "Demo snapshot", object_totals: { connections: 8, queues: DEMO_QUEUES.length }, message_stats: { publish: ticks * 120, deliver: ticks * 112, ack: ticks * 108 } });
  }
  if (path === "/metrics") return { status: 200, body: null, text: "rabbitmq_disk_space_available_bytes 12884901888\nrabbitmq_process_resident_memory_bytes 134217728\nrabbitmq_connections 8\n" };
  if (path === "/api/cluster-name") return reply(200, { name: "Example cluster" });
  if (path === "/api/nodes") return reply(200, [{ name: "demo-a", running: true, mem_used: 134217728, disk_free: 12884901888 }, { name: "demo-b", running: true, mem_used: 100663296, disk_free: 10737418240 }]);
  if (parts[2] === "queues") {
    if (parts[5] === "bindings") return reply(200, [{ source: queue === "orders.dead-letter" ? "orders.failures" : "orders", destination: queue, destination_type: "queue", routing_key: queue === "orders.dead-letter" ? "failed" : "created" }]);
    if (queue) return reply(200, DEMO_QUEUES.find((q) => q.name === queue) ?? {});
    return reply(200, { items: DEMO_QUEUES.map((q) => ({ ...q, messages: q.messages_ready + q.messages_unacknowledged, vhost: "/" })) });
  }
  if (parts[2] === "bindings") return reply(200, [{ source: "orders.failures", destination: "orders.dead-letter", destination_type: "queue", routing_key: "failed" }]);
  if (parts[2] === "exchanges") return reply(200, [{ name: "orders", type: "topic", durable: true }, { name: "orders.failures", type: "direct", durable: true }]);
  if (parts[2] === "connections") return reply(200, [{ name: "demo-storefront", user: "demo", peer_host: "127.0.0.1", peer_port: 5672, channels: 2 }]);
  if (parts[2] === "channels") return reply(200, [{ name: "demo-storefront (1)", user: "demo", vhost: "/", consumer_count: 4, messages_unacknowledged: 12 }]);
  if (parts[2] === "vhosts") return reply(200, [{ name: "/", tracing: false }]);
  if (parts[2] === "users") return reply(200, [{ name: "demo-operator", tags: ["administrator"] }]);
  if (parts[2] === "policies") return reply(200, [{ name: "orders-dlx", vhost: "/", pattern: "^orders$", definition: { "dead-letter-exchange": "orders.failures" }, priority: 0 }]);
  if (parts[2] === "feature-flags") return reply(200, [{ name: "raft", state: "enabled", stability: "stable" }]);
  if (parts[2] === "definitions") return reply(200, { queues: DEMO_QUEUES, exchanges: [{ name: "orders.failures", type: "direct" }], bindings: [], users: [], vhosts: [{ name: "/" }] });
  return reply(404, { error: "This endpoint has no demo fixture." });
}

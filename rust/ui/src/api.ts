/** Thin fetch helpers for the QueueForge management API. */

export type Whoami = {
  name: string;
  tags: string[];
};

export type Overview = {
  product_name: string;
  product_version: string;
  management_version: string;
  rabbitmq_version_compat: string;
  object_totals: {
    connections: number;
    channels: number;
    queues: number;
    exchanges: number;
    consumers: number;
    vhosts: number;
  };
  queue_totals: {
    messages: number;
    messages_ready: number;
    messages_unacknowledged: number;
  };
  message_stats?: {
    publish: number;
    deliver: number;
    ack: number;
  };
};

export type Page<T> = {
  items: T[];
  next_cursor?: string | null;
  total_count?: number | null;
};

export type QueueItem = {
  name: string;
  vhost: string;
  durable: boolean;
  exclusive: boolean;
  auto_delete: boolean;
  state: string;
  messages: number;
  messages_ready: number;
  messages_unacknowledged: number;
  consumers: number;
};

export type ExchangeItem = {
  name: string;
  vhost: string;
  type: string;
  durable: boolean;
  auto_delete: boolean;
  internal: boolean;
};

export type BindingItem = {
  source: string;
  destination: string;
  destination_type: string;
  routing_key: string;
  vhost: string;
  properties_key: string;
};

export type UserItem = {
  name: string;
  tags: string[];
};

export type ConnectionItem = {
  name: string;
  user: string;
  vhost: string;
  peer_host: string;
  peer_port: number;
  channels: number;
  connected_at: number;
};

export type VhostItem = { name: string };

export type PublishResult = { routed: boolean };

export type GetMessage = {
  payload: string;
  payload_encoding: string;
  payload_bytes: number;
  redelivered: boolean;
  exchange: string;
  routing_key: string;
  properties: Record<string, unknown>;
};

export class ApiError extends Error {
  status: number;
  body: unknown;

  constructor(status: number, body: unknown) {
    const msg =
      typeof body === "object" && body !== null && "error" in body
        ? String((body as { error: unknown }).error)
        : `HTTP ${status}`;
    super(msg);
    this.status = status;
    this.body = body;
  }
}

async function parseJson(res: Response): Promise<unknown> {
  const text = await res.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function api<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const headers = new Headers(init.headers);
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const res = await fetch(path, {
    ...init,
    headers,
    credentials: "same-origin",
  });
  if (res.status === 204) return undefined as T;
  const body = await parseJson(res);
  if (!res.ok) throw new ApiError(res.status, body);
  return body as T;
}

export async function login(
  username: string,
  password: string,
): Promise<Whoami> {
  return api<Whoami>("/api/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export async function logout(): Promise<void> {
  await api<void>("/api/logout", { method: "POST" });
}

export async function whoami(): Promise<Whoami | null> {
  const res = await fetch("/api/whoami", { credentials: "same-origin" });
  if (res.status === 401) return null;
  const body = await parseJson(res);
  if (!res.ok) throw new ApiError(res.status, body);
  return body as Whoami;
}

export async function getOverview(): Promise<Overview> {
  return api<Overview>("/api/overview");
}

export async function listVhosts(): Promise<Page<VhostItem>> {
  return api<Page<VhostItem>>("/api/vhosts");
}

export async function listQueues(vhost: string): Promise<Page<QueueItem>> {
  return api<Page<QueueItem>>(`/api/queues/${encodeURIComponent(vhost)}`);
}

export type QueueArguments = {
  "x-message-ttl"?: number;
  "x-expires"?: number;
  "x-max-length"?: number;
  "x-max-length-bytes"?: number;
  "x-overflow"?: "drop-head" | "reject-publish";
  "x-dead-letter-exchange"?: string;
  "x-dead-letter-routing-key"?: string;
  "x-max-death-hops"?: number;
  "x-max-priority"?: number;
};

export async function createQueue(
  vhost: string,
  name: string,
  body: {
    durable?: boolean;
    auto_delete?: boolean;
    arguments?: QueueArguments;
  } = {},
): Promise<void> {
  const argumentsPayload = body.arguments
    ? Object.fromEntries(
        Object.entries(body.arguments).filter(
          ([, v]) => v !== undefined && v !== "" && v !== null,
        ),
      )
    : undefined;
  await api(`/api/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify({
      durable: body.durable ?? true,
      auto_delete: body.auto_delete ?? false,
      ...(argumentsPayload && Object.keys(argumentsPayload).length > 0
        ? { arguments: argumentsPayload }
        : {}),
    }),
  });
}

export async function deleteQueue(vhost: string, name: string): Promise<void> {
  await api(`/api/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
}

export async function purgeQueue(vhost: string, name: string): Promise<{ message_count: number }> {
  return api(`/api/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}/purge`, {
    method: "POST",
  });
}

export async function listExchanges(vhost: string): Promise<Page<ExchangeItem>> {
  return api<Page<ExchangeItem>>(`/api/exchanges/${encodeURIComponent(vhost)}`);
}

export async function createExchange(
  vhost: string,
  name: string,
  body: { type?: string; durable?: boolean; auto_delete?: boolean } = {},
): Promise<void> {
  await api(
    `/api/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
    {
      method: "PUT",
      body: JSON.stringify({
        type: body.type ?? "direct",
        durable: body.durable ?? true,
        auto_delete: body.auto_delete ?? false,
      }),
    },
  );
}

export async function deleteExchange(vhost: string, name: string): Promise<void> {
  await api(
    `/api/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(name)}`,
    { method: "DELETE" },
  );
}

export async function listBindings(vhost: string): Promise<Page<BindingItem>> {
  return api<Page<BindingItem>>(`/api/bindings/${encodeURIComponent(vhost)}`);
}

export async function createBinding(
  vhost: string,
  body: { source: string; destination: string; routing_key?: string },
): Promise<void> {
  await api(`/api/bindings/${encodeURIComponent(vhost)}`, {
    method: "POST",
    body: JSON.stringify({
      source: body.source,
      destination: body.destination,
      routing_key: body.routing_key ?? "",
      destination_type: "queue",
    }),
  });
}

export async function deleteBinding(
  vhost: string,
  exchange: string,
  queue: string,
  routingKey: string,
): Promise<void> {
  await api(
    `/api/bindings/${encodeURIComponent(vhost)}/${encodeURIComponent(exchange)}/${encodeURIComponent(queue)}/${encodeURIComponent(routingKey)}`,
    { method: "DELETE" },
  );
}

export async function listUsers(): Promise<UserItem[]> {
  return api<UserItem[]>("/api/users");
}

export async function createUser(
  name: string,
  password: string,
  tags: string[] = ["management"],
): Promise<void> {
  await api(`/api/users/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify({ password, tags }),
  });
}

export async function deleteUser(name: string): Promise<void> {
  await api(`/api/users/${encodeURIComponent(name)}`, { method: "DELETE" });
}

export async function listConnections(): Promise<Page<ConnectionItem>> {
  return api<Page<ConnectionItem>>("/api/connections");
}

export async function forceCloseConnection(name: string): Promise<void> {
  await api(`/api/connections/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
}

export async function exportDefinitions(): Promise<unknown> {
  return api<unknown>("/api/definitions");
}

export async function importDefinitions(body: unknown): Promise<void> {
  await api("/api/definitions", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function publishMessage(
  vhost: string,
  exchange: string,
  body: {
    routing_key?: string;
    payload: string;
    payload_encoding?: string;
    properties?: { delivery_mode?: number; content_type?: string; priority?: number };
  },
): Promise<PublishResult> {
  return api<PublishResult>(
    `/api/exchanges/${encodeURIComponent(vhost)}/${encodeURIComponent(exchange)}/publish`,
    {
      method: "POST",
      body: JSON.stringify({
        routing_key: body.routing_key ?? "",
        payload: body.payload,
        payload_encoding: body.payload_encoding ?? "string",
        properties: body.properties ?? {},
      }),
    },
  );
}

export async function getMessages(
  vhost: string,
  queue: string,
  count = 1,
  ackmode = "ack_requeue_false",
): Promise<GetMessage[]> {
  return api<GetMessage[]>(
    `/api/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queue)}/get`,
    {
      method: "POST",
      body: JSON.stringify({ count, ackmode, encoding: "auto" }),
    },
  );
}

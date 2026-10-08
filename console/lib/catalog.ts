export type ExchangeRow = {
  name: string;
  type: string;
  durable: boolean;
};

export type ConnectionRow = {
  name: string;
  user: string;
  peerHost: string;
  peerPort: number;
  channels: number;
};

/**
 * The rows of a management list. RabbitMQ answers a bare array unless the
 * request pages; QueueForge answers `{items: [...]}`. Anything else is null.
 */
export function rowsOf(body: unknown): Record<string, unknown>[] | null {
  const items = Array.isArray(body)
    ? body
    : body !== null && typeof body === "object" && Array.isArray((body as { items?: unknown }).items)
      ? (body as { items: unknown[] }).items
      : null;
  if (!items) return null;
  return items.filter((item): item is Record<string, unknown> => item !== null && typeof item === "object");
}

export function parseExchanges(body: unknown): ExchangeRow[] | null {
  const items = rowsOf(body);
  if (!items) return null;
  return items.flatMap((item) => {
    const exchange = item !== null && typeof item === "object" ? (item as { name?: unknown; type?: unknown; durable?: unknown }) : null;
    if (!exchange || typeof exchange.name !== "string") return [];
    return [{ name: exchange.name, type: typeof exchange.type === "string" ? exchange.type : "", durable: exchange.durable === true }];
  });
}

export function parseConnections(body: unknown): { rows: ConnectionRow[]; total: number | null } | null {
  const items = rowsOf(body);
  if (!items) return null;
  const row = !Array.isArray(body) && body !== null && typeof body === "object" ? (body as { total_count?: unknown }) : {};
  const rows = items.flatMap((item) => {
    const connection = item !== null && typeof item === "object" ? (item as Record<string, unknown>) : null;
    if (!connection || typeof connection.name !== "string") return [];
    return [
      {
        name: connection.name,
        user: typeof connection.user === "string" ? connection.user : "",
        peerHost: typeof connection.peer_host === "string" ? connection.peer_host : "",
        peerPort: typeof connection.peer_port === "number" ? connection.peer_port : 0,
        channels: typeof connection.channels === "number" ? connection.channels : 0,
      },
    ];
  });
  const total = typeof row.total_count === "number" ? row.total_count : null;
  return { rows, total };
}

export type DefinitionCounts = {
  vhosts: number;
  queues: number;
  exchanges: number;
  bindings: number;
};

function countOf(value: unknown): number {
  if (Array.isArray(value)) return value.length;
  if (value !== null && typeof value === "object") return Object.keys(value).length;
  return 0;
}

export function definitionCounts(body: unknown): DefinitionCounts | null {
  if (body === null || typeof body !== "object") return null;
  const row = body as Record<string, unknown>;
  return {
    vhosts: countOf(row.vhosts),
    queues: countOf(row.queues),
    exchanges: countOf(row.exchanges),
    bindings: countOf(row.bindings),
  };
}

export function firstGauge(text: string, name: string): number | null {
  const pattern = new RegExp(`^${name}(?:\\{[^}]*\\})?\\s+(\\d+(?:\\.\\d+)?)$`);
  for (const line of text.split("\n")) {
    const match = pattern.exec(line.trim());
    if (match) return Number(match[1]);
  }
  return null;
}

export function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) return "—";
  if (value < 1024) return `${Math.round(value)} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let size = value / 1024;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size >= 10 ? size.toFixed(0) : size.toFixed(1)} ${units[unit]}`;
}

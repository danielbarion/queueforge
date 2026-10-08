export type QueueSnap = {
  name: string;
  type: string;
  messages: number;
  messagesReady: number;
  messagesUnacked: number;
  consumers: number;
};

export type OverviewSnap = {
  connections: number;
  publish: number;
  deliver: number;
  ack: number;
  version: string;
};

export type Rates = {
  publish: number | null;
  deliver: number | null;
  ack: number | null;
};

export type ActivityEvent = {
  id: string;
  at: number;
  text: string;
};

export type AlertRules = {
  readyEnabled: boolean;
  readyOver: number;
  unackedEnabled: boolean;
  unackedOver: number;
  fsyncEnabled: boolean;
  cooldownMs: number;
};

export type AlertMemory = {
  ready: boolean;
  unacked: boolean;
  fsync: boolean;
  readyAt: number;
  unackedAt: number;
  fsyncAt: number;
};

function num(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : null;
}

export function parseOverview(body: unknown): OverviewSnap | null {
  const row = record(body);
  if (!row) return null;
  // A broker with no traffic yet (RabbitMQ) can omit message_stats.
  const totals = record(row.object_totals) ?? {};
  const stats = record(row.message_stats) ?? {};
  return {
    connections: num(totals.connections),
    publish: num(stats.publish),
    deliver: num(stats.deliver),
    ack: num(stats.ack),
    version: typeof row.product_version === "string" ? row.product_version : "",
  };
}

export function parseQueues(body: unknown): QueueSnap[] {
  const row = record(body);
  const items = Array.isArray(body) ? body : row?.items;
  if (!Array.isArray(items)) return [];
  return items.flatMap((item) => {
    const queue = record(item);
    if (!queue || typeof queue.name !== "string") return [];
    const ready = num(queue.messages_ready);
    const unacked = num(queue.messages_unacknowledged);
    return [
      {
        name: queue.name,
        type: typeof queue.type === "string" ? queue.type : "classic",
        messages: num(queue.messages) || ready + unacked,
        messagesReady: ready,
        messagesUnacked: unacked,
        consumers: num(queue.consumers),
      },
    ];
  });
}

export type BindingSnap = {
  source: string;
  routingKey: string;
};

export function parseBindings(body: unknown, queue: string): BindingSnap[] {
  const row = record(body);
  const items = Array.isArray(body) ? body : row?.items;
  if (!Array.isArray(items)) return [];
  return items.flatMap((item) => {
    const binding = record(item);
    if (!binding || typeof binding.source !== "string") return [];
    if (typeof binding.destination === "string" && binding.destination !== queue) return [];
    if (typeof binding.destination_type === "string" && binding.destination_type !== "queue") return [];
    return [{ source: binding.source, routingKey: typeof binding.routing_key === "string" ? binding.routing_key : "" }];
  });
}

export function ratesFrom(prev: OverviewSnap, next: OverviewSnap, elapsedMs: number): Rates {
  if (elapsedMs <= 0) return { publish: null, deliver: null, ack: null };
  const perSecond = (before: number, after: number) => (after < before ? null : (after - before) / (elapsedMs / 1000));
  return {
    publish: perSecond(prev.publish, next.publish),
    deliver: perSecond(prev.deliver, next.deliver),
    ack: perSecond(prev.ack, next.ack),
  };
}

export function diffActivity(
  prevQueues: QueueSnap[],
  nextQueues: QueueSnap[],
  prevConnections: number,
  nextConnections: number,
  at: number,
): ActivityEvent[] {
  const events: ActivityEvent[] = [];
  const push = (text: string) => events.push({ id: `${at}-${events.length}-${text}`, at, text });
  if (prevConnections !== nextConnections) push(`Connections ${prevConnections} → ${nextConnections}`);
  const before = new Map(prevQueues.map((queue) => [queue.name, queue]));
  const after = new Map(nextQueues.map((queue) => [queue.name, queue]));
  for (const queue of nextQueues) {
    const prior = before.get(queue.name);
    if (!prior) push(`Queue ${queue.name} appeared`);
    else if (prior.messagesReady !== queue.messagesReady) push(`${queue.name} ready ${prior.messagesReady} → ${queue.messagesReady}`);
  }
  for (const queue of prevQueues) {
    if (!after.has(queue.name)) push(`Queue ${queue.name} disappeared`);
  }
  return events;
}

export function totals(queues: QueueSnap[]): { ready: number; unacked: number } {
  return queues.reduce(
    (sum, queue) => ({ ready: sum.ready + queue.messagesReady, unacked: sum.unacked + queue.messagesUnacked }),
    { ready: 0, unacked: 0 },
  );
}

export const EMPTY_ALERT_MEMORY: AlertMemory = {
  ready: false,
  unacked: false,
  fsync: false,
  readyAt: 0,
  unackedAt: 0,
  fsyncAt: 0,
};

export function nextAlerts(
  rules: AlertRules,
  memory: AlertMemory,
  sample: { ready: number; unacked: number; fsync: number | null; at: number },
  stale: boolean,
): { memory: AlertMemory; fired: string[] } {
  if (stale) return { memory, fired: [] };
  const fired: string[] = [];
  const step = (
    enabled: boolean,
    over: boolean,
    was: boolean,
    lastAt: number,
    text: string,
  ): { on: boolean; at: number } => {
    if (!enabled || !over) return { on: false, at: lastAt };
    if (!was && (lastAt === 0 || sample.at - lastAt >= rules.cooldownMs)) {
      fired.push(text);
      return { on: true, at: sample.at };
    }
    return { on: true, at: lastAt };
  };
  const ready = step(rules.readyEnabled, sample.ready > rules.readyOver, memory.ready, memory.readyAt, `Ready is ${sample.ready}, over ${rules.readyOver}`);
  const unacked = step(
    rules.unackedEnabled,
    sample.unacked > rules.unackedOver,
    memory.unacked,
    memory.unackedAt,
    `Unacked is ${sample.unacked}, over ${rules.unackedOver}`,
  );
  const fsync =
    sample.fsync === null
      ? { on: memory.fsync, at: memory.fsyncAt }
      : step(rules.fsyncEnabled, sample.fsync > 0, memory.fsync, memory.fsyncAt, `Confirm-before-fsync is ${sample.fsync}`);
  return {
    memory: { ready: ready.on, unacked: unacked.on, fsync: fsync.on, readyAt: ready.at, unackedAt: unacked.at, fsyncAt: fsync.at },
    fired,
  };
}

/**
 * Dynamic shovels, as RabbitMQ's `/api/parameters/shovel/{vhost}/{name}`.
 *
 * A shovel moves messages from a source to a destination. Either end speaks
 * `amqp091` (src-queue / src-exchange, dest-queue / dest-exchange) or
 * `amqp10` (src-address / dest-address such as `/queues/x`, or a plain queue
 * name). A URI with no host (`amqp://` or `amqp:///vhost`) means this broker,
 * as in RabbitMQ; those ends run in process with no socket. A stream queue is
 * a valid source: `src-consumer-args` such as `x-stream-offset` pick where it
 * starts. Each message is settled at the source only after the destination
 * accepted it (ack-mode on-confirm, the default). Definitions are stored, so
 * a shovel starts again with the broker.
 */
import amqp from "amqplib";
import { Amqp10Client } from "../amqp10/client.ts";
import { inbound, outbound, readProps, writeProps } from "../amqp10/map.ts";
import { readTable, R, type Field } from "../codec.ts";
import { Broker } from "./class.ts";
import type { Consumer, LiveMsg } from "./model.ts";

export type ShovelDef = {
  vhost: string;
  name: string;
  srcUri: string;
  /** Source queue: `src-queue`, or the queue an `amqp10` `src-address` names. */
  srcQueue: string;
  destUri: string;
  /** Destination queue, or "" when `destExchange` is set. */
  destQueue: string;
  srcProtocol?: "amqp091" | "amqp10";
  destProtocol?: "amqp091" | "amqp10";
  /** `amqp10` addresses as given. */
  srcAddress?: string;
  destAddress?: string;
  /** `amqp091` source exchange: the shovel binds its own queue to it. */
  srcExchange?: string;
  srcExchangeKey?: string;
  destExchange?: string;
  destExchangeKey?: string;
  srcConsumerArgs?: Record<string, unknown>;
  prefetch?: number;
  ackMode?: "on-confirm" | "on-publish" | "no-ack";
  /** The parameter value as the client sent it, returned by the parameters API. */
  value?: Record<string, unknown>;
};

type ShovelMsg = {
  body: Uint8Array;
  routingKey: string;
  propRaw: Uint8Array;
  headers: Array<[string, Field]>;
  persistent: boolean;
  priority: number;
};

export type ShovelRun = {
  def: ShovelDef;
  stop: () => void;
  state: "starting" | "running" | "terminated";
  reason?: string;
  timestamp: string;
};

/** The vhost a local `amqp://` URI names, or null for a remote URI. */
export function localVhost(uri: string, fallback: string): string | null {
  const m = /^amqps?:\/\/([^/]*)(?:\/(.*))?$/.exec(uri.trim());
  if (!m) return null;
  if (m[1] !== "") return null;
  return m[2] ? decodeURIComponent(m[2]) : fallback;
}

/**
 * A queue or exchange from an AMQP 1.0 address: `/queues/q`, `/exchanges/x[/key]`,
 * the v1 forms `/queue/q`, `/exchange/x[/key]`, `/amq/queue/q`, or a plain queue name.
 */
export function shovelAddress(address: string): { queue: string } | { exchange: string; key: string | null } | null {
  if (!address.startsWith("/")) return address ? { queue: address } : null;
  const parts = address.split("/").map((p) => {
    try {
      return decodeURIComponent(p);
    } catch {
      return p;
    }
  });
  if ((parts[1] === "queues" || parts[1] === "queue") && parts.length === 3 && parts[2]) return { queue: parts[2] };
  if (parts[1] === "amq" && parts[2] === "queue" && parts.length === 4 && parts[3]) return { queue: parts[3] };
  if ((parts[1] === "exchanges" || parts[1] === "exchange") && (parts.length === 3 || parts.length === 4)) {
    const exchange = parts[2] === "amq.default" ? "" : parts[2]!;
    return { exchange, key: parts.length === 4 ? parts[3]! : null };
  }
  return null;
}

function stripPassword(uri: string): string {
  return uri.replace(/^(amqps?:\/\/[^:@/]*):[^@/]*@/, "$1@");
}

/**
 * A shovel definition from the parameter value. Throws when the source or
 * destination is missing.
 */
export function shovelFrom(vhost: string, name: string, value: Record<string, unknown>): ShovelDef {
  const text = (k: string) => (typeof value[k] === "string" && value[k] !== "" ? (value[k] as string) : undefined);
  const uri = (k: string) => {
    const v = value[k];
    if (Array.isArray(v)) return String(v[0] ?? "amqp://") || "amqp://";
    return typeof v === "string" && v ? v : "amqp://";
  };
  const srcProtocol = text("src-protocol") === "amqp10" ? "amqp10" : "amqp091";
  const destProtocol = text("dest-protocol") === "amqp10" ? "amqp10" : "amqp091";
  const def: ShovelDef = { vhost, name, srcUri: uri("src-uri"), srcQueue: "", destUri: uri("dest-uri"), destQueue: "", srcProtocol, destProtocol, value };
  const queueOf = shovelAddress;
  const srcAddress = text("src-address");
  if (srcProtocol === "amqp10" || (srcAddress && !text("src-queue"))) {
    if (!srcAddress) throw new Error("src-address is required");
    const parsed = queueOf(srcAddress);
    if (!parsed || !("queue" in parsed)) throw new Error(`src-address '${srcAddress}' is not a queue`);
    def.srcAddress = srcAddress;
    def.srcQueue = parsed.queue;
  } else if (text("src-queue")) def.srcQueue = text("src-queue")!;
  else if (text("src-exchange")) {
    def.srcExchange = text("src-exchange")!;
    def.srcExchangeKey = text("src-exchange-key") ?? "#";
  } else throw new Error("src-queue and dest-queue are required");
  const destAddress = text("dest-address");
  if (destProtocol === "amqp10" || (destAddress && !text("dest-queue") && !text("dest-exchange"))) {
    if (!destAddress) throw new Error("dest-address is required");
    const parsed = queueOf(destAddress);
    if (!parsed) throw new Error(`dest-address '${destAddress}' is not valid`);
    def.destAddress = destAddress;
    if ("queue" in parsed) def.destQueue = parsed.queue;
    else {
      def.destExchange = parsed.exchange;
      def.destExchangeKey = parsed.key ?? "";
    }
  } else if (text("dest-queue")) def.destQueue = text("dest-queue")!;
  else if (text("dest-exchange")) {
    def.destExchange = text("dest-exchange")!;
    def.destExchangeKey = text("dest-exchange-key");
  } else throw new Error("src-queue and dest-queue are required");
  const args = value["src-consumer-args"];
  if (args && typeof args === "object" && !Array.isArray(args)) def.srcConsumerArgs = args as Record<string, unknown>;
  const prefetch = Number(value["src-prefetch-count"] ?? value["prefetch-count"]);
  if (Number.isFinite(prefetch) && prefetch > 0) def.prefetch = prefetch;
  const ack = text("ack-mode");
  if (ack === "on-publish" || ack === "no-ack" || ack === "on-confirm") def.ackMode = ack;
  return def;
}

/** The parameter value of a stored definition, also for rows written before `value` was kept. */
export function shovelValue(def: ShovelDef): Record<string, unknown> {
  return def.value ?? { "src-uri": def.srcUri, "src-queue": def.srcQueue, "dest-uri": def.destUri, "dest-queue": def.destQueue };
}

function fieldOf(v: unknown): Field {
  if (typeof v === "boolean") return { t: "t", v };
  if (typeof v === "number" && Number.isInteger(v)) return { t: "l", v };
  return { t: "S", v: String(v) };
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

/** amqplib's decoded properties back to raw 0-9-1 property bytes (via the 1.0 mapping's writer). */
function rawFromAmqplib(p: amqp.MessageProperties): Uint8Array {
  return writeProps({
    contentType: p.contentType ?? undefined,
    contentEncoding: p.contentEncoding ?? undefined,
    headers: p.headers ? Object.entries(p.headers).map(([k, v]) => [k, fieldOf(v)] as [string, Field]) : [],
    deliveryMode: p.deliveryMode == null ? undefined : Number(p.deliveryMode),
    priority: p.priority == null ? undefined : Number(p.priority),
    correlationId: p.correlationId ?? undefined,
    replyTo: p.replyTo ?? undefined,
    expiration: p.expiration == null ? undefined : String(p.expiration),
    messageId: p.messageId ?? undefined,
    timestamp: p.timestamp == null ? undefined : Number(p.timestamp),
    type: p.type ?? undefined,
    userId: p.userId ?? undefined,
    appId: p.appId ?? undefined,
  } as Parameters<typeof writeProps>[0]);
}

type Source = { start: (deliver: (m: ShovelMsg, settle: (ok: boolean) => Promise<void> | void) => void) => Promise<void>; stop: () => Promise<void> };
type Dest = { publish: (m: ShovelMsg) => Promise<boolean>; stop: () => Promise<void> };

function localSource(this: Broker, def: ShovelDef, vhost: string, onLost: (why: string) => void): Source {
  let tag = "";
  let queue = def.srcQueue;
  let ownQueue = false;
  return {
    start: async (deliver) => {
      if (def.srcExchange) {
        // An exchange source gets its own queue, as RabbitMQ's shovel declares one.
        queue = `amq.gen-shovel-${def.name}`;
        ownQueue = true;
        await this.declareQueue({ vhost, name: queue, durable: true, exclusive: false, autoDelete: false, passive: false, args: {} });
        await this.bind(vhost, def.srcExchange, queue, def.srcExchangeKey ?? "#", []);
      }
      const q = this.queues.get(this.key(vhost, queue));
      if (!q) throw new Error(`no queue '${queue}' in vhost '${vhost}'`);
      const stream = q.argsParsed.queueType === "stream";
      const prefetch = def.prefetch ?? 1000;
      const noAck = def.ackMode === "no-ack" && !stream;
      let inflight = 0;
      tag = `shovel-${def.vhost}-${def.name}-${crypto.randomUUID()}`;
      const offset = def.srcConsumerArgs?.["x-stream-offset"];
      const consumer: Consumer = {
        tag,
        session: this.nextSession(),
        noAck,
        exclusive: false,
        prefetch,
        ...(stream ? { streamOffset: offset == null ? { t: "S", v: "next" } : fieldOf(offset) } : {}),
        want: () => inflight < prefetch,
        onCancel: () => onLost(`source queue '${queue}' was deleted`),
        deliver: (msg: LiveMsg) => {
          inflight++;
          deliver(
            { body: msg.body, routingKey: msg.routingKey, propRaw: msg.propRaw, headers: msg.headers, persistent: msg.persistent, priority: msg.priority },
            async (ok) => {
              try {
                if (!noAck) {
                  if (ok) await this.ack(vhost, queue, msg.id);
                  else await this.nack(vhost, queue, msg.id, true);
                }
              } finally {
                inflight--;
                if (stream) this.pumpStream(q);
                else void this.kick(vhost, queue, consumer.session);
              }
            },
          );
        },
      };
      await this.consume(vhost, queue, consumer);
      await this.kick(vhost, queue, consumer.session);
    },
    stop: async () => {
      if (tag) await this.cancel(vhost, queue, tag).catch(() => {});
      if (ownQueue) await this.deleteQueue(vhost, queue).catch(() => 0);
    },
  };
}

function localDest(this: Broker, def: ShovelDef, vhost: string): Dest {
  return {
    publish: async (m) => {
      const exchange = def.destExchange ?? "";
      const routingKey = def.destExchange != null ? def.destExchangeKey ?? m.routingKey : def.destQueue;
      const result = await this.publish({
        vhost,
        exchange,
        routingKey,
        body: m.body,
        headers: m.headers,
        propRaw: m.propRaw,
        persistent: m.persistent,
        priority: m.priority,
        expiration: "",
        confirm: true,
      });
      return result === "ack";
    },
    stop: async () => {},
  };
}

function remote091Source(def: ShovelDef, onLost: (why: string) => void): Source {
  let conn: amqp.ChannelModel | null = null;
  return {
    start: async (deliver) => {
      conn = await amqp.connect(def.srcUri);
      conn.on("error", () => {});
      conn.on("close", () => onLost("source connection closed"));
      const ch = await conn.createChannel();
      let queue = def.srcQueue;
      if (def.srcExchange) {
        queue = (await ch.assertQueue("", { exclusive: true })).queue;
        await ch.bindQueue(queue, def.srcExchange, def.srcExchangeKey ?? "#");
      }
      await ch.prefetch(def.prefetch ?? 1000);
      const noAck = def.ackMode === "no-ack";
      await ch.consume(queue, (m) => {
        if (!m) return onLost("source consumer cancelled");
        const propRaw = rawFromAmqplib(m.properties);
        deliver(
          { body: new Uint8Array(m.content), routingKey: m.fields.routingKey, propRaw, headers: readHeaders(propRaw), persistent: m.properties.deliveryMode === 2, priority: Number(m.properties.priority ?? 0) },
          (ok) => {
            if (noAck) return;
            if (ok) ch.ack(m);
            else ch.nack(m, false, true);
          },
        );
      }, { noAck, arguments: def.srcConsumerArgs ?? {} });
    },
    stop: async () => {
      const c = conn;
      conn = null;
      c?.removeAllListeners("close");
      await c?.close().catch(() => {});
    },
  };
}

function remote091Dest(def: ShovelDef, onLost: (why: string) => void): Dest {
  let conn: amqp.ChannelModel | null = null;
  let ch: amqp.ConfirmChannel | null = null;
  return {
    publish: async (m) => {
      if (!ch) {
        conn = await amqp.connect(def.destUri);
        conn.on("error", () => {});
        conn.on("close", () => {
          ch = null;
          onLost("destination connection closed");
        });
        ch = await conn.createConfirmChannel();
      }
      const props = readProps(m.propRaw);
      const exchange = def.destExchange ?? "";
      const routingKey = def.destExchange != null ? def.destExchangeKey ?? m.routingKey : def.destQueue;
      const c = ch;
      return new Promise<boolean>((resolve) => {
        c.publish(exchange, routingKey, Buffer.from(m.body), {
          persistent: m.persistent,
          priority: m.priority || undefined,
          contentType: props.contentType,
          contentEncoding: props.contentEncoding,
          correlationId: props.correlationId,
          replyTo: props.replyTo,
          messageId: props.messageId,
          type: props.type,
          appId: props.appId,
          headers: Object.fromEntries(m.headers.map(([k, f]) => [k, "v" in f ? f.v : null])),
        }, (err) => resolve(!err));
      });
    },
    stop: async () => {
      const c = conn;
      conn = null;
      ch = null;
      c?.removeAllListeners("close");
      await c?.close().catch(() => {});
    },
  };
}

function remote10Source(def: ShovelDef, onLost: (why: string) => void): Source {
  let client: Amqp10Client | null = null;
  return {
    start: async (deliver) => {
      client = await Amqp10Client.connect(def.srcUri);
      client.onClose(() => onLost("source connection closed"));
      const c = client;
      const address = def.srcAddress?.startsWith("/") ? def.srcAddress : `/queues/${encodeURIComponent(def.srcQueue)}`;
      await c.receive(address, def.prefetch ?? 1000, (payload, id) => {
        const m = inbound(payload);
        const propRaw = writeProps(m.props);
        deliver(
          { body: m.body, routingKey: m.subject ?? "", propRaw, headers: m.props.headers ?? [], persistent: m.persistent, priority: m.priority },
          (ok) => c.settle(id, ok),
        );
      });
    },
    stop: async () => {
      client?.close();
      client = null;
    },
  };
}

function remote10Dest(def: ShovelDef, onLost: (why: string) => void): Dest {
  let send: ((payload: Uint8Array) => Promise<boolean>) | null = null;
  let client: Amqp10Client | null = null;
  return {
    publish: async (m) => {
      if (!send) {
        client = await Amqp10Client.connect(def.destUri);
        client.onClose(() => {
          send = null;
          onLost("destination connection closed");
        });
        const address = def.destAddress?.startsWith("/")
          ? def.destAddress
          : def.destExchange != null
            ? `/exchanges/${encodeURIComponent(def.destExchange)}${def.destExchangeKey ? `/${encodeURIComponent(def.destExchangeKey)}` : ""}`
            : `/queues/${encodeURIComponent(def.destQueue)}`;
        send = await client.sender(address);
      }
      return send(outbound({ id: "", rowId: null, body: m.body, exchange: "", routingKey: m.routingKey, headers: m.headers, propRaw: m.propRaw, persistent: m.persistent, priority: m.priority, expiresAt: null, redelivered: false }));
    },
    stop: async () => {
      client?.close();
      client = null;
      send = null;
    },
  };
}

/** Start one shovel. A running shovel with the same vhost and name is stopped first. */
export function startShovel(this: Broker, def: ShovelDef): void {
  this.stopShovel(def.vhost, def.name);
  const srcVhost = localVhost(def.srcUri, def.vhost);
  const destVhost = localVhost(def.destUri, def.vhost);
  let stopped = false;
  let timer: Timer | null = null;
  const run: ShovelRun = { def, state: "starting", timestamp: new Date().toISOString(), stop: () => {} };
  let source: Source;
  let dest: Dest;
  const restart = (why: string) => {
    if (stopped || timer) return;
    run.state = "starting";
    run.reason = why;
    run.timestamp = new Date().toISOString();
    void source.stop().catch(() => {});
    void dest.stop().catch(() => {});
    timer = setTimeout(() => {
      timer = null;
      void begin();
    }, 1000);
    timer.unref?.();
  };
  source = srcVhost != null ? localSource.call(this, def, srcVhost, restart) : def.srcProtocol === "amqp10" ? remote10Source(def, restart) : remote091Source(def, restart);
  dest = destVhost != null ? localDest.call(this, def, destVhost) : def.destProtocol === "amqp10" ? remote10Dest(def, restart) : remote091Dest(def, restart);
  const onPublish = def.ackMode === "on-publish";
  const begin = async () => {
    if (stopped) return;
    try {
      await source.start((m, settle) => {
        const sent = dest.publish(m).catch(() => false);
        if (onPublish) void settle(true);
        void sent.then((ok) => {
          if (!onPublish) void settle(ok);
          if (!ok) restart("destination refused a message");
        });
      });
      if (stopped) return void source.stop().catch(() => {});
      run.state = "running";
      delete run.reason;
      run.timestamp = new Date().toISOString();
    } catch (err) {
      restart(err instanceof Error ? err.message : String(err));
    }
  };
  run.stop = () => {
    stopped = true;
    if (timer) clearTimeout(timer);
    run.state = "terminated";
    void source.stop().catch(() => {});
    void dest.stop().catch(() => {});
  };
  this.shovels.set(`${def.vhost}\0${def.name}`, run);
  void begin();
}

/** Stop one shovel. Returns false when none was running. */
export function stopShovel(this: Broker, vhost: string, name: string): boolean {
  const key = `${vhost}\0${name}`;
  const running = this.shovels.get(key);
  if (!running) return false;
  running.stop();
  this.shovels.delete(key);
  return true;
}

/** Store and start a shovel. */
export function putShovel(this: Broker, def: ShovelDef): void {
  this.store.putParameter("shovel", def.vhost, def.name, JSON.stringify(def));
  this.startShovel(def);
}

/** Stop and forget a shovel. Returns false when it did not exist. */
export function deleteShovel(this: Broker, vhost: string, name: string): boolean {
  const stored = this.store.deleteParameter("shovel", vhost, name);
  return this.stopShovel(vhost, name) || stored;
}

/** Start every stored shovel. Called once the queues are loaded. */
export function resumeShovels(this: Broker): void {
  for (const row of this.store.listParameters("shovel")) {
    try {
      this.startShovel(JSON.parse(row.value) as ShovelDef);
    } catch {
      /* a corrupt row is skipped */
    }
  }
}

/** Rows for `/api/shovels`, as RabbitMQ's shovel management plugin reports them. */
export function shovelStatus(this: Broker, vhost?: string) {
  return [...this.shovels.values()]
    .filter((r) => vhost == null || r.def.vhost === vhost)
    .map(({ def, state, reason, timestamp }) => ({
      node: this.cfg.nodeId || "queueforge",
      name: def.name,
      vhost: def.vhost,
      type: "dynamic",
      state,
      ...(reason ? { reason } : {}),
      timestamp,
      src_protocol: def.srcProtocol ?? "amqp091",
      dest_protocol: def.destProtocol ?? "amqp091",
      src_uri: stripPassword(def.srcUri),
      dest_uri: stripPassword(def.destUri),
      ...(def.srcAddress ? { src_address: def.srcAddress } : def.srcExchange ? { src_exchange: def.srcExchange, src_exchange_key: def.srcExchangeKey } : { src_queue: def.srcQueue }),
      ...(def.destAddress ? { dest_address: def.destAddress } : def.destExchange != null ? { dest_exchange: def.destExchange, dest_exchange_key: def.destExchangeKey } : { dest_queue: def.destQueue }),
    }));
}

Broker.prototype.startShovel = startShovel;
Broker.prototype.stopShovel = stopShovel;
Broker.prototype.putShovel = putShovel;
Broker.prototype.deleteShovel = deleteShovel;
Broker.prototype.resumeShovels = resumeShovels;
Broker.prototype.shovelStatus = shovelStatus;

declare module "./class.ts" {
  interface Broker {
    startShovel: typeof startShovel;
    stopShovel: typeof stopShovel;
    putShovel: typeof putShovel;
    deleteShovel: typeof deleteShovel;
    resumeShovels: typeof resumeShovels;
    shovelStatus: typeof shovelStatus;
  }
}

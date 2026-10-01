/**
 * Accepted AMQP socket and the method dispatcher.
 *
 * Owns the per-connection state and routes each decoded method to the
 * sibling that implements that class. Frame bytes live in `frames.ts`.
 */
import type { Broker, LiveMsg } from "../broker.ts";
import { ChanError } from "../broker.ts";
import type { Amqp10State } from "../protocols.ts";
import { emptyProps, R } from "../codec.ts";

/** One basic.publish waiting for its header and body. */
export type Pub = { exchange: string; routingKey: string; mandatory: boolean; immediate: boolean };

/** State for one AMQP channel on this connection. */
export type Ch = {
  confirm: boolean;
  tx: boolean;
  txBatch: Array<() => Promise<unknown>>;
  nextPub: number;
  nextDel: number;
  deliveries: Map<number, { vhost: string; queue: string; id: string; consumer: string }>;
  byConsumer: Map<string, number>;
  prefetch: number;
  globalPrefetch: number | null;
  globalUnacked: number;
  flow: boolean;
  consumers: Map<string, string>;
  publish: Pub | null;
  bodySize: number;
  got: number;
  chunks: Uint8Array[];
  propRaw: Uint8Array;
  headers: LiveMsg["headers"];
  deliveryMode: number;
  priority: number;
  expiration: string;
};

/** Socket surface the listener uses. Tests can pass a fake with these methods. */
export type AmqpSocket = {
  write: (b: Uint8Array) => number;
  end: () => void;
  flush?: () => void;
  remoteAddress?: string;
  remotePort?: number;
};

/**
 * One AMQP connection.
 *
 * Fields are visible to the sibling modules that implement its methods.
 * Callers outside this folder use {@link startAmqp} only.
 */
export class Conn {
  buf = new Uint8Array(0);
  stage: "header" | "frames" | "amqp10" = "header";
  amqp10: Amqp10State = { phase: "header", sender: null, receiver: null };
  user = "";
  vhost = "/";
  closed = false;
  channels = new Map<number, Ch>();
  heartbeat = 0;
  timer: Timer | null = null;
  writeChain: Promise<unknown> = Promise.resolve();
  connClosed = false;
  metricsOpened = false;
  /** Set once so a double close does not decrement the connection gauge twice. */
  metricsClosed = false;
  /** Management connection id, empty until connection.open succeeds. */
  mgmtName = "";
  /** Frames waiting for the socket to accept more bytes. */
  outbound: Uint8Array[] = [];

  constructor(
    readonly socket: AmqpSocket,
    readonly broker: Broker,
  ) {}
}

export interface Conn {
  send(frame: Uint8Array): Promise<unknown>;
  sendMany(frames: Uint8Array[]): Promise<unknown>;
  flush(): void;
  push(data: Uint8Array): Promise<void>;
  ch(id: number): Ch;
  onMethod(channel: number, payload: Uint8Array): Promise<void>;
  onHeader(channel: number, payload: Uint8Array): Promise<void>;
  onBody(channel: number, payload: Uint8Array): Promise<void>;
  beginPublish(c: Ch, payload: Uint8Array): void;
  finishPublish(channel: number, c: Ch): Promise<void>;
  txCommit(channel: number, c: Ch): Promise<void>;
  exDeclare(channel: number, c: Ch, payload: Uint8Array): Promise<void>;
  exDelete(channel: number, payload: Uint8Array): Promise<void>;
  exBind(channel: number, payload: Uint8Array): Promise<void>;
  exUnbind(channel: number, payload: Uint8Array): Promise<void>;
  qDeclare(channel: number, payload: Uint8Array): Promise<void>;
  qBind(channel: number, payload: Uint8Array): Promise<void>;
  qUnbind(channel: number, payload: Uint8Array): Promise<void>;
  qPurge(channel: number, payload: Uint8Array): Promise<void>;
  qDelete(channel: number, payload: Uint8Array): Promise<void>;
  qos(channel: number, c: Ch, payload: Uint8Array): Promise<void>;
  creditOk(c: Ch, tag: string): boolean;
  consume(channel: number, c: Ch, payload: Uint8Array): Promise<void>;
  deliver(channel: number, tag: string, dtag: number, msg: LiveMsg): Promise<void>;
  cancel(channel: number, c: Ch, payload: Uint8Array): Promise<void>;
  ack(c: Ch, payload: Uint8Array): Promise<void>;
  reject(c: Ch, payload: Uint8Array, fromNack: boolean): Promise<void>;
  nack(c: Ch, payload: Uint8Array): Promise<void>;
  settle(c: Ch, tag: number, multiple: boolean, negative: boolean, requeue: boolean): Promise<void>;
  get(channel: number, c: Ch, payload: Uint8Array): Promise<void>;
  recover(channel: number, c: Ch, payload: Uint8Array, methodId: number): Promise<void>;
  requeueChannel(c: Ch): Promise<void>;
  dropConsumers(): Promise<void>;
  requeueAll(): Promise<void>;
  chanClose(channel: number, code: number, text: string, classId?: number, methodId?: number): Promise<void>;
  syncMgmt(): void;
  noteMetricsClosed(): void;
  connClose(code: number, text: string): Promise<void>;
  handleStartOk(payload: Uint8Array): Promise<void>;
  handleTuneOk(payload: Uint8Array): void;
  handleConnectionOpen(payload: Uint8Array): Promise<void>;
  handleConnectionClose(): Promise<void>;
  handleChannelOpen(channel: number): Promise<void>;
  handleChannelClose(channel: number): Promise<void>;
  handleChannelFlow(channel: number, payload: Uint8Array): Promise<void>;
  handleConfirmTx(channel: number, c: Ch, payload: Uint8Array, cls: number, mid: number): Promise<boolean>;
}

/**
 * Accept AMQP connections and attach a {@link Conn} to each socket.
 *
 * @param host Bind address. `0.0.0.0` listens on every interface.
 * @param port TCP port. Must be free; the broker config chooses it.
 * @param broker Process-wide broker the connection calls into.
 * @returns The Bun listen server. Stop it by calling `server.stop()`.
 */
export function startAmqp(host: string, port: number, broker: Broker) {
  return Bun.listen({
    hostname: host,
    port,
    socket: {
      open(socket) {
        const conn = new Conn(socket, broker);
        socket.data = conn;
      },
      data(socket, data) {
        const conn = socket.data as Conn;
        const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
        void conn.push(bytes).catch(() => {
          conn.closed = true;
          conn.connClosed = true;
          try {
            socket.end();
          } catch {
            /* this connection is already gone */
          }
        });
      },
      drain(socket) {
        (socket.data as Conn).flush();
      },
      close(socket) {
        const conn = socket.data as Conn | undefined;
        if (!conn) return;
        conn.closed = true;
        conn.connClosed = true;
        conn.noteMetricsClosed();
        void conn.dropConsumers().then(() => conn.requeueAll()).catch(() => {
          /* a missing queue home rejects nack; that must not exit the process */
        });
      },
      error(socket) {
        const conn = socket.data as Conn | undefined;
        if (conn) {
          conn.closed = true;
          conn.connClosed = true;
        }
        try {
          socket.end();
        } catch {
          /* this connection is already gone */
        }
      },
    },
  });
}

/**
 * Return the channel state for `id`, creating a fresh one on first use.
 *
 * @param id AMQP channel number from the frame. Channel 0 is the connection.
 * @returns The existing or new channel record. The caller must not replace the map entry.
 */
export function ch(this: Conn, id: number): Ch {
  let c = this.channels.get(id);
  if (!c) {
    c = {
      confirm: false,
      tx: false,
      txBatch: [],
      nextPub: 1,
      nextDel: 1,
      deliveries: new Map(),
      byConsumer: new Map(),
      prefetch: 0,
      globalPrefetch: null,
      globalUnacked: 0,
      flow: true,
      consumers: new Map(),
      publish: null,
      bodySize: 0,
      got: 0,
      chunks: [],
      propRaw: emptyProps(),
      headers: [],
      deliveryMode: 1,
      priority: 0,
      expiration: "",
    };
    this.channels.set(id, c);
  }
  return c;
}

/**
 * Dispatch one method frame.
 *
 * @param channel Channel from the frame header.
 * @param payload Method payload including class and method ids.
 * A channel exception closes that channel. Anything else closes the connection.
 */
export async function onMethod(this: Conn, channel: number, payload: Uint8Array) {
  const r = new R(payload);
  const cls = r.u16();
  const mid = r.u16();
  if (channel === 0 && cls === 10 && mid === 11) {
    r.u32(); // skip client props table by size... readTable expects to read size
    // we already consumed size. rewind 4 and use readTable
  }
  if (cls === 10 && mid === 11) return this.handleStartOk(payload);
  if (cls === 10 && mid === 31) return this.handleTuneOk(payload);
  if (cls === 10 && mid === 40) return this.handleConnectionOpen(payload);
  if (cls === 10 && mid === 50) return this.handleConnectionClose();
  if (cls === 20 && mid === 10) return this.handleChannelOpen(channel);
  if (cls === 20 && mid === 40) return this.handleChannelClose(channel);
  if (cls === 20 && mid === 20) return this.handleChannelFlow(channel, payload);
  const c = this.ch(channel);
  try {
    if (cls === 40 && mid === 10) await this.exDeclare(channel, c, payload);
    else if (cls === 40 && mid === 20) await this.exDelete(channel, payload);
    else if (cls === 40 && mid === 30) await this.exBind(channel, payload);
    else if (cls === 40 && mid === 40) await this.exUnbind(channel, payload);
    else if (cls === 50 && mid === 10) await this.qDeclare(channel, payload);
    else if (cls === 50 && mid === 20) await this.qBind(channel, payload);
    else if (cls === 50 && mid === 50) await this.qUnbind(channel, payload);
    else if (cls === 50 && mid === 30) await this.qPurge(channel, payload);
    else if (cls === 50 && mid === 40) await this.qDelete(channel, payload);
    else if (cls === 60 && mid === 10) await this.qos(channel, c, payload);
    else if (cls === 60 && mid === 20) await this.consume(channel, c, payload);
    else if (cls === 60 && mid === 30) await this.cancel(channel, c, payload);
    else if (cls === 60 && mid === 40) this.beginPublish(c, payload);
    else if (cls === 60 && mid === 80) await this.ack(c, payload);
    else if (cls === 60 && mid === 90) await this.reject(c, payload, false);
    else if (cls === 60 && mid === 120) await this.nack(c, payload);
    else if (cls === 60 && mid === 70) await this.get(channel, c, payload);
    else if (cls === 60 && mid === 110 || cls === 60 && mid === 100) await this.recover(channel, c, payload, mid);
    else if (await this.handleConfirmTx(channel, c, payload, cls, mid)) {
      /* confirm or tx handled the method */
    } else if (cls === 20 && mid === 41) {
      /* close-ok */
    } else {
      await this.chanClose(channel, 540, `NOT_IMPLEMENTED - ${cls}.${mid}`);
    }
  } catch (err) {
    if (err instanceof ChanError) await this.chanClose(channel, err.code, err.message, cls, mid);
    else throw err;
  }
}

Conn.prototype.ch = ch;
Conn.prototype.onMethod = onMethod;

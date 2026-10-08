/**
 * Accepted AMQP socket and the method dispatcher.
 *
 * Owns the per-connection state and routes each decoded method to the
 * sibling that implements that class. Frame bytes live in `frames.ts`.
 */
import type { Broker, LiveMsg } from "../broker/index.ts";
import { ChanError } from "../broker/index.ts";
import type { Socket as NodeSocket } from "node:net";
import type { Amqp10Session } from "../amqp10/session.ts";
import { emptyProps, R } from "../codec.ts";

/** One basic.publish waiting for its header and body. */
export type Pub = { exchange: string; routingKey: string; mandatory: boolean; immediate: boolean };

/** State for one AMQP channel on this connection. */
export type Ch = {
  confirm: boolean;
  tx: boolean;
  txBatch: Array<() => Promise<unknown> | void>;
  nextPub: number;
  nextDel: number;
  deliveries: Map<number, { vhost: string; queue: string; id: string; consumer: string }>;
  /** Deliveries held until this channel has prefetch credit again. */
  parked: Array<{ channel: number; tag: string; queue: string; msg: LiveMsg; release?: () => void }>;
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
  userId: string;
  /** reply-to property of the publish being assembled. */
  replyTo: string;
  /** This channel's direct reply-to address, or empty. */
  replyAddr: string;
  /** Consumer tag of the reply consumer, or empty. */
  replyTag: string;
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
/** Source of {@link Conn.id}. */
let connSeq = 0;

export class Conn {
  buf: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  stage: "header" | "frames" | "amqp10" = "header";
  /** Common name of a verified client certificate; offers SASL EXTERNAL. */
  peerCN: string | null = null;
  /** Set once an AMQP 1.0 header arrives; every later byte goes to it. */
  amqp10: Amqp10Session | null = null;
  user = "";
  vhost = "/";
  closed = false;
  channels = new Map<number, Ch>();
  heartbeat = 0;
  timer: Timer | null = null;
  writeChain: Promise<unknown> = Promise.resolve();
  /** Slow-path writes still queued on `writeChain`. The idle path writes directly. */
  writesPending = 0;
  /** Index of the first unsent frame in `outbound`. */
  outHead = 0;
  connClosed = false;
  /** After a server connection.close: ends the socket once close-ok arrives or 1 s passes. */
  closeOkWait: (() => void) | null = null;
  metricsOpened = false;
  /** Set once so a double close does not decrement the connection gauge twice. */
  metricsClosed = false;
  /** Node socket when the parent handed this connection off. Null for a direct listen. */
  handed: import("node:net").Socket | null = null;
  migrated = false;
  /** Management connection id, empty until connection.open succeeds. */
  mgmtName = "";
  /** Frames waiting for the socket to accept more bytes. */
  outbound: Uint8Array[] = [];
  /**
   * The socket copies what it accepts before `write` returns, so a gathered
   * burst may use the shared buffer. Only Bun's own listener promises that;
   * a node socket queues the buffer it was given.
   */
  reuseWrites = false;
  /**
   * Frames gathered for one socket write. A microtask flushes them so a burst
   * shares one write without waiting a timer tick.
   */
  staged: Uint8Array[] = [];
  /** Byte length of `staged`. A full buffer is written before the microtask. */
  stagedBytes = 0;
  /** True while a microtask is already queued to flush `staged`. */
  coalesceScheduled = false;
  /** Bytes that arrived while a burst of publishes was being parsed. */
  fresh: Uint8Array[] = [];
  /**
   * Persistent confirms staged by the frame parser. They run after ack frames
   * already in the buffer, and in groups when the ready queue is ahead of consumers.
   */
  deferredPublish: Array<() => Promise<unknown>> = [];
  inboundRunning = false;
  inboundDone: Promise<void> = Promise.resolve();
  /**
   * Resolves the publish hold when more socket bytes arrive.
   * `driveInbound` sets this while consumers on this connection are behind.
   */
  wakeInbound: (() => void) | null = null;
  /** Process-unique id. Owns exclusive queues declared on this connection. */
  id = ++connSeq;
  /** Permission results, valid while `permRef` and `permLen` match the broker table. */
  permCache = new Map<string, boolean>();
  permRef: unknown = null;
  permLen = -1;
  /** Exchange whose existence and write permission the last publish checked. */
  writeEx: string | null = null;
  /** The client advertised the connection.blocked capability. */
  wantsBlocked = false;
  /** Registered with the broker while this connection is told about alarms. */
  alarmListener: ((blocked: boolean, reason: string) => void) | null = null;

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
  onHeader(channel: number, payload: Uint8Array): Promise<unknown> | void;
  onBody(channel: number, payload: Uint8Array): Promise<unknown> | void;
  beginPublish(c: Ch, payload: Uint8Array): void;
  finishPublish(channel: number, c: Ch): Promise<unknown> | void;
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
  handOff(channel: number, c: Ch, tag: string, queue: string, msg: LiveMsg): void;
  flushParked(c: Ch): void;
  deliver(channel: number, tag: string, dtag: number, msg: LiveMsg): void;
  cancel(channel: number, c: Ch, payload: Uint8Array): Promise<void>;
  ack(c: Ch, payload: Uint8Array): Promise<void> | void;
  reject(c: Ch, payload: Uint8Array, fromNack: boolean): Promise<void>;
  nack(c: Ch, payload: Uint8Array): Promise<void>;
  settle(c: Ch, tag: number, multiple: boolean, negative: boolean, requeue: boolean): Promise<void> | void;
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
  sendTune(): Promise<void>;
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
export function startAmqp(host: string, port: number, broker: Broker, reusePort = false, tls: { cert: string; key: string; ca?: string } | null = null) {
  type ListenSocket = AmqpSocket & { setNoDelay(on: boolean): void; data?: Conn };
  const options = {
    hostname: host,
    port,
    reusePort,
    // With a CA, client certificates are asked for but optional, as RabbitMQ's
    // verify_peer with fail_if_no_peer_cert=false; an untrusted one is dropped.
    ...(tls
      ? {
          tls: {
            cert: Bun.file(tls.cert),
            key: Bun.file(tls.key),
            ...(tls.ca ? { ca: Bun.file(tls.ca), requestCert: true, rejectUnauthorized: false } : {}),
          },
        }
      : {}),
    socket: {
      handshake(socket: ListenSocket & { authorized?: boolean; getPeerCertificate?: () => { subject?: { CN?: string } } }) {
        if (!tls?.ca) return;
        const subject = socket.getPeerCertificate?.()?.subject;
        if (subject && !socket.authorized) {
          socket.end();
          return;
        }
        if (subject?.CN && socket.data) socket.data.peerCN = subject.CN;
      },
      open(socket: ListenSocket) {
        socket.setNoDelay(true);
        const conn = new Conn(socket, broker);
        conn.reuseWrites = true;
        socket.data = conn;
      },
      data(socket: ListenSocket, data: Uint8Array | ArrayBuffer) {
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
      drain(socket: ListenSocket) {
        (socket.data as Conn).flush();
      },
      close(socket: ListenSocket) {
        const conn = socket.data;
        if (!conn) return;
        conn.closed = true;
        conn.connClosed = true;
        conn.noteMetricsClosed();
        conn.amqp10?.closed();
        void conn.dropConsumers().then(() => conn.requeueAll()).then(() => conn.dropExclusive()).finally(() => conn.unwatchAlarms()).catch(() => {
          /* a missing queue home rejects nack; that must not exit the process */
        });
      },
      error(socket: ListenSocket) {
        const conn = socket.data;
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
  };
  return Bun.listen(options as unknown as Bun.TCPSocketListenOptions<Conn>);
}

/** Run one adopted TCP socket as an AMQP connection. The parent already accepted it. */
export function adoptNodeSocket(broker: Broker, socket: NodeSocket) {
  socket.setNoDelay(true);
  socket.resume();
  const wrapper: AmqpSocket = {
    write(bytes) {
      try {
        socket.write(bytes);
      } catch {
        return 0;
      }
      return bytes.length;
    },
    end() {
      socket.end();
    },
    remoteAddress: socket.remoteAddress,
    remotePort: socket.remotePort,
  };
  const conn = new Conn(wrapper, broker);
  conn.handed = socket;
  const fail = () => {
    conn.closed = true;
    conn.connClosed = true;
    conn.noteMetricsClosed();
    try {
      socket.end();
    } catch {
      /* already closed */
    }
  };
  socket.on("data", (buf: Buffer) => {
    void conn.push(new Uint8Array(buf)).catch(fail);
  });
  socket.on("close", () => {
    conn.closed = true;
    conn.connClosed = true;
    conn.noteMetricsClosed();
    void conn.dropConsumers().then(() => conn.requeueAll()).then(() => conn.dropExclusive()).finally(() => conn.unwatchAlarms()).catch(() => {});
  });
  socket.on("error", fail);
}

type MigratedState = {
  user: string;
  vhost: string;
  channels: Array<{ id: number; confirm: boolean; prefetch: number }>;
};

/** Continue a connection the previous child already handshook. `bytes` starts at the unparsed frame. */
export function adoptMigrated(broker: Broker, socket: NodeSocket, state: MigratedState, bytes: Uint8Array) {
  socket.setNoDelay(true);
  const wrapper: AmqpSocket = {
    write(chunk) {
      try {
        socket.write(chunk);
      } catch {
        return 0;
      }
      return chunk.length;
    },
    end() {
      socket.end();
    },
    remoteAddress: socket.remoteAddress,
    remotePort: socket.remotePort,
  };
  const conn = new Conn(wrapper, broker);
  conn.handed = socket;
  conn.stage = "frames";
  conn.user = state.user;
  conn.vhost = state.vhost;
  for (const item of state.channels) {
    const ch = conn.ch(item.id);
    ch.confirm = item.confirm;
    ch.prefetch = item.prefetch;
  }
  const fail = () => {
    conn.closed = true;
    conn.connClosed = true;
    try {
      socket.end();
    } catch {
      /* already closed */
    }
  };
  socket.on("data", (buf: Buffer) => {
    void conn.push(new Uint8Array(buf)).catch(fail);
  });
  socket.on("error", fail);
  socket.resume();
  void conn.push(bytes).catch(fail);
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
      parked: [],
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
      userId: "",
      replyTo: "",
      replyAddr: "",
      replyTag: "",
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

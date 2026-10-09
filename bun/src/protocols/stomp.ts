/**
 * STOMP 1.0, 1.1 and 1.2, as RabbitMQ's STOMP plugin maps them onto AMQP.
 *
 * Destinations:
 * - `/queue/<name>`: a durable queue, declared on first use, through the
 *   default exchange.
 * - `/topic/<key>`: `amq.topic` with that routing key. A subscription gets its
 *   own exclusive queue bound with the key.
 * - `/exchange/<name>/<key>`: any exchange; a subscription binds its own queue.
 * - `/amq/queue/<name>`: an existing queue, used as is.
 *
 * Logins check the user store. ACK and NACK settle the queue message;
 * `client` mode acks cumulatively. BEGIN, COMMIT and ABORT hold SEND, ACK and
 * NACK until commit. Any frame with a `receipt` header gets a RECEIPT.
 *
 * {@link StompSession} does not own a socket, so TCP and WebSocket share it.
 */
import type { Broker, LiveMsg } from "../broker/index.ts";
import { ChanError } from "../broker/index.ts";
import { W, writeTable, type Field } from "../codec.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

type Frame = { command: string; headers: Map<string, string>; body: Uint8Array };
type Sub = {
  id: string;
  destination: string;
  queue: string;
  temporary: boolean;
  ack: "auto" | "client" | "client-individual";
  tag: string;
  prefetch: number;
  /** Unacked deliveries in order: ack id to queue message id. */
  pending: Map<string, string>;
};
type TxOp = () => Promise<void>;

let ownerSeq = 0;

/** Header values in 1.1 and 1.2 escape `\`, `:`, CR and LF. */
function unescape(v: string, version: string): string {
  if (version === "1.0") return v;
  return v.replace(/\\(.)/g, (_, c: string) => (c === "n" ? "\n" : c === "r" ? "\r" : c === "c" ? ":" : c === "\\" ? "\\" : c));
}

function escape(v: string, version: string): string {
  if (version === "1.0") return v;
  return v.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/:/g, "\\c");
}

/** Headers that describe the STOMP frame, not the message. */
const FRAME_HEADERS = new Set(["destination", "receipt", "transaction", "content-length", "content-type", "persistent", "priority", "expiration", "reply-to", "correlation-id", "message-id", "ack", "id", "subscription", "prefetch-count", "amqp-message-id"]);

/** One STOMP client connection. */
export class StompSession {
  private buf: Uint8Array = new Uint8Array(0);
  private version = "1.2";
  private vhost = "/";
  private user = "";
  private connected = false;
  private closed = false;
  private subs = new Map<string, Sub>();
  private txs = new Map<string, TxOp[]>();
  private chain: Promise<void> = Promise.resolve();
  private nextMessage = 1;
  private readonly owner = -(1_000_000 + ++ownerSeq);

  constructor(
    private readonly broker: Broker,
    private readonly write: (frame: Uint8Array) => void,
    private readonly end: () => void,
  ) {}

  feed(data: Uint8Array): void {
    this.chain = this.chain.then(() => this.drain(data)).catch((err) => this.fail(err));
  }

  closedByPeer(): void {
    this.chain = this.chain.then(() => this.teardown()).catch(() => {});
  }

  private async drain(data: Uint8Array) {
    const merged = new Uint8Array(this.buf.length + data.length);
    merged.set(this.buf);
    merged.set(data, this.buf.length);
    this.buf = merged;
    for (;;) {
      if (this.closed) return;
      // Heart-beats and blank lines between frames.
      let start = 0;
      while (start < this.buf.length && (this.buf[start] === 0x0a || this.buf[start] === 0x0d)) start++;
      if (start) this.buf = this.buf.subarray(start);
      const frame = this.parse();
      if (!frame) return;
      await this.handle(frame);
    }
  }

  /** One frame from the buffer, or null until it is complete. */
  private parse(): Frame | null {
    const b = this.buf;
    let headEnd = -1;
    for (let i = 0; i + 1 < b.length; i++) {
      if (b[i] === 0x0a && b[i + 1] === 0x0a) {
        headEnd = i;
        break;
      }
      if (b[i] === 0x0a && b[i + 1] === 0x0d && b[i + 2] === 0x0a) {
        headEnd = i;
        break;
      }
    }
    if (headEnd < 0) return null;
    const lines = dec.decode(b.subarray(0, headEnd)).split("\n").map((l) => l.replace(/\r$/, ""));
    const bodyStart = headEnd + (b[headEnd + 1] === 0x0d ? 3 : 2);
    const command = lines[0] ?? "";
    const raw = command === "CONNECT" || command === "STOMP";
    const headers = new Map<string, string>();
    for (const line of lines.slice(1)) {
      const at = line.indexOf(":");
      if (at <= 0) continue;
      const k = raw ? line.slice(0, at) : unescape(line.slice(0, at), this.version);
      // The first occurrence of a repeated header wins.
      if (!headers.has(k)) headers.set(k, raw ? line.slice(at + 1) : unescape(line.slice(at + 1), this.version));
    }
    const length = headers.has("content-length") ? Number(headers.get("content-length")) : NaN;
    let bodyEnd: number;
    if (Number.isFinite(length) && length >= 0) {
      bodyEnd = bodyStart + length;
      if (b.length < bodyEnd + 1) return null;
    } else {
      bodyEnd = b.indexOf(0, bodyStart);
      if (bodyEnd < 0) return null;
    }
    const body = b.slice(bodyStart, bodyEnd);
    this.buf = b.subarray(bodyEnd + 1);
    return { command, headers, body };
  }

  private send(command: string, headers: Array<[string, string]>, body: Uint8Array = new Uint8Array(0)) {
    const head = headers.map(([k, v]) => `${escape(k, this.version)}:${escape(v, this.version)}`).join("\n");
    const top = enc.encode(`${command}\n${head}${head ? "\n" : ""}\n`);
    const out = new Uint8Array(top.length + body.length + 1);
    out.set(top);
    out.set(body, top.length);
    out[out.length - 1] = 0;
    this.write(out);
  }

  private error(message: string, detail = "", receipt?: string) {
    const headers: Array<[string, string]> = [["message", message], ["content-type", "text/plain"]];
    if (receipt) headers.push(["receipt-id", receipt]);
    this.send("ERROR", headers, enc.encode(detail));
  }

  private fail(err: unknown) {
    this.error(err instanceof ChanError ? err.message : "internal error", String(err instanceof Error ? err.message : err));
    void this.teardown();
  }

  private async handle(f: Frame) {
    if (!this.connected && f.command !== "CONNECT" && f.command !== "STOMP") {
      this.error("not connected");
      return this.teardown();
    }
    const receipt = f.headers.get("receipt");
    try {
      switch (f.command) {
        case "CONNECT":
        case "STOMP":
          return await this.onConnect(f);
        case "SEND":
          await this.inTx(f, () => this.onSend(f));
          break;
        case "SUBSCRIBE":
          await this.onSubscribe(f);
          break;
        case "UNSUBSCRIBE":
          await this.onUnsubscribe(f);
          break;
        case "ACK":
        case "NACK":
          await this.inTx(f, () => this.onAck(f, f.command === "NACK"));
          break;
        case "BEGIN":
          this.txs.set(this.need(f, "transaction"), []);
          break;
        case "COMMIT": {
          const id = this.need(f, "transaction");
          const ops = this.txs.get(id);
          if (!ops) throw new ChanError(406, `transaction ${id} is not active`);
          this.txs.delete(id);
          for (const op of ops) await op();
          break;
        }
        case "ABORT": {
          const id = this.need(f, "transaction");
          if (!this.txs.delete(id)) throw new ChanError(406, `transaction ${id} is not active`);
          break;
        }
        case "DISCONNECT":
          if (receipt) this.send("RECEIPT", [["receipt-id", receipt]]);
          return this.teardown();
        default:
          throw new ChanError(406, `unknown command ${f.command}`);
      }
    } catch (err) {
      if (!(err instanceof ChanError)) throw err;
      this.error(err.message.replace(/^[A-Z_]+ - /, ""), err.message, receipt);
      return this.teardown();
    }
    if (receipt) this.send("RECEIPT", [["receipt-id", receipt]]);
  }

  private need(f: Frame, header: string): string {
    const v = f.headers.get(header);
    if (v == null || v === "") throw new ChanError(406, `missing ${header} header`);
    return v;
  }

  private async inTx(f: Frame, op: TxOp) {
    const tx = f.headers.get("transaction");
    if (!tx) return op();
    const ops = this.txs.get(tx);
    if (!ops) throw new ChanError(406, `transaction ${tx} is not active`);
    ops.push(op);
  }

  private async onConnect(f: Frame) {
    const accept = (f.headers.get("accept-version") ?? "1.0").split(",");
    this.version = accept.includes("1.2") ? "1.2" : accept.includes("1.1") ? "1.1" : "1.0";
    const host = f.headers.get("host");
    if (host && this.broker.vhosts.has(host)) this.vhost = host;
    const login = f.headers.get("login") ?? "";
    const passcode = f.headers.get("passcode") ?? "";
    if (!login || !(await this.broker.verify(login, passcode)) || !this.broker.hasVhostAccess(login, this.vhost)) {
      this.error("Access refused", "Bad credentials or no access to the vhost");
      return this.teardown();
    }
    this.user = login;
    this.connected = true;
    this.send("CONNECTED", [
      ["version", this.version],
      ["server", "QueueForge"],
      ["heart-beat", "0,0"],
      ["session", `session-${crypto.randomUUID()}`],
    ]);
  }

  /** Resolve a destination to the exchange and routing key a SEND uses. */
  private target(dest: string): { exchange: string; key: string; queue: string | null } {
    if (dest.startsWith("/queue/")) return { exchange: "", key: dest.slice(7), queue: dest.slice(7) };
    if (dest.startsWith("/amq/queue/")) return { exchange: "", key: dest.slice(11), queue: null };
    if (dest.startsWith("/topic/")) return { exchange: "amq.topic", key: dest.slice(7), queue: null };
    if (dest.startsWith("/exchange/")) {
      const rest = dest.slice(10);
      const at = rest.indexOf("/");
      return at < 0 ? { exchange: rest, key: "", queue: null } : { exchange: rest.slice(0, at), key: rest.slice(at + 1), queue: null };
    }
    throw new ChanError(406, `unknown destination '${dest}'`);
  }

  private async declareShared(name: string) {
    if (this.broker.queues.has(this.broker.key(this.vhost, name))) return;
    if (!this.broker.can(this.user, this.vhost, "configure", name)) throw new ChanError(403, `ACCESS_REFUSED - configure access to queue '${name}'`);
    await this.broker.declareQueue({ vhost: this.vhost, name, durable: true, exclusive: false, autoDelete: false, passive: false, args: {} });
  }

  private async onSend(f: Frame) {
    const dest = this.need(f, "destination");
    const t = this.target(dest);
    if (t.queue) await this.declareShared(t.queue);
    if (!this.broker.can(this.user, this.vhost, "write", t.exchange || "amq.default")) {
      throw new ChanError(403, `ACCESS_REFUSED - write access to exchange '${t.exchange || "amq.default"}'`);
    }
    if (t.exchange && !this.broker.topicWriteAllowed(this.user, this.vhost, t.exchange, t.key)) throw new ChanError(403, "ACCESS_REFUSED - topic write");
    const app: Array<[string, Field]> = [];
    for (const [k, v] of f.headers) if (!FRAME_HEADERS.has(k)) app.push([k, { t: "S", v }]);
    const persistent = f.headers.get("persistent") === "true";
    const props = this.properties(f, app, persistent);
    const result = await this.broker.publish({
      vhost: this.vhost,
      exchange: t.exchange,
      routingKey: t.key,
      body: f.body,
      headers: app,
      propRaw: props,
      persistent,
      priority: Number(f.headers.get("priority") ?? 0) || 0,
      expiration: f.headers.get("expiration") ?? "",
      confirm: persistent,
    });
    if (result === "nack") throw new ChanError(406, "message was refused");
  }

  /** AMQP basic properties for a STOMP SEND: headers table, content type, delivery mode and the rest. */
  private properties(f: Frame, app: Array<[string, Field]>, persistent: boolean): Uint8Array {
    const w = new W();
    const contentType = f.headers.get("content-type");
    const correlation = f.headers.get("correlation-id");
    const replyTo = f.headers.get("reply-to");
    const expiration = f.headers.get("expiration");
    const messageId = f.headers.get("amqp-message-id");
    let flags = 0x2000 | 0x1000;
    if (contentType) flags |= 0x8000;
    if (correlation) flags |= 0x0400;
    if (replyTo) flags |= 0x0200;
    if (expiration) flags |= 0x0100;
    if (messageId) flags |= 0x0080;
    w.u16(flags);
    if (contentType) w.shortstr(contentType);
    writeTable(w, app);
    w.u8(persistent ? 2 : 1);
    if (correlation) w.shortstr(correlation);
    if (replyTo) w.shortstr(replyTo);
    if (expiration) w.shortstr(expiration);
    if (messageId) w.shortstr(messageId);
    return w.concat();
  }

  private async onSubscribe(f: Frame) {
    const id = f.headers.get("id") ?? (this.version === "1.0" ? f.headers.get("destination") ?? "" : "");
    if (!id) throw new ChanError(406, "missing id header");
    if (this.subs.has(id)) throw new ChanError(406, `duplicate subscription id ${id}`);
    const dest = this.need(f, "destination");
    const mode = f.headers.get("ack") ?? "auto";
    const ack = mode === "client" || mode === "client-individual" ? mode : "auto";
    let queue: string;
    let temporary = false;
    if (dest.startsWith("/queue/")) {
      queue = dest.slice(7);
      await this.declareShared(queue);
    } else if (dest.startsWith("/amq/queue/")) {
      queue = dest.slice(11);
      if (!this.broker.queues.has(this.broker.key(this.vhost, queue))) throw new ChanError(404, `NOT_FOUND - no queue '${queue}'`);
    } else {
      const t = this.target(dest);
      queue = `stomp-subscription-${crypto.randomUUID()}`;
      temporary = true;
      await this.broker.declareQueue({ vhost: this.vhost, name: queue, durable: false, exclusive: true, autoDelete: true, passive: false, args: {}, owner: this.owner });
      if (t.exchange && !this.broker.topicReadAllowed(this.user, this.vhost, t.exchange, t.key)) throw new ChanError(403, "ACCESS_REFUSED - topic read");
      await this.broker.bind(this.vhost, t.exchange, queue, t.key, []);
    }
    if (!this.broker.can(this.user, this.vhost, "read", queue)) throw new ChanError(403, `ACCESS_REFUSED - read access to queue '${queue}'`);
    const prefetch = Number(f.headers.get("prefetch-count") ?? 0) || 0;
    const sub: Sub = { id, destination: dest, queue, temporary, ack, tag: `stomp-${id}-${crypto.randomUUID()}`, prefetch, pending: new Map() };
    this.subs.set(id, sub);
    await this.broker.consume(this.vhost, queue, {
      tag: sub.tag,
      session: this.broker.nextSession(),
      noAck: ack === "auto",
      exclusive: false,
      want: () => !this.closed && (sub.prefetch === 0 || sub.pending.size < sub.prefetch),
      deliver: (msg) => this.deliver(sub, msg),
    });
    const consumer = this.broker.queues.get(this.broker.key(this.vhost, queue))?.consumers.find((c) => c.tag === sub.tag);
    if (consumer) await this.broker.kick(this.vhost, queue, consumer.session);
  }

  private deliver(sub: Sub, msg: LiveMsg) {
    if (this.closed) return;
    const messageId = `T_${sub.id}@@session@@${this.nextMessage++}`;
    const headers: Array<[string, string]> = [
      ["subscription", sub.id],
      ["destination", sub.destination],
      ["message-id", messageId],
      ["redelivered", String(msg.redelivered)],
    ];
    if (sub.ack !== "auto") {
      headers.push(["ack", messageId]);
      sub.pending.set(messageId, msg.id);
    }
    for (const [k, v] of msg.headers) {
      if (FRAME_HEADERS.has(k) || k.startsWith("x-mqtt")) continue;
      if (v.t === "S" || v.t === "s") headers.push([k, v.v]);
      else if (v.t === "I" || v.t === "l" || v.t === "T") headers.push([k, String(v.v)]);
      else if (v.t === "t") headers.push([k, String(v.v)]);
    }
    const contentType = readContentType(msg.propRaw);
    if (contentType) headers.push(["content-type", contentType]);
    headers.push(["content-length", String(msg.body.length)]);
    this.send("MESSAGE", headers, msg.body);
  }

  private async onAck(f: Frame, negative: boolean) {
    const id = f.headers.get("id") ?? f.headers.get("message-id") ?? "";
    for (const sub of this.subs.values()) {
      if (!sub.pending.has(id)) continue;
      const ids: string[] = [];
      if (sub.ack === "client") {
        // Cumulative: this and every earlier unacked delivery on the subscription.
        for (const [ackId, msgId] of sub.pending) {
          ids.push(msgId);
          sub.pending.delete(ackId);
          if (ackId === id) break;
        }
      } else {
        ids.push(sub.pending.get(id)!);
        sub.pending.delete(id);
      }
      const requeue = f.headers.get("requeue") !== "false";
      for (const msgId of ids) {
        if (negative) await Promise.resolve(this.broker.nack(this.vhost, sub.queue, msgId, requeue));
        else await Promise.resolve(this.broker.ack(this.vhost, sub.queue, msgId));
      }
      const q = this.broker.queues.get(this.broker.key(this.vhost, sub.queue));
      if (q) this.broker.pump(q);
      return;
    }
  }

  private async onUnsubscribe(f: Frame) {
    const id = f.headers.get("id") ?? f.headers.get("destination") ?? "";
    const sub = this.subs.get(id);
    if (!sub) return;
    await this.stop(sub);
  }

  private async stop(sub: Sub) {
    this.subs.delete(sub.id);
    await this.broker.cancel(this.vhost, sub.queue, sub.tag).catch(() => {});
    for (const msgId of sub.pending.values()) {
      await Promise.resolve(this.broker.nack(this.vhost, sub.queue, msgId, true)).catch(() => {});
    }
    sub.pending.clear();
    if (sub.temporary) await this.broker.deleteQueue(this.vhost, sub.queue).catch(() => 0);
  }

  private async teardown() {
    if (this.closed) return;
    this.closed = true;
    for (const sub of [...this.subs.values()]) await this.stop(sub);
    this.txs.clear();
    try {
      this.end();
    } catch {
      /* transport already gone */
    }
  }
}

/** content-type from raw basic properties, or "". */
function readContentType(propRaw: Uint8Array): string {
  if (propRaw.length < 3) return "";
  const flags = (propRaw[0]! << 8) | propRaw[1]!;
  if ((flags & 0x8000) === 0) return "";
  const n = propRaw[2]!;
  return dec.decode(propRaw.subarray(3, 3 + n));
}

/**
 * Listen for STOMP on `host`:`port`.
 *
 * @param host Bind address passed to Bun.listen.
 * @param port TCP port from config.
 * @param broker Broker that routes and stores the messages.
 */
export function startStomp(host: string, port: number, broker: Broker, tls: { cert: string; key: string } | null = null) {
  Bun.listen<StompSession | undefined>({
    hostname: host,
    port,
    ...(tls ? { tls: { cert: Bun.file(tls.cert), key: Bun.file(tls.key) } } : {}),
    socket: {
      open(socket) {
        socket.data = new StompSession(
          broker,
          (frame) => {
            try {
              socket.write(frame);
            } catch {
              /* closed */
            }
          },
          () => socket.end(),
        );
      },
      data(socket, data) {
        socket.data?.feed(new Uint8Array(data));
      },
      close(socket) {
        socket.data?.closedByPeer();
      },
      error(socket) {
        socket.data?.closedByPeer();
      },
    },
  });
}

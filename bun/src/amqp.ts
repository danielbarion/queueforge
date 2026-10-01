import { argsFromFields, Broker, ChanError, type LiveMsg } from "./broker.ts";
import { driveAmqp10, type Amqp10State } from "./protocols.ts";
import {
  bodyFrame,
  contentHeaderFrame,
  emptyProps,
  fieldStr,
  heartbeatFrame,
  method,
  methodFrame,
  R,
  readContentHeader,
  readTable,
  tableGet,
  writeTable,
  W,
} from "./codec.ts";

const FRAME_END = 0xce;

type Pub = { exchange: string; routingKey: string; mandatory: boolean; immediate: boolean };

type Ch = {
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

class Conn {
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
  private metricsClosed = false;
  private mgmtName = "";
  private outbound: Uint8Array[] = [];

  constructor(
    private socket: {
      write: (b: Uint8Array) => number;
      end: () => void;
      flush?: () => void;
      remoteAddress?: string;
      remotePort?: number;
    },
    private broker: Broker,
  ) {}

  send(frame: Uint8Array) {
    return this.sendMany([frame]);
  }

  sendMany(frames: Uint8Array[]) {
    this.writeChain = this.writeChain.then(() => {
      if (this.closed) return;
      for (const frame of frames) this.outbound.push(frame);
      this.flush();
    });
    return this.writeChain;
  }

  flush() {
    while (this.outbound.length) {
      const frame = this.outbound[0]!;
      const n = this.socket.write(frame);
      if (n === frame.length) {
        this.outbound.shift();
        continue;
      }
      if (n > 0) this.outbound[0] = frame.subarray(n);
      return;
    }
    this.socket.flush?.();
  }

  async push(data: Uint8Array) {
    const next = new Uint8Array(this.buf.length + data.length);
    next.set(this.buf);
    next.set(data, this.buf.length);
    this.buf = next;
    try {
      if (this.stage === "amqp10") {
        this.buf = await driveAmqp10(this.buf, this.amqp10, (frame) => this.socket.write(frame), this.broker);
        return;
      }
      if (this.stage === "header") {
        if (this.buf.length < 8) return;
        const head = this.buf.subarray(0, 8);
        const amqp10 = head[0] === 65 && head[1] === 77 && head[2] === 81 && head[3] === 80 && ((head[4] === 3 && head[5] === 1 && head[6] === 0 && head[7] === 0) || (head[4] === 0 && head[5] === 1 && head[6] === 0 && head[7] === 0));
        if (amqp10) {
          this.stage = "amqp10";
          this.buf = await driveAmqp10(this.buf, this.amqp10, (frame) => this.socket.write(frame), this.broker);
          return;
        }
        const ok = head[0] === 65 && head[1] === 77 && head[2] === 81 && head[3] === 80 && head[4] === 0 && head[5] === 0 && head[6] === 9 && head[7] === 1;
        if (!ok) {
          this.socket.end();
          return;
        }
        this.buf = this.buf.subarray(8);
        this.stage = "frames";
        await this.send(
          methodFrame(
            0,
            method(10, 10, (w) => {
              w.u8(0);
              w.u8(9);
              writeTable(w, [
                ["capabilities", { t: "F", v: [
                  ["publisher_confirms", { t: "t", v: true }],
                  ["consumer_cancel_notify", { t: "t", v: true }],
                  ["basic.nack", { t: "t", v: true }],
                ] }],
              ]);
              w.longstr("PLAIN");
              w.longstr("en_US");
            }),
          ),
        );
      }
      while (this.buf.length >= 7) {
        const r = new R(this.buf);
        const type = r.u8();
        const channel = r.u16();
        const size = r.u32();
        if (this.buf.length < 7 + size + 1) return;
        const payload = this.buf.subarray(7, 7 + size);
        const end = this.buf[7 + size];
        this.buf = this.buf.subarray(8 + size);
        if (end !== FRAME_END) {
          await this.connClose(501, "FRAME_ERROR");
          return;
        }
        if (type === 8) continue;
        if (type === 1) await this.onMethod(channel, payload);
        else if (type === 2) await this.onHeader(channel, payload);
        else if (type === 3) await this.onBody(channel, payload);
        if (this.connClosed) return;
      }
    } catch (err) {
      if (err instanceof ChanError) await this.chanClose(0, err.code, err.message);
      else await this.connClose(541, "INTERNAL_ERROR");
    }
  }

  ch(id: number): Ch {
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

  async onMethod(channel: number, payload: Uint8Array) {
    const r = new R(payload);
    const cls = r.u16();
    const mid = r.u16();
    if (channel === 0 && cls === 10 && mid === 11) {
      r.u32(); // skip client props table by size... readTable expects to read size
      // we already consumed size. rewind 4 and use readTable
    }
    if (cls === 10 && mid === 11) {
      const rr = new R(payload.subarray(4));
      readTable(rr);
      const mechanism = rr.shortstr();
      const response = rr.longstr();
      if (mechanism !== "PLAIN") return this.connClose(403, "ACCESS_REFUSED - mechanism");
      const parts = new TextDecoder().decode(response).split("\0");
      const user = parts.length >= 3 ? parts[1]! : parts[0] ?? "";
      const pass = parts.length >= 3 ? parts[2]! : parts[1] ?? "";
      if (!(await this.broker.verify(user, pass))) return this.connClose(403, "ACCESS_REFUSED - login");
      this.user = user;
      await this.send(methodFrame(0, method(10, 30, (w) => {
        w.u16(2047);
        w.u32(131072);
        w.u16(60);
      })));
      return;
    }
    if (cls === 10 && mid === 31) {
      const rr = new R(payload.subarray(4));
      rr.u16();
      rr.u32();
      this.heartbeat = rr.u16();
      if (this.heartbeat > 0) {
        this.timer = setInterval(() => void this.send(heartbeatFrame()), Math.max(1000, (this.heartbeat * 1000) / 2));
      }
      return;
    }
    if (cls === 10 && mid === 40) {
      const rr = new R(payload.subarray(4));
      const vhost = rr.shortstr() || "/";
      if (!this.broker.hasVhostAccess(this.user, vhost)) return this.connClose(403, `ACCESS_REFUSED - vhost ${vhost}`);
      if (!this.broker.connectionAllowed(this.user, vhost)) return this.connClose(403, "ACCESS_REFUSED - connection limit");
      this.vhost = vhost;
      if (!this.metricsOpened) {
        this.metricsOpened = true;
        this.broker.prom.connections++;
        this.broker.prom.connectionsOpened++;
        this.mgmtName = this.broker.openMgmtConnection({
          user: this.user,
          vhost,
          peerHost: this.socket.remoteAddress ?? "",
          peerPort: this.socket.remotePort ?? 0,
          close: () => {
            void this.connClose(320, "CONNECTION_FORCED - closed by management");
          },
        });
      }
      await this.send(methodFrame(0, method(10, 41, (w) => w.shortstr(""))));
      return;
    }
    if (cls === 10 && mid === 50) {
      await this.send(methodFrame(0, method(10, 51, () => {})));
      this.connClosed = true;
      this.socket.end();
      return;
    }
    if (cls === 20 && mid === 10) {
      const fresh = !this.channels.has(channel);
      if (fresh && !this.broker.channelAllowed(this.user)) {
        await this.chanClose(channel, 403, "ACCESS_REFUSED - channel limit");
        return;
      }
      this.ch(channel);
      if (fresh) {
        this.broker.prom.channels++;
        this.broker.prom.channelsOpened++;
        this.syncMgmt();
      }
      await this.send(methodFrame(channel, method(20, 11, (w) => w.u32(0))));
      return;
    }
    if (cls === 20 && mid === 40) {
      const open = this.channels.has(channel);
      await this.requeueChannel(this.ch(channel));
      await this.send(methodFrame(channel, method(20, 41, () => {})));
      this.channels.delete(channel);
      this.broker.forgetMgmtChannelConsumers(this.mgmtName, channel);
      if (open) {
        this.broker.prom.channels = Math.max(0, this.broker.prom.channels - 1);
        this.broker.prom.channelsClosed++;
        this.syncMgmt();
      }
      return;
    }
    if (cls === 20 && mid === 20) {
      const rr = new R(payload.subarray(4));
      const active = rr.u8() !== 0;
      this.ch(channel).flow = active;
      await this.send(methodFrame(channel, method(20, 21, (w) => w.u8(active ? 1 : 0))));
      return;
    }
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
      else if (cls === 85 && mid === 10) {
        c.confirm = true;
        const nowait = new R(payload.subarray(4)).u8() & 1;
        if (!nowait) await this.send(methodFrame(channel, method(85, 11, () => {})));
      } else if (cls === 90 && mid === 10) {
        c.tx = true;
        await this.send(methodFrame(channel, method(90, 11, () => {})));
      } else if (cls === 90 && mid === 20) await this.txCommit(channel, c);
      else if (cls === 90 && mid === 30) {
        c.txBatch = [];
        await this.send(methodFrame(channel, method(90, 31, () => {})));
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

  async onHeader(channel: number, payload: Uint8Array) {
    const c = this.ch(channel);
    const parsed = readContentHeader(payload);
    c.bodySize = parsed.bodySize;
    c.propRaw = parsed.props.raw;
    c.headers = parsed.props.headers;
    c.deliveryMode = parsed.props.deliveryMode;
    c.priority = parsed.props.priority;
    c.expiration = parsed.props.expiration;
    c.got = 0;
    c.chunks = [];
    if (c.bodySize === 0) await this.finishPublish(channel, c);
  }

  async onBody(channel: number, payload: Uint8Array) {
    const c = this.ch(channel);
    c.chunks.push(payload);
    c.got += payload.length;
    if (c.got >= c.bodySize) await this.finishPublish(channel, c);
  }

  beginPublish(c: Ch, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const exchange = r.shortstr();
    const routingKey = r.shortstr();
    const bits = r.u8();
    c.publish = { exchange, routingKey, mandatory: (bits & 1) !== 0, immediate: (bits & 2) !== 0 };
  }

  async finishPublish(channel: number, c: Ch) {
    const pub = c.publish;
    if (!pub) return;
    if (!c.flow) {
      c.publish = null;
      return;
    }
    const body = concat(c.chunks);
    const headers = c.headers;
    const propRaw = c.propRaw.length ? c.propRaw : emptyProps();
    const persistent = c.deliveryMode === 2;
    const priority = c.priority;
    const expiration = c.expiration;
    c.publish = null;
    if (pub.immediate) {
      await this.chanClose(channel, 540, "NOT_IMPLEMENTED - immediate=true", 60, 40);
      return;
    }
    if (!this.broker.topicWriteAllowed(this.user, this.vhost, pub.exchange, pub.routingKey)) {
      await this.chanClose(channel, 403, "ACCESS_REFUSED - write access to topic refused", 60, 40);
      return;
    }
    const op = async () => {
      const result = await this.broker.publish({
        vhost: this.vhost,
        exchange: pub.exchange,
        routingKey: pub.routingKey,
        body,
        headers,
        propRaw,
        persistent,
        priority,
        expiration,
        confirm: c.confirm,
        mandatory: pub.mandatory,
      });
      if (result === "return" && pub.mandatory) {
        await this.sendMany([
          methodFrame(
            channel,
            method(60, 50, (w) => {
              w.u16(312);
              w.shortstr("NO_ROUTE");
              w.shortstr(pub.exchange);
              w.shortstr(pub.routingKey);
            }),
          ),
          contentHeaderFrame(channel, body.length, propRaw),
          bodyFrame(channel, body),
        ]);
      }
      if (c.confirm) {
        const tag = c.nextPub++;
        const nack = result === "nack";
        await this.send(
          methodFrame(
            channel,
            method(60, nack ? 120 : 80, (w) => {
              w.u64(tag);
              w.bits(nack ? [false, false] : [false]);
            }),
          ),
        );
      }
    };
    if (c.tx) c.txBatch.push(op);
    else await op();
  }

  async txCommit(channel: number, c: Ch) {
    const batch = c.txBatch;
    c.txBatch = [];
    for (const op of batch) await op();
    await this.send(methodFrame(channel, method(90, 21, () => {})));
  }

  async exDeclare(channel: number, _c: Ch, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const name = r.shortstr();
    const kind = r.shortstr();
    const bits = r.u8();
    const passive = (bits & 1) !== 0;
    const durable = (bits & 2) !== 0;
    const autoDelete = (bits & 4) !== 0;
    const internal = (bits & 8) !== 0;
    const nowait = (bits & 16) !== 0;
    const args = readTable(r);
    const alt = fieldStr(tableGet(args, "alternate-exchange")) || null;
    if (!passive) await this.broker.declareExchange(this.vhost, name, kind || "direct", durable, autoDelete, internal, alt);
    if (!nowait) await this.send(methodFrame(channel, method(40, 11, () => {})));
  }

  async exDelete(channel: number, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const name = r.shortstr();
    const nowait = (r.u8() & 2) !== 0;
    await this.broker.deleteExchange(this.vhost, name);
    if (!nowait) await this.send(methodFrame(channel, method(40, 21, () => {})));
  }

  async exBind(channel: number, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const destination = r.shortstr();
    const source = r.shortstr();
    const routingKey = r.shortstr();
    const nowait = (r.u8() & 1) !== 0;
    await this.broker.bindExchange(this.vhost, source, destination, routingKey);
    if (!nowait) await this.send(methodFrame(channel, method(40, 31, () => {})));
  }

  async exUnbind(channel: number, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const destination = r.shortstr();
    const source = r.shortstr();
    const routingKey = r.shortstr();
    const nowait = (r.u8() & 1) !== 0;
    await this.broker.unbindExchange(this.vhost, source, destination, routingKey);
    if (!nowait) await this.send(methodFrame(channel, method(40, 51, () => {})));
  }

  async qDeclare(channel: number, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const name = r.shortstr();
    const bits = r.u8();
    const args = readTable(r);
    const fields = argsFromFields(args);
    const passive = (bits & 1) !== 0;
    const durable = (bits & 2) !== 0;
    const exclusive = (bits & 4) !== 0;
    const qtype = String(fields["x-queue-type"] ?? "");
    if (!passive && !durable && !exclusive && qtype !== "quorum" && !this.broker.transientNonexcl) {
      await this.connClose(
        541,
        "INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated.\nBy default, this feature is not permitted anymore.",
      );
      return;
    }
    const res = await this.broker.declareQueue({
      vhost: this.vhost,
      name,
      passive,
      durable,
      exclusive,
      autoDelete: (bits & 8) !== 0,
      args: fields,
    });
    if ((bits & 16) === 0) {
      await this.send(
        methodFrame(
          channel,
          method(50, 11, (w) => {
            w.shortstr(res.name);
            w.u32(res.messages);
            w.u32(res.consumers);
          }),
        ),
      );
    }
  }

  async qBind(channel: number, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const queue = r.shortstr();
    const exchange = r.shortstr();
    const routingKey = r.shortstr();
    const nowait = (r.u8() & 1) !== 0;
    const args = readTable(r);
    await this.broker.bind(this.vhost, exchange, queue, routingKey, args);
    if (!nowait) await this.send(methodFrame(channel, method(50, 21, () => {})));
  }

  async qUnbind(channel: number, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const queue = r.shortstr();
    const exchange = r.shortstr();
    const routingKey = r.shortstr();
    const args = readTable(r);
    await this.broker.unbind(this.vhost, exchange, queue, routingKey, args);
    await this.send(methodFrame(channel, method(50, 51, () => {})));
  }

  async qPurge(channel: number, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const queue = r.shortstr();
    const nowait = (r.u8() & 1) !== 0;
    const n = await this.broker.purge(this.vhost, queue);
    if (!nowait) await this.send(methodFrame(channel, method(50, 31, (w) => w.u32(n))));
  }

  async qDelete(channel: number, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const queue = r.shortstr();
    const nowait = (r.u8() & 4) !== 0;
    const n = await this.broker.deleteQueue(this.vhost, queue);
    if (!nowait) await this.send(methodFrame(channel, method(50, 41, (w) => w.u32(n))));
  }

  async qos(channel: number, c: Ch, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u32();
    const count = r.u16();
    r.u8();
    c.prefetch = count;
    c.globalPrefetch = null;
    await this.send(methodFrame(channel, method(60, 11, () => {})));
  }

  creditOk(c: Ch, tag: string): boolean {
    if (!c.flow) return false;
    const own = c.byConsumer.get(tag) ?? 0;
    if (c.prefetch !== 0 && own >= c.prefetch) return false;
    if (c.globalPrefetch != null && c.globalPrefetch !== 0 && c.globalUnacked >= c.globalPrefetch) return false;
    return true;
  }

  async consume(channel: number, c: Ch, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const queue = r.shortstr();
    let tag = r.shortstr();
    const bits = r.u8();
    const noAck = (bits & 2) !== 0;
    const exclusive = (bits & 4) !== 0;
    const nowait = (bits & 8) !== 0;
    const args = readTable(r);
    const pri = tableGet(args, "x-priority");
    const priority = pri && (pri.t === "I" || pri.t === "l" || pri.t === "s" || pri.t === "S") ? Number(pri.t === "I" || pri.t === "l" ? pri.v : pri.v) : 0;
    if (!tag) tag = `ctag-${crypto.randomUUID()}`;
    const session = this.broker.nextSession();
    c.consumers.set(tag, queue);
    this.broker.noteMgmtConsumer({
      consumer_tag: tag,
      connection: this.mgmtName,
      channel,
      queue,
      vhost: this.vhost,
    });
    await this.broker.consume(this.vhost, queue, {
      tag,
      session,
      noAck,
      exclusive,
      priority: Number.isFinite(priority) ? priority : 0,
      want: () => this.creditOk(c, tag),
      onCancel: () => {
        c.consumers.delete(tag);
        void this.send(methodFrame(channel, method(60, 30, (w) => {
          w.shortstr(tag);
          w.bits([true]);
        })));
      },
      deliver: (msg) => {
        if (!this.creditOk(c, tag)) {
          void this.broker.nack(this.vhost, queue, msg.id, true);
          return;
        }
        const dtag = c.nextDel++;
        c.deliveries.set(dtag, { vhost: this.vhost, queue, id: msg.id, consumer: tag });
        c.byConsumer.set(tag, (c.byConsumer.get(tag) ?? 0) + 1);
        c.globalUnacked++;
        void this.deliver(channel, tag, dtag, msg);
      },
    });
    if (!nowait) await this.send(methodFrame(channel, method(60, 21, (w) => w.shortstr(tag))));
    await this.broker.kick(this.vhost, queue, session);
  }

  async deliver(channel: number, tag: string, dtag: number, msg: LiveMsg) {
    await this.sendMany([
      methodFrame(
        channel,
        method(60, 60, (w) => {
          w.shortstr(tag);
          w.u64(dtag);
          w.bits([msg.redelivered]);
          w.shortstr(msg.exchange);
          w.shortstr(msg.routingKey);
        }),
      ),
      contentHeaderFrame(channel, msg.body.length, msg.propRaw.length ? msg.propRaw : emptyProps()),
      bodyFrame(channel, msg.body),
    ]);
  }

  async cancel(channel: number, c: Ch, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    const tag = r.shortstr();
    const queue = c.consumers.get(tag);
    if (queue) await this.broker.cancel(this.vhost, queue, tag);
    c.consumers.delete(tag);
    this.broker.forgetMgmtConsumer(this.mgmtName, channel, tag);
    await this.send(methodFrame(channel, method(60, 31, (w) => w.shortstr(tag))));
  }

  async ack(c: Ch, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    const tag = r.u64();
    const multiple = (r.u8() & 1) !== 0;
    const op = async () => this.settle(c, tag, multiple, false, false);
    if (c.tx) c.txBatch.push(op);
    else await op();
  }

  async reject(c: Ch, payload: Uint8Array, fromNack: boolean) {
    const r = new R(payload.subarray(4));
    const tag = r.u64();
    const bits = r.u8();
    const requeue = fromNack ? (bits & 2) !== 0 : (bits & 1) !== 0;
    const multiple = fromNack ? (bits & 1) !== 0 : false;
    const op = async () => this.settle(c, tag, multiple, true, requeue);
    if (c.tx) c.txBatch.push(op);
    else await op();
  }

  async nack(c: Ch, payload: Uint8Array) {
    await this.reject(c, payload, true);
  }

  async settle(c: Ch, tag: number, multiple: boolean, negative: boolean, requeue: boolean) {
    const ids = [...c.deliveries.keys()].filter((t) => (multiple ? t <= tag : t === tag));
    for (const t of ids) {
      const d = c.deliveries.get(t)!;
      c.deliveries.delete(t);
      c.byConsumer.set(d.consumer, Math.max(0, (c.byConsumer.get(d.consumer) ?? 1) - 1));
      c.globalUnacked = Math.max(0, c.globalUnacked - 1);
      if (negative) await this.broker.nack(d.vhost, d.queue, d.id, requeue);
      else await this.broker.ack(d.vhost, d.queue, d.id);
    }
  }

  async get(channel: number, c: Ch, payload: Uint8Array) {
    const r = new R(payload.subarray(4));
    r.u16();
    const queue = r.shortstr();
    const noAck = (r.u8() & 1) !== 0;
    const msg = await this.broker.get(this.vhost, queue, noAck);
    if (!msg) {
      await this.send(methodFrame(channel, method(60, 72, (w) => w.shortstr(""))));
      return;
    }
    const dtag = c.nextDel++;
    if (!noAck) c.deliveries.set(dtag, { vhost: this.vhost, queue, id: msg.id, consumer: "" });
    await this.sendMany([
      methodFrame(
        channel,
        method(60, 71, (w) => {
          w.u64(dtag);
          w.bits([msg.redelivered]);
          w.shortstr(msg.exchange);
          w.shortstr(msg.routingKey);
          w.u32(0);
        }),
      ),
      contentHeaderFrame(channel, msg.body.length, msg.propRaw.length ? msg.propRaw : emptyProps()),
      bodyFrame(channel, msg.body),
    ]);
  }

  async recover(channel: number, c: Ch, payload: Uint8Array, methodId: number) {
    const requeue = (new R(payload.subarray(4)).u8() & 1) !== 0;
    if (!requeue) {
      await this.chanClose(channel, 540, "NOT_IMPLEMENTED - basic.recover requeue=false", 60, methodId);
      return;
    }
    const pending = [...c.deliveries.values()];
    c.deliveries.clear();
    for (const d of pending) {
      c.byConsumer.set(d.consumer, Math.max(0, (c.byConsumer.get(d.consumer) ?? 1) - 1));
      c.globalUnacked = Math.max(0, c.globalUnacked - 1);
    }
    // Lapin drops deliveries buffered before recover-ok, so reply first, then requeue.
    if (methodId === 110) await this.send(methodFrame(channel, method(60, 111, () => {})));
    for (const d of pending) await this.broker.nack(d.vhost, d.queue, d.id, true);
  }

  async requeueChannel(c: Ch) {
    const pending = [...c.deliveries.values()];
    c.deliveries.clear();
    for (const d of pending) {
      try {
        await this.broker.nack(d.vhost, d.queue, d.id, true);
      } catch {
        /* home is gone; this connection is already closing */
      }
    }
  }

  async dropConsumers() {
    for (const [channel, c] of this.channels) {
      for (const [tag, queue] of c.consumers) {
        await this.broker.cancel(this.vhost, queue, tag);
        this.broker.forgetMgmtConsumer(this.mgmtName, channel, tag);
      }
      c.consumers.clear();
    }
  }

  async requeueAll() {
    for (const c of this.channels.values()) await this.requeueChannel(c);
  }

  async chanClose(channel: number, code: number, text: string, classId = 0, methodId = 0) {
    await this.send(
      methodFrame(
        channel,
        method(20, 40, (w) => {
          w.u16(code);
          w.shortstr(text.slice(0, 180));
          w.u16(classId);
          w.u16(methodId);
        }),
      ),
    );
  }

  syncMgmt() {
    if (!this.mgmtName) return;
    this.broker.syncMgmtChannels(
      this.mgmtName,
      this.user,
      this.vhost,
      this.socket.remoteAddress ?? "",
      this.socket.remotePort ?? 0,
      [...this.channels.keys()],
    );
  }

  noteMetricsClosed() {
    if (this.metricsClosed || !this.metricsOpened) return;
    this.metricsClosed = true;
    if (this.mgmtName) {
      this.broker.forgetMgmtConnection(this.mgmtName);
      this.mgmtName = "";
    }
    this.broker.prom.connections = Math.max(0, this.broker.prom.connections - 1);
    this.broker.prom.connectionsClosed++;
    this.broker.prom.channels = Math.max(0, this.broker.prom.channels - this.channels.size);
    this.broker.prom.channelsClosed += this.channels.size;
  }

  async connClose(code: number, text: string) {
    this.connClosed = true;
    this.noteMetricsClosed();
    const bytes = new TextEncoder().encode(text);
    const reply = bytes.length > 200 ? bytes.subarray(0, 200) : bytes;
    await this.send(
      methodFrame(
        0,
        method(10, 50, (w) => {
          w.u16(code);
          w.u8(reply.length);
          w.bytes(reply);
          w.u16(0);
          w.u16(0);
        }),
      ),
    );
    const socket = this.socket;
    setTimeout(() => {
      try {
        socket.end();
      } catch {
        /* the peer already closed after connection.close */
      }
    }, 50);
  }
}

function concat(parts: Uint8Array[]): Uint8Array {
  const n = parts.reduce((a, p) => a + p.length, 0);
  const out = new Uint8Array(n);
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

void W;

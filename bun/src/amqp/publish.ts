/**
 * basic.publish, content header, and body frames.
 *
 * A publish is stored on the channel until the header and body arrive.
 * Immediate publishes are rejected. Confirms and returns are sent from here.
 */
import { ChanError } from "../broker/index.ts";
import { bodyFrame, contentHeaderFrame, emptyProps, encodeSettle, method, methodFrame, R, readContentHeader } from "../codec.ts";
import { Conn, type Ch, type PendingPublish, type Pub } from "./listen.ts";
import { SENT } from "./frames.ts";
import { routeEpoch } from "../broker/topology.ts";
import { headerList } from "../broker/routing.ts";
import { poolBody } from "../body-pool.ts";

/**
 * Join body chunks into one buffer, in arrival order.
 * The result is always a copy: a chunk is a view of the socket read buffer.
 */
function concat(parts: Uint8Array[]): Uint8Array {
  if (parts.length === 1) return parts[0]!.slice();
  let n = 0;
  for (let i = 0; i < parts.length; i++) n += parts[i]!.length;
  const out = new Uint8Array(n);
  let o = 0;
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i]!;
    out.set(p, o);
    o += p.length;
  }
  return out;
}

/**
 * Bodies at least this large are copied into a buffer of their declared size
 * as their frames arrive (see {@link onBody}), and the parser streams a body
 * frame that spans socket reads into it. Below this, a copy per publish is
 * cheaper than tracking a partial frame.
 */
export const DIRECT_BODY = 16 * 1024;

/**
 * A buffer for `n` body bytes, every one written before it is read: a reused
 * one when the body pool has it (see `body-pool.ts`). Otherwise
 * `allocUnsafe` skips the zero fill; at this size it is not Bun's shared
 * pool, so the view covers its own buffer and `slice` keeps copying.
 */
function bodyBuffer(n: number): Uint8Array {
  const pooled = poolBody(n);
  if (pooled !== null) return pooled;
  const b = Buffer.allocUnsafe(n);
  return new Uint8Array(b.buffer, b.byteOffset, n);
}

/**
 * Remember the exchange and routing key from basic.publish.
 *
 * @param c Channel that will receive the following header and body.
 * @param payload Method payload. Bit 0 is mandatory, bit 1 is immediate.
 * The body is not routed until {@link finishPublish}.
 */
export function beginPublish(this: Conn, c: Ch, buf: Uint8Array, at = 0, size = buf.length - at) {
  // Past class, method and the reserved short. A burst repeats the same two
  // names: when their bytes match the last publish, its strings are reused.
  const limit = at + size;
  const exAt = at + 6;
  const rkAt = exAt + 1 + (buf[exAt] ?? 0);
  const end = rkAt + 1 + (buf[rkAt] ?? 0);
  const last = c.pubNames;
  let exchange: string;
  let routingKey: string;
  if (last !== null && last.length === end - exAt && end < limit && sameBytes(last, buf, exAt)) {
    exchange = c.pubExchange;
    routingKey = c.pubRoutingKey;
  } else {
    const r = new R(buf.subarray(at, limit));
    r.o = 6;
    exchange = r.shortstr();
    routingKey = r.shortstr();
    if (end < limit) {
      c.pubNames = buf.slice(exAt, end);
      c.pubExchange = exchange;
      c.pubRoutingKey = routingKey;
    }
  }
  const bits = end < limit ? buf[end]! : 0;
  c.publish = { exchange, routingKey, mandatory: (bits & 1) !== 0, immediate: (bits & 2) !== 0 };
}

/**
 * Send the ack or nack for publish `tag`.
 *
 * Confirms of a burst coalesce as RabbitMQ's do: while this channel's last
 * basic.ack is still the connection's last staged frame, and every tag up to
 * the new one has been settled, that frame is rewritten in place to
 * `multiple` up to the highest settled tag instead of staging another one.
 * Every tag it then covers was acked by this broker: an earlier nack went out
 * in its own frame first, and any frame staged after the ack ends the run.
 * The staged frame must not already ack a tag past that run: an out-of-order
 * confirm staged on its own (tag 501 while 473 is still pending) would be
 * rewritten down to `multiple` 472, and 501 would never be confirmed.
 */
export function sendConfirm(conn: Conn, channel: number, c: Ch, tag: number, nack: boolean): Promise<unknown> {
  if (tag === c.confirmLow + 1) {
    c.confirmLow = tag;
    const ahead = c.confirmAhead;
    if (ahead !== null) {
      while (ahead.delete(c.confirmLow + 1)) c.confirmLow++;
      if (ahead.size === 0) c.confirmAhead = null;
    }
  } else if (tag > c.confirmLow && c.confirmLow >= 0) {
    const ahead = (c.confirmAhead ??= new Set());
    ahead.add(tag);
    // A tag that never settles would grow this forever: stop coalescing.
    if (ahead.size > 65536) {
      c.confirmLow = -1;
      c.confirmAhead = null;
    }
  }
  const staged = conn.staged;
  const last = c.ackFrame;
  if (!nack && last !== null && tag <= c.confirmLow && c.ackTag <= c.confirmLow && staged.length !== 0 && staged[staged.length - 1] === last) {
    setSettleTag(last, c.confirmLow);
    c.ackTag = c.confirmLow;
    return SENT;
  }
  const frame = encodeSettle(channel, tag, nack, conn);
  c.ackFrame = nack ? null : frame;
  c.ackTag = tag;
  return conn.send(frame);
}

/** Rewrite a staged basic.ack to `multiple` up to `tag` (see {@link encodeSettle}). */
function setSettleTag(b: Uint8Array, tag: number) {
  const hi = Math.floor(tag / 0x100000000);
  const lo = tag >>> 0;
  b[11] = hi >>> 24;
  b[12] = (hi >>> 16) & 0xff;
  b[13] = (hi >>> 8) & 0xff;
  b[14] = hi & 0xff;
  b[15] = lo >>> 24;
  b[16] = (lo >>> 16) & 0xff;
  b[17] = (lo >>> 8) & 0xff;
  b[18] = lo & 0xff;
  b[19] = 1;
}

/** `a` equals `b` from offset `at`. */
function sameBytes(a: Uint8Array, b: Uint8Array, at: number): boolean {
  for (let i = 0; i < a.length; i++) if (a[i] !== b[at + i]) return false;
  return true;
}

/**
 * Store the content header and finish the publish when the body is empty.
 *
 * @param channel Channel the header arrived on.
 * @param payload Content-header payload. Properties are kept raw for redelivery.
 */
export function onHeader(this: Conn, channel: number, payload: Uint8Array): Promise<unknown> | void {
  const c = this.ch(channel);
  const parsed = readContentHeader(payload);
  c.bodySize = parsed.bodySize;
  if (c.bodySize > 64 * 1024 * 1024) throw new ChanError(506, "RESOURCE_ERROR - message exceeds 64 MiB");
  // A copy: the parsed bytes are a view of the socket read buffer, and a stored
  // message holding that view kept the whole buffer (64 KiB and more) alive.
  // A publisher usually repeats its properties, so the last copy is reused
  // when the bytes match; nothing writes into a stored property block.
  const from = parsed.props.rawStart;
  const n = payload.length - from;
  const last = c.propRaw;
  let same = last.length === n;
  for (let i = 0; same && i < n; i++) same = last[i] === payload[from + i];
  if (!same) c.propRaw = payload.slice(from);
  c.headers = parsed.props.headers;
  c.deliveryMode = parsed.props.deliveryMode;
  c.priority = parsed.props.priority;
  c.expiration = parsed.props.expiration;
  c.userId = parsed.props.userId;
  c.replyTo = parsed.props.replyTo;
  c.got = 0;
  c.chunks = [];
  c.body = c.bodySize >= DIRECT_BODY && reserved(this) + c.bodySize <= RESERVE_MAX ? bodyBuffer(c.bodySize) : null;
  if (c.bodySize === 0) return this.finishPublish(channel, c);
}

/**
 * Body bytes one connection may hold in buffers sized from content headers,
 * not yet received: the inbound buffering limit in `push`. Past it a body is
 * gathered from its frames as before, so a client that declares large bodies
 * on many channels cannot reserve memory it has not sent.
 */
const RESERVE_MAX = 64 * 1024 * 1024;

function reserved(conn: Conn): number {
  let n = 0;
  for (const ch of conn.channels.values()) if (ch.body) n += ch.body.length;
  return n;
}

/**
 * Append a body frame and finish the publish once every byte has arrived.
 *
 * @param channel Channel the body arrived on.
 * @param payload Body bytes for this frame, not including the frame header.
 */
export function onBody(this: Conn, channel: number, payload: Uint8Array, owned = false): Promise<unknown> | void {
  const c = this.ch(channel);
  if (c.got + payload.length > c.bodySize) throw new ChanError(505, "UNEXPECTED_FRAME - body exceeds declared size");
  if (c.body) c.body.set(payload, c.got);
  else {
    c.chunks.push(payload);
    c.chunksOwned = owned && c.chunks.length === 1;
  }
  c.got += payload.length;
  if (c.got >= c.bodySize) return this.finishPublish(channel, c);
}

/**
 * Copy the leading part of a body frame that has not fully arrived.
 *
 * @returns False when this channel does not stream bodies or the frame is
 * larger than what is left of the body; the parser then waits for the whole
 * frame, and {@link onBody} reports the error.
 */
export function streamBody(this: Conn, channel: number, size: number, part: Uint8Array): boolean {
  const c = this.channels.get(channel);
  if (!c?.body || c.publish === null || size > c.bodySize - c.got) return false;
  c.body.set(part, c.got);
  c.got += part.length;
  return true;
}

/**
 * Route the finished publish, then send a return or confirm if required.
 *
 * @param channel Channel the publish started on.
 * @param c Channel state holding the publish, header, and body chunks.
 * A missing publish is ignored. A channel with flow inactive drops the publish.
 * `immediate` closes the channel with 540. Topic write denial closes the channel
 * with 403. Inside a transaction the work is queued until tx.commit.
 * Mandatory no-route sends basic.return. Confirms send ack, or nack when the
 * broker returns `"nack"`.
 */
export function finishPublish(this: Conn, channel: number, c: Ch): Promise<unknown> | void {
  const pub = c.publish;
  if (!pub) return;
  // A memory or disk alarm holds the publish. The parser awaits this, so the
  // connection reads nothing more until the alarm clears, as in RabbitMQ.
  if (this.broker.blocked) return this.broker.whenUnblocked().then(() => this.finishPublish(channel, c));
  if (!c.flow) {
    c.publish = null;
    c.chunks = [];
    c.body = null;
    c.got = 0;
    return;
  }
  // A whole body the parser already copied out of the read buffer is used as is.
  const body = c.body ?? (c.chunksOwned && c.chunks.length === 1 ? c.chunks[0]! : concat(c.chunks));
  c.body = null;
  c.chunksOwned = false;
  c.chunks = [];
  c.got = 0;
  const headers = c.headers;
  let propRaw = c.propRaw.length ? c.propRaw : emptyProps();
  const persistent = c.deliveryMode === 2;
  const priority = c.priority;
  const expiration = c.expiration;
  c.publish = null;
  if (pub.immediate) {
    return this.chanClose(channel, 540, "NOT_IMPLEMENTED - immediate=true", 60, 40);
  }
  // The last exchange checked is remembered, so a publish burst to one
  // exchange pays for the lookup and the permission test once.
  if (pub.exchange !== this.writeEx || this.permRef !== this.broker.perms || this.permLen !== this.broker.perms.length) {
    this.writeExTopic = false;
    if (pub.exchange !== "") {
      const ex = this.broker.exchanges.get(this.broker.key(this.vhost, pub.exchange));
      if (!ex) return this.chanClose(channel, 404, `NOT_FOUND - no exchange '${pub.exchange}' in vhost '${this.vhost}'`, 60, 40);
      if (ex.internal) {
        return this.chanClose(channel, 403, `ACCESS_REFUSED - cannot publish to internal exchange '${pub.exchange}' in vhost '${this.vhost}'`, 60, 40);
      }
      this.writeExTopic = ex.kind === "topic";
    }
    const resource = pub.exchange || "amq.default";
    if (!this.allowed("write", resource)) {
      return this.chanClose(
        channel,
        403,
        `ACCESS_REFUSED - write access to exchange '${resource}' in vhost '${this.vhost}' refused for user '${this.user}'`,
        60,
        40,
      );
    }
    this.writeEx = pub.exchange;
  }
  if (c.userId && c.userId !== this.user) {
    return this.chanClose(channel, 406, `PRECONDITION_FAILED - user_id property set to '${c.userId}' but authenticated user was '${this.user}'`, 60, 40);
  }
  // Topic permissions apply to topic exchanges only, as in RabbitMQ.
  if (this.writeExTopic && !this.broker.topicWriteAllowed(this.user, this.vhost, pub.exchange, pub.routingKey)) {
    return this.chanClose(channel, 403, "ACCESS_REFUSED - write access to topic refused", 60, 40);
  }
  // Direct reply-to: rewrite the requester's reply-to, or hand a reply to it.
  if (c.replyTo || (pub.exchange === "" && pub.routingKey.startsWith("amq.rabbitmq.reply-to."))) {
    let routed: Uint8Array | "sent" | "dropped";
    try {
      routed = this.routeReply(c, pub.exchange, pub.routingKey, body, propRaw);
    } catch (err) {
      if (err instanceof ChanError) return this.chanClose(channel, err.code, err.message, 60, 40);
      throw err;
    }
    if (routed === "sent" || routed === "dropped") {
      if (c.confirm) return sendConfirm(this, channel, c, c.nextPub++, false);
      return;
    }
    propRaw = routed;
  }
  // Outside a transaction the tag is the arrival order, before the fsync wait.
  // Inside a transaction the tag is assigned when the op runs, at commit.
  const confirm = c.confirm;
  const pubTag = confirm && !c.tx ? c.nextPub++ : 0;
  const bytes = body.length + propRaw.length;
  const op = new Deferred(this, channel, c, pub, body, headers, propRaw, persistent, priority, expiration, confirm, pubTag, bytes);
  if (c.tx) {
    this.stageTransaction(c, () => {
      const pending = op.runNow();
      this.deferredPublish.push({ channel, bytes: 0, pressured: () => op.pressured(), run: () => this.publishSettled(0) });
      return pending;
    }, bytes);
    return;
  }
  // A persistent confirm waits on the group-commit flush. Parking it lets the
  // parser reach basic.ack frames before this body is enqueued, so one prefetch
  // window can turn over inside a publish burst. `driveInbound` starts the batch.
  if (this.deferredPublish.length + this.inFlightPublishes >= 65536) throw new ChanError(506, "RESOURCE_ERROR - pending publish operation limit");
  this.publishBytes += bytes;
  if (this.publishBytes > 64 * 1024 * 1024) throw new ChanError(506, "RESOURCE_ERROR - pending publishes exceed 64 MiB");
  this.deferredPublish.push(op);
}

/**
 * One routed publish waiting its turn on `deferredPublish`.
 *
 * Its steps are methods on one object, not eight closures and their contexts
 * per publish: those allocations were most of `finishPublish`'s own time.
 * `run` calls `conn.publishSettled(bytes)` once, in this turn or when the
 * confirm is queued.
 */
/**
 * A queue whose consumers are behind: 256 ready and the oldest deliverable.
 * Quorum messages wait in `ready` behind their confirm gate until the fsync
 * covers them; while the head is gated the consumers are waiting on the disk,
 * not lagging, and holding publishers then only shrank the next group commit.
 */
function behind(queue: { consumers: unknown[]; ready: { length: number; at(i: number): { confirmGate?: { hold: boolean } } | undefined } }): boolean {
  return queue.consumers.length !== 0 && queue.ready.length >= 256 && queue.ready.at(0)?.confirmGate?.hold !== true;
}

class Deferred implements PendingPublish {
  readonly batchConfirm: boolean;
  /** Destinations for the back-pressure test, computed when first asked. */
  private names: Iterable<string> | null = null;

  constructor(
    private readonly conn: Conn,
    readonly channel: number,
    private readonly c: Ch,
    private readonly pub: Pub,
    private readonly body: Uint8Array,
    private readonly headers: Ch["headers"],
    private readonly propRaw: Uint8Array,
    private readonly persistent: boolean,
    private readonly priority: number,
    private readonly expiration: string,
    private readonly confirm: boolean,
    private pubTag: number,
    readonly bytes: number,
  ) {
    this.batchConfirm = confirm && persistent;
  }

  // Not an async function. An async wrapper allocates a promise per publish
  // before the durable wait, and `await` on an already-resolved send yields
  // again. The durable wait is the only promise on this path.
  private start() {
    if (this.c.tx && this.confirm) this.pubTag = this.c.nextPub++;
    const conn = this.conn;
    const pub = this.pub;
    return conn.broker.publish({
      vhost: conn.vhost,
      exchange: pub.exchange,
      routingKey: pub.routingKey,
      body: this.body,
      headers: this.headers,
      propRaw: this.propRaw,
      persistent: this.persistent,
      priority: this.priority,
      expiration: this.expiration,
      confirm: this.confirm,
      mandatory: pub.mandatory,
    });
  }

  private finish(result: "ack" | "nack" | "return"): Promise<unknown> | void {
    const conn = this.conn;
    const channel = this.channel;
    const pub = this.pub;
    if (result === "return" && pub.mandatory) {
      const returned = conn.sendMany([
        methodFrame(
          channel,
          method(60, 50, (w) => {
            w.u16(312);
            w.shortstr("NO_ROUTE");
            w.shortstr(pub.exchange);
            w.shortstr(pub.routingKey);
          }),
        ),
        contentHeaderFrame(channel, this.body.length, this.propRaw),
        bodyFrame(channel, this.body, conn.frameMax),
      ]);
      if (!this.confirm) return returned;
      return returned.then(() => sendConfirm(conn, channel, this.c, this.pubTag, false));
    }
    if (!this.confirm) return;
    return sendConfirm(conn, channel, this.c, this.pubTag, result === "nack");
  }

  private fail(err: unknown) {
    const code = err instanceof ChanError ? err.code : 541;
    const text = err instanceof ChanError ? err.message : "INTERNAL_ERROR";
    if (!(err instanceof ChanError)) console.error("publish failed", err);
    return this.conn.chanClose(this.channel, code, text, 60, 40);
  }

  private failed(err: unknown) {
    void this.fail(err);
    this.conn.publishSettled(this.bytes);
  }

  // One reaction per publish on the durable wait, and the settle is a plain
  // call: a tracked promise and a `finally` per confirm were most of the
  // promises a deep confirm window held.
  private done(result: "ack" | "nack" | "return") {
    let sent: Promise<unknown> | void;
    try {
      sent = this.finish(result);
    } catch (err) {
      this.failed(err);
      return;
    }
    // A staged write has nothing to wait for. A queued one still reports a failure.
    if (sent && sent !== SENT) sent.then(undefined, (err) => this.fail(err));
    this.conn.publishSettled(this.bytes);
  }

  run() {
    let pending: ReturnType<Deferred["start"]>;
    try {
      pending = this.start();
    } catch (err) {
      this.failed(err);
      return;
    }
    if (typeof pending === "string") this.done(pending);
    else pending.then((result) => this.done(result), (err) => this.failed(err));
  }

  /** The transaction path: publish and send the outcome, without the settle. */
  runNow(): Promise<unknown> | void {
    const pending = this.start();
    if (typeof pending === "string") return this.finish(pending);
    return pending.then((result) => this.finish(result));
  }

  // The destinations, for the back-pressure test only: the publish itself is
  // routed when it runs. Computed when the parser first asks, since most
  // publishes run before it does. `route` gives each queue once; only CC and
  // BCC can add a duplicate. A Set and two header lists per publish were 3%
  // of a 1 CPU transient load.
  private destinations(): Iterable<string> {
    if (this.names) return this.names;
    const conn = this.conn;
    const pub = this.pub;
    const headers = this.headers;
    let names: Iterable<string> = conn.broker.route(conn.vhost, pub.exchange, pub.routingKey, headers);
    if (headers.length && headers.some(([key]) => key === "CC" || key === "BCC")) {
      const all = new Set(names);
      for (const key of [...headerList(headers, "CC"), ...headerList(headers, "BCC")]) {
        for (const queue of conn.broker.route(conn.vhost, pub.exchange, key, headers)) all.add(queue);
      }
      names = all;
    }
    return (this.names = names);
  }

  pressured() {
    const conn = this.conn;
    const broker = conn.broker;
    // A burst asks this for every parked publish to the same exchange and key:
    // the route and the queue keys of the last one are reused while the
    // topology is unchanged. Headers may route (CC, BCC, a headers exchange).
    if (this.names === null && this.headers.length === 0) {
      const epoch = routeEpoch(broker);
      const pub = this.pub;
      let memo = conn.destMemo;
      if (memo === null || memo.epoch !== epoch || memo.exchange !== pub.exchange || memo.routingKey !== pub.routingKey) {
        const names = broker.route(conn.vhost, pub.exchange, pub.routingKey, this.headers);
        memo = { epoch, exchange: pub.exchange, routingKey: pub.routingKey, names, keys: names.map((n) => broker.key(conn.vhost, n)) };
        conn.destMemo = memo;
      }
      const keys = memo.keys;
      for (let i = 0; i < keys.length; i++) {
        const queue = broker.queues.get(keys[i]!);
        if (queue && behind(queue) && broker.isLocalHome(queue.home)) return true;
      }
      return false;
    }
    for (const name of this.destinations()) {
      const queue = broker.queues.get(broker.key(conn.vhost, name));
      if (queue && behind(queue) && broker.isLocalHome(queue.home)) return true;
    }
    return false;
  }
}

Conn.prototype.beginPublish = beginPublish;
Conn.prototype.onHeader = onHeader;
Conn.prototype.onBody = onBody;
Conn.prototype.streamBody = streamBody;
Conn.prototype.finishPublish = finishPublish;

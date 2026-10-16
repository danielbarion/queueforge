/**
 * basic.publish, content header, and body frames.
 *
 * A publish is stored on the channel until the header and body arrive.
 * Immediate publishes are rejected. Confirms and returns are sent from here.
 */
import { ChanError } from "../broker/index.ts";
import { bodyFrame, contentHeaderFrame, emptyProps, encodeSettle, method, methodFrame, R, readContentHeader } from "../codec.ts";
import { Conn, type Ch } from "./listen.ts";
import { SENT } from "./frames.ts";
import { headerList } from "../broker/routing.ts";

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
 * Remember the exchange and routing key from basic.publish.
 *
 * @param c Channel that will receive the following header and body.
 * @param payload Method payload. Bit 0 is mandatory, bit 1 is immediate.
 * The body is not routed until {@link finishPublish}.
 */
export function beginPublish(this: Conn, c: Ch, payload: Uint8Array) {
  // Past class, method and the reserved short, without a view of its own.
  const r = new R(payload);
  r.o = 6;
  const exchange = r.shortstr();
  const routingKey = r.shortstr();
  const bits = r.u8();
  c.publish = { exchange, routingKey, mandatory: (bits & 1) !== 0, immediate: (bits & 2) !== 0 };
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
  c.propRaw = parsed.props.raw.slice();
  c.headers = parsed.props.headers;
  c.deliveryMode = parsed.props.deliveryMode;
  c.priority = parsed.props.priority;
  c.expiration = parsed.props.expiration;
  c.userId = parsed.props.userId;
  c.replyTo = parsed.props.replyTo;
  c.got = 0;
  c.chunks = [];
  if (c.bodySize === 0) return this.finishPublish(channel, c);
}

/**
 * Append a body frame and finish the publish once every byte has arrived.
 *
 * @param channel Channel the body arrived on.
 * @param payload Body bytes for this frame, not including the frame header.
 */
export function onBody(this: Conn, channel: number, payload: Uint8Array, owned = false): Promise<unknown> | void {
  const c = this.ch(channel);
  c.chunks.push(payload);
  c.chunksOwned = owned && c.chunks.length === 1;
  c.got += payload.length;
  if (c.got > c.bodySize) throw new ChanError(505, "UNEXPECTED_FRAME - body exceeds declared size");
  if (c.got >= c.bodySize) return this.finishPublish(channel, c);
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
    c.got = 0;
    return;
  }
  // A whole body the parser already copied out of the read buffer is used as is.
  const body = c.chunksOwned && c.chunks.length === 1 ? c.chunks[0]! : concat(c.chunks);
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
    if (pub.exchange !== "") {
      const ex = this.broker.exchanges.get(this.broker.key(this.vhost, pub.exchange));
      if (!ex) return this.chanClose(channel, 404, `NOT_FOUND - no exchange '${pub.exchange}' in vhost '${this.vhost}'`, 60, 40);
      if (ex.internal) {
        return this.chanClose(channel, 403, `ACCESS_REFUSED - cannot publish to internal exchange '${pub.exchange}' in vhost '${this.vhost}'`, 60, 40);
      }
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
  if (!this.broker.topicWriteAllowed(this.user, this.vhost, pub.exchange, pub.routingKey)) {
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
      if (c.confirm) return this.send(encodeSettle(channel, c.nextPub++, false));
      return;
    }
    propRaw = routed;
  }
  // Outside a transaction the tag is the arrival order, before the fsync wait.
  // Inside a transaction the tag is assigned when the op runs, at commit.
  const confirm = c.confirm;
  let pubTag = confirm && !c.tx ? c.nextPub++ : 0;
  // Not an async function. An async wrapper allocates a promise per publish
  // before the durable wait, and `await` on an already-resolved send yields
  // again. The durable wait is the only promise on this path.
  const start = () => {
    if (c.tx && confirm) pubTag = c.nextPub++;
    return this.broker.publish({
      vhost: this.vhost,
      exchange: pub.exchange,
      routingKey: pub.routingKey,
      body,
      headers,
      propRaw,
      persistent,
      priority,
      expiration,
      confirm,
      mandatory: pub.mandatory,
    });
  };
  const finish = (result: "ack" | "nack" | "return"): Promise<unknown> | void => {
    if (result === "return" && pub.mandatory) {
      const returned = this.sendMany([
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
        bodyFrame(channel, body, this.frameMax),
      ]);
      if (!confirm) return returned;
      return returned.then(() => this.send(encodeSettle(channel, pubTag, false)));
    }
    if (!confirm) return;
    return this.send(encodeSettle(channel, pubTag, result === "nack"));
  };
  const run = (): Promise<unknown> | void => {
    const pending = start();
    if (typeof pending === "string") return finish(pending);
    return pending.then(finish);
  };
  const fail = (err: unknown) => {
    const code = err instanceof ChanError ? err.code : 541;
    const text = err instanceof ChanError ? err.message : "INTERNAL_ERROR";
    if (!(err instanceof ChanError)) console.error("publish failed", err);
    return this.chanClose(channel, code, text, 60, 40);
  };
  // The destinations, for the back-pressure test only: the publish itself is
  // routed when it runs. Computed when the parser first asks, since most
  // publishes run before it does. `route` gives each queue once; only CC and
  // BCC can add a duplicate. A Set and two header lists per publish were 3%
  // of a 1 CPU transient load.
  let names: Iterable<string> | null = null;
  const destinations = (): Iterable<string> => {
    if (names) return names;
    names = this.broker.route(this.vhost, pub.exchange, pub.routingKey, headers);
    if (headers.length && headers.some(([key]) => key === "CC" || key === "BCC")) {
      const all = new Set(names);
      for (const key of [...headerList(headers, "CC"), ...headerList(headers, "BCC")]) {
        for (const queue of this.broker.route(this.vhost, pub.exchange, key, headers)) all.add(queue);
      }
      names = all;
    }
    return names;
  };
  const pressured = () => {
    for (const name of destinations()) {
      const queue = this.broker.queues.get(this.broker.key(this.vhost, name));
      if (queue && this.broker.isLocalHome(queue.home) && queue.consumers.length && queue.ready.length >= 256) return true;
    }
    return false;
  };
  if (c.tx) {
    this.stageTransaction(c, () => {
      const pending = run();
      this.deferredPublish.push({ channel, bytes: 0, pressured, run: () => this.publishSettled(0) });
      return pending;
    }, body.length + propRaw.length);
    return;
  }
  // A persistent confirm waits on the group-commit flush. Parking it lets the
  // parser reach basic.ack frames before this body is enqueued, so one prefetch
  // window can turn over inside a publish burst. `driveInbound` starts the batch.
  {
    const bytes = body.length + propRaw.length;
    if (this.deferredPublish.length + this.inFlightPublishes >= 65536) throw new ChanError(506, "RESOURCE_ERROR - pending publish operation limit");
    this.publishBytes += bytes;
    if (this.publishBytes > 64 * 1024 * 1024) throw new ChanError(506, "RESOURCE_ERROR - pending publishes exceed 64 MiB");
    // One reaction per publish on the durable wait, and the settle is a plain
    // call: a tracked promise and a `finally` per confirm were most of the
    // promises a deep confirm window held.
    const failed = (err: unknown) => {
      void fail(err);
      this.publishSettled(bytes);
    };
    const done = (result: "ack" | "nack" | "return") => {
      let sent: Promise<unknown> | void;
      try {
        sent = finish(result);
      } catch (err) {
        failed(err);
        return;
      }
      // A staged write has nothing to wait for. A queued one still reports a failure.
      if (sent && sent !== SENT) sent.then(undefined, fail);
      this.publishSettled(bytes);
    };
    this.deferredPublish.push({ channel, bytes, batchConfirm: confirm && persistent, pressured, run: () => {
      let pending: ReturnType<typeof start>;
      try {
        pending = start();
      } catch (err) {
        failed(err);
        return;
      }
      if (typeof pending === "string") done(pending);
      else pending.then(done, failed);
    }});
    return;
  }
}

Conn.prototype.beginPublish = beginPublish;
Conn.prototype.onHeader = onHeader;
Conn.prototype.onBody = onBody;
Conn.prototype.finishPublish = finishPublish;

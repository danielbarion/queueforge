/**
 * basic.publish, content header, and body frames.
 *
 * A publish is stored on the channel until the header and body arrive.
 * Immediate publishes are rejected. Confirms and returns are sent from here.
 */
import { ChanError } from "../broker/index.ts";
import { bodyFrame, contentHeaderFrame, emptyProps, encodeSettle, method, methodFrame, R, readContentHeader } from "../codec.ts";
import { Conn, type Ch } from "./listen.ts";

/** Join body chunks into one buffer, in arrival order. */
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

/**
 * Remember the exchange and routing key from basic.publish.
 *
 * @param c Channel that will receive the following header and body.
 * @param payload Method payload. Bit 0 is mandatory, bit 1 is immediate.
 * The body is not routed until {@link finishPublish}.
 */
export function beginPublish(this: Conn, c: Ch, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  r.u16();
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
  c.propRaw = parsed.props.raw;
  c.headers = parsed.props.headers;
  c.deliveryMode = parsed.props.deliveryMode;
  c.priority = parsed.props.priority;
  c.expiration = parsed.props.expiration;
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
export function onBody(this: Conn, channel: number, payload: Uint8Array): Promise<unknown> | void {
  const c = this.ch(channel);
  c.chunks.push(payload);
  c.got += payload.length;
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
    return this.chanClose(channel, 540, "NOT_IMPLEMENTED - immediate=true", 60, 40);
  }
  if (!this.broker.topicWriteAllowed(this.user, this.vhost, pub.exchange, pub.routingKey)) {
    return this.chanClose(channel, 403, "ACCESS_REFUSED - write access to topic refused", 60, 40);
  }
  // Outside a transaction the tag is the arrival order, before the fsync wait.
  // Inside a transaction the tag is assigned when the op runs, at commit.
  const confirm = c.confirm;
  const tag = confirm && !c.tx ? c.nextPub++ : 0;
  // Not an async function. An async wrapper allocates a promise per publish
  // before the durable wait, and `await` on an already-resolved send yields
  // again. The durable wait is the only promise on this path.
  const run = (): Promise<unknown> | void => {
    const pubTag = c.tx && confirm ? c.nextPub++ : tag;
    const pending = this.broker.publish({
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
          bodyFrame(channel, body),
        ]);
        if (!confirm) return returned;
        return returned.then(() => this.send(encodeSettle(channel, pubTag, false)));
      }
      if (!confirm) return;
      return this.send(encodeSettle(channel, pubTag, result === "nack"));
    };
    if (typeof pending === "string") return finish(pending);
    return pending.then(finish);
  };
  const fail = (err: unknown) => {
    const code = err instanceof ChanError ? err.code : 541;
    const text = err instanceof ChanError ? err.message : "INTERNAL_ERROR";
    console.error("publish confirm failed", err);
    return this.chanClose(channel, code, text, 60, 40);
  };
  if (c.tx) {
    c.txBatch.push(run);
    return;
  }
  // A persistent confirm waits on the group-commit flush. Parking it lets the
  // parser reach basic.ack frames before this body is enqueued, so one prefetch
  // window can turn over inside a publish burst. `driveInbound` starts the batch.
  if (confirm && persistent) {
    this.deferredPublish.push(() => {
      try {
        const pending = run();
        if (!pending) return Promise.resolve();
        return Promise.resolve(pending).catch(fail);
      } catch (err) {
        return fail(err);
      }
    });
    return;
  }
  return run();
}

Conn.prototype.beginPublish = beginPublish;
Conn.prototype.onHeader = onHeader;
Conn.prototype.onBody = onBody;
Conn.prototype.finishPublish = finishPublish;

/**
 * basic.publish, content header, and body frames.
 *
 * A publish is stored on the channel until the header and body arrive.
 * Immediate publishes are rejected. Confirms and returns are sent from here.
 */
import { bodyFrame, contentHeaderFrame, emptyProps, method, methodFrame, R, readContentHeader } from "../codec.ts";
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
export async function onHeader(this: Conn, channel: number, payload: Uint8Array) {
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

/**
 * Append a body frame and finish the publish once every byte has arrived.
 *
 * @param channel Channel the body arrived on.
 * @param payload Body bytes for this frame, not including the frame header.
 */
export async function onBody(this: Conn, channel: number, payload: Uint8Array) {
  const c = this.ch(channel);
  c.chunks.push(payload);
  c.got += payload.length;
  if (c.got >= c.bodySize) await this.finishPublish(channel, c);
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
export async function finishPublish(this: Conn, channel: number, c: Ch) {
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

Conn.prototype.beginPublish = beginPublish;
Conn.prototype.onHeader = onHeader;
Conn.prototype.onBody = onBody;
Conn.prototype.finishPublish = finishPublish;

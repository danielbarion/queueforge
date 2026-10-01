/**
 * basic.qos, consume, deliver, get, ack, nack, reject, and recover.
 *
 * Owns delivery tags and prefetch credit for one channel. Ack and reject
 * inside a transaction wait for tx.commit.
 */
import { bodyFrame, contentHeaderFrame, emptyProps, method, methodFrame, R, readTable, tableGet } from "../codec.ts";
import type { LiveMsg } from "../broker/index.ts";
import { Conn, type Ch } from "./listen.ts";

/**
 * Set this channel's prefetch count from basic.qos.
 *
 * @param channel Channel to send qos-ok on.
 * @param c Channel whose prefetch is replaced.
 * @param payload Method payload. Prefetch size and the global bit are read
 * and ignored. Global prefetch on the channel is cleared.
 */
export async function qos(this: Conn, channel: number, c: Ch, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  r.u32();
  const count = r.u16();
  r.u8();
  c.prefetch = count;
  c.globalPrefetch = null;
  await this.send(methodFrame(channel, method(60, 11, () => {})));
}

/**
 * Report whether this consumer may take another delivery.
 *
 * @param c Channel holding prefetch and the unacked counts.
 * @param tag Consumer tag whose own unacked count is checked.
 * @returns False when flow is stopped, the per-consumer prefetch is full,
 * or a global prefetch (when set) is full. Zero prefetch means unlimited.
 */
export function creditOk(this: Conn, c: Ch, tag: string): boolean {
  if (!c.flow) return false;
  const own = c.byConsumer.get(tag) ?? 0;
  if (c.prefetch !== 0 && own >= c.prefetch) return false;
  if (c.globalPrefetch != null && c.globalPrefetch !== 0 && c.globalUnacked >= c.globalPrefetch) return false;
  return true;
}

/**
 * Register a consumer and send consume-ok unless nowait is set.
 *
 * @param channel Channel the consumer lives on.
 * @param c Channel state that will track this consumer's deliveries.
 * @param payload Method payload. An empty tag becomes `ctag-<uuid>`.
 * `x-priority` is read from the arguments. Deliveries that fail the credit
 * check are nacked with requeue. The broker is kicked so a waiting message
 * can be delivered immediately.
 */
export async function consume(this: Conn, channel: number, c: Ch, payload: Uint8Array) {
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

/**
 * Write basic.deliver plus the header and body for one message.
 *
 * @param channel Channel the consumer was registered on.
 * @param tag Consumer tag to put in the deliver method.
 * @param dtag Delivery tag the client will ack or reject.
 * @param msg Message body, properties, and routing fields from the broker.
 */
export async function deliver(this: Conn, channel: number, tag: string, dtag: number, msg: LiveMsg) {
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

/**
 * Cancel one consumer and send cancel-ok.
 *
 * @param channel Channel the consumer was registered on.
 * @param c Channel state holding the consumer map.
 * @param payload Method payload. The tag is the first short string.
 * An unknown tag still gets cancel-ok.
 */
export async function cancel(this: Conn, channel: number, c: Ch, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  const tag = r.shortstr();
  const queue = c.consumers.get(tag);
  if (queue) await this.broker.cancel(this.vhost, queue, tag);
  c.consumers.delete(tag);
  this.broker.forgetMgmtConsumer(this.mgmtName, channel, tag);
  await this.send(methodFrame(channel, method(60, 31, (w) => w.shortstr(tag))));
}

/**
 * Ack one delivery tag, or every tag up to it when multiple is set.
 *
 * @param c Channel whose delivery map is settled.
 * @param payload Method payload. Bit 0 of the flags is multiple.
 * Inside a transaction the settle waits for tx.commit.
 */
export async function ack(this: Conn, c: Ch, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  const tag = r.u64();
  const multiple = (r.u8() & 1) !== 0;
  const op = async () => this.settle(c, tag, multiple, false, false);
  if (c.tx) c.txBatch.push(op);
  else await op();
}

/**
 * Reject or nack a delivery tag.
 *
 * @param c Channel whose delivery map is settled.
 * @param payload Method payload.
 * @param fromNack True for basic.nack, where bit 0 is multiple and bit 1 is requeue.
 * For basic.reject, bit 0 is requeue and multiple is always false.
 * Inside a transaction the settle waits for tx.commit.
 */
export async function reject(this: Conn, c: Ch, payload: Uint8Array, fromNack: boolean) {
  const r = new R(payload.subarray(4));
  const tag = r.u64();
  const bits = r.u8();
  const requeue = fromNack ? (bits & 2) !== 0 : (bits & 1) !== 0;
  const multiple = fromNack ? (bits & 1) !== 0 : false;
  const op = async () => this.settle(c, tag, multiple, true, requeue);
  if (c.tx) c.txBatch.push(op);
  else await op();
}

/**
 * basic.nack. Same settlement as reject, with nack's flag layout.
 *
 * @param c Channel whose delivery map is settled.
 * @param payload Method payload passed through to {@link reject}.
 */
export async function nack(this: Conn, c: Ch, payload: Uint8Array) {
  await this.reject(c, payload, true);
}

/**
 * Ack or nack the selected delivery tags and free their credit.
 *
 * @param c Channel holding the delivery map and prefetch counters.
 * @param tag Delivery tag from the client.
 * @param multiple When true, every tag less than or equal to `tag` is settled.
 * @param negative When true, the broker nacks; otherwise it acks.
 * @param requeue Passed to nack. Ignored for acks.
 */
export async function settle(this: Conn, c: Ch, tag: number, multiple: boolean, negative: boolean, requeue: boolean) {
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

/**
 * basic.get one message, or send get-empty.
 *
 * @param channel Channel to write the reply on.
 * @param c Channel that records the delivery tag when noAck is false.
 * @param payload Method payload. Bit 0 is noAck.
 * The message count field in get-ok is always 0.
 */
export async function get(this: Conn, channel: number, c: Ch, payload: Uint8Array) {
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

/**
 * basic.recover. Only requeue=true is implemented.
 *
 * @param channel Channel to close or to send recover-ok on.
 * @param c Channel whose unacked deliveries are returned to the queue.
 * @param payload Method payload. Bit 0 is requeue.
 * @param methodId 110 for basic.recover (replies recover-ok) or 100 for recover-async (no reply).
 * Recover-ok is sent before the messages are requeued, because clients drop
 * deliveries that were buffered ahead of that reply.
 */
export async function recover(this: Conn, channel: number, c: Ch, payload: Uint8Array, methodId: number) {
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
  if (methodId === 110) await this.send(methodFrame(channel, method(60, 111, () => {})));
  for (const d of pending) await this.broker.nack(d.vhost, d.queue, d.id, true);
}

Conn.prototype.qos = qos;
Conn.prototype.creditOk = creditOk;
Conn.prototype.consume = consume;
Conn.prototype.deliver = deliver;
Conn.prototype.cancel = cancel;
Conn.prototype.ack = ack;
Conn.prototype.reject = reject;
Conn.prototype.nack = nack;
Conn.prototype.settle = settle;
Conn.prototype.get = get;
Conn.prototype.recover = recover;

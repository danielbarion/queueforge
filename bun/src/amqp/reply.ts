/**
 * Direct reply-to (`amq.rabbitmq.reply-to`), as in RabbitMQ.
 *
 * A requester consumes from the pseudo-queue with no-ack and publishes with
 * `reply-to: amq.rabbitmq.reply-to`. The broker writes a per-channel address
 * into that property. A reply published to the address on the default
 * exchange goes straight to the requester's channel; no queue is created.
 */
import { bodyFrame, contentHeaderFrame, emptyProps, method, methodFrame, replaceReplyTo } from "../codec.ts";
import { ChanError } from "../broker/index.ts";
import { Conn, type Ch } from "./listen.ts";

export const REPLY_TO = "amq.rabbitmq.reply-to";
const PREFIX = `${REPLY_TO}.`;

/** Deliver one reply. Returns false when the requester has gone. */
export type ReplySink = (msg: { routingKey: string; body: Uint8Array; propRaw: Uint8Array }) => boolean;

/**
 * basic.consume on the pseudo-queue: register this channel's reply address.
 *
 * @param channel Channel the consumer is on.
 * @param c Channel state. Only one reply consumer is allowed per channel.
 * @param tag Consumer tag already chosen by the caller.
 * @param noAck RabbitMQ refuses a reply consumer that acknowledges.
 */
export function consumeReplies(this: Conn, channel: number, c: Ch, tag: string, noAck: boolean): void {
  if (!noAck) throw new ChanError(406, "PRECONDITION_FAILED - reply consumer cannot acknowledge");
  if (c.replyAddr) throw new ChanError(406, "PRECONDITION_FAILED - reply consumer already set");
  const id = Buffer.from(crypto.getRandomValues(new Uint8Array(18))).toString("base64url");
  const addr = `${PREFIX}${id}`;
  c.replyAddr = addr;
  c.replyTag = tag;
  const sink: ReplySink = (msg) => {
    if (this.closed || this.channels.get(channel) !== c || c.replyAddr !== addr) return false;
    const dtag = c.nextDel++;
    void this.sendMany([
      methodFrame(
        channel,
        method(60, 60, (w) => {
          w.shortstr(tag);
          w.u64(dtag);
          w.bits([false]);
          w.shortstr("");
          w.shortstr(msg.routingKey);
        }),
      ),
      contentHeaderFrame(channel, msg.body.length, msg.propRaw.length ? msg.propRaw : emptyProps()),
      bodyFrame(channel, msg.body, this.frameMax),
    ]);
    return true;
  };
  this.broker.replySinks.set(addr, sink);
}

/** Drop the reply address when its consumer is cancelled. Returns true when `tag` was the reply consumer. */
export function cancelReplies(this: Conn, c: Ch, tag: string): boolean {
  if (!c.replyAddr || c.replyTag !== tag) return false;
  this.broker.replySinks.delete(c.replyAddr);
  c.replyAddr = "";
  c.replyTag = "";
  return true;
}

/**
 * Apply direct reply-to to one publish.
 *
 * @returns The property block to publish with (reply-to rewritten), or
 * "sent" when the message was a reply and went straight to its requester,
 * or "dropped" when the requester is gone (RabbitMQ drops it too).
 */
export function routeReply(
  this: Conn,
  c: Ch,
  exchange: string,
  routingKey: string,
  body: Uint8Array,
  propRaw: Uint8Array,
): Uint8Array | "sent" | "dropped" {
  if (exchange === "" && routingKey.startsWith(PREFIX)) {
    const sink = this.broker.replySinks.get(routingKey);
    if (!sink) return "dropped";
    if (sink({ routingKey, body, propRaw })) return "sent";
    this.broker.replySinks.delete(routingKey);
    return "dropped";
  }
  if (c.replyTo !== REPLY_TO) return propRaw;
  if (!c.replyAddr) throw new ChanError(406, "PRECONDITION_FAILED - fast reply consumer does not exist");
  return replaceReplyTo(propRaw, c.replyAddr);
}

Conn.prototype.consumeReplies = consumeReplies;
Conn.prototype.cancelReplies = cancelReplies;
Conn.prototype.routeReply = routeReply;

declare module "./listen.ts" {
  interface Conn {
    consumeReplies: typeof consumeReplies;
    cancelReplies: typeof cancelReplies;
    routeReply: typeof routeReply;
  }
}

/**
 * channel.open, channel.close, and channel.flow.
 *
 * Owns per-channel lifetime, the management channel list, and requeue of
 * unacked deliveries when a channel or the connection goes away.
 */
import { method, methodFrame, R } from "../codec.ts";
import { Conn, type Ch } from "./listen.ts";

/**
 * Open a channel, or reuse one that is already open.
 *
 * @param channel Channel number from the frame. Channel 0 is not a channel.open.
 * A new channel counts against the user's channel limit. Over the limit, the
 * channel is closed with 403 and no channel state is kept.
 */
export async function handleChannelOpen(this: Conn, channel: number) {
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
}

/**
 * Requeue unacked deliveries, answer channel.close, and drop the channel.
 *
 * @param channel Channel being closed. Unknown channels still get a close-ok
 * after an empty requeue, because {@link Conn.ch} creates the state first.
 */
export async function handleChannelClose(this: Conn, channel: number) {
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
}

/**
 * Apply channel.flow and answer with the resulting active bit.
 *
 * @param channel Channel the flow method arrived on.
 * @param payload Method payload. A non-zero active bit resumes delivery.
 */
export async function handleChannelFlow(this: Conn, channel: number, payload: Uint8Array) {
  const rr = new R(payload.subarray(4));
  const active = rr.u8() !== 0;
  this.ch(channel).flow = active;
  await this.send(methodFrame(channel, method(20, 21, (w) => w.u8(active ? 1 : 0))));
}

/**
 * Send channel.close. The channel stays open until the peer closes it.
 *
 * @param channel Channel to close. 0 closes the connection channel.
 * @param code AMQP reply code.
 * @param text Reply text, truncated to 180 characters.
 * @param classId Class id of the method that failed. Defaults to 0.
 * @param methodId Method id of the method that failed. Defaults to 0.
 */
export async function chanClose(this: Conn, channel: number, code: number, text: string, classId = 0, methodId = 0) {
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

/**
 * Publish the open channel numbers to the management connection row.
 *
 * Does nothing before connection.open has stored a management id.
 */
export function syncMgmt(this: Conn) {
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

/**
 * Requeue every unacked delivery on one channel.
 *
 * @param c Channel state. Its delivery map is cleared first.
 * A missing queue home is ignored so connection teardown can continue.
 */
export async function requeueChannel(this: Conn, c: Ch) {
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

/**
 * Cancel every consumer on this connection and drop the management rows.
 *
 * Called when the socket closes, before {@link requeueAll}.
 */
export async function dropConsumers(this: Conn) {
  for (const [channel, c] of this.channels) {
    for (const [tag, queue] of c.consumers) {
      await this.broker.cancel(this.vhost, queue, tag);
      this.broker.forgetMgmtConsumer(this.mgmtName, channel, tag);
    }
    c.consumers.clear();
  }
}

/** Requeue unacked deliveries on every channel still open on this connection. */
export async function requeueAll(this: Conn) {
  for (const c of this.channels.values()) await this.requeueChannel(c);
}

Conn.prototype.handleChannelOpen = handleChannelOpen;
Conn.prototype.handleChannelClose = handleChannelClose;
Conn.prototype.handleChannelFlow = handleChannelFlow;
Conn.prototype.chanClose = chanClose;
Conn.prototype.syncMgmt = syncMgmt;
Conn.prototype.requeueChannel = requeueChannel;
Conn.prototype.dropConsumers = dropConsumers;
Conn.prototype.requeueAll = requeueAll;

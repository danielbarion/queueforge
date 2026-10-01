/**
 * Exchange and queue declare, bind, unbind, purge, and delete.
 *
 * Each method reads its frame, calls the broker, and sends the matching
 * ok unless the client set nowait.
 */
import { argsFromFields } from "../broker/index.ts";
import { fieldStr, method, methodFrame, R, readTable, tableGet } from "../codec.ts";
import { Conn, type Ch } from "./listen.ts";

/**
 * Declare or passively check an exchange.
 *
 * @param channel Channel to send declare-ok on.
 * @param _c Unused channel state. Present so the dispatcher signature matches
 * the other class-40 methods.
 * @param payload Method payload. `alternate-exchange` is read from the arguments table.
 * Passive skips the broker declare. nowait skips declare-ok.
 */
export async function exDeclare(this: Conn, channel: number, _c: Ch, payload: Uint8Array) {
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

/**
 * Delete an exchange.
 *
 * @param channel Channel to send delete-ok on.
 * @param payload Method payload. The if-unused bit is not enforced.
 * nowait (bit 1) skips delete-ok.
 */
export async function exDelete(this: Conn, channel: number, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  r.u16();
  const name = r.shortstr();
  const nowait = (r.u8() & 2) !== 0;
  await this.broker.deleteExchange(this.vhost, name);
  if (!nowait) await this.send(methodFrame(channel, method(40, 21, () => {})));
}

/**
 * Bind one exchange to another.
 *
 * @param channel Channel to send bind-ok on.
 * @param payload Method payload. Destination is the first name, source the second.
 * nowait skips bind-ok.
 */
export async function exBind(this: Conn, channel: number, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  r.u16();
  const destination = r.shortstr();
  const source = r.shortstr();
  const routingKey = r.shortstr();
  const nowait = (r.u8() & 1) !== 0;
  await this.broker.bindExchange(this.vhost, source, destination, routingKey);
  if (!nowait) await this.send(methodFrame(channel, method(40, 31, () => {})));
}

/**
 * Remove an exchange-to-exchange binding.
 *
 * @param channel Channel to send unbind-ok on.
 * @param payload Method payload, in the same order as bind.
 * nowait skips unbind-ok.
 */
export async function exUnbind(this: Conn, channel: number, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  r.u16();
  const destination = r.shortstr();
  const source = r.shortstr();
  const routingKey = r.shortstr();
  const nowait = (r.u8() & 1) !== 0;
  await this.broker.unbindExchange(this.vhost, source, destination, routingKey);
  if (!nowait) await this.send(methodFrame(channel, method(40, 51, () => {})));
}

/**
 * Declare or passively check a queue.
 *
 * @param channel Channel to send declare-ok on.
 * @param payload Method payload. Arguments become the queue argument map.
 * A non-passive, non-durable, non-exclusive, non-quorum queue closes the
 * connection unless `broker.transientNonexcl` is set. nowait (bit 4) skips
 * declare-ok. Declare-ok carries the queue name, message count, and consumer count.
 */
export async function qDeclare(this: Conn, channel: number, payload: Uint8Array) {
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

/**
 * Bind a queue to an exchange.
 *
 * @param channel Channel to send bind-ok on.
 * @param payload Method payload. Binding arguments are forwarded to the broker.
 * nowait skips bind-ok.
 */
export async function qBind(this: Conn, channel: number, payload: Uint8Array) {
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

/**
 * Remove a queue binding.
 *
 * @param channel Channel to send unbind-ok on. Unbind has no nowait bit here.
 * @param payload Method payload, including the binding arguments.
 */
export async function qUnbind(this: Conn, channel: number, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  r.u16();
  const queue = r.shortstr();
  const exchange = r.shortstr();
  const routingKey = r.shortstr();
  const args = readTable(r);
  await this.broker.unbind(this.vhost, exchange, queue, routingKey, args);
  await this.send(methodFrame(channel, method(50, 51, () => {})));
}

/**
 * Purge a queue.
 *
 * @param channel Channel to send purge-ok on.
 * @param payload Method payload. Purge-ok carries the number of messages removed.
 * nowait skips purge-ok.
 */
export async function qPurge(this: Conn, channel: number, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  r.u16();
  const queue = r.shortstr();
  const nowait = (r.u8() & 1) !== 0;
  const n = await this.broker.purge(this.vhost, queue);
  if (!nowait) await this.send(methodFrame(channel, method(50, 31, (w) => w.u32(n))));
}

/**
 * Delete a queue.
 *
 * @param channel Channel to send delete-ok on.
 * @param payload Method payload. if-unused and if-empty are not enforced.
 * nowait is bit 2. Delete-ok carries the number of messages removed.
 */
export async function qDelete(this: Conn, channel: number, payload: Uint8Array) {
  const r = new R(payload.subarray(4));
  r.u16();
  const queue = r.shortstr();
  const nowait = (r.u8() & 4) !== 0;
  const n = await this.broker.deleteQueue(this.vhost, queue);
  if (!nowait) await this.send(methodFrame(channel, method(50, 41, (w) => w.u32(n))));
}

Conn.prototype.exDeclare = exDeclare;
Conn.prototype.exDelete = exDelete;
Conn.prototype.exBind = exBind;
Conn.prototype.exUnbind = exUnbind;
Conn.prototype.qDeclare = qDeclare;
Conn.prototype.qBind = qBind;
Conn.prototype.qUnbind = qUnbind;
Conn.prototype.qPurge = qPurge;
Conn.prototype.qDelete = qDelete;

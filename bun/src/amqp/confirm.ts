/**
 * confirm.select and the tx class: select, commit, and rollback.
 *
 * Confirm mode makes each publish send basic.ack or basic.nack. A transaction
 * holds those publishes until tx.commit or drops them on tx.rollback.
 */
import { method, methodFrame, R } from "../codec.ts";
import { Conn, type Ch } from "./listen.ts";

/**
 * Handle confirm.select, tx.select, tx.commit, and tx.rollback.
 *
 * @param channel Channel the method arrived on.
 * @param c Channel whose confirm and tx flags are updated.
 * @param payload Method payload. confirm.select uses bit 0 as nowait.
 * @param cls AMQP class id. 85 is confirm, 90 is tx.
 * @param mid Method id inside that class.
 * @returns True when this class and method were handled. False leaves the
 * dispatcher to try another class.
 */
export async function handleConfirmTx(this: Conn, channel: number, c: Ch, payload: Uint8Array, cls: number, mid: number): Promise<boolean> {
  if (cls === 85 && mid === 10) {
    c.confirm = true;
    const nowait = new R(payload.subarray(4)).u8() & 1;
    if (!nowait) await this.send(methodFrame(channel, method(85, 11, () => {})));
    return true;
  }
  if (cls === 90 && mid === 10) {
    c.tx = true;
    await this.send(methodFrame(channel, method(90, 11, () => {})));
    return true;
  }
  if (cls === 90 && mid === 20) {
    await this.txCommit(channel, c);
    return true;
  }
  if (cls === 90 && mid === 30) {
    c.txBatch = [];
    await this.send(methodFrame(channel, method(90, 31, () => {})));
    return true;
  }
  return false;
}

/**
 * Run the publishes and settles queued on this channel, then send tx.commit-ok.
 *
 * @param channel Channel to send commit-ok on.
 * @param c Channel whose batch is drained before the operations run.
 * Operations see confirm and channel state as it is at commit time.
 */
export async function txCommit(this: Conn, channel: number, c: Ch) {
  const batch = c.txBatch;
  c.txBatch = [];
  for (const op of batch) await op();
  await this.send(methodFrame(channel, method(90, 21, () => {})));
}

Conn.prototype.handleConfirmTx = handleConfirmTx;
Conn.prototype.txCommit = txCommit;

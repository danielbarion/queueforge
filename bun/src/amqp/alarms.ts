/**
 * connection.blocked and connection.unblocked for one connection.
 *
 * A client that advertised the `connection.blocked` capability is told when
 * a memory or disk alarm starts and ends. Publishes on every connection wait
 * in {@link finishPublish} while the alarm lasts.
 */
import { method, methodFrame } from "../codec.ts";
import { Conn } from "./listen.ts";

/** Start sending alarm changes to this connection. Called once connection.open succeeds. */
export function watchAlarms(this: Conn): void {
  if (!this.wantsBlocked || this.alarmListener) return;
  const listener = (blocked: boolean, reason: string) => {
    if (this.closed) return;
    const frame = blocked
      ? methodFrame(0, method(10, 60, (w) => w.shortstr(reason)))
      : methodFrame(0, method(10, 61, () => {}));
    void this.send(frame);
  };
  this.alarmListener = listener;
  this.broker.alarmListeners.add(listener);
  if (this.broker.blocked) listener(true, this.broker.memAlarm ? "low on memory" : "low on disk");
}

/** Stop sending alarm changes. Called when the connection is gone. */
export function unwatchAlarms(this: Conn): void {
  if (!this.alarmListener) return;
  this.broker.alarmListeners.delete(this.alarmListener);
  this.alarmListener = null;
}

Conn.prototype.watchAlarms = watchAlarms;
Conn.prototype.unwatchAlarms = unwatchAlarms;

declare module "./listen.ts" {
  interface Conn {
    watchAlarms: typeof watchAlarms;
    unwatchAlarms: typeof unwatchAlarms;
  }
}

/**
 * Memory and disk alarms, as RabbitMQ raises them.
 *
 * The sweep samples resident memory and free disk. When either crosses its
 * limit every publishing connection is blocked: connections whose client
 * advertised `connection.blocked` are told so, and all publishes wait until
 * the alarm clears. Consumers keep running so the backlog can drain.
 */
import { availableBytes } from "../disk.ts";
import { totalmem } from "node:os";
import { Broker } from "./class.ts";
import type { AlarmListener } from "./model.ts";

/** RabbitMQ's defaults: 0.6 of RAM, 50 MB of free disk. */
const MEM_RELATIVE = 0.6;
const DISK_FREE_LIMIT = 50 * 1024 * 1024;

export type { AlarmListener };

/**
 * The memory limit in bytes. `QUEUEFORGE_MEM_HIGH_WATERMARK` overrides the
 * fraction of RAM, or gives an absolute byte count when it is above 1.
 */
function memLimit(): number {
  const raw = Number(process.env.QUEUEFORGE_MEM_HIGH_WATERMARK);
  if (Number.isFinite(raw) && raw > 1) return raw;
  const fraction = Number.isFinite(raw) && raw > 0 ? raw : MEM_RELATIVE;
  return totalmem() * fraction;
}

/** Sample memory and disk, and tell listeners when the blocked state changes. */
export function checkAlarms(this: Broker): void {
  const mem = process.memoryUsage.rss();
  const disk = availableBytes(this.cfg.dataDir || ".");
  this.memAlarm = mem >= memLimit();
  this.diskAlarm = disk > 0 && disk < DISK_FREE_LIMIT;
  const blocked = this.memAlarm || this.diskAlarm;
  if (blocked === this.blocked) return;
  this.blocked = blocked;
  const reason = this.memAlarm ? "low on memory" : this.diskAlarm ? "low on disk" : "";
  if (!blocked) {
    const waiting = this.unblockWaiters.splice(0);
    for (const wake of waiting) wake();
  }
  for (const listener of this.alarmListeners) listener(blocked, reason);
}

/** A promise that settles when publishes may run again. */
export function whenUnblocked(this: Broker): Promise<void> {
  if (!this.blocked) return Promise.resolve();
  return new Promise((resolve) => this.unblockWaiters.push(resolve));
}

Broker.prototype.checkAlarms = checkAlarms;
Broker.prototype.whenUnblocked = whenUnblocked;

declare module "./class.ts" {
  interface Broker {
    checkAlarms: typeof checkAlarms;
    whenUnblocked: typeof whenUnblocked;
  }
}

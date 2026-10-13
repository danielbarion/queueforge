/**
 * Memory and disk alarms, as RabbitMQ raises them.
 *
 * The sweep samples resident memory and free disk. When either crosses its
 * limit every publishing connection is blocked: connections whose client
 * advertised `connection.blocked` are told so, and all publishes wait until
 * the alarm clears. Consumers keep running so the backlog can drain.
 */
import { availableBytes } from "../disk.ts";
import { grantedCores } from "../cores.ts";
import { readFileSync } from "node:fs";
import { totalmem } from "node:os";
import { Broker } from "./class.ts";
import type { AlarmListener } from "./model.ts";

/** RabbitMQ's defaults: 0.6 of RAM, 50 MB of free disk. */
const MEM_RELATIVE = 0.6;
const DISK_FREE_LIMIT = 50 * 1024 * 1024;

export type { AlarmListener };

/**
 * The memory limit in bytes. `QUEUEFORGE_MEM_HIGH_WATERMARK` overrides the
 * fraction, or gives an absolute byte count when it is above 1.
 *
 * Under a cgroup `memory.max` the fraction is of that limit and applies to
 * the whole container: every cores-mode child compares the container's
 * anonymous memory, so the processes together stop at the watermark however
 * the load is spread. Without one, each process compares its own RSS against
 * its share of RAM.
 * Computed once: the sweep runs every 200 ms and `totalmem()` showed in profiles.
 */
let limitCache: { bytes: number; container: boolean } | null = null;
function memLimit(): { bytes: number; container: boolean } {
  if (limitCache !== null) return limitCache;
  return (limitCache = memoryLimit(
    process.env.QUEUEFORGE_MEM_HIGH_WATERMARK,
    readCgroup("memory.max"),
    readCgroup("memory.stat") !== null,
    totalmem(),
    grantedCores(),
  ));
}

/**
 * The watermark from its inputs. `container` is true when the measure is the
 * cgroup's anonymous memory rather than this process's RSS.
 *
 * @param raw `QUEUEFORGE_MEM_HIGH_WATERMARK`: a fraction, or bytes above 1.
 * @param memMax The cgroup's `memory.max` text, or null without one.
 * @param hasStat Whether `memory.stat` can be read, to measure the container.
 * @param ram Host memory in bytes.
 * @param processes Broker processes sharing the host when there is no cgroup limit.
 */
export function memoryLimit(raw: string | undefined, memMax: string | null, hasStat: boolean, ram: number, processes: number) {
  const value = Number(raw);
  const max = memMax === null ? null : cgroupBytes(memMax);
  const container = max !== null && hasStat;
  if (Number.isFinite(value) && value > 1) return { bytes: value, container };
  const fraction = Number.isFinite(value) && value > 0 ? value : MEM_RELATIVE;
  if (container && max !== null) return { bytes: max * fraction, container };
  return { bytes: (Math.min(ram, max ?? ram) * fraction) / Math.max(1, processes), container };
}

/** A cgroup v2 byte limit such as `memory.max`; null for `max` or text that is not a number. */
export function cgroupBytes(text: string): number | null {
  const t = text.trim();
  if (!/^\d+$/.test(t)) return null;
  const n = Number(t);
  return n > 0 ? n : null;
}

/** The `anon` line of a cgroup v2 `memory.stat`. */
export function cgroupAnon(stat: string): number | null {
  const match = /^anon (\d+)$/m.exec(stat);
  return match ? Number(match[1]) : null;
}

function readCgroup(name: string): string | null {
  try {
    return readFileSync(`/sys/fs/cgroup/${name}`, "utf8");
  } catch {
    return null;
  }
}

/** Bytes the watermark is compared with: the container's anonymous memory, or this process's RSS. */
function memUsed(container: boolean): number {
  if (container) {
    const stat = readCgroup("memory.stat");
    const anon = stat === null ? null : cgroupAnon(stat);
    if (anon !== null) return anon;
  }
  return process.memoryUsage.rss();
}

/**
 * Free disk, re-sampled every 5 s, or on every sweep once it is near the limit.
 * `statfs` took ~17 ms on Docker Desktop's volume: 9% of a 1 CPU broker's time
 * at the 200 ms sweep, all of it holding the event loop.
 */
const DISK_SAMPLE_MS = 5000;
let diskCache = { at: -Infinity, path: "", bytes: 0 };
function diskFree(path: string): number {
  const now = performance.now();
  const near = diskCache.bytes > 0 && diskCache.bytes < 4 * DISK_FREE_LIMIT;
  if (path !== diskCache.path || near || now - diskCache.at >= DISK_SAMPLE_MS) {
    diskCache = { at: now, path, bytes: availableBytes(path) };
  }
  return diskCache.bytes;
}

/** Sample memory and disk, and tell listeners when the blocked state changes. */
export function checkAlarms(this: Broker): void {
  const limit = memLimit();
  const mem = memUsed(limit.container);
  const disk = diskFree(this.cfg.dataDir || ".");
  this.memAlarm = mem >= limit.bytes;
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

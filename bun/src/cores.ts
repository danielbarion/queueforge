import { closeSync, fsyncSync, mkdirSync, openSync, readFileSync, writeSync } from "node:fs";
import net from "node:net";
import { dirname } from "node:path";

/**
 * How many broker processes a container may run, and the plan for each one.
 *
 * The count comes from the cgroup, not the machine. A one-CPU limit stays one
 * process. Each child has its own data directory and cluster port. Only the
 * first binds the management listener.
 */

export type ChildPlan = {
  id: string;
  dataDir: string;
  clusterListen: string;
  metricsListen: string;
  bindMgmt: boolean;
};

/** Count CPUs in a cpuset list such as `0-3` or `0,2,4-5`. */
export function countCpuset(text: string): number {
  let n = 0;
  for (const part of text.split(",")) {
    const piece = part.trim();
    const match = piece.match(/^(\d+)(?:-(\d+))?$/);
    if (!match) continue;
    const start = Number(match[1]);
    const end = match[2] === undefined ? start : Number(match[2]);
    if (end >= start) n += end - start + 1;
  }
  return n;
}

/**
 * CPUs granted by a cgroup v2 `cpu.max` line.
 *
 * @returns Null when the quota is `max` or the line is not a quota. A quota of
 * one period is one CPU. A fractional quota rounds to the nearest count, and
 * never below one.
 */
export function quotaCpus(text: string): number | null {
  const [quota, period] = text.trim().split(/\s+/);
  if (!quota || !period || quota === "max") return null;
  const q = Number(quota);
  const p = Number(period);
  if (!Number.isFinite(q) || !Number.isFinite(p) || p <= 0) return null;
  return Math.max(1, Math.round(q / p));
}

/** The tighter of the quota and the cpuset. Missing files mean one process. */
export function coresFromCgroup(cpuMax: string | null, cpuset: string | null): number {
  const quota = cpuMax ? quotaCpus(cpuMax) : null;
  const set = cpuset ? countCpuset(cpuset) : 0;
  if (quota && set) return Math.min(quota, set);
  if (quota) return quota;
  if (set) return set;
  return 1;
}

/** The default first internal port of a cores-mode broker. */
export const CORES_PORT_BASE = 25672;

/** The first internal port: `QUEUEFORGE_CORES_PORT`, or 25672. */
export function coresPortBase(env: Record<string, string | undefined> = process.env): number {
  const n = Number(env.QUEUEFORGE_CORES_PORT);
  return Number.isInteger(n) && n > 0 && n < 65536 ? n : CORES_PORT_BASE;
}

/**
 * One plan per granted core. Cluster ports are base + i; metrics ports follow
 * the last of them, base + cores + i, so the two ranges never meet. Metrics
 * at a fixed 25680 + i took worker 8's cluster port from worker 0 and so on,
 * and from 9 workers on the later ones exited with EADDRINUSE and restarted
 * in a loop (PENDING.md B16). `ports`, when given, replaces the 2 * cores
 * ports of that range (cluster ports first), as `freeInternalPorts` picks.
 */
export function childPlans(cores: number, dataDir: string, base = CORES_PORT_BASE, ports?: number[]): ChildPlan[] {
  const plans: ChildPlan[] = [];
  const root = dataDir.replace(/\/$/, "");
  for (let i = 0; i < cores; i++) {
    const id = `n${i}`;
    plans.push({
      id,
      dataDir: `${root}/${id}`,
      clusterListen: `127.0.0.1:${ports?.[i] ?? base + i}`,
      metricsListen: `127.0.0.1:${ports?.[cores + i] ?? base + cores + i}`,
      bindMgmt: i === 0,
    });
  }
  return plans;
}

/** Whether a TCP port on 127.0.0.1 can be bound now. */
export function portFree(port: number): Promise<boolean> {
  return new Promise((done) => {
    const server = net.createServer();
    server.once("error", () => done(false));
    server.listen({ port, host: "127.0.0.1", exclusive: true }, () => server.close(() => done(true)));
  });
}

/**
 * The `count` internal ports to use from `base`: the range base..base+count-1
 * when all of it is free, as before. When any of it is taken (another broker,
 * or RabbitMQ's own inter-node port 25672), each taken one is replaced by the
 * next free port above the range, so a worker never fails to bind or dials
 * a stranger's port.
 */
export async function freeInternalPorts(base: number, count: number, isFree: (port: number) => Promise<boolean> = portFree): Promise<number[]> {
  const range = Array.from({ length: count }, (_, i) => base + i);
  const free = await Promise.all(range.map((port) => isFree(port)));
  if (free.every(Boolean)) return range;
  const out: number[] = [];
  let next = base + count;
  for (let i = 0; i < count; i++) {
    if (free[i]) {
      out.push(range[i]!);
      continue;
    }
    while (next < 65536 && !(await isFree(next))) next++;
    if (next >= 65536) throw new Error(`no free internal port from ${base}`);
    out.push(next++);
  }
  return out;
}

export function memberList(plans: ChildPlan[]): Array<{ id: string; addr: string }> {
  return plans.map((plan) => ({ id: plan.id, addr: plan.clusterListen }));
}

/**
 * The id of the physical broker whose workers live under `dataDir`. Written
 * once, so it survives restarts and only these workers ever share it.
 */
export function brokerIdentity(dataDir: string): string {
  const path = `${dataDir.replace(/\/$/, "")}/broker-id`;
  try {
    const id = readFileSync(path, "utf8").trim();
    if (id) return id;
  } catch {
    /* first start */
  }
  const id = crypto.randomUUID();
  mkdirSync(dirname(path), { recursive: true });
  const fd = openSync(path, "wx");
  try {
    writeSync(fd, id + "\n");
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  return id;
}

/** Cores this process may use. One when the host has no cgroup, as on a Mac. */
export function grantedCores(): number {
  // QUEUEFORGE_CORES overrides the cgroup, for hosts without one (macOS) and tests.
  const forced = Number(process.env.QUEUEFORGE_CORES);
  if (Number.isInteger(forced) && forced > 0) return forced;
  const read = (path: string): string | null => {
    try {
      return readFileSync(path, "utf8");
    } catch {
      return null;
    }
  };
  return coresFromCgroup(
    read("/sys/fs/cgroup/cpu.max"),
    read("/sys/fs/cgroup/cpuset.cpus.effective") ?? read("/sys/fs/cgroup/cpuset.cpus"),
  );
}

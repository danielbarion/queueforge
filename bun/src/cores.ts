import { readFileSync } from "node:fs";

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

/** One plan per granted core. Cluster ports start at 25672 and do not overlap AMQP. */
export function childPlans(cores: number, dataDir: string): ChildPlan[] {
  const plans: ChildPlan[] = [];
  const root = dataDir.replace(/\/$/, "");
  for (let i = 0; i < cores; i++) {
    const id = `n${i}`;
    plans.push({
      id,
      dataDir: `${root}/${id}`,
      clusterListen: `127.0.0.1:${25672 + i}`,
      metricsListen: `127.0.0.1:${25680 + i}`,
      bindMgmt: i === 0,
    });
  }
  return plans;
}

export function memberList(plans: ChildPlan[]): Array<{ id: string; addr: string }> {
  return plans.map((plan) => ({ id: plan.id, addr: plan.clusterListen }));
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

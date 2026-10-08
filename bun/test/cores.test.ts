import { describe, expect, test } from "bun:test";
import { childPlans, coresFromCgroup, countCpuset, memberList, quotaCpus } from "../src/cores.ts";

describe("cgroup cores", () => {
  test("counts a cpuset range and a list", () => {
    expect(countCpuset("0-3")).toBe(4);
    expect(countCpuset("0")).toBe(1);
    expect(countCpuset("0,2,4-5")).toBe(4);
  });

  test("reads a cpu.max quota and ignores an unlimited line", () => {
    expect(quotaCpus("100000 100000")).toBe(1);
    expect(quotaCpus("400000 100000")).toBe(4);
    expect(quotaCpus("max 100000")).toBeNull();
  });

  test("uses the tighter of the quota and the cpuset", () => {
    expect(coresFromCgroup("400000 100000", "0-7")).toBe(4);
    expect(coresFromCgroup("800000 100000", "0-1")).toBe(2);
    expect(coresFromCgroup(null, null)).toBe(1);
  });
});

describe("child plans", () => {
  test("one child per core, distinct directories, management only on the first", () => {
    const plans = childPlans(4, "/var/lib/queueforge");
    expect(plans.map((plan) => plan.id)).toEqual(["n0", "n1", "n2", "n3"]);
    expect(new Set(plans.map((plan) => plan.dataDir)).size).toBe(4);
    expect(new Set(plans.map((plan) => plan.clusterListen)).size).toBe(4);
    expect(plans.filter((plan) => plan.bindMgmt).map((plan) => plan.id)).toEqual(["n0"]);
    expect(memberList(plans)).toEqual([
      { id: "n0", addr: "127.0.0.1:25672" },
      { id: "n1", addr: "127.0.0.1:25673" },
      { id: "n2", addr: "127.0.0.1:25674" },
      { id: "n3", addr: "127.0.0.1:25675" },
    ]);
  });
});

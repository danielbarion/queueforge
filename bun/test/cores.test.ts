import { describe, expect, test } from "bun:test";
import net from "node:net";
import { childPlans, coresFromCgroup, coresPortBase, countCpuset, freeInternalPorts, memberList, portFree, quotaCpus } from "../src/cores.ts";
import { workerBaseEnv } from "../src/supervise.ts";

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

  // Fixed metrics ports from 25680 collided with cluster ports from 9 workers on.
  test("no two listeners share a port, at any worker count", () => {
    for (const cores of [1, 4, 8, 9, 16, 64, 128]) {
      const plans = childPlans(cores, "/var/lib/queueforge");
      const ports = plans.flatMap((plan) => [plan.clusterListen, plan.metricsListen].map((a) => Number(a.split(":")[1])));
      expect(new Set(ports).size).toBe(2 * cores);
      expect(ports.every((port) => port !== 5672 && port !== 15672 && port < 65536)).toBe(true);
    }
  });
});

describe("internal ports", () => {
  test("QUEUEFORGE_CORES_PORT moves the base; anything else keeps 25672", () => {
    expect(coresPortBase({})).toBe(25672);
    expect(coresPortBase({ QUEUEFORGE_CORES_PORT: "26100" })).toBe(26100);
    expect(coresPortBase({ QUEUEFORGE_CORES_PORT: "nope" })).toBe(25672);
    expect(coresPortBase({ QUEUEFORGE_CORES_PORT: "70000" })).toBe(25672);
    expect(memberList(childPlans(2, "/d", 26100))).toEqual([
      { id: "n0", addr: "127.0.0.1:26100" },
      { id: "n1", addr: "127.0.0.1:26101" },
    ]);
    expect(childPlans(2, "/d", 26100).map((p) => p.metricsListen)).toEqual(["127.0.0.1:26102", "127.0.0.1:26103"]);
  });

  test("a free range is used as it is", async () => {
    expect(await freeInternalPorts(26100, 4, async () => true)).toEqual([26100, 26101, 26102, 26103]);
  });

  // RabbitMQ's inter-node port is 25672: a broker started beside it must not
  // fail to bind or dial the other's port.
  test("taken ports are replaced by free ones above the range", async () => {
    const taken = new Set([26100, 26102, 26104]);
    const ports = await freeInternalPorts(26100, 4, async (p) => !taken.has(p));
    expect(ports).toEqual([26105, 26101, 26106, 26103]);
    const plans = childPlans(2, "/d", 26100, ports);
    expect(plans.map((p) => p.clusterListen)).toEqual(["127.0.0.1:26105", "127.0.0.1:26101"]);
    expect(plans.map((p) => p.metricsListen)).toEqual(["127.0.0.1:26106", "127.0.0.1:26103"]);
  });

  test("portFree sees a listener", async () => {
    const server = net.createServer();
    await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
    const port = (server.address() as net.AddressInfo).port;
    expect(await portFree(port)).toBe(false);
    await new Promise<void>((done) => server.close(() => done()));
    expect(await portFree(port)).toBe(true);
  });
});

describe("worker environment", () => {
  // One GC marker per worker: the default helper pool is sized from the host's
  // cores, and every worker's helpers competed for the same CPUs.
  test("one GC marker unless the environment sets its own", () => {
    expect(workerBaseEnv({ PATH: "/bin" })).toEqual({ BUN_JSC_numberOfGCMarkers: "1", PATH: "/bin" });
    expect(workerBaseEnv({ BUN_JSC_numberOfGCMarkers: "4" }).BUN_JSC_numberOfGCMarkers).toBe("4");
  });
});

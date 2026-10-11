/** Opt-in one-second samples for benchmark diagnosis; no work when disabled. */
import { heapStats } from "bun:jsc";
import type { Broker } from "./broker/index.ts";

export function profileSample(broker: Broker) {
  let ready = 0, readyBytes = 0, unacked = 0, replicas = 0, consumers = 0;
  for (const q of broker.queues.values()) {
    ready += q.ready.length;
    readyBytes += q.ready.bytes;
    unacked += q.unacked.size;
    replicas += q.replicas.size;
    consumers += q.consumers.length;
  }
  const heap = heapStats();
  return {
    time: new Date().toISOString(), pid: process.pid, node: broker.cfg.nodeId,
    uptime: process.uptime(), memory: process.memoryUsage(), cpu: process.cpuUsage(),
    heap: { heapSize: heap.heapSize, heapCapacity: heap.heapCapacity,
      extraMemorySize: heap.extraMemorySize, objectCount: heap.objectCount,
      protectedObjectCount: heap.protectedObjectCount },
    queues: { count: broker.queues.size, ready, readyBytes, unacked, replicas, consumers },
    store: {
      flushes: broker.store.fullFlushCount,
      timing: broker.store.profileTiming ? { ...broker.store.profileTiming } : null,
    },
    published: broker.prom.received, delivered: broker.prom.delivered,
  };
}

export function startProfile(broker: Broker) {
  if (process.env.QUEUEFORGE_PROFILE !== "1") return;
  // Event-loop lag: how late a 1 ms timer fires. A stall delays every socket.
  let last = performance.now(), lagMax = 0, stalls = 0;
  const lag = setInterval(() => {
    const now = performance.now();
    const late = now - last - 1;
    last = now;
    if (late > lagMax) lagMax = late;
    if (late > 5) stalls++;
  }, 1);
  lag.unref();
  const timer = setInterval(() => {
    const loop = { lagMaxMs: Math.round(lagMax * 100) / 100, stallsOver5ms: stalls };
    lagMax = 0;
    stalls = 0;
    console.log("queueforge-profile " + JSON.stringify({ ...profileSample(broker), loop }));
  }, 1000);
  timer.unref();
  return () => {
    clearInterval(timer);
    clearInterval(lag);
  };
}

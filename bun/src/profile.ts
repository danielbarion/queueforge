/** Opt-in one-second samples for benchmark diagnosis; no work when disabled. */
import { heapStats } from "bun:jsc";
import type { Broker } from "./broker/index.ts";

export const profiling = process.env.QUEUEFORGE_PROFILE === "1";

/** Open AMQP connections, tracked only while profiling. */
export const profiledConns = new Set<{
  closed: boolean; outboundBytes: number; stagedBytes: number; writeQueuedBytes: number;
  outputBlocked: boolean; readsPaused: boolean; inboundRunning: boolean; deferredPublish: unknown[];
  inFlightPublishes: Set<unknown>; publishBytes: number; fresh: unknown[]; wakeInbound: unknown;
  outputWakers: Map<string, unknown>; channels: Map<number, { deliveries: Map<unknown, unknown>; consumers: Map<unknown, unknown> }>;
  socket: { pendingBytes?: () => number };
}>();

function connSample() {
  const rows = [];
  for (const c of profiledConns) {
    if (c.closed) continue;
    let unacked = 0, consumers = 0;
    for (const ch of c.channels.values()) { unacked += ch.deliveries.size; consumers += ch.consumers.size; }
    rows.push({
      out: c.outboundBytes, staged: c.stagedBytes, queued: c.writeQueuedBytes, pending: c.socket.pendingBytes?.() ?? 0,
      blocked: c.outputBlocked, paused: c.readsPaused, running: c.inboundRunning, waiting: c.wakeInbound !== null,
      deferred: c.deferredPublish.length, inFlight: c.inFlightPublishes.size, publishBytes: c.publishBytes,
      fresh: c.fresh.length, wakers: c.outputWakers.size, consumers, unacked,
    });
  }
  return rows;
}

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
    conns: connSample(),
  };
}

export function startProfile(broker: Broker) {
  if (!profiling) return;
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

import { expect, test } from "bun:test";
import { RATE_HISTORY_MS, useLiveStore } from "../stores/live";
const queues = { items: [] };
const sample = (stats: Record<string, number>, at: number) => useLiveStore.getState().applySample({ object_totals: { connections: 0 }, message_stats: stats }, queues, null, at);

test("throughput history preserves missing counters, resets and stale gaps; broker switches clear it", () => {
  const initial = useLiveStore.getState();
  try {
    useLiveStore.getState().reset("test");
    sample({ publish: 10, deliver: 10, ack: 10 }, 1000);
    sample({ publish: 30, deliver: 20, ack: 20 }, 3000);
    expect(useLiveStore.getState().rates.publish).toBe(10);
    sample({ publish: 40, ack: 30 }, 5000);
    expect(useLiveStore.getState().rates.deliver).toBeNull();
    sample({ publish: 2, deliver: 40, ack: 40 }, 7000);
    expect(useLiveStore.getState().rates.publish).toBeNull();
    useLiveStore.getState().markStale("offline");
    sample({ publish: 20, deliver: 50, ack: 50 }, 9000);
    expect(useLiveStore.getState().rates.publish).toBeNull();
    sample({ publish: 30, deliver: 60, ack: 60 }, RATE_HISTORY_MS + 10_000);
    expect(useLiveStore.getState().history).toHaveLength(1);
    useLiveStore.getState().reset("next"); expect(useLiveStore.getState().history).toEqual([]);
  } finally { useLiveStore.setState(initial); }
});

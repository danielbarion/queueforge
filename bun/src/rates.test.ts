import { describe, expect, test } from "bun:test";
import { chartScaleSplit, chartSeries, formatDashboard, sampleFromMetrics, trafficRates, type TrafficSample } from "../../rust/ui/src/rates.ts";

const prev: TrafficSample = {
  publish: 100,
  deliver: 80,
  ack: 70,
  confirmed: 90,
  ready: 4,
  unacked: 1,
  consumers: 2,
  confirmBeforeFsync: 10,
  fsync: 3,
};

const next: TrafficSample = {
  publish: 150,
  deliver: 110,
  ack: 100,
  confirmed: 130,
  ready: 9,
  unacked: 3,
  consumers: 4,
  confirmBeforeFsync: 16,
  fsync: 4,
};

describe("trafficRates", () => {
  test("rates are the counter differences over the elapsed interval", () => {
    const rates = trafficRates(prev, next, 2);
    expect(rates.publishPerSec).toBe(25);
    expect(rates.deliverPerSec).toBe(15);
    expect(rates.ackPerSec).toBe(15);
    expect(rates.confirmPerSec).toBe(20);
    expect(rates.ready).toBe(9);
    expect(rates.unacked).toBe(3);
    expect(rates.consumers).toBe(4);
    expect(rates.confirmsBeforeFsync).toBe(true);
  });
});

describe("chartSeries", () => {
  test("keeps successive sample rates and the later depth", () => {
    const points = chartSeries([
      { at: 0, sample: prev },
      { at: 2000, sample: next },
    ]);
    expect(points).toHaveLength(1);
    expect(points[0]?.publishPerSec).toBe(25);
    expect(points[0]?.deliverPerSec).toBe(15);
    expect(points[0]?.ackPerSec).toBe(15);
    expect(points[0]?.ready).toBe(9);
    expect(points[0]?.unacked).toBe(3);
  });
});

describe("chartScaleSplit", () => {
  test("rate scale ignores depth and depth scale ignores rates", () => {
    const base = { ...prev, publish: 0, deliver: 0, ack: 0, ready: 4, unacked: 1 };
    const deep = chartSeries([
      { at: 0, sample: base },
      { at: 1000, sample: { ...base, publish: 25, deliver: 10, ack: 8, ready: 1_000_000, unacked: 40_000 } },
    ]);
    const deepSplit = chartScaleSplit(deep);
    expect(deepSplit.series.map((series) => series.name)).toEqual(["publish", "deliver", "ack", "ready", "unacked"]);
    expect(deepSplit.series.filter((series) => series.scale === "rate").map((series) => series.name)).toEqual(["publish", "deliver", "ack"]);
    expect(deepSplit.series.filter((series) => series.scale === "depth").map((series) => series.name)).toEqual(["ready", "unacked"]);
    expect(deepSplit.rateMax).toBe(25);
    expect(deepSplit.depthMax).toBe(1_000_000);

    const loud = chartSeries([
      { at: 0, sample: { ...base, ready: 2, unacked: 1 } },
      { at: 1000, sample: { ...base, publish: 500_000, deliver: 12, ack: 9, ready: 8, unacked: 3 } },
    ]);
    const loudSplit = chartScaleSplit(loud);
    expect(loudSplit.rateMax).toBe(500_000);
    expect(loudSplit.depthMax).toBe(8);
  });
});

describe("sampleFromMetrics", () => {
  test("sums labeled queue gauges and global counters", () => {
    const text = [
      "rabbitmq_global_messages_received_total 150",
      "rabbitmq_global_messages_delivered_total 110",
      "rabbitmq_global_messages_acknowledged_total 100",
      "rabbitmq_global_messages_confirmed_total 130",
      'rabbitmq_queue_messages_ready{vhost="/",queue="a"} 4',
      'rabbitmq_queue_messages_ready{vhost="/",queue="b"} 5',
      'rabbitmq_queue_messages_unacked{vhost="/",queue="a"} 3',
      "rabbitmq_consumers 4",
      "queueforge_confirm_before_fsync_total 16",
      "queueforge_wal_fsync_seconds_count 4",
    ].join("\n");
    expect(sampleFromMetrics(text)).toEqual(next);
    const view = formatDashboard("http://127.0.0.1:36673", prev, next, 2);
    expect(view).toContain("publish_rate=25.00");
    expect(view).toContain("confirm_rate=20.00");
    expect(view).toContain("queue_depth=12");
    expect(view).toContain("consumers=4");
    expect(view).toContain("confirms_before_fsync=yes");
    expect(view).toContain("sample_prev_publish=100 sample_next_publish=150");
  });
});

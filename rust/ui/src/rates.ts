/** Cumulative counters sampled from one broker. */
export type TrafficSample = {
  publish: number;
  deliver: number;
  ack: number;
  confirmed: number;
  ready: number;
  unacked: number;
  consumers: number;
  confirmBeforeFsync: number;
  fsync: number;
};

/** Rates are the later sample minus the earlier one, divided by elapsed seconds. */
export type TrafficRates = {
  publishPerSec: number;
  deliverPerSec: number;
  ackPerSec: number;
  confirmPerSec: number;
  ready: number;
  unacked: number;
  consumers: number;
  confirmsBeforeFsync: boolean;
};

export function trafficRates(prev: TrafficSample, next: TrafficSample, elapsedSec: number): TrafficRates {
  const dt = elapsedSec > 0 ? elapsedSec : 1;
  const perSec = (before: number, after: number) => Math.max(0, after - before) / dt;
  const confirmBefore = Math.max(0, next.confirmBeforeFsync - prev.confirmBeforeFsync);
  const fsync = Math.max(0, next.fsync - prev.fsync);
  return {
    publishPerSec: perSec(prev.publish, next.publish),
    deliverPerSec: perSec(prev.deliver, next.deliver),
    ackPerSec: perSec(prev.ack, next.ack),
    confirmPerSec: perSec(prev.confirmed, next.confirmed),
    ready: next.ready,
    unacked: next.unacked,
    consumers: next.consumers,
    confirmsBeforeFsync: confirmBefore >= fsync,
  };
}

/** Sum a Prometheus counter or labeled gauge family. */
export function promSum(text: string, name: string): number {
  let total = 0;
  for (const line of text.split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const match = line.match(/^([a-zA-Z0-9_:]+)(\{[^}]*\})?\s+(-?\d+(?:\.\d+)?(?:e[+-]?\d+)?)/);
    if (!match || match[1] !== name) continue;
    total += Number(match[3]);
  }
  return total;
}

export function sampleFromMetrics(text: string): TrafficSample {
  return {
    publish: promSum(text, "rabbitmq_global_messages_received_total"),
    deliver: promSum(text, "rabbitmq_global_messages_delivered_total"),
    ack: promSum(text, "rabbitmq_global_messages_acknowledged_total"),
    confirmed: promSum(text, "rabbitmq_global_messages_confirmed_total"),
    ready: promSum(text, "rabbitmq_queue_messages_ready"),
    unacked: promSum(text, "rabbitmq_queue_messages_unacked"),
    consumers: promSum(text, "rabbitmq_consumers"),
    confirmBeforeFsync: promSum(text, "queueforge_confirm_before_fsync_total"),
    fsync: promSum(text, "queueforge_wal_fsync_seconds_count"),
  };
}

export type ChartPoint = {
  t: number;
  publishPerSec: number;
  deliverPerSec: number;
  ackPerSec: number;
  ready: number;
  unacked: number;
};

export const CHART_SERIES = ["publish", "deliver", "ack", "ready", "unacked"] as const;

/** Series drawn on the live chart. Rate series never share a scale with depth. */
export type ChartSeriesName = (typeof CHART_SERIES)[number];

export type ChartScaleName = "rate" | "depth";

/** Rate ceiling from publish, deliver, and ack. Depth ceiling from ready and unacked. */
export type ChartScaleSplit = {
  rateMax: number;
  depthMax: number;
  series: { name: ChartSeriesName; scale: ChartScaleName }[];
};

const SCALE_BY_SERIES: Record<ChartSeriesName, ChartScaleName> = {
  publish: "rate",
  deliver: "rate",
  ack: "rate",
  ready: "depth",
  unacked: "depth",
};

function seriesValue(point: ChartPoint, name: ChartSeriesName): number {
  switch (name) {
    case "publish":
      return point.publishPerSec;
    case "deliver":
      return point.deliverPerSec;
    case "ack":
      return point.ackPerSec;
    case "ready":
      return point.ready;
    case "unacked":
      return point.unacked;
  }
}

/**
 * Split the five series onto two scales.
 * A large ready or unacked count cannot raise the rate ceiling, and a large
 * per-second rate cannot raise the depth ceiling.
 */
export function chartScaleSplit(points: ChartPoint[]): ChartScaleSplit {
  let rateMax = 1;
  let depthMax = 1;
  for (const point of points) {
    for (const name of CHART_SERIES) {
      const value = seriesValue(point, name);
      if (SCALE_BY_SERIES[name] === "rate") rateMax = Math.max(rateMax, value);
      else depthMax = Math.max(depthMax, value);
    }
  }
  return {
    rateMax,
    depthMax,
    series: CHART_SERIES.map((name) => ({ name, scale: SCALE_BY_SERIES[name] })),
  };
}

/** Seconds from the first chart point to the last. Zero until two samples exist. */
export function chartWindowSec(points: ChartPoint[]): number {
  if (points.length < 2) return 0;
  const first = points[0]!.t;
  const last = points[points.length - 1]!.t;
  return Math.max(0, (last - first) / 1000);
}

/** One point per pair of successive samples. Depth comes from the later sample. */
export function chartSeries(samples: { at: number; sample: TrafficSample }[]): ChartPoint[] {
  const points: ChartPoint[] = [];
  for (let i = 1; i < samples.length; i++) {
    const prev = samples[i - 1]!;
    const next = samples[i]!;
    const rates = trafficRates(prev.sample, next.sample, (next.at - prev.at) / 1000);
    points.push({
      t: next.at,
      publishPerSec: rates.publishPerSec,
      deliverPerSec: rates.deliverPerSec,
      ackPerSec: rates.ackPerSec,
      ready: rates.ready,
      unacked: rates.unacked,
    });
  }
  return points;
}

export function formatDashboard(url: string, prev: TrafficSample, next: TrafficSample, elapsedSec: number): string {
  const rates = trafficRates(prev, next, elapsedSec);
  const depth = rates.ready + rates.unacked;
  return [
    `queueforge dashboard ${url}`,
    `elapsed_sec=${elapsedSec.toFixed(3)}`,
    `publish_rate=${rates.publishPerSec.toFixed(2)}`,
    `deliver_rate=${rates.deliverPerSec.toFixed(2)}`,
    `ack_rate=${rates.ackPerSec.toFixed(2)}`,
    `confirm_rate=${rates.confirmPerSec.toFixed(2)}`,
    `queue_depth=${depth}`,
    `ready=${rates.ready}`,
    `unacked=${rates.unacked}`,
    `consumers=${rates.consumers}`,
    `confirms_before_fsync=${rates.confirmsBeforeFsync ? "yes" : "no"}`,
    `sample_prev_publish=${prev.publish} sample_next_publish=${next.publish}`,
  ].join("\n");
}

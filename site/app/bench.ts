export const GITHUB = "https://github.com/danielbarion/queueforge";
export const BENCH_MD = `${GITHUB}/blob/main/BENCHMARK.md`;

export type Tone = "mq" | "rust" | "bun" | "php";

export type Bar = {
  name: string;
  rate: number;
  tone: Tone;
  note?: string;
};

export { MEASURED, paced, scale, loadRows, scenarioRows } from "./bench-data";

export function formatRate(rate: number) {
  return rate.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

export function formatTimes(rate: number, base: number) {
  return (rate / base).toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function fixed(value: number, digits: number) {
  return value.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

/** A size in MiB, in GiB from 1 GiB up: "298.6 MiB", "5.55 GiB". */
export function formatMemory(mib: number) {
  return mib >= 1024 ? `${fixed(mib / 1024, 2)} GiB` : `${fixed(mib, 1)} MiB`;
}

/** A size as the harness writes it ("454.7 MiB", "3.579 GiB", or bare MiB), in MiB. Null when it is not one. */
export function parseMemory(text: string): number | null {
  const m = /^([\d.,]+)\s*(MiB|GiB)?$/.exec(text.trim());
  if (!m) return null;
  const value = Number(m[1]!.replace(/,/g, ""));
  if (!Number.isFinite(value)) return null;
  return m[2] === "GiB" ? value * 1024 : value;
}

/** A scenario score with its unit: "43,341 msg/s", "778.0 MB/s", "p99 1.73 ms". */
export function formatScore(kind: "rate" | "bytes" | "latency", text: string) {
  if (kind === "latency") return `p99 ${text} ms`;
  return `${text} ${kind === "bytes" ? "MB/s" : "msg/s"}`;
}

export type RateKey = "one" | "shared" | "spread";

/**
 * Why a cell is not a like-for-like rate. `failed` cells delivered nothing.
 * `split` cells were measured, but each PHP process kept its own copy of the
 * queue (QUEUEFORGE_LOCAL=1), so "one shared queue" was one queue per process.
 */
export type Caveat = "failed" | "split";

export type LoadRow = {
  app: string;
  size: string;
  one: number | null;
  shared: number;
  spread: number;
  caveats?: Partial<Record<RateKey, Caveat>>;
  connections: string;
  mib: string;
  login: string;
};

/** One PerfTest scenario at one container size. Values are per tone; null did not run cleanly. */
export type ScenarioRow = {
  size: string;
  id: string;
  title: string;
  real: string;
  /** rate and bytes: higher is better. latency: p99 ms at the offered rate, lower is better. */
  kind: "rate" | "bytes" | "latency";
  values: Record<Tone, number | null>;
  text: Record<Tone, string>;
  /** Sampled container memory peak in MiB, page cache included. */
  memory: Record<Tone, number | null>;
};

const AVERAGE_BROKERS: { tone: Tone; name: string }[] = [
  { tone: "mq", name: "RabbitMQ" },
  { tone: "rust", name: "Rust" },
  { tone: "bun", name: "Bun" },
  { tone: "php", name: "PHP" },
];

export type ScenarioAverage = {
  tone: Tone;
  name: string;
  /** Geometric mean of this broker ÷ RabbitMQ. Latency is inverted. RabbitMQ is 1. */
  mean: number;
  /** Scenarios that contributed. A missing cell is not a zero. */
  used: number;
  /** Scenarios where RabbitMQ has a number, so a ratio was possible. */
  possible: number;
  /** Container sizes that contributed, in table order. */
  sizes: string[];
};

/**
 * Average relative performance across PerfTest scenarios.
 * Rate and MB/s use broker ÷ RabbitMQ. Latency uses RabbitMQ ÷ broker.
 * A cell is included only when both sides have a number.
 */
export function scenarioAverages(rows: ScenarioRow[]): ScenarioAverage[] {
  const baseline = rows.filter((row) => row.values.mq != null);
  return AVERAGE_BROKERS.map(({ tone, name }) => {
    if (tone === "mq") {
      return { tone, name, mean: 1, used: baseline.length, possible: baseline.length, sizes: [...new Set(baseline.map((row) => row.size))] };
    }
    const hits = baseline.flatMap((row) => {
      const base = row.values.mq;
      const value = row.values[tone];
      if (base == null || value == null || base <= 0 || value <= 0) return [];
      return [{ size: row.size, ratio: row.kind === "latency" ? base / value : value / base }];
    });
    const mean = hits.length === 0 ? 0 : Math.exp(hits.reduce((sum, hit) => sum + Math.log(hit.ratio), 0) / hits.length);
    return { tone, name, mean, used: hits.length, possible: baseline.length, sizes: [...new Set(hits.map((hit) => hit.size))] };
  });
}

export const GITHUB = "https://github.com/danielbarion/queueforge";
export const BENCH_MD = `${GITHUB}/blob/main/BENCHMARK.md`;

export type Tone = "mq" | "rust" | "bun" | "php";

export type Bar = {
  name: string;
  rate: number;
  tone: Tone;
};

export { paced, scale, loadRows, scenarioRows } from "./bench-data";

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
};

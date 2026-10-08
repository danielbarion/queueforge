export const GITHUB = "https://github.com/danielbarion/queueforge";
export const BENCH_MD = `${GITHUB}/blob/main/BENCHMARK.md`;

export type Tone = "mq" | "rust" | "bun" | "php";

export type Bar = {
  name: string;
  rate: number;
  tone: Tone;
};

export const paced: Bar[] = [
  { name: "RabbitMQ", rate: 10110.95, tone: "mq" },
  { name: "Rust", rate: 20377.46, tone: "rust" },
  { name: "Bun", rate: 17926.84, tone: "bun" },
  { name: "PHP", rate: 6579.89, tone: "php" },
];

export const scale: Bar[] = [
  { name: "RabbitMQ", rate: 163281.1, tone: "mq" },
  { name: "Rust", rate: 417402.2, tone: "rust" },
  { name: "Bun", rate: 519568.0, tone: "bun" },
  { name: "PHP", rate: 379881.0, tone: "php" },
];

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

export const loadRows: LoadRow[] = [
  { app: "RabbitMQ", size: "1 CPU / 512 MiB", one: 54413.9, shared: 41963.4, spread: 43022.9, connections: "3,500", mib: "183", login: "477.8 MiB" },
  { app: "Rust", size: "1 CPU / 512 MiB", one: 165856.0, shared: 64029.5, spread: 150311.5, connections: "8,980", mib: "363", login: "492.7 MiB" },
  { app: "Bun", size: "1 CPU / 512 MiB", one: 219556.0, shared: 161088.0, spread: 152448.0, connections: "43,828", mib: "145", login: "442.3 MiB" },
  { app: "PHP", size: "1 CPU / 512 MiB", one: 10704.0, shared: 84384.0, spread: 100537.0, connections: "1,000", mib: "208", login: "18.75 MiB" },
  { app: "RabbitMQ", size: "1 CPU / 1 GiB", one: 56560.0, shared: 40483.1, spread: 41014.6, connections: "n/a", mib: "194", login: "n/a" },
  { app: "Rust", size: "1 CPU / 1 GiB", one: 163104.0, shared: 64015.2, spread: 153594.0, connections: "n/a", mib: "367", login: "n/a" },
  { app: "Bun", size: "1 CPU / 1 GiB", one: 229916.0, shared: 181136.0, spread: 193992.0, connections: "n/a", mib: "153", login: "n/a" },
  { app: "PHP", size: "1 CPU / 1 GiB", one: 10720.0, shared: 92092.0, spread: 105243.0, connections: "n/a", mib: "245", login: "n/a" },
  { app: "RabbitMQ", size: "2 CPU / 2 GiB", one: 71453.1, shared: 73688.0, spread: 81863.2, connections: "18,750", mib: "245", login: "1.775 GiB" },
  { app: "Rust", size: "2 CPU / 2 GiB", one: 201546.6, shared: 95089.6, spread: 269444.0, connections: "34,600", mib: "565", login: "1.838 GiB" },
  { app: "Bun", size: "2 CPU / 2 GiB", one: 183544.0, shared: 147680.0, spread: 313816.0, connections: "121,284", mib: "251", login: "1.467 GiB" },
  { app: "PHP", size: "2 CPU / 2 GiB", one: 35008.0, shared: 92092.1, spread: 206664.0, connections: "n/a", mib: "140", login: "n/a" },
  { app: "RabbitMQ", size: "4 CPU / 4 GiB", one: 78450.9, shared: 67505.4, spread: 163281.1, connections: "38,217", mib: "252", login: "3.884 GiB" },
  { app: "Rust", size: "4 CPU / 4 GiB", one: 201456.8, shared: 110926.5, spread: 417402.2, connections: "68,467", mib: "793", login: "3.636 GiB" },
  { app: "Bun", size: "4 CPU / 4 GiB", one: 173436.0, shared: 166752.0, spread: 519568.0, connections: "251,968", mib: "384", login: "2.934 GiB" },
  { app: "PHP", size: "4 CPU / 4 GiB", one: 33072.0, shared: 92640.8, spread: 379881.0, connections: "n/a", mib: "318", login: "n/a" },
  { app: "RabbitMQ", size: "4 CPU / 8 GiB", one: 80848.9, shared: 75887.1, spread: 162923.1, connections: "65,449", mib: "250", login: "6.34 GiB" },
  { app: "Rust", size: "4 CPU / 8 GiB", one: 200998.9, shared: 118865.5, spread: 410258.1, connections: "95,000", mib: "810", login: "5.025 GiB" },
  { app: "Bun", size: "4 CPU / 8 GiB", one: 196044.0, shared: 176320.0, spread: 501680.0, connections: "260,000", mib: "398", login: "3.217 GiB" },
  { app: "PHP", size: "4 CPU / 8 GiB", one: 33767.0, shared: 86273.5, spread: 380481.8, connections: "n/a", mib: "321", login: "n/a" },
];

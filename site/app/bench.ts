export const GITHUB = "https://github.com/danielbarion/queueforge";
export const BENCH_MD = `${GITHUB}/blob/main/BENCHMARK.md`;

export type Tone = "mq" | "rust" | "bun" | "php";

export type Bar = {
  name: string;
  rate: number;
  tone: Tone;
};

export const paced: Bar[] = [
  { name: "RabbitMQ", rate: 13139.01, tone: "mq" },
  { name: "Rust", rate: 19230.1, tone: "rust" },
  { name: "Bun", rate: 18396.86, tone: "bun" },
];

export const scale: Bar[] = [
  { name: "RabbitMQ", rate: 159453.6, tone: "mq" },
  { name: "Rust", rate: 339656.9, tone: "rust" },
  { name: "Bun", rate: 169120, tone: "bun" },
  { name: "PHP", rate: 67594.8, tone: "php" },
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

export type LoadRow = {
  app: string;
  size: string;
  one: number;
  shared: number;
  spread: number;
  connections: string;
  mib: string;
  login: string;
};

export const loadRows: LoadRow[] = [
  { app: "RabbitMQ", size: "1 CPU / 512 MiB", one: 51505.4, shared: 37190.1, spread: 40603.8, connections: "3,500", mib: "187", login: "503.3 MiB" },
  { app: "Rust", size: "1 CPU / 512 MiB", one: 141278.2, shared: 60154.2, spread: 135840.6, connections: "9,191", mib: "348", login: "468.1 MiB" },
  { app: "Bun", size: "1 CPU / 512 MiB", one: 140988.0, shared: 118944.0, spread: 148240.0, connections: "43,184", mib: "384", login: "422.9 MiB" },
  { app: "PHP", size: "1 CPU / 512 MiB", one: 9968.0, shared: 61326.2, spread: 64600.5, connections: "n/a", mib: "115", login: "n/a" },
  { app: "RabbitMQ", size: "1 CPU / 1 GiB", one: 53602.4, shared: 39660.9, spread: 41877.0, connections: "n/a", mib: "190", login: "n/a" },
  { app: "Rust", size: "1 CPU / 1 GiB", one: 161930.6, shared: 62948.4, spread: 144892.1, connections: "n/a", mib: "344", login: "n/a" },
  { app: "Bun", size: "1 CPU / 1 GiB", one: 135216.0, shared: 120592.5, spread: 147968.0, connections: "n/a", mib: "447", login: "n/a" },
  { app: "PHP", size: "1 CPU / 1 GiB", one: 10000.0, shared: 66571.1, spread: 66739.4, connections: "n/a", mib: "125", login: "n/a" },
  { app: "RabbitMQ", size: "2 CPU / 2 GiB", one: 71411.1, shared: 67480.6, spread: 86316.0, connections: "18,750", mib: "216", login: "1.726 GiB" },
  { app: "Rust", size: "2 CPU / 2 GiB", one: 187547.9, shared: 78149.5, spread: 249498.5, connections: "37,300", mib: "469", login: "1.837 GiB" },
  { app: "Bun", size: "2 CPU / 2 GiB", one: 165656.0, shared: 131344.0, spread: 165440.0, connections: "170,100", mib: "491", login: "1.571 GiB" },
  { app: "PHP", size: "2 CPU / 2 GiB", one: 9824.0, shared: 66820.4, spread: 68165.4, connections: "n/a", mib: "122", login: "n/a" },
  { app: "RabbitMQ", size: "4 CPU / 4 GiB", one: 78982.8, shared: 67820.9, spread: 159453.6, connections: "37,862", mib: "262", login: "3.551 GiB" },
  { app: "Rust", size: "4 CPU / 4 GiB", one: 188426.8, shared: 110058.8, spread: 339656.9, connections: "73,714", mib: "700", login: "3.629 GiB" },
  { app: "Bun", size: "4 CPU / 4 GiB", one: 170184.0, shared: 136616.0, spread: 169120.0, connections: "160,000", mib: "505", login: "1.473 GiB" },
  { app: "PHP", size: "4 CPU / 4 GiB", one: 9744.0, shared: 67678.0, spread: 67594.8, connections: "n/a", mib: "126", login: "n/a" },
  { app: "RabbitMQ", size: "4 CPU / 8 GiB", one: 77459.4, shared: 71467.6, spread: 151569.8, connections: "61,562", mib: "240", login: "5.292 GiB" },
  { app: "Rust", size: "4 CPU / 8 GiB", one: 193370.8, shared: 120737.6, spread: 344963.6, connections: "95,000", mib: "682", login: "4.663 GiB" },
  { app: "Bun", size: "4 CPU / 8 GiB", one: 170488.0, shared: 140456.0, spread: 174560.0, connections: "160,000", mib: "535", login: "1.476 GiB" },
  { app: "PHP", size: "4 CPU / 8 GiB", one: 9776.0, shared: 65964.0, spread: 67545.8, connections: "n/a", mib: "125", login: "n/a" },
];

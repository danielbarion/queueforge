import { formatDashboard, sampleFromMetrics } from "../rust/ui/src/rates.ts";

const url = process.argv[2];
const samples = Number(process.argv.find((arg) => arg.startsWith("--samples="))?.slice("--samples=".length) ?? "2");
if (!url) {
  console.error("usage: queueforge-dashboard <management-url> [--samples=2]");
  process.exit(2);
}

const intervalMs = 1000;
let prev = sampleFromMetrics(await (await fetch(new URL("/metrics", url))).text());
let prevAt = Date.now();
for (let i = 1; i < samples; i++) {
  await new Promise((resolve) => setTimeout(resolve, intervalMs));
  const next = sampleFromMetrics(await (await fetch(new URL("/metrics", url))).text());
  const now = Date.now();
  const elapsed = Math.max(0.001, (now - prevAt) / 1000);
  console.log(formatDashboard(url, prev, next, elapsed));
  console.log("");
  prev = next;
  prevAt = now;
}

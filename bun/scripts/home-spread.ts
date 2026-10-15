import { queueHome } from "../src/broker/routing.ts";

// Queues per child for the scaling `weak` load: 2 loadgen processes per CPU, 2 queues each.
for (const n of [1, 2, 3, 4, 5, 6, 8, 16, 32, 64]) {
  const members = Array.from({ length: n }, (_, i) => ({ id: `n${i}` }));
  const counts = new Map<string, number>();
  const procs = 2 * n;
  for (let p = 0; p < procs; p++) for (let q = 0; q < 2; q++) {
    const home = queueHome("/", `p${p}-q${q}`, members)!;
    counts.set(home, (counts.get(home) ?? 0) + 1);
  }
  const loads = members.map((m) => counts.get(m.id) ?? 0);
  const total = 4 * n;
  const max = Math.max(...loads);
  console.log(`N=${n} queues=${total} per child=[${loads.join(",")}] busiest=${max} cap=${(total / (n * max)).toFixed(2)}`);
}

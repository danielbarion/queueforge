/** Counters each child keeps for itself, summed across children. */
const SUMMED = [
  "queueforge_confirm_before_fsync_total",
  "queueforge_wal_fsync_seconds_count",
  "queueforge_wal_fsync_seconds_sum",
];

/** Join child Prometheus pages. Only the per-process fsync counters are summed. */
export function mergeChildMetrics(bodies: string[]): string {
  let merged = bodies[0] ?? "";
  for (const name of SUMMED) {
    let sum = 0;
    let seen = false;
    for (const body of bodies) {
      const line = body.split("\n").find((row) => row.startsWith(name + " "));
      if (!line) continue;
      seen = true;
      sum += Number(line.split(" ")[1]) || 0;
    }
    if (!seen && name !== SUMMED[0]) continue;
    const pattern = new RegExp(`^${name} .*`, "m");
    if (pattern.test(merged)) merged = merged.replace(pattern, `${name} ${sum}`);
    else merged += `${merged && !merged.endsWith("\n") ? "\n" : ""}${name} ${sum}\n`;
  }
  return merged;
}

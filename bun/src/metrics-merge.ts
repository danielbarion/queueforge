/** Join child Prometheus pages. Only the confirm-before-fsync counter is summed. */
export function mergeChildMetrics(bodies: string[]): string {
  let sum = 0;
  for (const body of bodies) {
    const line = body.split("\n").find((row) => row.startsWith("queueforge_confirm_before_fsync_total "));
    if (line) sum += Number(line.split(" ")[1]) || 0;
  }
  const base = bodies[0] ?? "";
  const replaced = base.replace(
    /^queueforge_confirm_before_fsync_total .*/m,
    `queueforge_confirm_before_fsync_total ${sum}`,
  );
  return replaced || `queueforge_confirm_before_fsync_total ${sum}\n`;
}

import { describe, expect, test } from "bun:test";
import { mergeChildMetrics } from "../src/metrics-merge.ts";

describe("child metrics", () => {
  test("adds the confirm-before-fsync counter and keeps the first child's other lines", () => {
    const first = [
      "# TYPE queueforge_messages_published_total counter",
      "queueforge_messages_published_total 10",
      "queueforge_confirm_before_fsync_total 1",
    ].join("\n");
    const second = [
      "queueforge_messages_published_total 90",
      "queueforge_confirm_before_fsync_total 4",
    ].join("\n");
    const merged = mergeChildMetrics([first, second]);
    expect(merged).toContain("queueforge_messages_published_total 10");
    expect(merged).not.toContain("queueforge_messages_published_total 90");
    expect(merged).toContain("queueforge_confirm_before_fsync_total 5");
  });

  test("reports zero when no child answered", () => {
    expect(mergeChildMetrics(["", ""])).toBe("queueforge_confirm_before_fsync_total 0\n");
  });
});

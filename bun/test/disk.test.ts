import { describe, expect, test } from "bun:test";
import { availableBytes } from "../src/disk.ts";
import { mergeChildMetrics } from "../src/metrics-merge.ts";

describe("available bytes", () => {
  test("a real directory is not the placeholder zero", () => {
    expect(availableBytes(".")).toBeGreaterThan(0);
  });

  test("a missing path is a failed probe", () => {
    expect(availableBytes("/no/such/queueforge/path")).toBe(0);
  });
});

describe("child metrics", () => {
  test("does not add free disk across children", () => {
    const first = "rabbitmq_disk_space_available_bytes 100\nqueueforge_confirm_before_fsync_total 1\n";
    const second = "rabbitmq_disk_space_available_bytes 250\nqueueforge_confirm_before_fsync_total 4\n";
    const merged = mergeChildMetrics([first, second]);
    expect(merged).toContain("rabbitmq_disk_space_available_bytes 100");
    expect(merged).not.toContain("rabbitmq_disk_space_available_bytes 250");
    expect(merged).not.toContain("rabbitmq_disk_space_available_bytes 350");
  });
});

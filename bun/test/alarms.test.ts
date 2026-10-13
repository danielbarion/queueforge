import { describe, expect, test } from "bun:test";
import { cgroupAnon, cgroupBytes, memoryLimit } from "../src/broker/alarms.ts";

const GiB = 1024 ** 3;

describe("memory watermark", () => {
  test("reads a cgroup byte limit and ignores max", () => {
    expect(cgroupBytes("1073741824\n")).toBe(GiB);
    expect(cgroupBytes("max\n")).toBeNull();
    expect(cgroupBytes("")).toBeNull();
  });

  test("reads the anon line of memory.stat", () => {
    expect(cgroupAnon("anon 123\nfile 456\nanon_thp 0\n")).toBe(123);
    expect(cgroupAnon("file 456\n")).toBeNull();
  });

  test("a container limit sets the watermark for the whole container", () => {
    expect(memoryLimit(undefined, "1073741824", true, 24 * GiB, 4)).toEqual({ bytes: 0.6 * GiB, container: true });
    expect(memoryLimit("0.4", "1073741824", true, 24 * GiB, 4)).toEqual({ bytes: 0.4 * GiB, container: true });
  });

  test("without a cgroup limit each process gets its share of RAM", () => {
    expect(memoryLimit(undefined, "max", true, 8 * GiB, 4)).toEqual({ bytes: 1.2 * GiB, container: false });
    expect(memoryLimit(undefined, null, false, 8 * GiB, 1)).toEqual({ bytes: 0.6 * 8 * GiB, container: false });
  });

  test("a limit without a readable memory.stat falls back to per-process RSS", () => {
    expect(memoryLimit(undefined, "1073741824", false, 24 * GiB, 4)).toEqual({ bytes: 0.15 * GiB, container: false });
  });

  test("an absolute watermark is kept as given", () => {
    expect(memoryLimit("500000000", "1073741824", true, 24 * GiB, 4)).toEqual({ bytes: 500_000_000, container: true });
    expect(memoryLimit("500000000", null, false, 24 * GiB, 4)).toEqual({ bytes: 500_000_000, container: false });
  });
});

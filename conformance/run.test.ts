import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { main, parseJunit, runFailed } from "./run.ts";

const dirs: string[] = [];
afterEach(() => { for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true }); });
const xml = (body = '<testcase name="Routing :: mandatory"/>') => `<testsuites><testsuite>${body}</testsuite></testsuites>`;
const directory = () => { const dir = mkdtempSync(join(tmpdir(), "qf-run-test-")); dirs.push(dir); return dir; };
const noFixtures = async () => {};

describe("conformance release gate", () => {
  test("parses passing, failing, and skipped testcases", () => {
    expect(parseJunit(xml('<testcase name="pass"/><testcase name="fail"><failure/></testcase><testcase name="skip"><skipped/></testcase>')))
      .toEqual({ pass: "pass", fail: "fail", skip: "skip" });
  });
  test("rejects missing, empty, malformed, and suite-error results", () => {
    for (const input of ["", "garbage" + xml(), xml(""), '<testsuites><testsuite><testcase name="x"/></testsuites>', xml('<testcase name="x">'), '<testsuite errors="1"><testcase name="x"/></testsuite>', xml('<testcase/>')]) {
      expect(() => parseJunit(input)).toThrow();
    }
  });
  test("honors exit status even when all reported tests passed", () => {
    expect(runFailed("bun", { results: { test: "pass" }, exitCode: 1 })).toBe(true);
    expect(runFailed("bun", { results: { test: "fail" }, exitCode: 0 })).toBe(true);
    expect(runFailed("bun", { results: { test: "pass" }, exitCode: 0 })).toBe(false);
  });
  test("only excuses RabbitMQ's expected assertion failures", () => {
    const results = { "Delayed messages :: plugin": "fail" as const };
    expect(runFailed("rabbitmq", { results, exitCode: 1 })).toBe(false);
    expect(runFailed("bun", { results, exitCode: 1 })).toBe(true);
    expect(runFailed("rabbitmq", { results, exitCode: 2 })).toBe(true);
    expect(runFailed("rabbitmq", { results: { ...results, unexpected: "fail" }, exitCode: 1 })).toBe(true);
  });
  test("startup failure never restores stale target results", async () => {
    const resultsDir = directory();
    writeFileSync(join(resultsDir, "bun.xml"), xml());
    writeFileSync(join(resultsDir, "rust.xml"), xml());
    expect(await main(["bun"], { resultsDir, stopFixtures: noFixtures, runTarget: async () => { throw new Error("broker did not come up"); } })).toBe(1);
    expect(JSON.parse(readFileSync(join(resultsDir, "summary.json"), "utf8"))).toEqual({ Routing: { rust: "y" } });
  });
  test("new assertion failures propagate to the runner exit code", async () => {
    const resultsDir = directory();
    expect(await main(["bun"], { resultsDir, stopFixtures: noFixtures, runTarget: async () => ({ results: { "Routing :: mandatory": "fail" }, exitCode: 1 }) })).toBe(1);
  });
  test("filtered runs still fail without changing the historical matrix", async () => {
    const resultsDir = directory();
    writeFileSync(join(resultsDir, "bun.xml"), xml());
    expect(await main(["bun", "--filter=Routing"], { resultsDir, stopFixtures: noFixtures, runTarget: async (_, filter) => {
      expect(filter).toBe("Routing");
      return { results: { "Routing :: mandatory": "fail" }, exitCode: 1 };
    } })).toBe(1);
    expect(readFileSync(join(resultsDir, "bun.xml"), "utf8")).toBe(xml());
    expect(JSON.parse(readFileSync(join(resultsDir, "summary.json"), "utf8"))).toEqual({});
  });
  test("report mode prints historical failures without running brokers", async () => {
    const resultsDir = directory();
    writeFileSync(join(resultsDir, "bun.xml"), xml('<testcase name="Routing :: mandatory"><failure/></testcase>'));
    expect(await main(["--report"], { resultsDir, stopFixtures: async () => { throw new Error("must not start fixtures"); }, runTarget: async () => { throw new Error("must not start brokers"); } })).toBe(0);
  });
});

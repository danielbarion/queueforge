import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FastLog, type LogInsert, type LogOp } from "../src/fast-log.ts";

// One 3 MiB row per 4 MiB chunk, so every second write rotates.
const BODY = 3 * 1024 * 1024;

function row(id: number): LogInsert {
  return { id, vhost: "/", queue: "q", body: new Uint8Array(BODY).fill(id), meta: "{}" };
}

function replayed(path: string): number[] {
  const log = FastLog.open(path)!;
  const ops: LogOp[] = [];
  log.replay((op) => ops.push(op));
  log.close();
  for (const op of ops) if (op.kind === "insert") expect(op.body[BODY - 1]).toBe(op.id);
  return ops.map((op) => (op.kind === "insert" ? op.id : -op.id));
}

test("a rotation waits out a slow sync and reports its batch as synced", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-stall-sync-"));
  const path = join(dir, "t.sqlite");
  const log = FastLog.open(path)!;
  log.workerDelayMs = 300;
  // The warning falls between the worker's two syncs (~300 and ~600 ms).
  log.stallWarnMs = 450;
  const reports: Array<[bigint, boolean]> = [];
  log.onSynced = (seq, ok) => reports.push([seq, ok]);
  log.appendRows([row(1)], []);
  // The worker is inside its first sync, so the second batch needs another.
  await Bun.sleep(50);
  log.appendRows([{ ...row(2), body: new Uint8Array([2]) }], []);
  // Does not fit the first chunk: the rotation drains the worker. Its first
  // sync finishes during that wait, and its message cannot arrive until the
  // wait ends, so only the wait can report it.
  log.syncRows([row(3)], []);
  expect(log.workerStalls).toBe(1);
  expect(reports.length).toBeGreaterThan(0);
  expect(reports.every(([, ok]) => ok)).toBe(true);
  expect(log.canAppend).toBe(true);
  log.close();
  const ops = FastLog.open(path)!;
  const ids: number[] = [];
  ops.replay((op) => op.kind === "insert" && ids.push(op.id));
  ops.close();
  expect(ids).toEqual([1, 2, 3]);
  rmSync(dir, { recursive: true, force: true });
});

test("a rotation waits out a slow chunk preallocation", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-stall-prealloc-"));
  const path = join(dir, "t.sqlite");
  const log = FastLog.open(path)!;
  log.workerDelayMs = 300;
  log.stallWarnMs = 50;
  // Past the eight chunks a fresh log writes up front, the next one comes
  // from the prealloc worker.
  for (let id = 1; id <= 11; id++) log.syncRows([row(id)], []);
  expect(log.workerStalls).toBeGreaterThan(0);
  log.close();
  expect(replayed(path)).toEqual(Array.from({ length: 11 }, (_, k) => k + 1));
  rmSync(dir, { recursive: true, force: true });
});

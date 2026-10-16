import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CHUNK, FastLog, type LogInsert, type LogOp } from "../src/fast-log.ts";

// These count rotations and reuse of fixed-size chunks.
const grown = FastLog.defaultMaxChunk;
beforeAll(() => {
  FastLog.defaultMaxChunk = CHUNK;
});
afterAll(() => {
  FastLog.defaultMaxChunk = grown;
});

/** Body of row `id`: `len` bytes that encode the id, so a misplaced byte shows. */
function body(id: number, len: number): Uint8Array {
  const b = new Uint8Array(len);
  for (let k = 0; k < len; k++) b[k] = (id * 31 + k) & 0xff;
  return b;
}

test("staged, inline and grouped batches written by the sync worker replay in order", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-staged-"));
  const path = join(dir, "t.sqlite");
  const log = FastLog.open(path)!;
  let failed = 0;
  log.onSynced = (_seq, ok) => {
    if (!ok) failed++;
  };
  const expected: Array<{ kind: "insert"; id: number; len: number; queue: string } | { kind: "delete"; id: number }> = [];
  let id = 0;
  // Sizes chosen so batches do not divide the 8 MiB ring (it wraps and skips
  // its tail end), some exceed the 2 MiB staging limit (written on the main
  // thread), and chunks rotate through close jobs.
  const sizes = [10, 300_000, 1_100_000, 3_000_000, 77, 900_000, 2_500_000, 64 * 1024, 1_500_000];
  let previous: number[] = [];
  for (let round = 0; round < 60; round++) {
    const len = sizes[round % sizes.length]!;
    const rows: LogInsert[] = [];
    if (round % 4 === 3) {
      // A fanout: one body object, consecutive ids, one type 3 record.
      const shared = body(id + 1, len);
      for (let k = 0; k < 5; k++) {
        id++;
        rows.push({ id, vhost: "/", queue: `fan-${k}`, body: shared, meta: `{"r":${round}}` });
        expected.push({ kind: "insert", id, len, queue: `fan-${k}` });
      }
    } else {
      id++;
      rows.push({ id, vhost: "/", queue: "q", body: body(id, len), meta: `{"r":${round}}` });
      expected.push({ kind: "insert", id, len, queue: "q" });
    }
    for (const d of previous) expected.push({ kind: "delete", id: d });
    // Deletes come first in a batch's records.
    const batchStart = expected.length - rows.length - previous.length;
    const mine = expected.splice(batchStart, rows.length + previous.length);
    expected.push(...mine.filter((e) => e.kind === "delete"), ...mine.filter((e) => e.kind === "insert"));
    log.appendRows(rows, previous);
    // Keep every third batch: the rest is deleted by the next one.
    previous = round % 3 === 0 ? [] : rows.map((r) => r.id);
    if (round % 7 === 0) await Bun.sleep(1);
  }
  while (!log.settled) await Bun.sleep(1);
  expect(failed).toBe(0);
  expect(log.handOffs).toBeGreaterThan(5);
  log.close();

  // What is left on disk: inserts not deleted, each body byte intact.
  const again = FastLog.open(path)!;
  const ops: LogOp[] = [];
  again.replay((op) => ops.push(op));
  const live = new Map<number, Extract<LogOp, { kind: "insert" }>>();
  for (const op of ops) {
    if (op.kind === "insert") live.set(op.id, op);
    else live.delete(op.id);
  }
  const want = new Map<number, { len: number; queue: string }>();
  for (const e of expected) {
    if (e.kind === "insert") want.set(e.id, e);
    else want.delete(e.id);
  }
  expect([...live.keys()].sort((a, b) => a - b)).toEqual([...want.keys()].sort((a, b) => a - b));
  for (const [rowId, op] of live) {
    const w = want.get(rowId)!;
    expect(op.queue).toBe(w.queue);
    expect(op.body.length).toBe(w.len);
    // A fanout row carries the body of its group's first id.
    const first = w.queue.startsWith("fan-") ? rowId - Number(w.queue.slice(4)) : rowId;
    expect(Buffer.from(op.body).equals(Buffer.from(body(first, w.len)))).toBe(true);
  }
  // Rows read back from the log, as a paged-out body is.
  for (const rowId of live.keys()) expect(again.readRow(rowId)?.body.length).toBe(want.get(rowId)!.len);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("an inline sync after staged batches waits for them, so replay keeps every one", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-staged-inline-"));
  const path = join(dir, "t.sqlite");
  const log = FastLog.open(path)!;
  log.workerDelayMs = 50;
  for (let n = 1; n <= 5; n++) log.appendRows([{ id: n, vhost: "/", queue: "q", body: body(n, 1000), meta: "{}" }], []);
  // No wait: the worker still holds the staged batches when this one syncs here.
  log.syncRows([{ id: 6, vhost: "/", queue: "q", body: body(6, 1000), meta: "{}" }], []);
  expect(log.settled).toBe(true);
  log.close();
  const ids: number[] = [];
  const again = FastLog.open(path)!;
  again.replay((op) => op.kind === "insert" && ids.push(op.id));
  again.close();
  expect(ids).toEqual([1, 2, 3, 4, 5, 6]);
  rmSync(dir, { recursive: true, force: true });
});

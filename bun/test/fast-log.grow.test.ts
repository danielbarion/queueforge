import { expect, test } from "bun:test";
import { mkdtempSync, readdirSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CHUNK, FastLog, type LogOp } from "../src/fast-log.ts";

/**
 * Chunks double while they rotate faster than 250 ms apart, up to 32 MiB, so a
 * 1 GB/s stream rotates a few times a second instead of 250. Rows written
 * across chunks of different sizes replay intact, and a slow log shrinks again.
 */

function body(id: number, len: number): Uint8Array {
  const b = new Uint8Array(len);
  for (let k = 0; k < len; k++) b[k] = (id * 31 + k) & 0xff;
  return b;
}

test("chunks grow under a fast stream and replay intact across sizes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-grow-"));
  const path = join(dir, "t.sqlite");
  try {
    const log = FastLog.open(path)!;
    let failed = 0;
    log.onSynced = (_seq, ok) => {
      if (!ok) failed++;
    };
    const len = 256 * 1024;
    const kept = new Map<number, number>();
    let id = 0;
    let previous: number[] = [];
    // About 120 MiB, a few rows per batch; every fifth row stays live.
    for (let round = 0; round < 120; round++) {
      const rows = [];
      for (let k = 0; k < 4; k++) {
        id++;
        rows.push({ id, vhost: "/", queue: "q", body: body(id, len), meta: "{}" });
      }
      log.appendRows(rows, previous);
      previous = [];
      for (const r of rows) {
        if (r.id % 5 === 0) kept.set(r.id, len);
        else previous.push(r.id);
      }
      if (round % 8 === 0) await Bun.sleep(0);
    }
    log.appendRows([], previous);
    while (!log.settled) await Bun.sleep(1);
    expect(failed).toBe(0);
    expect(log.chunkSize).toBeGreaterThan(CHUNK);
    expect(log.chunkSize).toBeLessThanOrEqual(32 * 1024 * 1024);
    const sizes = new Set(
      readdirSync(`${path}.dlog`).filter((n) => /^\d+$/.test(n)).map((n) => statSync(join(`${path}.dlog`, n)).size),
    );
    expect(sizes.size).toBeGreaterThan(1);
    log.close();

    const again = FastLog.open(path)!;
    const ops: LogOp[] = [];
    again.replay((op) => ops.push(op));
    const live = new Map<number, Extract<LogOp, { kind: "insert" }>>();
    for (const op of ops) {
      if (op.kind === "insert") live.set(op.id, op);
      else if (op.kind === "delete") live.delete(op.id);
    }
    expect([...live.keys()].sort((a, b) => a - b)).toEqual([...kept.keys()].sort((a, b) => a - b));
    for (const [rowId, op] of live) expect(Buffer.compare(op.body, body(rowId, len))).toBe(0);
    again.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}, 60000);

test("a log that rotates slowly goes back to 4 MiB chunks", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-shrink-"));
  const path = join(dir, "t.sqlite");
  try {
    const log = FastLog.open(path)!;
    // As if a fast stream had grown it, then traffic slowed to one rotation
    // every few seconds.
    log.chunkSize = 16 * 1024 * 1024;
    const internals = log as unknown as { lastRotateAt: number };
    let id = 0;
    for (let n = 0; n < 3; n++) {
      internals.lastRotateAt = performance.now() - 5000;
      const before = log.chunkSize;
      // One batch larger than what is left of the active chunk forces a rotation.
      const rows = [{ id: ++id, vhost: "/", queue: "q", body: body(id, 20 * 1024 * 1024), meta: "{}" }];
      log.appendRows(rows, []);
      while (!log.settled) await Bun.sleep(1);
      expect(log.chunkSize).toBe(Math.max(CHUNK, before / 2));
    }
    log.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}, 60000);

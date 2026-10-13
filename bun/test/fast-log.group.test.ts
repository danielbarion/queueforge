import { expect, test } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, truncateSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FastLog, type LogInsert, type LogOp } from "../src/fast-log.ts";

const BODY = 4096;

function fanout(firstId: number, queues: number, fill: number): LogInsert[] {
  const body = new Uint8Array(BODY).fill(fill);
  const meta = '{"e":"fan"}';
  return Array.from({ length: queues }, (_, k) => ({ id: firstId + k, vhost: "/", queue: `q${k}`, body, meta }));
}

function replay(log: FastLog): LogOp[] {
  const ops: LogOp[] = [];
  log.replay((op) => ops.push(op));
  return ops;
}

function written(sqlitePath: string): number {
  const dir = `${sqlitePath}.dlog`;
  // Every record here fits the first chunk. A clean close stores its logical end at byte 8.
  const first = readFileSync(join(dir, readdirSync(dir).sort()[0]!));
  return Number(first.readBigUInt64LE(8));
}

test("the copies of one fanout share one record and replay as one insert each", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-group-"));
  const path = join(dir, "t.sqlite");
  const log = FastLog.open(path)!;
  log.syncRows(fanout(1, 20, 7), []);
  // Every member reads back from the shared record.
  for (let k = 0; k < 20; k++) {
    const row = log.readRow(1 + k)!;
    expect(row.queue).toBe(`q${k}`);
    expect(row.body.length).toBe(BODY);
    expect(row.body[0]).toBe(7);
  }
  expect(log.readRow(21)).toBeNull();
  log.close();
  const again = FastLog.open(path)!;
  const ops = replay(again);
  expect(ops.map((op) => (op.kind === "insert" ? `${op.id}:${op.queue}` : `del ${op.id}`))).toEqual(
    Array.from({ length: 20 }, (_, k) => `${1 + k}:q${k}`),
  );
  for (const op of ops) if (op.kind === "insert") expect(op.body[BODY - 1]).toBe(7);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a group record holds the body once", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-group-size-"));
  const shared = FastLog.open(join(dir, "a.sqlite"))!;
  const copies = FastLog.open(join(dir, "b.sqlite"))!;
  const rows = fanout(1, 20, 1);
  shared.syncRows(rows, []);
  // Distinct body objects are never grouped.
  copies.syncRows(rows.map((row) => ({ ...row, body: row.body.slice() })), []);
  const a = shared.liveMessageIds();
  expect([...a]).toHaveLength(20);
  shared.close();
  copies.close();
  // Both files are preallocated chunks; compare the records by replaying the logical end instead.
  const logical = (p: string) => {
    const log = FastLog.open(p)!;
    let bytes = 0;
    log.replay((op) => {
      if (op.kind === "insert") bytes += op.body.length;
    });
    log.close();
    return bytes;
  };
  // Replay still yields every member's body.
  expect(logical(join(dir, "a.sqlite"))).toBe(20 * BODY);
  expect(logical(join(dir, "b.sqlite"))).toBe(20 * BODY);
  const one = written(join(dir, "a.sqlite"));
  const many = written(join(dir, "b.sqlite"));
  expect(one).toBeLessThan(BODY + 1024);
  expect(many).toBeGreaterThan(20 * BODY);
  rmSync(dir, { recursive: true, force: true });
});

test("deleting some members of a group keeps the others", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-group-del-"));
  const path = join(dir, "t.sqlite");
  const log = FastLog.open(path)!;
  log.syncRows(fanout(10, 5, 3), []);
  log.syncRows([], [10, 12, 14]);
  expect(log.hasLiveMessage(11)).toBe(true);
  expect(log.hasLiveMessage(12)).toBe(false);
  log.close();
  const again = FastLog.open(path)!;
  const live = new Map<number, string>();
  again.replay((op) => {
    if (op.kind === "insert") live.set(op.id, op.queue);
    else live.delete(op.id);
  });
  expect([...live.entries()]).toEqual([[11, "q1"], [13, "q3"]]);
  expect(again.readRow(13)!.queue).toBe("q3");
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a torn group record at the tail is dropped and the records before it stay", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-group-torn-"));
  const path = join(dir, "t.sqlite");
  const log = FastLog.open(path)!;
  log.syncRows([{ id: 1, vhost: "/", queue: "solo", body: Uint8Array.of(1, 2, 3), meta: "{}" }], []);
  log.syncRows(fanout(2, 8, 9), []);
  log.close();
  // Cut into the group record: everything after the first record goes.
  const chunk = join(`${path}.dlog`, readdirSync(`${path}.dlog`).sort()[0]!);
  // The first record: the 16-byte header, the fixed fields, "/", "solo", "{}" and 3 body bytes.
  const groupAt = 16 + (4 + 1 + 8 + 2 + 2 + 4 + 4 + 4) + 1 + 4 + 2 + 3;
  truncateSync(chunk, groupAt + 10);
  const again = FastLog.open(path)!;
  const ops = replay(again);
  expect(ops).toHaveLength(1);
  expect(ops[0]!.kind === "insert" && ops[0]!.queue).toBe("solo");
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

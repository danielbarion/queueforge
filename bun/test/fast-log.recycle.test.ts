import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store.ts";
import type { FastLog } from "../src/fast-log.ts";

const BODY = 64 * 1024;
/** Rows per 4 MiB chunk at this body size, a little under. */
const PER_CHUNK = 60;

const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

function body(seed: number): Uint8Array {
  return new Uint8Array(BODY).fill(seed & 0xff);
}

function chunkIds(sqlitePath: string): number[] {
  return readdirSync(`${sqlitePath}.dlog`)
    .filter((name) => /^\d+$/.test(name))
    .map(Number)
    .sort((a, b) => a - b);
}

function logOf(store: Store): FastLog {
  return (store as unknown as { fastLog: FastLog }).fastLog;
}

/** Rows written and consumed after their fsync, `chunks` chunks' worth. */
async function churn(store: Store, chunks: number, seed = 0) {
  for (let c = 0; c < chunks; c++) {
    const ids: number[] = [];
    for (let i = 0; i < PER_CHUNK; i++) ids.push(store.insertMessage("/", "q", body(seed + i), "{}"));
    await store.whenDurable();
    for (const id of ids) store.deleteMessage(id);
    await store.whenDurable();
    await turn();
  }
}

/** Drop sqlite's copy so a reopen rebuilds from the durable log alone. */
function clearSqlite(path: string) {
  const db = new Database(path);
  db.exec("DELETE FROM messages");
  db.close();
}

test("retired chunks are reused, and a restart replays only the live rows", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-recycle-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_messages", 10, 100_000);
  await churn(store, 30);
  const log = logOf(store);
  expect(log.recycledChunks).toBeGreaterThan(0);
  // No new chunk file was made for the reused ones: ids stay contiguous past the active one.
  const ids = chunkIds(path);
  expect(ids.length).toBeLessThan(12);
  for (let i = 1; i < ids.length; i++) expect(ids[i]).toBe(ids[i - 1]! + 1);
  const live: number[] = [];
  for (let i = 0; i < 5; i++) live.push(store.insertMessage("/", "q", body(200 + i), "{}"));
  await store.whenDurable();
  store.close();
  clearSqlite(path);
  const again = new Store(path, "every_n_messages", 10, 100_000);
  const rows = again.listMessages();
  expect(rows.map((r) => r.id).sort((a, b) => a - b)).toEqual(live);
  for (const row of rows) expect(row.body[0]).toBe((200 + live.indexOf(row.id)) & 0xff);
  // More traffic after the restart keeps reusing chunks.
  await churn(again, 10, 50);
  expect(logOf(again).recycledChunks).toBeGreaterThan(0);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a reused file's old records do not read back under its new id", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-recycle-salt-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_messages", 10, 100_000);
  // Inserted in the first chunk, deleted several chunks later.
  const kept: number[] = [];
  for (let i = 0; i < PER_CHUNK; i++) kept.push(store.insertMessage("/", "q", body(i), "{}"));
  await store.whenDurable();
  const first = chunkIds(path)[0]!;
  const firstFile = join(`${path}.dlog`, String(first).padStart(8, "0"));
  await churn(store, 4, 100);
  const bytes = readFileSync(firstFile);
  for (const id of kept) store.deleteMessage(id);
  await store.whenDurable();
  await turn();
  store.close();
  // What a reuse leaves behind: those inserts under a new, higher id, the
  // header rewound to an empty chunk. Replayed, they would come after the
  // deletes and bring every row back.
  const top = chunkIds(path).at(-1)!;
  bytes.writeBigUInt64LE(16n, 8);
  const renamed = join(`${path}.dlog`, String(top + 1).padStart(8, "0"));
  writeFileSync(renamed, bytes);
  expect(statSync(renamed).size).toBe(bytes.length);
  clearSqlite(path);
  const again = new Store(path, "every_n_messages", 10, 100_000);
  expect(again.listMessages()).toHaveLength(0);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a version 1 log is still read, and its chunks are never reused", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-recycle-v1-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_messages", 10, 100_000);
  const kept: number[] = [];
  for (let i = 0; i < 3; i++) kept.push(store.insertMessage("/", "q", body(i), "{}"));
  await store.whenDurable();
  store.close();
  // Rewrite every chunk as version 1: unsalted crcs, header version 1.
  for (const id of chunkIds(path)) {
    const file = join(`${path}.dlog`, String(id).padStart(8, "0"));
    const bytes = readFileSync(file);
    const salt = Math.imul(id, 0x9e3779b1) >>> 0;
    unsaltRecords(bytes, salt);
    bytes.writeUInt32LE(1, 4);
    writeFileSync(file, bytes);
  }
  clearSqlite(path);
  const again = new Store(path, "every_n_messages", 10, 100_000);
  expect(again.listMessages().map((r) => r.id).sort((a, b) => a - b)).toEqual(kept);
  for (const id of kept) again.deleteMessage(id);
  await again.whenDurable();
  await churn(again, 20);
  // The version 1 chunks went by unlink; later ones are version 2 and reused.
  expect(logOf(again).recycledChunks).toBeGreaterThan(0);
  again.close();
  clearSqlite(path);
  const third = new Store(path, "every_n_messages", 10, 100_000);
  expect(third.listMessages()).toHaveLength(0);
  third.close();
  rmSync(dir, { recursive: true, force: true });
});

/** XOR the crc of each record (type 1, 2 or 3) from the header on with `salt`. */
function unsaltRecords(buf: Buffer, salt: number) {
  let o = 16;
  while (o + 4 <= buf.length && buf.toString("ascii", o, o + 4) === "QFML") {
    const start = o;
    const type = buf[o + 4]!;
    o += 13;
    const vhostLen = buf.readUInt16LE(o);
    o += 2 + vhostLen;
    if (type === 3) {
      const metaLen = buf.readUInt32LE(o);
      o += 4 + metaLen;
      const bodyLen = buf.readUInt32LE(o);
      o += 4 + bodyLen;
      const count = buf.readUInt32LE(o);
      o += 4;
      for (let k = 0; k < count; k++) o += 2 + buf.readUInt16LE(o);
    } else {
      const queueLen = buf.readUInt16LE(o);
      o += 2 + queueLen;
      const metaLen = buf.readUInt32LE(o);
      o += 4 + metaLen;
      const bodyLen = buf.readUInt32LE(o);
      o += 4 + bodyLen;
    }
    if (o + 4 > buf.length || o <= start) return;
    buf.writeUInt32LE((buf.readUInt32LE(o) ^ salt) >>> 0, o);
    o += 4;
  }
}

test("commit fsyncs are counted and timed for the metrics page", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-sync-stats-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_messages", 10, 100_000);
  const before = store.fsyncStats();
  await churn(store, 2);
  const after = store.fsyncStats();
  expect(after.count).toBeGreaterThan(before.count);
  expect(after.seconds).toBeGreaterThan(before.seconds);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

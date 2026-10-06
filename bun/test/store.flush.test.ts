import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { closeSync, cpSync, fdatasyncSync, mkdtempSync, openSync, readSync, readdirSync, rmSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store.ts";

test("every_n_ms lone durable wait finishes after its fsync and under 10 ms", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 80);
  const started = performance.now();
  store.insertMessage("/", "q", new Uint8Array([1, 2, 3]), "{}");
  expect(store.fullFlushCount).toBe(0);
  await store.whenDurable();
  const elapsed = performance.now() - started;
  expect(store.fullFlushCount).toBe(1);
  expect(store.confirmsBeforeFsync).toBe(0);
  expect(elapsed).toBeLessThan(10);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("every_n_ms batches durable writes before the interval fsync", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-store-batch-"));
  const store = new Store(join(dir, "queueforge.db"), "every_n_ms", 80);
  const started = performance.now();
  for (let i = 0; i < 20; i++) {
    store.insertMessage("/", "q", new Uint8Array([i]), "{}");
  }
  expect(performance.now() - started).toBeLessThan(40);
  expect(store.fullFlushCount).toBe(0);
  expect(store.listMessages()).toHaveLength(0);
  await store.whenDurable();
  expect(performance.now() - started).toBeLessThan(10);
  expect(store.fullFlushCount).toBe(1);
  expect(store.listMessages()).toHaveLength(20);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a deep burst after the interval flush does not wait another tick", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-store-deep-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 400);
  store.insertMessage("/", "q", new Uint8Array([1]), "{}");
  const first = performance.now();
  await store.whenDurable();
  expect(performance.now() - first).toBeLessThan(10);

  const burst = performance.now();
  const waits: Promise<void>[] = [];
  for (let i = 0; i < 96; i++) {
    store.insertMessage("/", "q", new Uint8Array([i]), "{}");
    waits.push(store.whenDurable());
  }
  await Promise.all(waits);
  const elapsed = performance.now() - burst;
  expect(store.fullFlushCount).toBe(2);
  expect(elapsed).toBeLessThan(250);

  store.insertMessage("/", "q", new Uint8Array([7]), "{}");
  const quiet = performance.now();
  await store.whenDurable();
  expect(performance.now() - quiet).toBeLessThan(10);

  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("an ack delete is covered by the next group commit", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-store-ack-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_ms", 400);
  const id = store.insertMessage("/", "q", new Uint8Array([9]), "{}");
  await store.whenDurable();
  expect(store.listMessages()).toHaveLength(1);
  store.deleteMessage(id);
  expect(store.listMessages()).toHaveLength(1);
  store.close();
  const again = new Store(path, "every_n_ms", 400);
  expect(again.listMessages()).toHaveLength(0);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("128 durable waits share one fsync without the interval", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-store-window-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 400);
  const started = performance.now();
  const waits: Promise<void>[] = [];
  for (let i = 0; i < 128; i++) {
    store.insertMessage("/", "q", new Uint8Array([i]), "{}");
    waits.push(store.whenDurable());
  }
  await Promise.all(waits);
  expect(performance.now() - started).toBeLessThan(250);
  expect(store.fullFlushCount).toBe(1);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a lone confirm is durable in the log before sqlite has the row", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-shadow-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_ms", 80);
  store.immediateLone = true;
  const body = new Uint8Array([8, 8, 8]);
  const id = store.insertMessage("/", "q", body, "{\"k\":1}");
  expect(store.flushIfLone()).toBe(true);
  expect(store.confirmsBeforeFsync).toBe(0);
  expect(store.fullFlushCount).toBe(1);
  const raw = store.db.query("SELECT COUNT(*) AS c FROM messages").get() as { c: number };
  expect(Number(raw.c)).toBe(0);
  expect(store.listMessages()).toHaveLength(1);
  expect(store.readStored(id)?.body).toEqual(body);
  store.close();
  const db = new Database(path);
  db.exec("DELETE FROM messages");
  db.close();
  const again = new Store(path, "every_n_ms", 80);
  const rows = again.listMessages();
  expect(rows).toHaveLength(1);
  expect(new Uint8Array(rows[0]!.body)).toEqual(body);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a learned lone flush finishes in the caller's turn", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-lone-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 80);
  store.immediateLone = true;
  store.insertMessage("/", "q", new Uint8Array([1, 2, 3]), "{}");
  expect(store.fullFlushCount).toBe(0);
  expect(store.flushIfLone()).toBe(true);
  expect(store.fullFlushCount).toBe(1);
  expect(store.confirmsBeforeFsync).toBe(0);
  expect(store.listMessages()).toHaveLength(1);
  expect(store.flushIfLone()).toBe(false);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("immediateLone does not split a synchronous whenDurable burst", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-lone-burst-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 400);
  store.immediateLone = true;
  const waits: Promise<void>[] = [];
  for (let i = 0; i < 128; i++) {
    store.insertMessage("/", "q", new Uint8Array([i]), "{}");
    waits.push(store.whenDurable());
  }
  await Promise.all(waits);
  expect(store.fullFlushCount).toBe(1);
  expect(store.confirmsBeforeFsync).toBe(0);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a durable body is replayed from the log after sqlite rows are removed", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-replay-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_ms", 80);
  store.insertMessage("/", "q", new Uint8Array([4, 5, 6]), "{\"k\":1}");
  await store.whenDurable();
  expect(store.confirmsBeforeFsync).toBe(0);
  expect(store.fullFlushCount).toBe(1);
  store.close();
  const db = new Database(path);
  db.exec("DELETE FROM messages");
  db.close();
  const again = new Store(path, "every_n_ms", 80);
  const rows = again.listMessages();
  expect(rows).toHaveLength(1);
  expect(new Uint8Array(rows[0]!.body)).toEqual(new Uint8Array([4, 5, 6]));
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("replay finds a record when the chunk header was left at the start", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-header-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_ms", 80);
  store.insertMessage("/", "q", new Uint8Array([7, 8]), "{\"k\":2}");
  await store.whenDurable();
  expect(store.confirmsBeforeFsync).toBe(0);
  store.close();
  const chunk = readdirSync(`${path}.dlog`)
    .filter((name) => /^\d+$/.test(name))
    .sort()[0];
  expect(chunk).toBeTruthy();
  const fd = openSync(join(`${path}.dlog`, chunk!), "r+");
  const header = Buffer.alloc(16);
  header.write("QFHD", 0, 4, "ascii");
  header.writeUInt32LE(1, 4);
  header.writeBigUInt64LE(16n, 8);
  writeSync(fd, header, 0, 16, 0);
  fdatasyncSync(fd);
  closeSync(fd);
  const db = new Database(path);
  db.exec("DELETE FROM messages");
  db.close();
  const again = new Store(path, "every_n_ms", 80);
  const rows = again.listMessages();
  expect(rows).toHaveLength(1);
  expect(new Uint8Array(rows[0]!.body)).toEqual(new Uint8Array([7, 8]));
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("always durable wait does not wait for the interval", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-"));
  const store = new Store(join(dir, "t.sqlite"), "always", 200);
  store.insertMessage("/", "q", new Uint8Array([9]), "{}");
  const started = performance.now();
  await store.whenDurable();
  expect(performance.now() - started).toBeLessThan(40);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

function chunkHas(sqlitePath: string, body: Uint8Array): boolean {
  const needle = Buffer.from(body);
  const scratch = Buffer.alloc(65536);
  for (const name of readdirSync(`${sqlitePath}.dlog`)) {
    if (!/^\d+$/.test(name)) continue;
    const fd = openSync(join(`${sqlitePath}.dlog`, name), "r");
    try {
      const n = readSync(fd, scratch, 0, scratch.length, 0);
      if (scratch.subarray(0, n).includes(needle)) return true;
    } finally {
      closeSync(fd);
    }
  }
  return false;
}

test("a pre-flush delete stays out of the confirm fsync and lands on the next one", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-noack-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_ms", 400);
  store.immediateLone = true;
  const body = new Uint8Array([9, 4, 2, 7]);
  const id = store.insertMessage("/", "q", body, "{\"k\":1}");
  store.deleteMessage(id);
  expect(store.stagedWithoutFlush).toBe(true);
  expect(store.flushIfLone()).toBe(true);
  expect(store.fullFlushCount).toBe(1);
  expect(store.confirmsBeforeFsync).toBe(0);
  expect(store.stagedWithoutFlush).toBe(false);
  expect(store.listMessages()).toHaveLength(0);
  expect(chunkHas(path, body)).toBe(true);
  store.noteConfirmReleased();
  expect(store.confirmsBeforeFsync).toBe(0);
  // The confirm fsync has the insert and not the delete. A crash here replays the body.
  const crashPath = join(dir, "crash.sqlite");
  cpSync(`${path}.dlog`, `${crashPath}.dlog`, { recursive: true });
  const crashed = new Store(crashPath, "every_n_ms", 400);
  const crashedRows = crashed.listMessages();
  expect(crashedRows).toHaveLength(1);
  expect(new Uint8Array(crashedRows[0]!.body)).toEqual(body);
  crashed.close();
  store.close();
  const again = new Store(path, "every_n_ms", 400);
  expect(again.listMessages()).toHaveLength(0);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("noteConfirmReleased counts only an append that is still staged", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-count-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 400);
  store.insertMessage("/", "q", new Uint8Array([3]), "{}");
  expect(store.stagedWithoutFlush).toBe(true);
  store.noteConfirmReleased();
  expect(store.confirmsBeforeFsync).toBe(1);
  await store.whenDurable();
  expect(store.fullFlushCount).toBe(1);
  expect(store.stagedWithoutFlush).toBe(false);
  store.noteConfirmReleased();
  expect(store.confirmsBeforeFsync).toBe(1);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("an uncovered every_n_ms waiter rejects and counts", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-uncovered-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 400);
  store.stagedWithoutFlush = true;
  await expect(store.whenDurable()).rejects.toThrow("confirm before fsync");
  expect(store.confirmsBeforeFsync).toBe(1);
  expect(store.fullFlushCount).toBe(0);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

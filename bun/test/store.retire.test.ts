import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store.ts";
import { FastLog } from "../src/fast-log.ts";

const BODY = 64 * 1024;
/** Rows per 4 MiB chunk at this body size, a little under. */
const PER_CHUNK = 60;

function chunkFiles(sqlitePath: string): number {
  return readdirSync(`${sqlitePath}.dlog`).filter((name) => /^\d+$/.test(name)).length;
}

function body(seed: number): Uint8Array {
  return new Uint8Array(BODY).fill(seed & 0xff);
}

const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Fill `chunks` log chunks with rows that are consumed right after their fsync. */
async function churn(store: Store, chunks: number) {
  for (let c = 0; c < chunks; c++) {
    const ids: number[] = [];
    for (let i = 0; i < PER_CHUNK; i++) ids.push(store.insertMessage("/", "q", body(i), "{}"));
    await store.whenDurable();
    for (const id of ids) store.deleteMessage(id);
    await store.whenDurable();
    await turn();
  }
}

test("consumed chunks are removed and a restart has no rows", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-retire-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_messages", 10, 100_000);
  await churn(store, 24);
  // 24 chunks were written. Only the active chunk and the prepared ones stay.
  expect(chunkFiles(path)).toBeLessThan(12);
  store.close();
  const db = new Database(path);
  db.exec("DELETE FROM messages");
  db.close();
  const again = new Store(path, "every_n_messages", 10, 100_000);
  expect(again.listMessages()).toHaveLength(0);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a caught-up row stays deleted after its log chunks retire without clearing sqlite", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-retire-sqlite-delete-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_messages", 10, 100_000);
  const id = store.insertMessage("/", "q", new Uint8Array([9, 8, 7]), "{}");
  await store.whenDurable();
  // Exercise the real threshold catch-up directly without a 64 MiB fixture.
  (store as unknown as { applyShadow(): void }).applyShadow();
  expect(store.db.query("SELECT id FROM messages WHERE id=?").get(id)).not.toBeNull();
  const firstChunk = readdirSync(`${path}.dlog`).filter((name) => /^\d+$/.test(name)).sort()[0]!;
  store.deleteMessage(id);
  await store.whenDurable();
  expect(store.readStored(id)).toBeNull();
  expect(store.listMessages()).toHaveLength(0);
  await churn(store, 3);
  expect(readdirSync(`${path}.dlog`)).not.toContain(firstChunk);
  store.close();
  // Preserve sqlite: clearing it would hide a stale cached row.
  const again = new Store(path, "every_n_messages", 10, 100_000);
  try {
    expect(again.listMessages()).toHaveLength(0);
    expect(again.readStored(id)).toBeNull();
  } finally {
    again.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("an abrupt exit after retirement cannot resurrect a stale sqlite row", () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-retire-abrupt-"));
  const path = join(dir, "t.sqlite");
  const script = `
    import { Store } from ${JSON.stringify(new URL("../src/store.ts", import.meta.url).pathname)};
    import { readdirSync } from "node:fs";
    const path = ${JSON.stringify(path)};
    const store = new Store(path, "every_n_messages", 10, 100000);
    const id = store.insertMessage("/", "q", new Uint8Array([9, 8, 7]), "{}");
    await store.whenDurable();
    store.applyShadow();
    const first = readdirSync(path + ".dlog").filter(n => /^\\d+$/.test(n)).sort()[0];
    store.deleteMessage(id);
    await store.whenDurable();
    for (let c = 0; c < 3; c++) {
      const ids = [];
      for (let i = 0; i < 60; i++) ids.push(store.insertMessage("/", "q", new Uint8Array(65536).fill(i), "{}"));
      await store.whenDurable();
      for (const row of ids) store.deleteMessage(row);
      await store.whenDurable();
      await new Promise(resolve => setImmediate(resolve));
    }
    const live = store.insertMessage("/", "q", new Uint8Array([4, 5, 6]), '{"kept":true}');
    await store.whenDurable();
    console.log(JSON.stringify({retired: !readdirSync(path + ".dlog").includes(first),
      stale: !!store.db.query("SELECT id FROM messages WHERE id=?").get(id), live}));
    process.kill(process.pid, "SIGKILL");
  `;
  try {
    const child = Bun.spawnSync([process.execPath, "-e", script]);
    expect(child.signalCode).toBe("SIGKILL");
    const proof = JSON.parse(new TextDecoder().decode(child.stdout).trim());
    expect(proof.retired).toBe(true);
    expect(proof.stale).toBe(true);
    const again = new Store(path, "every_n_messages", 10, 100_000);
    try {
      expect(again.listMessages().map((row) => Number(row.id))).toEqual([proof.live]);
      expect(again.readStored(proof.live)?.body).toEqual(new Uint8Array([4, 5, 6]));
    } finally {
      again.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("storage mode transitions keep newer sqlite state and retry interrupted adoption", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-storage-transition-"));
  const path = join(dir, "t.sqlite");
  let store = new Store(path, "every_n_messages", 10, 100_000);
  const removed = store.insertMessage("/", "q", new Uint8Array([1]), "{}");
  const kept = store.insertMessage("/", "q", new Uint8Array([2]), "{}");
  await store.whenDurable();
  store.close();
  store = new Store(path, "always");
  expect(store.listMessages().map((row) => Number(row.id))).toEqual([removed, kept]);
  store.deleteMessage(removed);
  const newer = store.insertMessage("/", "q", new Uint8Array([3]), "{}");
  store.close();
  const sync = FastLog.prototype.syncRows;
  try {
    FastLog.prototype.syncRows = function(rows, deletes) {
      sync.call(this, rows, deletes);
      throw new Error("interrupted after log adoption before source marker");
    };
    expect(() => new Store(path, "every_n_messages", 10, 100_000)).toThrow("interrupted after log adoption");
  } finally {
    FastLog.prototype.syncRows = sync;
  }
  store = new Store(path, "every_n_messages", 10, 100_000);
  expect(store.listMessages().map((row) => Number(row.id))).toEqual([kept, newer]);
  store.deleteMessage(kept);
  await store.whenDurable();
  store.close();
  store = new Store(path, "never");
  expect(store.listMessages().map((row) => Number(row.id))).toEqual([newer]);
  const transient = store.insertMessage("/", "q", new Uint8Array([4]), "{}");
  store.close();
  store = new Store(path, "every_n_messages", 10, 100_000);
  expect(store.listMessages().map((row) => Number(row.id))).toEqual([newer, transient]);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("unmarked legacy sqlite rows are adopted without guessing their provenance", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-storage-legacy-"));
  const path = join(dir, "t.sqlite");
  const seed = new Store(path, "always");
  const id = seed.insertMessage("/", "q", new Uint8Array([7]), "{\"legacy\":true}");
  seed.close();
  const db = new Database(path);
  db.exec("DROP TABLE message_storage");
  db.close();
  const sync = FastLog.prototype.syncRows;
  try {
    FastLog.prototype.syncRows = function(rows, deletes) {
      sync.call(this, rows, deletes);
      throw new Error("interrupted legacy adoption");
    };
    expect(() => new Store(path, "every_n_messages", 10, 100_000)).toThrow("interrupted legacy adoption");
  } finally {
    FastLog.prototype.syncRows = sync;
  }
  const store = new Store(path, "every_n_messages", 10, 100_000);
  expect(store.readStored(id)?.body).toEqual(new Uint8Array([7]));
  store.close();
  const again = new Store(path, "every_n_messages", 10, 100_000);
  expect(again.listMessages().map((row) => Number(row.id))).toEqual([id]);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("an unmarked existing log preserves ambiguous sql-only rows and honors known deletes", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-storage-legacy-log-"));
  const path = join(dir, "t.sqlite");
  const seed = new Store(path, "every_n_messages", 10, 100_000);
  const gone = seed.insertMessage("/", "q", new Uint8Array([1]), "{}");
  const kept = seed.insertMessage("/", "q", new Uint8Array([2]), "{}");
  await seed.whenDurable();
  seed.deleteMessage(gone);
  await seed.whenDurable();
  seed.close();
  const unknown = kept + 100;
  const db = new Database(path);
  db.exec("DROP TABLE message_storage");
  db.query("INSERT OR REPLACE INTO messages (id, vhost, queue, body, meta) VALUES (?, '/', 'q', ?, '{}')").run(gone, new Uint8Array([1]));
  db.query("INSERT INTO messages (id, vhost, queue, body, meta) VALUES (?, '/', 'q', ?, '{}')").run(unknown, new Uint8Array([3]));
  db.close();
  for (let i = 0; i < 2; i++) {
    const store = new Store(path, "every_n_messages", 10, 100_000);
    expect(store.listMessages().map((row) => Number(row.id))).toEqual([kept, unknown]);
    expect(store.readStored(gone)).toBeNull();
    store.close();
  }
  rmSync(dir, { recursive: true, force: true });
});

test("shadow bytes track live rows across replacement, deletion, and catch-up", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-shadow-bytes-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_messages", 10, 100_000);
  const state = store as unknown as {
    shadowBytes: number;
    setShadow(id: number, row: {id: number; vhost: string; queue: string; body: Uint8Array; meta: string} | null): void;
    applyShadow(): void;
  };
  const id = store.insertMessage("/", "q", new Uint8Array(8), "{}");
  await store.whenDurable();
  expect(state.shadowBytes).toBe(8);
  state.setShadow(id, {id, vhost: "/", queue: "q", body: new Uint8Array(3), meta: "{}"});
  expect(state.shadowBytes).toBe(3);
  store.deleteMessage(id);
  await store.whenDurable();
  expect(state.shadowBytes).toBe(0);
  const queueId = store.insertMessage("/", "gone", new Uint8Array(5), "{}");
  await store.whenDurable();
  expect(state.shadowBytes).toBe(5);
  store.deleteQueue("/", "gone");
  expect(state.shadowBytes).toBe(0);
  await store.whenDurable();
  expect(store.readStored(queueId)).toBeNull();
  state.applyShadow();
  expect(state.shadowBytes).toBe(0);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("one unacked row does not pin the log, and survives a replay", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-retire-keep-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_messages", 10, 100_000);
  const kept = new Uint8Array([1, 2, 3, 4, 5]);
  const keptId = store.insertMessage("/", "q", kept, "{\"k\":1}");
  await store.whenDurable();
  await churn(store, 40);
  expect(chunkFiles(path)).toBeLessThan(30);
  expect(store.readStored(keptId)?.body).toEqual(kept);
  store.close();
  // sqlite is only a cache: the log alone must still bring the row back.
  const db = new Database(path);
  db.exec("DELETE FROM messages");
  db.close();
  const again = new Store(path, "every_n_messages", 10, 100_000);
  const rows = again.listMessages();
  expect(rows).toHaveLength(1);
  expect(Number(rows[0]!.id)).toBe(keptId);
  expect(new Uint8Array(rows[0]!.body)).toEqual(kept);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a deleted queue's rows are not replayed", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-retire-queue-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_messages", 10, 100_000);
  store.insertMessage("/", "gone", new Uint8Array([9]), "{}");
  store.insertMessage("/", "stays", new Uint8Array([7]), "{}");
  await store.whenDurable();
  store.deleteQueue("/", "gone");
  await store.whenDurable();
  store.close();
  const db = new Database(path);
  db.exec("DELETE FROM messages");
  db.close();
  const again = new Store(path, "every_n_messages", 10, 100_000);
  const rows = again.listMessages();
  expect(rows.map((row) => row.queue)).toEqual(["stays"]);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a record with multibyte names and an id past 2^32 replays exactly", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-retire-utf8-"));
  const path = join(dir, "t.sqlite");
  const seed = new Store(path, "every_n_messages", 10, 100_000);
  const big = 2 ** 33 + 5;
  seed.db.query("INSERT INTO messages (id, vhost, queue, body, meta) VALUES (?, '/', 'seed', x'00', '{}')").run(big);
  seed.close();
  const store = new Store(path, "every_n_messages", 10, 100_000);
  const body = new Uint8Array([0, 255, 1, 254]);
  const meta = "{\"k\":\"héllo ✓ 日本\"}";
  const id = store.insertMessage("/vh-ü", "cola-ñ-✓", body, meta);
  expect(id).toBe(big + 1);
  await store.whenDurable();
  store.close();
  const db = new Database(path);
  db.exec("DELETE FROM messages");
  db.close();
  const again = new Store(path, "every_n_messages", 10, 100_000);
  const rows = again.listMessages();
  expect(rows).toHaveLength(1);
  expect(Number(rows[0]!.id)).toBe(big + 1);
  expect(rows[0]!.vhost).toBe("/vh-ü");
  expect(rows[0]!.queue).toBe("cola-ñ-✓");
  expect(rows[0]!.meta).toBe(meta);
  expect(new Uint8Array(rows[0]!.body)).toEqual(body);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a batch larger than one chunk is written after the next chunk was preallocated", async () => {
  // A 300,000-message backlog hit this: the preallocated 4 MiB chunk was
  // reused for a bigger batch, the write threw, and the process exited.
  const dir = mkdtempSync(join(tmpdir(), "qf-big-batch-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_ms", 400);
  const body = new Uint8Array(BODY).fill(3);
  // Leave under 2 MiB in the first chunk so the next one is preallocated.
  for (let i = 0; i < 40; i++) store.insertMessage("/", "q", body, "{}");
  await store.whenDurable();
  await Bun.sleep(300);
  const waits: Promise<void>[] = [];
  for (let i = 0; i < 100; i++) {
    store.insertMessage("/", "q", body, "{}");
    waits.push(store.whenDurable());
  }
  await Promise.all(waits);
  expect(store.confirmsBeforeFsync).toBe(0);
  store.close();
  const again = new Store(path, "every_n_ms", 400);
  expect(again.listMessages()).toHaveLength(140);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store.ts";

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

import { expect, test } from "bun:test";
import { cpSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store.ts";

type Internals = { shadow: Map<number, { body: Uint8Array } | null>; db: Store["db"] };

test("a backlog past the memory cap pages bodies to the log, not sqlite", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-page-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_ms", 400);
  const body = (i: number) => new Uint8Array(64 * 1024).fill(i % 251);
  const ids: number[] = [];
  // 1,200 x 64 KiB = 75 MiB, past the 64 MiB cap.
  for (let i = 0; i < 1200; i++) {
    ids.push(store.insertMessage("/", "q", body(i), `{"i":${i}}`));
    if (i % 50 === 49) await store.whenDurable();
  }
  await store.whenDurable();
  const internals = store as unknown as Internals;
  let paged = 0;
  for (const row of internals.shadow.values()) if (row && row.body.length === 0) paged++;
  expect(paged).toBeGreaterThan(0);
  // sqlite was not given the bodies.
  const inSqlite = store.db.query("SELECT COUNT(*) AS c FROM messages").get() as { c: number };
  expect(Number(inSqlite.c)).toBe(0);
  // Every body still reads back, paged or not.
  for (const i of [0, 1, 500, 1199]) {
    const row = store.readStored(ids[i]!);
    expect(row?.meta).toBe(`{"i":${i}}`);
    expect(row?.body).toEqual(body(i));
  }
  // A delete of a paged row sticks across a restart.
  store.deleteMessage(ids[0]!);
  store.insertMessage("/", "q", new Uint8Array([1]), "{}");
  await store.whenDurable();
  expect(store.readStored(ids[0]!)).toBeNull();
  const crash = join(dir, "crash.sqlite");
  cpSync(`${path}.dlog`, `${crash}.dlog`, { recursive: true });
  const again = new Store(crash, "every_n_ms", 400);
  const rows = again.listMessages();
  expect(rows).toHaveLength(1200);
  expect(rows.some((row) => row.id === ids[0])).toBe(false);
  expect(new Uint8Array(rows.find((row) => row.id === ids[700])!.body)).toEqual(body(700));
  again.close();
  store.close();
  // A clean close still leaves a consistent database.
  const third = new Store(path, "every_n_ms", 400);
  expect(third.listMessages()).toHaveLength(1200);
  third.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a mostly live backlog is not relocated forward", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-page-reloc-"));
  const path = join(dir, "t.sqlite");
  const store = new Store(path, "every_n_ms", 400);
  const body = new Uint8Array(512 * 1024).fill(3);
  // About 21 chunks of live rows, past the 16-chunk relocation horizon.
  for (let i = 0; i < 170; i++) {
    store.insertMessage("/", "q", body, "{}");
    await store.whenDurable();
  }
  const log = (store as unknown as { fastLog: { keptChunks: number; retiredChunks: number } }).fastLog;
  const files = readdirSync(`${path}.dlog`).filter((name) => /^\d+$/.test(name)).length;
  // Nothing retired, and the backlog was written once: seven 512 KiB rows fit a
  // 4 MiB chunk, so 170 rows take 25 chunks. Relocation would add more.
  expect(log.retiredChunks).toBe(0);
  expect(log.keptChunks).toBeLessThanOrEqual(25);
  expect(files).toBeGreaterThanOrEqual(log.keptChunks);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

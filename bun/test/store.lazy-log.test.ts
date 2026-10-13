import { expect, test } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store.ts";

function chunkFiles(sqlitePath: string): number {
  const dir = `${sqlitePath}.dlog`;
  return existsSync(dir) ? readdirSync(dir).filter((name) => /^\d+$/.test(name)).length : 0;
}

function fresh(prefix: string) {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  return { dir, path: join(dir, "t.sqlite") };
}

test("a fresh store writes no log chunks until it is used", () => {
  const { dir, path } = fresh("qf-lazy-idle-");
  const store = new Store(path, "every_n_ms", 10);
  expect(chunkFiles(path)).toBe(0);
  store.close();
  // Closing and reopening an unused log leaves it unused.
  const again = new Store(path, "every_n_ms", 10);
  expect(chunkFiles(path)).toBe(0);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("prepareLog writes the chunks before the first confirm", async () => {
  const { dir, path } = fresh("qf-lazy-prepare-");
  const store = new Store(path, "every_n_ms", 10);
  store.prepareLog();
  expect(chunkFiles(path)).toBe(8);
  store.prepareLog();
  expect(chunkFiles(path)).toBe(8);
  const id = store.insertMessage("/", "q", new Uint8Array([1, 2, 3]), "{}");
  await store.whenDurable();
  store.close();
  const again = new Store(path, "every_n_ms", 10);
  expect(again.readMessage(id)).toEqual(new Uint8Array([1, 2, 3]));
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a first insert without prepareLog still lands in the log and survives a restart", async () => {
  const { dir, path } = fresh("qf-lazy-insert-");
  for (const mode of ["every_n_ms", "every_n_messages"] as const) {
    rmSync(dir, { recursive: true, force: true });
    const store = new Store(path, mode, 10, 1);
    const id = store.insertMessage("/", "q", new Uint8Array([4, 5]), "{}");
    await store.whenDurable();
    expect(chunkFiles(path)).toBe(8);
    store.close();
    const again = new Store(path, mode, 10, 1);
    expect(again.readMessage(id)).toEqual(new Uint8Array([4, 5]));
    again.close();
  }
  rmSync(dir, { recursive: true, force: true });
});

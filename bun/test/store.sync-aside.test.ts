import { expect, test } from "bun:test";
import { cpSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store.ts";

function fresh(prefix: string) {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  return { dir, path: join(dir, "t.sqlite") };
}

test("a lone confirm syncs on the worker and resolves after that sync", async () => {
  const { dir, path } = fresh("qf-aside-lone-");
  const store = new Store(path, "every_n_ms", 400);
  expect(store.syncsAside).toBe(true);
  const body = new Uint8Array([3, 1, 4, 1, 5]);
  const id = store.insertMessage("/", "q", body, "{}");
  let done = false;
  const wait = store.whenDurable().then(() => {
    done = true;
  });
  // The confirm is not released in this turn: the worker has not synced yet.
  expect(done).toBe(false);
  await wait;
  expect(store.fullFlushCount).toBe(1);
  expect(store.syncFlushCount).toBe(0);
  expect(store.confirmsBeforeFsync).toBe(0);
  expect(store.stagedWithoutFlush).toBe(false);
  expect(store.readStored(id)?.body).toEqual(body);
  // The synced log alone restores the row.
  const crash = join(dir, "crash.sqlite");
  cpSync(`${path}.dlog`, `${crash}.dlog`, { recursive: true });
  const again = new Store(crash, "every_n_ms", 400);
  expect(again.listMessages().map((row) => new Uint8Array(row.body))).toEqual([body]);
  again.close();
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a confirm that arrives while its row is being synced waits for that sync", async () => {
  const { dir, path } = fresh("qf-aside-late-");
  const store = new Store(path, "every_n_ms", 1);
  store.insertMessage("/", "q", new Uint8Array([7]), "{}");
  // The 1 ms interval writes the row with no waiter. Catch it mid-sync.
  const deadline = Date.now() + 1000;
  while (store.fullFlushCount === 0 && Date.now() < deadline) await Bun.sleep(0);
  expect(store.fullFlushCount).toBe(1);
  await store.whenDurable();
  expect(store.confirmsBeforeFsync).toBe(0);
  expect(store.stagedWithoutFlush).toBe(false);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a delete of a row in a syncing batch lands after its insert", async () => {
  const { dir, path } = fresh("qf-aside-delete-");
  const store = new Store(path, "every_n_ms", 400);
  const id = store.insertMessage("/", "q", new Uint8Array([1, 2]), "{}");
  const wait = store.whenDurable();
  await Promise.resolve();
  await Promise.resolve();
  // Written, not yet synced. The delete is queued for the next batch.
  store.deleteMessage(id);
  await wait;
  await store.whenDurable();
  store.close();
  const again = new Store(path, "every_n_ms", 400);
  expect(again.listMessages()).toHaveLength(0);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("an inline sync covers a batch still on the worker", async () => {
  const { dir, path } = fresh("qf-aside-deep-");
  const store = new Store(path, "every_n_ms", 400);
  store.insertMessage("/", "q", new Uint8Array([1]), "{}");
  const first = store.whenDurable();
  await Promise.resolve();
  await Promise.resolve();
  // The first batch is on the worker. The next group fsyncs in this thread.
  store.syncAside = false;
  const deep: Promise<void>[] = [];
  for (let i = 0; i < 128; i++) {
    store.insertMessage("/", "q", new Uint8Array([i]), "{}");
    deep.push(store.whenDurable());
  }
  await Promise.all([first, ...deep]);
  expect(store.syncFlushCount).toBe(1);
  expect(store.confirmsBeforeFsync).toBe(0);
  expect(store.listMessages()).toHaveLength(129);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a deep confirm window syncs on the worker too", async () => {
  const { dir, path } = fresh("qf-aside-window-");
  const store = new Store(path, "every_n_ms", 400);
  const waits: Promise<void>[] = [];
  for (let i = 0; i < 200; i++) {
    store.insertMessage("/", "q", new Uint8Array([i]), "{}");
    waits.push(store.whenDurable());
  }
  await Promise.all(waits);
  expect(store.syncFlushCount).toBe(0);
  expect(store.confirmsBeforeFsync).toBe(0);
  store.close();
  const again = new Store(path, "every_n_ms", 400);
  expect(again.listMessages()).toHaveLength(200);
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

test("batches that rotate the log while syncing aside all survive a restart", async () => {
  const { dir, path } = fresh("qf-aside-rotate-");
  const store = new Store(path, "every_n_ms", 400);
  const body = new Uint8Array(700 * 1024).fill(5);
  // About 14 MiB in lone confirms: several chunk rotations behind the worker.
  for (let i = 0; i < 20; i++) {
    store.insertMessage("/", "q", body, `{"i":${i}}`);
    await store.whenDurable();
  }
  expect(store.confirmsBeforeFsync).toBe(0);
  const crash = join(dir, "crash.sqlite");
  cpSync(`${path}.dlog`, `${crash}.dlog`, { recursive: true });
  const again = new Store(crash, "every_n_ms", 400);
  const rows = again.listMessages();
  expect(rows).toHaveLength(20);
  expect(rows.map((row) => row.meta)).toEqual(Array.from({ length: 20 }, (_, i) => `{"i":${i}}`));
  again.close();
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("a rotation hands the old chunk to the worker without waiting on its sync", async () => {
  const { dir, path } = fresh("qf-aside-handoff-");
  const store = new Store(path, "every_n_ms", 400);
  const body = new Uint8Array(600 * 1024).fill(9);
  // Confirms are not awaited one by one, so rotations land while a sync runs.
  const waits: Promise<void>[] = [];
  for (let i = 0; i < 30; i++) {
    store.insertMessage("/", "q", body, `{"i":${i}}`);
    waits.push(store.whenDurable());
    if (i % 3 === 0) await Promise.resolve();
  }
  await Promise.all(waits);
  expect(store.confirmsBeforeFsync).toBe(0);
  expect((store as unknown as { fastLog: { handOffs: number } }).fastLog.handOffs).toBeGreaterThan(0);
  const crash = join(dir, "crash.sqlite");
  cpSync(`${path}.dlog`, `${crash}.dlog`, { recursive: true });
  const again = new Store(crash, "every_n_ms", 400);
  expect(again.listMessages()).toHaveLength(30);
  again.close();
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

test("an acked row that sqlite never held leaves no tombstone", async () => {
  const { dir, path } = fresh("qf-aside-tomb-");
  const store = new Store(path, "every_n_ms", 400);
  const shadow = (store as unknown as { shadow: Map<number, unknown> }).shadow;
  const id = store.insertMessage("/", "q", new Uint8Array([1]), "{}");
  await store.whenDurable();
  expect(shadow.has(id)).toBe(true);
  store.deleteMessage(id);
  store.insertMessage("/", "q", new Uint8Array([2]), "{}");
  await store.whenDurable();
  expect(shadow.has(id)).toBe(false);
  expect(store.listMessages()).toHaveLength(1);
  store.close();
  const again = new Store(path, "every_n_ms", 400);
  expect(again.listMessages().map((row) => new Uint8Array(row.body))).toEqual([new Uint8Array([2])]);
  // That row is now in sqlite: its delete still needs a tombstone.
  const kept = again.listMessages()[0]!.id;
  again.deleteMessage(kept);
  again.insertMessage("/", "q", new Uint8Array([3]), "{}");
  await again.whenDurable();
  const shadowAgain = (again as unknown as { shadow: Map<number, unknown> }).shadow;
  expect(shadowAgain.get(kept)).toBeNull();
  expect(again.listMessages().map((row) => new Uint8Array(row.body))).toEqual([new Uint8Array([3])]);
  again.close();
  const third = new Store(path, "every_n_ms", 400);
  expect(third.listMessages().map((row) => new Uint8Array(row.body))).toEqual([new Uint8Array([3])]);
  third.close();
  rmSync(dir, { recursive: true, force: true });
});

test("QUEUEFORGE_ASYNC_FSYNC=0 keeps every fsync inline", async () => {
  const { dir, path } = fresh("qf-aside-off-");
  const store = new Store(path, "every_n_ms", 400);
  store.syncAside = false;
  expect(store.syncsAside).toBe(false);
  store.insertMessage("/", "q", new Uint8Array([1]), "{}");
  await store.whenDurable();
  expect(store.syncFlushCount).toBe(1);
  store.close();
  rmSync(dir, { recursive: true, force: true });
});

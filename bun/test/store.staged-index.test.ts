import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../src/store.ts";

test("a deep staged group finds its rows by id for reads and acks", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-staged-index-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 400);
  store.prepareLog();
  const ids: number[] = [];
  // More than the scan threshold, so the index is built and then extended.
  for (let i = 0; i < 40; i++) ids.push(store.insertMessage("/", "q", Uint8Array.of(i), "{}"));
  expect(store.readMessage(ids[39]!)).toEqual(Uint8Array.of(39));
  for (let i = 40; i < 300; i++) ids.push(store.insertMessage("/", "q", Uint8Array.of(i & 0xff), "{}"));
  expect(store.readMessage(ids[0]!)).toEqual(Uint8Array.of(0));
  expect(store.readMessage(ids[299]!)).toEqual(Uint8Array.of(299 & 0xff));
  // Acks of staged rows are held for the next group, not deleted inside this fsync.
  for (let i = 0; i < 300; i += 2) store.deleteMessage(ids[i]!);
  await store.whenDurable();
  // After the flush the buffer is empty: reads go to the synced index.
  expect(store.readMessage(ids[1]!)).toEqual(Uint8Array.of(1));
  await store.whenDurable();
  store.close();
  const again = new Store(join(dir, "t.sqlite"), "every_n_ms", 400);
  const kept = again.listMessages().map((row) => Number(row.id));
  expect(kept).toEqual(ids.filter((_, i) => i % 2 === 1));
  again.close();
  rmSync(dir, { recursive: true, force: true });
});

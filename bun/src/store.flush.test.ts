import { expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "./store.ts";

test("every_n_ms durable wait lasts one group-commit interval", async () => {
  const dir = mkdtempSync(join(tmpdir(), "qf-flush-"));
  const store = new Store(join(dir, "t.sqlite"), "every_n_ms", 80);
  const started = performance.now();
  store.insertMessage("/", "q", new Uint8Array([1, 2, 3]), "{}");
  await store.whenDurable();
  const elapsed = performance.now() - started;
  expect(elapsed).toBeGreaterThan(50);
  expect(elapsed).toBeLessThan(500);
  store.close();
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

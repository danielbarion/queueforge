/**
 * PASSIVE WAL checkpoint off the broker event loop.
 *
 * The group-commit thread posts the database path. Checkpoint work stays here
 * so a long checkpoint cannot sit inside a publisher confirm.
 */
import { Database } from "bun:sqlite";

const open = new Map<string, Database>();

declare const self: Worker;

self.onmessage = (event: MessageEvent<string>) => {
  const path = event.data;
  if (!path || path === ":memory:") return;
  try {
    let db = open.get(path);
    if (!db) {
      db = new Database(path);
      db.exec("PRAGMA journal_mode=WAL");
      open.set(path, db);
    }
    db.exec("PRAGMA wal_checkpoint(PASSIVE)");
  } catch {
    /* The writer keeps the WAL. The next poke tries again. */
  }
};

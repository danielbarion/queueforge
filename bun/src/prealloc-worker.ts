/**
 * Materialize the next durable-log chunk off the confirm path.
 *
 * The file is a different inode from the chunk the confirm is syncing.
 */
import { closeSync } from "node:fs";
import { createChunkFile } from "./fast-log.ts";

self.onmessage = (event: MessageEvent<{ path: string; size: number; state: SharedArrayBuffer }>) => {
  const { path, size, state } = event.data;
  const view = new BigInt64Array(state);
  try {
    const fd = createChunkFile(path, size);
    closeSync(fd);
    Atomics.store(view, 0, 2n);
  } catch {
    Atomics.store(view, 0, 3n);
  }
  Atomics.notify(view, 0);
};

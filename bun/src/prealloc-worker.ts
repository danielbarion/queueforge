/**
 * Materialize the next durable-log chunk off the confirm path.
 *
 * The file is a different inode from the chunk the confirm is syncing.
 */
import { closeSync } from "node:fs";
import { createChunkFile } from "./fast-log.ts";

declare const self: Worker;

self.onmessage = (event: MessageEvent<{ path: string; size: number; state: SharedArrayBuffer; delayMs: number }>) => {
  const { path, size, state, delayMs } = event.data;
  const view = new BigInt64Array(state);
  try {
    // Tests only: hold each chunk this long, as a stalled disk would.
    if (delayMs) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
    const fd = createChunkFile(path, size);
    closeSync(fd);
    Atomics.store(view, 0, 2n);
  } catch {
    Atomics.store(view, 0, 3n);
  }
  Atomics.notify(view, 0);
};

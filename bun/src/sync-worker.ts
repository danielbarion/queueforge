/**
 * Run the overwrite log's `fdatasync` off the event loop.
 *
 * The main thread writes a batch, then raises `state[0]` to that batch's
 * sequence number. This thread syncs the file in `state[2]` and reports the
 * highest sequence it covered in `state[1]`. Requests that arrive during one
 * sync share the next one, which is the group commit.
 *
 * A rotation hands over the previous chunk in `state[4]`. This thread syncs
 * that file first, closes it and clears the slot, so the main thread never
 * waits on the old chunk's fsync.
 */
import { closeSync, fdatasyncSync } from "node:fs";

declare const self: Worker;

/** Slots of the shared state. */
const REQUESTED = 0;
const DONE = 1;
const FD = 2;
const FAILED = 3;
const PREVIOUS = 4;

self.onmessage = (event: MessageEvent<{ state: SharedArrayBuffer; delayMs: number }>) => {
  const state = new BigInt64Array(event.data.state);
  // Tests only: hold each sync this long, as a stalled disk would.
  const delayMs = event.data.delayMs;
  /** Sync and close a handed-over chunk, then free the slot for the next one. */
  const syncPrevious = () => {
    const previous = Atomics.load(state, PREVIOUS);
    if (previous <= 0n) return;
    fdatasyncSync(Number(previous));
    closeSync(Number(previous));
    Atomics.store(state, PREVIOUS, 0n);
  };
  let done = Atomics.load(state, DONE);
  for (;;) {
    Atomics.wait(state, REQUESTED, done);
    const want = Atomics.load(state, REQUESTED);
    // A negative request is close.
    if (want < 0n) return;
    if (want === done) continue;
    try {
      if (delayMs) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
      syncPrevious();
      fdatasyncSync(Number(Atomics.load(state, FD)));
      // A rotation after `want` was read moves the active chunk, so batches
      // up to `want` may be in the chunk it handed over. Sync that one too
      // before reporting them.
      syncPrevious();
    } catch {
      Atomics.store(state, FAILED, 1n);
    }
    done = want;
    Atomics.store(state, DONE, done);
    Atomics.notify(state, DONE);
    postMessage(done);
  }
};

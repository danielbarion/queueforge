/**
 * Write and `fdatasync` the overwrite log off the event loop.
 *
 * The main thread lays out each batch of records in the shared staging ring,
 * crc fields still zero, and publishes a job: the batch sequence number, the
 * chunk fd, the file offset and the staged range (its position, released in
 * `state[STAGE_TAIL]`, and its offset in the ring). This thread takes the jobs
 * in order, fills in each record's salted crc, writes the bytes, then syncs
 * every file it wrote and reports the highest batch it covered in
 * `state[DONE]`. Jobs published during one round share the next round's
 * syncs, which is the group commit.
 *
 * A batch too large to stage is written by the main thread; its job carries
 * no staged bytes and only joins the next sync.
 *
 * A rotation publishes a close job for the old chunk after that chunk's last
 * write job, so this thread syncs and closes the file only once all of its
 * writes are done.
 *
 * The main thread raises `state[DIR_CHANGES]` when the log directory changed
 * (a chunk created, renamed into place or removed). Before the next sync this
 * thread syncs the directory, so a batch in a new or reused chunk is not
 * reported before its file name is durable. `state[WAITING]` is 1 while this
 * thread waits, so the main thread wakes it only then.
 */
import { closeSync, fdatasyncSync, fsyncSync, openSync, writeSync } from "node:fs";
import { fillCrcs } from "./log-crc.ts";

declare const self: Worker;

/** Slots of the shared state. */
const JOB_HEAD = 0;
const DONE = 1;
const JOB_TAIL = 2;
const FAILED = 3;
const STAGE_TAIL = 4;
/** Completed syncs, and their total nanoseconds, for the metrics page. */
const SYNCS = 5;
const SYNC_NS = 6;
const DIR_CHANGES = 7;
const WAITING = 8;

/** Fields of one job; see `FastLog.pushJob`. */
const JOB_FIELDS = 8;
const KIND = 0;
const SEQ = 1;
const FD = 2;
const FILE_AT = 3;
const STAGE_AT = 4;
const LEN = 5;
const SALT = 6;
/** The staged bytes' offset in the ring; `STAGE_AT` is their position, which only grows. */
const RING_AT = 7;
const WRITE = 1;
const CLOSE = 2;

export type SyncWorkerInit = { state: SharedArrayBuffer; jobs: SharedArrayBuffer; stage: SharedArrayBuffer; delayMs: number; dir: string };

self.onmessage = (event: MessageEvent<SyncWorkerInit>) => {
  const state = new BigInt64Array(event.data.state);
  const jobs = new Float64Array(event.data.jobs);
  const slots = jobs.length / JOB_FIELDS;
  const stage = Buffer.from(event.data.stage);
  const dir = event.data.dir;
  // Tests only: hold each round this long, as a stalled disk would.
  const delayMs = event.data.delayMs;
  let dirSynced = 0n;
  /** Make the directory's entries durable once it changed since the last time. */
  const syncDir = () => {
    const changes = Atomics.load(state, DIR_CHANGES);
    if (changes === dirSynced) return;
    const fd = openSync(dir, "r");
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    dirSynced = changes;
  };
  let tail = Number(Atomics.load(state, JOB_TAIL));
  let done = Atomics.load(state, DONE);
  for (;;) {
    // Raised before the wait: a job published after it sees the flag and
    // wakes this thread; one published before it fails the wait's value check.
    Atomics.store(state, WAITING, 1n);
    Atomics.wait(state, JOB_HEAD, BigInt(tail));
    Atomics.store(state, WAITING, 0n);
    const head = Number(Atomics.load(state, JOB_HEAD));
    // A negative head is close.
    if (head < 0) return;
    if (head === tail) continue;
    if (delayMs) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
    const started = performance.now();
    const written = new Set<number>();
    let failed = false;
    let covered = done;
    for (; tail < head; tail++) {
      const o = (tail % slots) * JOB_FIELDS;
      const fd = jobs[o + FD]!;
      const len = jobs[o + KIND] === WRITE ? jobs[o + LEN]! : 0;
      try {
        if (jobs[o + KIND] === CLOSE) {
          // Its last write job came before this one.
          if (written.delete(fd)) fdatasyncSync(fd);
          closeSync(fd);
        } else {
          if (len > 0) {
            const at = jobs[o + RING_AT]!;
            const bytes = stage.subarray(at, at + len);
            fillCrcs(bytes, jobs[o + SALT]!);
            let off = 0;
            while (off < len) {
              const n = writeSync(fd, bytes, off, len - off, jobs[o + FILE_AT]! + off);
              if (n <= 0) throw new Error("durable log short write");
              off += n;
            }
          }
          written.add(fd);
          const seq = BigInt(jobs[o + SEQ]!);
          if (seq > covered) covered = seq;
        }
      } catch {
        failed = true;
      } finally {
        // The staged bytes are in the page cache (or lost with this round's
        // failure): the main thread may stage over them.
        if (len > 0) {
          Atomics.store(state, STAGE_TAIL, BigInt(jobs[o + STAGE_AT]! + len));
          Atomics.notify(state, STAGE_TAIL);
        }
      }
      Atomics.store(state, JOB_TAIL, BigInt(tail + 1));
    }
    try {
      syncDir();
      for (const fd of written) fdatasyncSync(fd);
      Atomics.add(state, SYNCS, 1n);
      Atomics.add(state, SYNC_NS, BigInt(Math.round((performance.now() - started) * 1e6)));
    } catch {
      failed = true;
    }
    if (failed) Atomics.store(state, FAILED, 1n);
    done = covered;
    Atomics.store(state, DONE, done);
    Atomics.notify(state, DONE);
    postMessage(done);
  }
};

# Pending work

Open problems found by the benchmarks, for any session (human or AI) to pick up.
Each item says what is wrong, the evidence, where to look, and how to prove a fix.
Finished items are removed; their history is in git and in `BENCHMARK.md`.

Numbers come from the published tables (`BENCHMARK.md`, "Session (2026-10-12)";
raw cells in `bench/results/2026-10-12/`, Bun's from the 2026-10-14b re-run in
`bench/results/2026-10-14b/`) unless an item names another run. "Best" is the
best broker in that row.

## How to use this file

- **Claim an item** before working on it: set `Status: in progress (<who>, <date>)`.
  Two sessions on the same item collide on the same files.
- **Stay in scope.** Each item names the engine it belongs to. Don't fix Bun
  while working a Rust item.
- **Measure before and after** with the same harness and image tags (an A/B), not
  against numbers from another day. Bun varies a lot between runs (see B6), so
  repeat a cell at least twice.
- **Done means:** the engine's test suite passes, conformance has not regressed
  (`cd conformance && bun run.ts <engine>`), and the A/B shows the gain. Then
  delete the item, put the before/after numbers in the commit message, add the
  change to "Waiting for the next full run" below, and update `BENCHMARK.md`
  once a published run carries it.
- **Don't publish numbers you didn't measure.** If a cause is a guess, say so here.

Current tree (2026-10-14): Bun suite 278/278, Rust 445/445, PHP 40/40 files;
conformance Bun, Rust and PHP 135/135, RabbitMQ 134/135 (no delayed-message
plugin).

### Measurement rules

- **One benchmark at a time.** `bench/run.py` and `bench/ab.py` hold
  `/tmp/queueforge-benchmark.lock` themselves and refuse to start while it
  exists; don't create it by hand before them. Lightweight tests can run
  alongside; CPU-heavy ones can't.
- **A/B from fixed images.** Build baseline and candidate from known source
  (`python3 bench/ab.py <kind> before=<image> after=<image> --scenarios ...
  --repeat 3 --cpus 1`), alternate the cells, at least two repeats each (five
  for latency). Keep captures outside `bench/results`. Report the median and
  the range, failures included.
- **Daytime work is 1 CPU only.** The host is in use during the day; 4 CPU cells
  and full runs are for the overnight run, which the user starts.
- **Don't buy speed with semantics.** FIFO, priority, TTL, requeue, transaction
  order, confirms after the durable commit and permissions must hold.
- **Throughput is not enough.** Record delivery completeness and backlog growth;
  latency comparisons must keep the offered rate. Scenario rows also record disk
  MB/s and fsyncs per cell (`disk_mb_s`, `fsyncs_s`, `fsync_ms`).
- See `docs/benchmark-repeats.md` for how repeats, run directories and resource
  sampling work.

### Benchmark commands

The full run is `python3 bench/run.py` (README, "Running the benchmarks"),
about 3½ hours; progress is in `bench/last-run.json`. `python3 bench/run.py
--quick` measures only brokers whose image changed, one repeat each, and writes
`compare.md`. AI sessions: ask the user to start either one, then read the
results once they say it finished. Single cells:

```
docker compose -f docker-compose.bench.yml build <bun|rust|php>
python3 bench/scenarios.py one <scenario> <rabbit|rust|bun|php>       # one cell, 1 CPU / 1 GiB
QF_BENCH_OUT=/tmp/ab python3 bench/scenarios.py one <scenario> bun    # keep cells out of bench/results
python3 bench/disk.py                                                 # disk write+fdatasync ceiling
python3 bench/report.py                                               # tables + site/app/bench-data.ts
```

Scenario ids: `work-queue telemetry fanout-20 routing-64 large-64k large-1m
large-64k-quorum large-1m-quorum latency-1k latency-10k many-queues
slow-consumers quorum stream priority transactions heavy backlog` (definitions
in `bench/scenarios.py`).

Shell note for AI sessions: zsh heredocs containing `'''` or `"""` can hang the
tool shell. Write scripts to a file and run the file.

---

## Waiting for the next full run

2026-10-14c (Bun only, image `90340fd`, results in `bench/results/2026-10-14c/`,
not published) against 2026-10-14b, medians of two repeats: 1 CPU large-1m
548 → 1,064 MB/s, large-64k 527 → 856, quorum +20%, work-queue +15%; 4 CPU
work-queue 165,339 → 203,577 msg/s, fanout-20 +9%. Three cells fell and are
not explained yet: 4 CPU heavy 302k → 238k msg/s (−21%), 4 CPU quorum −28%
(the 14b repeats spread 55k–85k), 1 CPU transactions −18% (a 1 CPU A/B of
`90340fd` against `f0a8d6e` showed both images bimodal, 37k–65k, so not B4).
A 4 CPU A/B of heavy and quorum across `881a2eb` (the 14b code), `7ef8c7c`,
`eb4760e` and `90340fd`, three interleaved repeats (2026-10-15), found no
regression: heavy 255,931 / 224,975 / 267,083 / 284,198 msg/s, quorum 64,756 /
70,776 / 65,676 / 70,225, every image spreading as widely (quorum 28k–85k,
heavy 180k–332k). The 14c drops were the host. It did show what B4 costs:
4 CPU heavy anonymous memory 466–511 → 616–878 MiB, p99 about 140 → 180 ms. The connections step stopped at
4 CPU / 8 GiB when Docker's engine returned 500s; 4 GiB held 249,768 (14b
247,534).

Measured in A/Bs, not yet in the published tables:

- Bun: retired durable-log chunks are reused (`a8e3962`). 1 CPU: large-64k
  403.8 → 468.3 MB/s, large-1m 387.5 → 618.6. Then the directory fsync left
  the event loop for the sync worker, which runs it before the next commit
  that covers a new or reused chunk (a fifth of the 64 KiB profile was that
  call), and the worker is woken only when it waits. 1 CPU: large-64k
  566.4 → 590.4 MB/s, large-1m 620.3 → 680.7, work-queue 135,485 →
  144,419 msg/s; latency-1k p99 1.68 → 1.82 ms and latency-10k 1.97 → 0.76
  ms over five repeats, ranges overlapping. Raft groups also compact by
  applied bytes, as PHP's. Without Raft, a quorum consumed id is forgotten once
  every configured member accepted its drop and a forget round (every second)
  made every member's delete durable; before, the list grew forever. Ids whose
  drop missed a member stay listed and are retried when a peer connects.
- Rust: the group commit also starts on 1 MiB of waiting bodies (`a0fc4ed`),
  then compacted WAL segments are reused instead of zero-filled (`5c8fd4d`).
  1 CPU: large-64k 254.6 → 553.4 → 848.8 MB/s, large-1m 524.9 → 700.8 →
  762.0 (separate A/Bs, so the steps are not additive).
- PHP: Raft groups compact by applied bytes (`2b4c899`). large-1m-quorum: no
  delivery → 172 MB/s.
- Rust: a quorum ack on a broker without a cluster no longer waits for the
  actor's report (only the cluster's drop uses it), and with Raft on, or no
  other member, no consumed id is kept (the set grew by one per ack). 1 CPU
  quorum 90,842 → 135,402 msg/s, p99 5.1 s → 3.1 ms, anonymous 790 → 8 MiB:
  delivery now keeps up with confirms (it was "missed rate").
- Rust: a delivery wake that staged less than 16 KiB yields once and takes
  what the queue actors add in the same write (a send per wake was a third of
  the 1 CPU telemetry profile); frames are parsed from an offset and the
  consumed prefix dropped once per read (a `drain` per frame moved the rest of
  the buffer three times per publish); AMQP sockets get a 1 MiB receive
  buffer. Telemetry's backlog was in the kernel: unconfirmed publishers filled
  auto-tuned receive buffers to ~27 MB each, every message waited ~2 s, and
  the broker's queue stayed empty. 1 CPU: telemetry 91,941 → 202,949 msg/s,
  p99 3.66 s → 123 ms; work-queue +7%, transactions 40,696 → 51,733, latency,
  slow-consumers and many-queues within their ranges, large-1m within 1%.
- Rust R6: a queue whose confirm window is full (5+ waiters, mailbox quiet
  for 1 ms) syncs then instead of on the 10 ms tick. Each routing-64 queue
  synced once per tick, so the cell sat at 64 x 20 x 100 = 128k msg/s with
  half the CPU idle. 4 CPU: routing-64 124,551 → 216,719 msg/s (CPU 166% →
  294%, confirm p50 9.9 → 4.9 ms), fanout-20 +15%; 1 CPU within range. A
  process-wide `syncfs` group commit was slower in every cell and was dropped.
- Rust per-message costs: the queue actor caches its deliver and ack counter
  handles (a `counter!` per message built and hashed two label strings), and
  a connection reads the clock once per drained read instead of per frame.
  1 CPU: telemetry 203,241 → 218,120, work-queue, routing-64, fanout-20 +5%.
- PHP: with confirms waiting, the loop selects for at most 1 ms and a pass
  that reads nothing commits them; it used to sleep to the 10 ms interval,
  so publishers with full windows waited a tick per batch. 1 CPU work-queue
  36,108 → 54,787 msg/s (CPU 50% → 73%); latency cells within range.
- PHP: routing looks bindings up by exchange instead of filtering every
  binding per publish (13% of routing-64). 1 CPU: routing-64 +14%, fanout-20
  +13%, heavy +8%. Writing each connection once per pump instead of per
  delivery was tried with it: routing +15% but fanout-20 -15%, not kept.
- Harness: `QF_BENCH_VM_CPUS=4` keeps Docker Desktop at 4 CPUs / 12 GiB for
  A/B runs while the machine is in use (broker CPU 0, client 1-2, sidecars 3;
  1 CPU cells only). Published runs keep the 8 CPU default.
- Harness: the `disk` step, per-cell disk and fsync columns, and the
  `large-64k-quorum` / `large-1m-quorum` cells (`660e589`). The site's average
  score will include the two new cells.

---

## Bun

### B4. One queue is limited to one core

Status: in progress. Chosen (2026-10-14): move bounded log work off the queue's
thread, keeping one owner per queue. Done: the sync worker now fills in the
record crcs and writes the batch from a shared staging ring (1 MiB, growing to
8 MiB for large batches); rotations queue a close job for the old chunk. A/B
at 1 CPU, three repeats: large-64k 532.2 → 572.0 MB/s, large-1m 618.1 →
891.4, work-queue 136,850 → 143,903 msg/s, latency-1k p99 within the range.
4 CPU (2026-10-15, against the 14b code): heavy +11%, quorum +8%, work-queue
165,339 → 203,577 in the 14c run. Cost: anonymous memory +23–44 MiB per cell
at 1 CPU, +150 MiB in 4 CPU heavy (four processes). Not the ring: untouched
SharedArrayBuffer pages are not resident, and a variant that never stages has
the same increase in the container. Locally the extra is in the staging path;
a table crc in the worker (no view per record) saved ~10 MiB in work-queue but
nothing at 64 KiB and costs worker CPU that shares the core at 1 CPU, so it
was not kept. Hypothesis, not proven: the main heap now allocates one buffer
less per batch, so its GC runs later and keeps more released bodies.

Tried 2026-10-15, 2 CPU broker: leaving publisher connections on the process
that accepted them, forwarding each publish to the queue's home, instead of
moving the connection there. work-queue 202,387 → 183,090 msg/s at more CPU
(119% → 157%), routing-64 196,962 → 178,820; heavy mixed. A forwarded publish
costs the home more than parsing it. Not kept.

A queue lives on one child process, so a single busy queue gets one JavaScript
thread however many cores the container has.

Evidence: work-queue (one queue) at 4 CPU 165,339 msg/s at 1.3 of 4 CPUs used;
Rust 197,010 at 3 CPUs. The 16-queue shape does scale (522,318 at 4 CPU).

Where: the cores supervisor (`bun/src/main.ts`, `bun/src/supervise.ts`) and
`queueHome()`.

### B10. Quorum in cores mode pays for a Raft group of one

Status: open, needs a design. With one process per core (2 CPU or more), each
quorum queue is a single-voter Raft group on a sibling process
(`siblingsOnly`, `soloQueueGroup`). A publish writes a JSON entry with a
base64 body to the group's log (`enqueueQuorumRaft`, `stringify`,
`encodeRecords`, `crc32`, an `fsyncSync` on that log) and the message again to
the queue's store. 2 CPU (2026-10-15, broker on 2 CPUs, client on 2 others):
quorum 83,811 msg/s against work-queue 201,992; 4 CPU published 86,567, below
1 CPU 129,627. A one-voter group could skip the Raft entry and use the
queue's own log as its durable copy, as a lone broker's quorum queue now does,
but recovery, failover and membership change (B3's rules in
`docs/bun-scaling.md`) have to be designed and tested for it first.

### B5. Large messages: CPU per byte

Status: open. Chunk reuse and the off-loop directory sync are in the tree
(see above). What the 1 CPU 64 KiB profile shows now, main thread: socket
`write` of deliveries 16%, `crc32` of log records 9%, the body copy in
`concat` (`finishPublish`) 9.5%, three more copies of ~3% each (`absorbFresh`,
`writeRecord`, `encodeDeliver`). Tried 2026-10-14: keeping a single-chunk body
as a view of the inbound buffer instead of `slice()` (no copy when the body is
at least half the buffer). A/B at 1 CPU, three repeats: large-64k 623.5 →
427.5 MB/s, work-queue 138,309 → 117,094 msg/s, large-1m −1%, ranges
overlapping; reverted. Fewer copies did not mean more throughput, as with
the gather copy before.

Evidence: 1 CPU large-64k 526.3 and large-1m 547.5 MB/s against RabbitMQ 778.0
and 1,345.8, with Bun at 85–93% CPU. Part of the gap is durability: RabbitMQ
classic confirms without an fsync, and at 64 KiB it writes 0.4 MB/s because its
consumers keep up.

Where: the body is copied on the way in (`concat` in `bun/src/amqp/publish.ts`),
into the log (`encodeRows` in `bun/src/fast-log.ts`), and on the way out
(`bodyFrame` in `bun/src/codec.ts`, the gather buffer in `flushCoalesced` in
`bun/src/amqp/frames.ts`). Writing large staged frames without the gather copy
measured slower (348 vs 404 MB/s at 64 KiB) and was reverted: profile first.

### B7. Low-rate latency: one thread stalls every socket

Status: open, mostly the host.

Evidence: 1 CPU p99 latency-1k 2.00 ms and latency-10k 1.99 ms against Rust
1.08 and 1.07; slow-consumers 12.25 ms against Rust 2.25 (0.4–15.5 ms across
repeats of one image). An idle container with only a 1 ms timer sees 6–24 ms
stalls about once a second on Docker Desktop (the VM descheduling the vCPU);
GC cycles are p99 5.7 ms and there is no cgroup throttling. A stall in Bun's one
JavaScript thread delays every socket; Rust's other threads keep serving.

2026-10-15, with the host disk healthy: an idle Bun broker shows 5–17 ms loop
stalls about twice a second, and so do a bare Bun timer loop and a bare Python
loop in the same 1 CPU container (5–11 ms about once a second). The stalls are
the VM, not Bun's code; Rust keeps serving through them on its other threads.
latency-10k p99 2.8 ms (Rust 0.7 ms) in the 2026-10-15 run.

Where: the lone-publish path (`SOCKET_WAIT_MS`, lone mode in `driveInbound`,
`bun/src/amqp/frames.ts`; `armLoneFlush`, `SMALL_GROUP` in `bun/src/store.ts`).
Anything that shortens the work done per event-loop turn helps; the VM stall
itself can't be fixed in the broker.

### B6. Peak memory and latency vary between runs

Status: open, cause unknown. Five fixed-image repeats did not tie high cgroup
peaks to process RSS or heap. Hypothesis, not a diagnosis: JavaScriptCore GC
timing. Freeze one image and run a cell five times, logging
`process.memoryUsage()` and `bun:jsc` `heapStats()` over time.

### B9. Fanout stall at 1 CPU

Status: watch. Not reproduced since 2026-10-12 (22 repeats passed). Once, one
fanout-20 repeat stopped about 2 s in at 2% CPU. If it stalls again, rerun with
`QF_BENCH_PROFILE=1` and read the per-connection state in the broker log before
changing code.

---

## Rust

---

## PHP

### P8. One process, one core

Status: open, architectural. PHP runs one process unless `QUEUEFORGE_CORES` is
set, so its rates do not grow with the container: 16-queue load 75,296 msg/s at
1 CPU, 82,387 at 4 CPU. The per-core mode measured worse in the scenario cells
and is off by default.

### P9. Per-frame cost

Status: open. One connection moves about 11,800 msg/s at every size; Rust
151,312 at 1 CPU. The frame parse and dispatch run in PHP userland.

### P10. Latency: the fsync runs on the event loop

Status: open. 1 CPU p99 latency-1k 20.69 ms, latency-10k 53.74 ms,
slow-consumers 260.07 ms (Rust 1.08, 1.07, 2.25). PHP has no threads, so the
group-commit fsync (10 ms interval) blocks every socket while it runs, and a
prefetch-1 worker waits for it on every round trip.

### P11. Quorum throughput

Status: improved, open. The voter scan read the whole Raft log on every
proposal (76% of the profile); it now reads only new entries. 1 CPU quorum
8,124 → 29,569 msg/s. What remains is mostly encoding each entry as JSON
(`RaftNode::logBytes`, 12%) and socket writes (12%); Bun does 125k.

---

## Benchmark harness

### H5. Throughput through a failover

Status: open. No harness measures any engine while a quorum leader dies.
`bun/test/quorum-failover.test.ts` and `php/test/quorum-failover.test.php`
prove correctness (every confirmed body delivered once), not rate.

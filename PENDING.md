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

Current tree (2026-10-14): Bun suite 274/274, Rust 444/444, PHP 40/40 files;
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
  every configured member accepted its drop (it had grown forever); a drop sent
  while a member is away is kept by every receiver and retried when a peer
  connects.
- Rust: the group commit also starts on 1 MiB of waiting bodies (`a0fc4ed`),
  then compacted WAL segments are reused instead of zero-filled (`5c8fd4d`).
  1 CPU: large-64k 254.6 → 553.4 → 848.8 MB/s, large-1m 524.9 → 700.8 →
  762.0 (separate A/Bs, so the steps are not additive).
- PHP: Raft groups compact by applied bytes (`2b4c899`). large-1m-quorum: no
  delivery → 172 MB/s.
- Harness: the `disk` step, per-cell disk and fsync columns, and the
  `large-64k-quorum` / `large-1m-quorum` cells (`660e589`). The site's average
  score will include the two new cells.

---

## Bun

### B4. One queue is limited to one core

Status: open, needs a design choice. Options are in `docs/bun-scaling.md`.

A queue lives on one child process, so a single busy queue gets one JavaScript
thread however many cores the container has.

Evidence: work-queue (one queue) at 4 CPU 165,339 msg/s at 1.3 of 4 CPUs used;
Rust 197,010 at 3 CPUs. The 16-queue shape does scale (522,318 at 4 CPU).

Where: the cores supervisor (`bun/src/main.ts`, `bun/src/supervise.ts`) and
`queueHome()`.

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

### R5. Telemetry: per-message CPU

Status: open, not profiled.

Evidence: telemetry (4 producers, no confirms, transient 256 B, auto-ack)
94,260 msg/s at 1 CPU and 175,799 at 4 CPU, with the broker at 99% and 330%
CPU; Bun 544,774 / 479,113, RabbitMQ 104,101 / 326,615. At 1 CPU the publishers
sent less than was delivered (85,916 sent, 94,143 received), so publishers were
held back.

Guess, to check with a profile: the per-message hops between the connection
task, the queue actor and the consumer task (`queueforge-core/src/queue/actor`,
`queueforge-broker/src/connection`), where Bun handles a batch per event-loop
turn.

### R6. Routing and fanout at 4 CPU use half the cores

Status: open, not profiled.

Evidence: 4 CPU routing-64 123,730 msg/s and fanout-20 183,530 deliveries/s
at 182–204% of 400% CPU; Bun 295,157 / 490,062, RabbitMQ 139,264 on routing.
Something makes the broker wait: lock contention, or fsyncs that serialize.
Needs the overnight run to measure.

### R7. Quorum delivery falls behind

Status: open, not profiled.

Evidence: the quorum cell is "missed rate" at both sizes. 1 CPU 85,211–87,126
confirmed/s, 75,479–79,628 delivered; 4 CPU 145,620–146,920 confirmed/s,
77,356–86,320 delivered, so the queue grows. Bun keeps up (101,988 at 1 CPU).

Where: the quorum apply and delivery path (`queueforge-broker/src/cluster/`).

### R8. Transactions

Status: open, not profiled. 1 CPU 35,336 msg/s at 66% CPU; Bun 57,323. Partly
waiting rather than working.

### R9. Raft compaction by entry count only

Status: open. `compact_after` in `queueforge-broker/src/cluster/raft_apply.rs`
counts entries only, so a group of large bodies holds up to 10,000 of them. PHP
(`2b4c899`) and Bun now also compact after 64 MiB of applied bodies.

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

Status: open. 1 CPU quorum 8,414 msg/s; Bun 101,988. Each publish is a JSON
Raft entry with a base64 body, encoded and persisted in PHP code
(`php/src/RaftNode.php`, `php/src/Broker.php`).

### P12. List endpoints are not paged

Status: open, small. The management list endpoints return the
`{items, total_count}` shape but ignore `page` and `page_size`. The console and
`php/test/mgmt.test.php` read that shape.

---

## Benchmark harness

### H5. Throughput through a failover

Status: open. No harness measures any engine while a quorum leader dies.
`bun/test/quorum-failover.test.ts` and `php/test/quorum-failover.test.php`
prove correctness (every confirmed body delivered once), not rate.

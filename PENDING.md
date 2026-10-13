# Pending work

Open problems found by the benchmarks, for any session (human or AI) to pick up.
Each item says what is wrong, the evidence, where to look, and how to prove a fix.
Numbers come from `BENCHMARK.md`, section "Session (2026-10-11)", and the raw
cells in `bench/results/2026-10-11/`, unless an item names an older run.

## How to use this file

- **Claim an item** before working on it: set `Status: in progress (<who>, <date>)`.
  Two sessions on the same item collide on the same files.
- **Stay in scope.** Each item names the engine it belongs to. Don't fix Bun
  while working a Rust item.
- **Measure before and after** with the same harness and image tags (an A/B), not
  against numbers from another day. Bun varies a lot between runs (see B6), so
  repeat a cell at least twice.
- **Done means:** the engine's test suite passes, conformance has not regressed
  (`cd conformance && bun run.ts <engine>`, then `bun run.ts --report`), and the
  A/B shows the gain. Then move the item to "Done" at the bottom with the date and
  the before/after numbers, and update `BENCHMARK.md` if the published numbers change.
- **Don't publish numbers you didn't measure.** If a cause is a guess, say so here.

Current tree (2026-10-13, Cortex Code): Bun suite 206/206, Rust 437/437, PHP
38/38 files; conformance Bun 135/135, Rust 135/135, PHP 135/135, RabbitMQ
134/135 (no delayed-message plugin). Published numbers: `BENCHMARK.md` section
"Session (2026-10-12)"; raw cells in `bench/results/2026-10-12/`, Bun's from the
2026-10-13 re-run in `bench/results/2026-10-13/` (two repeats per cell, median shown).

### Measurement rules

- **One benchmark at a time.** Take the host lease before building benchmark
  images, resizing Docker or measuring: `mkdir /tmp/queueforge-benchmark.lock`,
  and write the owner, purpose and start time into it. If it exists, someone else
  is measuring. Release it after child processes stop and Docker is back at
  2 CPUs / 8 GiB. Lightweight tests can run alongside; CPU-heavy ones can't.
- **A/B from fixed images.** Build baseline and candidate from known source,
  record the image digests, alternate the cells, at least two repeats each (five
  for latency). Keep A/B captures outside `bench/results` (`QF_BENCH_OUT`).
  Report the median and the range, failures included.
- **Don't buy speed with semantics.** FIFO, priority, TTL, requeue, transaction
  order, confirms after the durable commit and permissions must hold. A gain that
  weakens them is a regression.
- **Throughput is not enough.** Record delivery completeness and backlog growth;
  latency comparisons must keep the offered rate.
- See `docs/benchmark-repeats.md` for how repeats, run directories and resource
  sampling work.

### Benchmark commands

The full run is `python3 bench/run.py` (see the README, "Running the benchmarks"),
about 3¼ hours; it reports progress in `bench/last-run.json`. To check a change,
ask for `python3 bench/run.py --quick` instead. It measures only brokers whose
image differs from the newest full results, one repeat each (about 15 minutes per
broker), and writes `compare.md` in its results directory (`compare` in
`bench/last-run.json`). AI sessions: ask the user to start either one rather than
running it from the chat, then read that file once they say it finished. Single
cells for A/B work:

```
docker compose -f docker-compose.bench.yml build <bun|rust|php>
python3 bench/scenarios.py one <scenario> <rabbit|rust|bun|php>   # one cell, 1 CPU / 1 GiB
QF_BENCH_OUT=/tmp/ab python3 bench/scenarios.py one <scenario> bun  # keep A/B cells out of bench/results
QF_BENCH_SIDECARS=<containers to keep> python3 bench/run-all.py --kinds bun --skip connections
python3 bench/report.py                                             # tables + site/app/bench-data.ts
```

Scenario ids: `work-queue telemetry fanout-20 routing-64 large-64k large-1m
latency-1k latency-10k many-queues slow-consumers quorum stream priority
transactions heavy backlog` (definitions in `bench/scenarios.py`). The 4 CPU
cells need Docker Desktop at 8 CPUs; `bench/run-all.py` resizes it and restores
2 CPUs at the end.

Shell note for AI sessions: zsh heredocs containing `'''` or `"""` can hang the
tool shell. Write scripts to a file and run the file.

---

## Bun

### B1. No publisher flow control

Status: implemented, final A/B not run. Destination-aware publish holds, bounded
pending/transaction work, ACK-safe reads and socket output credit are in the tree
(Codex, 2026-10-09/10). In the 2026-10-11 run at 1 CPU, telemetry is 450k msg/s
at 59 MiB anonymous (RabbitMQ 103k) and transactions 53.6k (RabbitMQ 20.5k); a
dedicated before/after with the backlog slope is still missing.

Publishers that don't wait for confirms (telemetry), or that publish inside
transactions, outrun the consumers, and the queue grows without bound. RabbitMQ's
credit flow slows those publishers down to what the queues can process.

Evidence (1 CPU / 1 GiB):
- telemetry: p50 54–472 ms across passes, 790+ MiB anonymous memory; RabbitMQ p50 66 ms.
- transactions: 10.8k–37k msg/s and p50 0.67–2.6 s across passes; RabbitMQ 19.2k
  at p50 0.42 ms. In an A/B on a quiet host the same cell ran at 46.5k, p50 0.35 ms.
  Once the queue passes 2,048 ready messages, new messages are paged to SQLite
  (B8), which slows delivery further and feeds the backlog.

Where: `bun/src/amqp/frames.ts`. `READY_BACKLOG` (256), `HOLD_MS` (20) and
`readyBacklog()` only hold a connection that also has unacked deliveries of its
own, so a publish-only connection is never held.

Idea: hold reading from a publishing connection while the queues it routes to
are past a ready depth and have active consumers. Don't hold when no queue has a
consumer: the backlog scenario must still fill at full speed.

Verify: telemetry p50 near RabbitMQ's, with memory flat; transactions with the
queue not growing (`kept=yes`); backlog fill not slower.

### B2. Fanout on more than one core

Status: open, improved. 4 CPU fanout-20 171k deliveries/s at 372 MiB in the
2026-10-12c run (`BENCHMARK.md`, "Bun re-measured (2026-10-13)"); 169k at 379 MiB on 2026-10-13. It had
grown to 2 GiB: a message forwarded to one queue on another process got the
id `""`, so every such message in that queue shared it and its rows were never
deleted (`7b571bf`). 278k when forced to one process.

With one child process per core, a fanout exchange's queues are spread across
the children, so most copies of each publish cross processes as base64 JSON.

Evidence: fanout-20 at 4 CPU delivers 28.6k–30.1k deliveries/s, against Bun's own
215–236k at 1 CPU, Rust 189k and RabbitMQ 144k at 4 CPU. The parallel, batched
forwards (one `enqueue` with `queues: [...]` per process, feature `enqueue_many`)
already lifted it from 6.9k.

Where: `bun/src/broker/routing.ts` `queueHome()` (a hash of vhost and queue
name); `bun/src/broker/publish.ts` `publishSlow`; `bun/src/amqp/frames.ts`
`exchangeHome()` (moves a connection only when every routed queue shares a home);
`bun/src/cluster.ts` (the `enqueue` handler).

Ideas, cheapest first:
1. A binary forward frame instead of base64 JSON.
2. Home every queue bound to one fanout exchange on the same child. This changes
   placement, and must keep working for queues already declared.

Verify: `test/fanout-cores.test.ts` still passes (order and completeness); the
fanout-20 cell at 4 CPU.

### B3. Quorum queues on more than one core

Status: implemented, partly better (Cortex Code, 2026-10-12). Worker processes
of one supervisor share a broker id (`data/broker-id`); a quorum queue declared
there gets a single-voter group, `q:s1:<voter>:<vhost>:<name>`, whose voter is
its leader, and the other workers forward to it. Multi-node clusters keep
whole-membership groups. `bun/test/quorum-cores.test.ts` 2/2 (one voter,
messages kept across a restart); Bun suite 196/196. Quick run (one repeat): 4 CPU
quorum 11.4k → 16.8k msg/s. That is still far below one process (97.9k) and
RabbitMQ (89.8k): the voter is a single worker, and every publish arriving on
another worker is forwarded to it. Next: profile the 4 CPU cell; compare with
routing quorum publishes to the voter's worker at connection handoff, as PHP
does for classic queues.

Before: same image at 4 CPU, one process (`QUEUEFORGE_CORES=1`) 97.9k msg/s, two
13.6k, four 11.8k.

Every quorum queue gets a Raft group across all of the sibling child processes,
which share one disk. That means more fsyncs and IPC for no added safety on one
node. RabbitMQ on one node runs a quorum queue with a single member.

Evidence: quorum at 4 CPU delivers 12.0k–12.5k msg/s, against 81k–88k at 1 CPU;
RabbitMQ 89.8k and Rust 73.6k at 4 CPU.

Where: `bun/src/raft/glue.ts` `groupForNewQueue()`; `bun/src/raft/node.ts`
`queueGroup()`. Group membership is the whole member list.

Idea: when every member of the cluster is a sibling process of one broker (the
cores supervisor in `bun/src/main.ts`), make each queue's group a single member,
its home, and forward to it as classic queues do. Keep multi-node clusters as they
are. Check `test/stream-raft.test.ts`, `test/mixed-raft.test.ts` and
`test/raft-snapshot.test.ts`.

Verify: quorum cell at 4 CPU; the Raft tests; conformance.

### B4. One queue is limited to one core

Status: open, design only. Options are written up in `docs/bun-scaling.md`; no
sharding implementation.

A queue lives on one child process, so a single busy queue doesn't scale.

Evidence: work-queue (one queue) 92.6k–111.9k msg/s at 1 CPU, 95.3k–101.5k at
4 CPU; Rust goes from 125k to 181k. The 16-queue load shape does scale (585k at 4 CPU).

Where: the cores supervisor (`bun/src/main.ts`, `bun/src/supervise.ts`) and
`queueHome()`. This is architectural; write up options here before changing code.

### B5. Large messages

Status: open, improved. The inbound single-chunk body no longer copies
(2026-10-10); no further measured change.

Evidence (2026-10-09 run): 64 KiB messages 312–376 MB/s and 1 MiB messages 363–407 MB/s, against
RabbitMQ 634 and 1,240 MB/s. Part of the gap is that RabbitMQ confirms before
writing to disk and QueueForge after. Bun also runs at 74–79% CPU on these cells,
where Rust uses 33–43%, so Bun spends CPU per byte.

Where: the body is copied on the way in (`concat` in `bun/src/amqp/publish.ts`),
into the log (`bun/src/fast-log.ts`), and on the way out (`bodyFrame` in
`bun/src/codec.ts`, the gather buffer in `flushCoalesced` in
`bun/src/amqp/frames.ts`).

Already tried: writing large staged frames without the gather copy. It measured
slower (348 vs 404 MB/s at 64 KiB) and was reverted. Profile before trying again.

### B11. Memory against Rust (2026-10-13)

Status: partly done (Cortex Code, `1473d9e`). Bun 1.4 is Rust, but the broker
runs on JavaScriptCore: idle is ~22 MiB anonymous (plus the 41 MiB binary,
file-backed), and every message, callback and promise is a GC object.

- Done: quorum consumed ids are no longer kept without a peer that could hold
  a copy (lone broker, or sibling workers with single-voter groups). The set
  never shrank: 1 CPU quorum ~660 MiB after 25 s, now 79 MiB.
- Open, clusters: with real peers the consumed-id set still grows without
  bound (`bun/src/broker/consumed-set.ts`). Trim an id once every peer has
  applied its drop, or after the peers' next hello.
- Measured, not adopted: `BUN_JSC_smallHeapGrowthFactor` (default 2) trades
  throughput for memory at 1 CPU, where cells run at 95% CPU. 3: quorum,
  work-queue, telemetry, heavy +5–11% rate, +30–50% memory. 4: +7–9% rate,
  about twice the memory. `MIMALLOC_PURGE_DELAY=0`: 5–10% less memory, 3–9%
  slower. `--smol`: no saving.
- What remains, by cell: backlog ~660 bytes of JS objects per queued message
  plus the 48 MiB shadow-body cap (freed once drained); heavy ~110k closures and
  ~55k promises for ~7k unacked messages (the per-publish confirm chains).

### B6. Peak memory and latency vary between runs

Status: open, cause unknown. Five fixed-image repeats of work-queue, latency-1k
and latency-10k (Codex, 2026-10-10) did not tie the high cgroup peaks to sampled
process RSS or heap. Resource samples now use the Docker Engine stats API instead
of an exec into the measured container.

The recorded builds and cells give different peaks and low-rate latencies. The
second and third benchmark passes used different image hashes, so their ranges
do not establish same-image variability.

Evidence (2026-10-09, 1 CPU, anonymous MiB across three passes): work-queue 77–798,
large-64k 90–960, latency-10k 61–883 (two later reruns: 54 and 60). latency-1k p50
0.15, 0.67 and 1.05 ms.

Hypothesis, not a diagnosis: JavaScriptCore's garbage collector timing. First
freeze one image and run a cell five times, logging `process.memoryUsage()` and `bun:jsc` `heapStats()` over time.

Verify: five repeats of work-queue and latency-1k with a tight spread.

### B7. Low-rate latency

Status: partly done. 2026-10-12 (Cortex Code): the disk alarm's `statfsSync`
took ~17 ms a call in Docker Desktop and ran every 200 ms on the event loop,
9% of a 1 CPU broker; it now samples every 5 s (`08d1bb5`), latency-10k p99
2.27 → 0.91 ms in an A/B. many-queues and slow-consumers p99 still vary
(3–8 ms and 0.3–17 ms in one A/B, both images). Earlier, 2026-10-10: fdatasync moved to a worker thread
(`bun/src/sync-worker.ts`): many-queues p99 38 → 4–8 ms, latency-10k p99 9–18 → 1–4
ms. slow-consumers is still bimodal (1 or 15 ms); an idle Bun process with a 1 ms
timer sees stalls up to 11 ms on Docker Desktop, so part of it is the VM's timers.

Evidence: latency-1k p99 3.4–6.5 ms against Rust 1.46 ms; slow-consumers p99
14.6–24.6 ms against Rust 0.67 ms; many-queues p99 38 ms against Rust 7.4 ms.

Where: the lone-publish path. `SOCKET_WAIT_MS` and the lone mode in
`driveInbound` (`bun/src/amqp/frames.ts`); `armLoneFlush` and `SMALL_GROUP` in
`bun/src/store.ts`. `store.beforeSync` already writes staged deliveries before a
small fsync. Applying it to every fsync cost the paced 128-confirm run about 10%,
so it is limited to groups of 16 or fewer.

### B10. Stream throughput (done, 2026-10-12)

Status: done (Cortex Code, `08d1bb5`). 1 CPU stream 26.2k → 81.9k msg/s at
216 MiB; Rust 101k. Appends share one sqlite transaction per task, readers
behind the resident 16 MiB read 512-entry ranges, and entries out of memory
keep a timestamp and a size only. Next, if needed: profile again (sqlite
inserts were 55% of the 1 CPU profile before; `bun/src/store.ts`
`flushStreamAppends`).

Verify: `test/stream-read-ahead.test.ts`, `test/stream-raft.test.ts`; the
stream cell at 1 CPU with memory flat.

### B9. Fanout stall at 1 CPU (new, 2026-10-11)

Status: open, not reproduced since the sync-worker fix (Cortex Code, 2026-10-12):
22 fanout-20 repeats in a row at 1 CPU passed (225k–244k deliveries/s), 8 of
them with `QF_BENCH_PROFILE=1`. At 1 in 8, 22 straight passes happen about 5%
of the time, so the fix may have been the cause, but that is not proven. The
profile sample now includes per-connection state (`conns`: buffered output,
`outputBlocked`, paused reads, deferred and in-flight publishes, unacked): if it
stalls again, run the cell with `QF_BENCH_PROFILE=1` and read the broker log.

Evidence: in the published run one of two Bun fanout-20 repeats at 1 CPU stalled
about 2 s in: publishes, confirms and deliveries all stopped, broker CPU 2%, for the
rest of the cell (`bench/results/2026-10-11/runs/c21d8f63775842edb335c34b8ff3dd14`).
The other repeat delivered 233,672/s; six more repeats on the same image afterwards
all passed (249k–260k). About 1 in 8.

Where to look first: the publish hold in `driveInbound` (`bun/src/amqp/frames.ts`,
`pressured()` in `bun/src/amqp/publish.ts`) waits on socket bytes while a routed
queue has 256+ ready and consumers; the output-credit wake (`wakeOutput`,
`creditOk`); and the sync-worker hand-off (`bun/src/sync-worker.ts`). The worker
read `PREVIOUS` before `FD`, so a rotation between the two could report a batch
synced from the new chunk's fsync only. That durability race was fixed after the
published run (the worker checks the hand-off slot again after its fsync; the
published Bun image predates it). It does not explain a stall. Capture the broker's queue depths, unacked counts and
`outputBlocked` at the stall before changing code.

### B8. Backlog fill is slow

Status: done (Cortex Code, 2026-10-10); see Done.

Evidence: filling 300,000 durable 1 KiB messages with no consumer runs at about
30k msg/s; Rust 145k, RabbitMQ 72k. The drain (75k) is fine.

Where: classic durable messages are paged out past 2,048 ready or 64 MiB
(`PAGE_AFTER_READY` / `PAGE_AFTER_BYTES` in `bun/src/broker/publish.ts`); the
SQLite catch-up is `applyShadow` in `bun/src/store.ts` (at 80,000 rows or 64 MiB).
Profile the fill first.

---

## Rust

### R1. Transactions at 199 msg/s

Status: done (Cortex Code, 2026-10-10); see Done.

Evidence: transactions cell (tx.select, commit every 10 messages) 199 msg/s,
p50 45 ms, at 7% CPU, so it's waiting, not working. RabbitMQ 19.2k, Bun 17.7k–37k.

Lead: `tx_commit` in `rust/crates/queueforge-broker/src/connection/confirm.rs`
applies each queued op in order with `.await` (`finish_publish` per publish). If
every durable publish waits for its own fsync or the fsync timer, a 10-message
commit pays ten waits. Bun had the same bug (fixed by starting every op, then
awaiting them together). Not verified for Rust.

Verify: transactions cell; the Rust tx tests; conformance.

### R2. fan-2x2 with one confirm in flight

Status: done (Cortex Code, 2026-10-10); see Done.

Evidence: paced fan-2x2 at one confirm keeps only 197.87 msg/s (first miss 800);
RabbitMQ 2,273, Bun 3,678. With 128 confirms in flight Rust is fine (24.0k).

Lead: two producers on one queue, each waiting for its confirm. That looks like a
timer wait rather than CPU. Compare with durable-256 at one confirm (2,087, fine).

### R3. Stream throughput

Status: done (Cortex Code, 2026-10-12); see Done.

Evidence (2026-10-11 run): stream cell (two consumers reading from the start)
11.5k msg/s; RabbitMQ 26.7k, Bun 24.9k.

---

## PHP

### P1. More than one CPU: a connection can miss its queue

Status: done (Cortex Code, 2026-10-11); see Done. Historical notes follow.

Current code already migrates sockets, gates worker readiness on peer hello,
and waits for committed queue metadata on migrated sockets. Do not reimplement
the historical missing sender; reproduce the current failure first.

With more than one CPU, a connection can reach a child process that hasn't seen
its queue's declare yet.

Evidence: load sweep `404 NOT_FOUND - no queue 'q0'` at 2 CPU (shared) and 4 CPU / 8
GiB (single); 4 CPU scenarios: work-queue 10k, fanout-20 and heavy exited, quorum
delivered nothing.

Where: `php/src/Broker.php` `remoteHomeOf()`, `php/src/Supervise.php`, connection
handoff in `php/src/Server.php`.

### P2. 1 MiB messages run out of memory

Status: done (Cortex Code, 2026-10-11); see Done.

Evidence: large-1m OOM-killed in 1 GiB (993 MiB anonymous).

### P3. Backlog drain

Status: done (Cortex Code, 2026-10-12); see Done. Earlier notes follow.

2026-10-11: local drain 6.5k → 9.8k msg/s.

Evidence: draining 300,000 messages runs at 811 msg/s (Rust 150k, RabbitMQ 50k).
The fill is 60k. Only 76,200 of 300,000 messages were received in the recorded
90 s drain window; this is an incomplete drain, not a completed backlog test.

### P4. Quorum and stream throughput

Status: done for single node (Cortex Code, 2026-10-11); see Done. Multi-node
failure behaviour was not re-measured.

Evidence: quorum 589 msg/s, stream 791 msg/s at 1 CPU (RabbitMQ 26.7k for both).

### P5. Connection hold

Status: done (Cortex Code, 2026-10-12); see Done. Earlier notes follow. 1,000–2,500 connections (Bun 260,000, RabbitMQ 64,370). Larger
asks are cut by the login window near 1,010 connections per process.

2026-10-11: with one process by default (P1), PHP holds 1,000 at every size, and
the load sweep's spread shape at 4 CPU fell from 288,669 to 88,558 msg/s.
`QUEUEFORGE_CORES=cgroup` restores the per-core mode for those shapes; the
scenario cells were better in single-process mode (see P1).

### P6. Conformance 61/135

Status: done (Cortex Code, 2026-10-11); see Done.

Run `cd conformance && bun run.ts php` and work through the current failure list.
The recorded 61/135 predates the recent parity work: 126 non-auth shared client
tests and all 34 PHP test files subsequently passed. Those different test sets
do not prove full 135-test conformance; rerun with LDAP/OAuth/PKI fixtures first.

---

## Benchmark harness

### H1. Run each Bun cell more than once

Status: done (2026-10-10); see Done.

### H2. Memory columns

Status: done (2026-10-10); see Done.

---

## Done

Move finished items here: id, date, what changed, before → after. A/B numbers are
Docker, PerfTest 2.25, 1 CPU / 1 GiB unless noted; captures in `/tmp/qf-ab/coco/`.

- **B8** (2026-10-10). Synced bodies past 64 MiB stay only in the durable log and
  are read back with `pread`, instead of being copied into SQLite in 80,000-row
  transactions; relocation skips a chunk that is mostly live. Backlog fill
  22–24k → 122–124k msg/s, drain 50–75k → 100k. `bun/test/store.page.test.ts`.
- **Bun publish path** (2026-10-10, not an item). Named exchanges routing to one
  local classic queue take the fast path; routes cached; delivery encoding
  caches tag/exchange/key bytes. work-queue 97.6k → 115.6k, priority 95.9k →
  106.7k, heavy 95k → 121.5k, fanout-20 230k → 249k msg/s.
- **R1** (2026-10-10). `tx_commit` flushes each queue once (`QueueCmd::FlushDurable`)
  and awaits them together, instead of one fsync wait per publish. Transactions
  193 → 41–43k msg/s.
- **R2** (2026-10-10). The queue actor flushes 2–4 lone waiters after 1 ms of
  quiet, then at once while nothing else arrives, instead of waiting out the
  10 ms interval. Paced fan-2x2 at one confirm 198 → 3,368 msg/s; heavy,
  routing-64, work-queue and latency-1k unchanged.
- **P1** (2026-10-11). No 404s at 4 CPU on the current tree. PHP now runs one
  process unless `QUEUEFORGE_CORES` is set, which delivered more in every 4 CPU
  cell: work-queue 10.4k → 37.1k, telemetry 25.5k → 84.7k, heavy 2.6k → 63.8k.
- **P2** (2026-10-11). Deliveries are split at the client's frame_max; a 1 MiB
  single frame made the Java client drop the connection and redeliver forever.
  large-1m OOM → 611 msg/s (641 MB/s), 16 MiB anonymous.
- **P4** (2026-10-11). Quorum: one store sync per applied Raft batch and no
  topology rewrite per message, a qid index, cached Raft voters, confirms sent
  right after the Raft tick: 535 → 8,408 msg/s. Streams: running byte count,
  one fsync per loop pass, records past 64 MiB paged to the log: 465 → 18.6k
  msg/s published, anonymous memory 798 → 278 MiB.
- **P6** (2026-10-11). PLAIN over TLS without a client certificate: 134 → 135/135.
- **H1** (2026-10-10). `bench/scenarios.py --repeat N` keeps every repeat in its
  own run directory with image digests and settings; `bench/report.py` shows the
  median and range and counts failed repeats. The 2026-10-11 run uses two.
- **H2** (2026-10-10). Every scenario cell shows anonymous / total cgroup peak,
  failures included, on the site and in `report.md`.
- **P5** (2026-10-12). `stream_select` refuses descriptors past FD_SETSIZE (1,024),
  which was the "login window" near 1,010 connections. `php/src/Poller.php` uses
  the `ev` extension (epoll; built into the image) and keeps `stream_select` as
  the fallback. One cell at 1 CPU / 512 MiB: 1,000 → 5,000 held (asked 5,000),
  104 MiB, 40/40 confirms. `php/test/fd-limit.test.php` holds 1,100. The full
  sweep is not re-run yet.
- **P3** (2026-10-12). `Server::pump` iterated copies of the queue rows, so every
  removal in `getReady` copied the 300,000-entry ready list. Drain by hand at
  1 CPU / 1 GiB: 6.4k msg/s (47 s) → ~100k msg/s (3 s); fill unchanged (61k).
- **PHP TLS PLAIN race** (2026-10-12, not an item). On Linux a fast client could
  finish the handshake in the first `stream_socket_enable_crypto` step, while
  PHP's own `verify_peer` check (which refuses a client without a certificate)
  was still on. The listeners leave `verify_peer` unset with a CA: OpenSSL still
  refuses certificates that do not chain. `tls.test.php` 10/10 on Linux (failed
  almost every run before).
- **PHP handoff on macOS** (2026-10-12, not an item). The parent closed its copy
  of a socket right after passing it in a datagram; on macOS the child's copy
  then read end of file. It closes a second later. `homes.test.php` 60/60.
- **R3** (2026-10-12). Stream appends share one fsync (the mailbox drains first,
  at most 512 per sync) and readers stop at the synced offset; a failed fsync
  nacks the batch and reuses its offsets. Synced bodies past 64 MiB are dropped
  from memory and read back from the WAL (`DurableQueueLog::read_at`). Stream
  cell 11.5k → 110k msg/s received, 247 MiB anonymous (without paging it was
  OOM-killed at 1 GiB). Rust suite 436/436 with RabbitMQ. Not done: after a
  restart, recovery still loads the whole stream into memory, since recovered
  entries have no log position yet.
- **PHP parity** (2026-10-12, not an item). Brings PHP to Bun's behaviour in
  five places. Each has a focused test; the full PHP suite and the benchmark
  cells have not been rerun yet.
  - Memory alarm: 0.6 of RAM, or the cgroup limit, as Rust does. While it or the
    disk alarm is on, connections that have published are not read, and
    `connection.blocked`/`unblocked` go to clients that ask
    (`alarms.test.php` 7/7). Before, only the disk alarm was reported, and
    nothing was blocked.
  - Classic paging: persistent classic bodies past 64 MiB are read back from
    the log, through compaction and restart (`classic-page.test.php` 9/9).
    Without paging, the new memory alarm would have blocked the backlog cell
    (~706 MiB against a 614 MiB watermark). Quick run 2026-10-12: backlog anonymous
    648 → 352 MiB, but drain 100k → 60k msg/s, from four syscalls and a JSON decode
    per paged message. `Store::bodyAt` now reads through a 256 KiB read-ahead and
    skips the metadata. In-process drain of 300,000 × 1 KiB: 160k → 480k msg/s
    (unpaged: ~700k). Docker, two repeats: drain 87k median (one repeat 99.7k), anonymous
    352–462 MiB.
  - Cores mode with TLS or an `amqps` port: the parent hands raw TCP to a child,
    which runs the handshake (`tls-cores.test.php` 11/11, five runs). Before,
    TLS disabled cores mode.
  - Remote consumers. A delivery pushed by a queue's home was queued in the
    consuming child's local copy, which nothing drained. A consumer on a child
    other than the home got nothing, and `unsub` named no queue. Deliveries now
    go to the subscribed channel; acks, nacks and rejects travel back to the
    home.
  - Raft metadata lag. A declare is acknowledged on the leader's commit, and a
    follower applies it on the next heartbeat (~150 ms later):
    - A publish or consume naming a queue the node has not applied yet now
      waits up to 0.5 s; before, it got 404 NOT_FOUND.
    - A forwarded enqueue for such a queue now waits up to 1 s, then is
      refused. Before, it was dropped while the publisher was still confirmed.
    - With this, plain cores mode passed 12/12 declare/publish/consume orders,
      up from 10/12.
  - `fanout-cores.test.php` matches `bun/test/fanout-cores.test.ts`: 9
    queues × 200 messages in order, across 3 children.
- **Orphaned cores workers** (2026-10-12, not an item). A supervisor killed with
  SIGKILL left its workers running on the fixed cluster and metrics ports
  (25672+, 25680+), so the next cores-mode start failed with EADDRINUSE. A
  cancelled `quorum-cores` run caused `fanout-cores` to fail in the Bun suite.
  - Bun workers now exit when the supervisor's IPC channel closes; PHP workers
    when their parent pid changes.
  - `quorum-cores.test.ts` stops with SIGTERM, not SIGKILL.
  - Checked: SIGKILL of a 3-worker supervisor left 3 listeners before the fix,
    0 after, in both Bun and PHP.
- **Quick run 2026-10-12** (`bench/results-quick/2026-10-12T143411Z`, Rust, Bun and
  PHP; 43 min, against 3¼ h for the full run).
  - Noise: with one repeat, cells whose code did not change moved by up to
    ±13% (Rust telemetry +11%, fanout-20 4 CPU +13%).
  - Bun: some 1 CPU cells went down (transactions −34%, large-1m −26%, quorum
    −21%, work-queue −15%). At 1 CPU Bun runs a single process, and none of the
    changes since the baseline (B3, cores-only) run there. Rust lost 7–9% on the
    same cells, so these look like machine variation. Rechecked with two repeats:
    transactions +13%, quorum +6%, so those were noise.
    - large-1m stayed low (340 against 445 MB/s), so I ran an A/B
      (`bench/ab.py`, three repeats, `bench/results-quick/ab-2026-10-12T181818Z`).
      This morning's code on Bun 1.4.2 gave 380 MB/s and today's code on 1.4.2
      gave 395, so the code did not cause the drop.
    - Bun 1.4.3 gave 527 MB/s, +34%, with no overlap between the ranges. See
      BENCHMARK.md, 2026-10-12.
  - PHP: backlog drain fell 100k → 60k from paging, and rose to 87k after the
    read-ahead (see PHP parity).

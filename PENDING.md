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

### B16. Scaling past 6 cores is extrapolated, not measured

Status: open. `bench/scaling.py` (`weak`, `weak-t`) is the curve to watch
for multithreading changes: load grows with the core count, every point had
the broker at 95% or more of its CPUs, and repeats sit within ~4%. After
B14 and B15, at 6 CPUs: `weak-t` 2,031,339 msg/s (efficiency 0.68, 96% of
the `bench/ceiling.py` host ceiling), `weak` 1,412,531 (0.77); Rust 560,500
and 498,464. The USL fits have k = 0, so the model never peaks, but this Mac
cannot show what only many cores bring:

- every worker is a cluster member with a link to every other, a voter in
  the metadata Raft group, and sends heartbeats and peer status to all the
  others: N(N-1)/2 links and N voters, for 64 or 128 workers;
- the supervisor accepts every connection and hands it to a worker over IPC;
- NUMA, network-card queues and memory bandwidth on a large host.

Fixed 2026-10-15: from 9 workers on, worker metrics ports (fixed at 25680 +
i) took the cluster ports of workers 8 and up (25672 + i). Those workers
exited with EADDRINUSE and restarted in a loop, and `weak-t` with 12
workers delivered nothing. Metrics ports now follow the last cluster port
(`childPlans`); 12 workers on 6 CPUs: 1,923,602 msg/s. No benchmark or test
had used more than 8 workers.

Idle cost after that fix (`QUEUEFORGE_CORES=N` in a 4-CPU container, no
clients, 20 s after start):

| workers | ready after | idle CPU | anonymous memory | established TCP |
| ---: | ---: | ---: | ---: | ---: |
| 4 | <1 s | 6% | 120 MiB | 12 |
| 8 | <1 s | 11% | 237 MiB | 56 |
| 16 | 1 s | 20% | 463 MiB | 240 |
| 32 | 1 s | 39% | 870 MiB | 992 |
| 64 | 2 s | 62% | 1,781 MiB | 4,032 |

About 1% of a core and 28 MiB per worker; links grow as N(N-1). Next: run
`bench/scaling.py` on a 32-64 vCPU cloud host to check the projection, and
measure the metadata group with 64 voters under declare and bind churn. The
group could keep a few voters and make the other workers learners.

### B14. GC helper threads competed across workers

Status: done 2026-10-15 (`docs/bun-scaling.md`, B14). JavaScriptCore sized
each worker's parallel GC marker pool from the host's cores. At 6 workers
each ran five `HeapHelper` threads, ~30% of a core per worker. The supervisor
now gives workers `BUN_JSC_numberOfGCMarkers=1` unless the environment sets
it; a one-process broker keeps the default. `weak-t` at 6 CPUs 1,785,342 to 2,056,610–2,062,166
msg/s, at 4 CPUs +11%, at 1 CPU +2–4%; anonymous memory +2–10%.

### B15. Hash placement overloaded one worker

Status: done 2026-10-15 (`docs/bun-scaling.md`, B15). With 4 queues per
worker the name hash gave the busiest worker 5 of 20 queues at 5 workers and
9 of 128 at 32 (efficiency caps 0.80 and 0.44). Among a broker's own workers,
`n0` now assigns each new classic queue to the least-loaded worker. `weak`
at 5 CPUs 1,081,373 to 1,228,382 (+14%, with B14).

### H8. 4 CPU PerfTest cells with many publishers measure the client

Status: open. `bench/ab.py --cpus 4` and the published 4 CPU scenarios give
PerfTest 3 CPUs. In routing-64 (64 publishers) it used all three, and broker
and client CPU rose and fell together: the same images ranged 96,853 to
297,328 msg/s between repeats. `bench/scaling.py` (client on 8 CPUs) measured
440,000-492,000 for the same cell. A 4 CPU A/B there showed routing-64 -41%
for B14-B16 that was not reproducible. Give the 4 CPU client more CPUs, or
flag cells whose client is near its limit, before trusting those cells.
Durable cells late in a long session also drift (`weak` at 6 CPUs 949,000 to
1,412,562 for one image, broker partly idle): the VM's disk latency, not code.

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

Tried 2026-10-15, 2 CPU broker (client on 2 others, client at ~130% of 2 CPUs,
so not the limit): the queue's main thread runs at 92-94% of a core; its
largest single cost is the socket write (`writeBuffered`, 18% of samples,
24,000 writes/s of ~9.6 KB and 16 frames each). A local microbenchmark puts a
9.6 KB write at 2.7-3.0 us through `node:net` or `fs.writeSync` on the fd and
2.2-2.4 us through a Bun native socket: the cost is the kernel send, so a
different socket API saves little. Three prototypes, all within noise:
coalescing writes per event-loop turn (`setImmediate` instead of a
microtask): work-queue 205,501 -> 196,584, routing-64 196,349 -> 190,355;
a writer thread per process sending from a 1 MiB shared ring per connection
(`writeSync` on the socket fd, woken by `Atomics.notify`): work-queue 183,726
-> 184,376, routing-64 194,647 -> 188,033; the same writer polling every
50 us instead of being woken: work-queue 177,715 -> 180,912, routing-64
183,689 -> 182,240. The writer took the sends off the queue's thread (it ran
at ~13% of a core), but the ring copy and the wakeups cost most of what was
saved. None kept. Rust in the same 2 CPU setup: work-queue 203,989 at 176%
CPU, so at 2 CPU Bun's single queue (180,000-205,000) is level with Rust;
the gap is in the 4 CPU cell. What is left on the queue's thread is spread out
(body copy at `finishPublish` 6%, `encodeDeliver` 8%, routing 5%, parsing),
so no single cost is left to move.

2026-10-16, 4 CPU, image `6bae16d`, per-thread CPU over 10 s in three
work-queue runs: every connection is on the queue's worker; its main thread
at 79%, 90% and 99% of a core, the sync worker 12–14%, the other three
workers idle; 185,329 / 188,551 / 213,875 msg/s with the PerfTest client at
151–171% (H8). Interleaved against Rust: work-queue 144,745 / 302,344 and
221,023 / 234,919; quorum 204,321 / 265,976 and 202,016 / 272,135 (Bun was
127,614 in the 2026-10-16 run; the one-voter quorum publish now confirms per
group commit, `localQuorumFast`). The home's profile: `writeBuffered` (the
node:net write of adopted sockets) 20%, `encodeDeliver` 9.5%, then many costs
of 1–4%. Untried: keeping consumer connections on their own workers, so the
delivery encode and send leave the home. Forwarded publishes were slower
(above), and a forwarded delivery probably is too.

A queue lives on one child process, so a single busy queue gets one JavaScript
thread however many cores the container has.

2026-10-18 (`90625e0`): fewer allocations and copies per message on the
queue's thread instead of moving work off it. Hot outbound frames (deliveries
and confirms) are encoded back to back into a per-connection arena and a
burst is written as one view, so there is no buffer per frame and, on the
adopted `node:net` sockets of cores mode, no fresh gather buffer per write
(an arena that a socket may still hold is never reused); a single frame is
staged without an array; the fast publish path reuses the last exchange/key
to queue-name route while the route cache is unchanged; `beginPublish`
reuses the last publish's name strings when the bytes match; topic
permissions are checked on topic exchanges only (as RabbitMQ), with their
patterns compiled once. Interleaved A/B in both orders against the committed
code, medians over every repeat: 1 CPU work-queue +16%, quorum +9%,
fanout-20 +5%, telemetry +3%, latency-10k p99 1.74 -> 0.83 ms; 4 CPU
work-queue +8-20%, quorum +8-36% (both 4 CPU cells spread with the VM disk;
the first pair after a Docker restart: work-queue 225,756 -> 240,828,
quorum 201,136 -> 224,806), heavy +13%. The arena also exposed a bug:
`encodeSettle` never wrote the bits octet (byte 19) and relied on a zeroed
buffer; it now writes it, and `test/out-arena.test.ts` starts from a dirty
arena so any unwritten byte fails. Full run 2026-10-18 (published): 4 CPU
work-queue 214,488 -> 251,475 msg/s (Rust 302,894), quorum 153,380 ->
225,295 (Rust 292,528), heavy 247,414 (Rust 321,282 on its one complete
repeat). The single-queue gap at 4 CPU is ~17-23%, down from ~30-43%.

2026-10-18, second round (`a871f55`, `3fb4946`, `e4d4112`, `6ace4e1`): confirms
of a burst coalesce into `multiple` acks; adopted sockets rewind the frame
arena once every write callback ran; the back-pressure check of parked
publishes reuses its route and queue keys (it rerouted every parked
publish); basic.publish is parsed in place; quorum messages reuse the cached
stored-meta string (JSON.stringify per message was 5.5% of the 4 CPU quorum
thread). Fresh-VM A/B pairs (a Docker restart before every pair; plain
both-order runs on one VM spread 2x on durable cells): 4 CPU quorum +17.5%,
work-queue +5.5%. Fresh-VM pairs against Rust, 3 rounds, Bun `hot8`: 4 CPU
work-queue 281,413 / 281,360 (Bun 2 of 3), heavy 328,085 / 309,560,
many-queues p99 1.81 / 2.15 ms, quorum 250,094 / 286,463 (Rust 3 of 3);
1 CPU transactions 78,251 / 58,308 (Bun 3 of 3), slow-consumers p99 0.84 /
3.13, large-64k 1,063 / 1,172 MB/s and large-64k-quorum 1,089 / 1,239 (Rust
3 of 3), backlog 149,717 / 149,993 (both at the drain's ceiling),
latency-10k p99 1.56 / 1.10 (Rust's own repeats 0.13-1.76).

Found on the way: when the VM's fsync slows (0.14 -> 0.56 ms) Bun's 4 CPU
work-queue falls 279k -> 122k while Rust keeps 258k; with a simulated
+0.5 ms per round (the sync worker's test delay) 290k -> 173k. Publishers
sit at their 100-confirm window: Bun's confirm round trip spans several
fsyncs (worker round, postMessage to the main thread, confirm, the next
publish). Rust's per-queue WAL syncs more bytes per round. Next: shorten that
loop (report rounds without postMessage, start the next round's staging
while one syncs) and measure it under the simulated delay.

Third round (2026-10-19, `24a5866`): 4 CPU quorum was not the queue thread.
Quorum messages wait in `ready` behind their confirm gate until the group
commit covers them, and the publisher back-pressure test counted them as
consumer lag (256 ready): publishers parked on every deep confirm window,
which shrank the next group commit. A queue now counts as behind only when
its oldest ready message is deliverable. Fresh-VM pairs against Rust, 4 CPU
quorum: 250,094 / 286,463 -> 281,769 / 282,287 msg/s (even).

The 64 KiB cells (large-64k 1 CPU, -5 to -10% against Rust in fresh-VM
pairs; large-64k-quorum -7%) are CPU per byte: Rust is also at ~90% of the
CPU there, and Bun spends ~7% more per byte. JavaScriptCore runs a full
collection every few MiB of external memory once the heap passes
`largeHeapSize` (8 MiB): 1,340 full collections in a 30 s cell (p50
2.1 ms each), and a confirm caught behind one is the 4-5 ms p99 against
Rust's 2.9. The buffer kind does not matter (ArrayBuffer, SharedArrayBuffer
and allocUnsafeSlow trigger the same), and slabs report the same bytes.
`BUN_JSC_largeHeapSize=64 MiB` cuts full collections 14x and gave +13% on
large-64k over five fresh-VM pairs, but costs +90 to +160 MiB anonymous in
every cell and still lost 2 of 3 pairs to Rust on a slower disk: not made
the default. Also tried: completing synced batches from the worker's shared
counter on every socket read instead of waiting for its message (within
noise, not kept). What would close it: no copy from Bun's read buffer into
the body (Bun has no read into a caller buffer) and recycling body buffers
(a body is shared by queues, the store and queued socket views, so a pool
needs reference counts on every path).

Published run (2026-10-19, `e452756`, `bench/results/2026-10-19`, Bun only,
no connections step): quorum +9 to +11%, transactions +20%, 4 CPU heavy
+33% against the 2026-10-18 Bun cells. Single-run losses not yet re-checked
in fresh-VM pairs: 1 CPU heavy -11.5%, 1 MiB quorum -13.6%, priority -6.7%,
latency-10k p99 1.10 -> 2.42 ms. Site average 3.18 -> 3.09, driven by the
two latency cells. Next: fresh-VM pairs for those four cells against
`cf0d960c` (the 2026-10-18 image).

Body pool (fourth round, `src/body-pool.ts`): bodies of 16 KiB to 1 MiB come
from a free list and go back only after one observed chain: eligible
enqueue by the single-queue fast path (local classic, or quorum with no Raft
group), one hand-off, one AMQP `deliver` in this process, its ack, then at
reuse time the store holds no row with it (`Store.holdsBody`) and the
connection has written everything (the arena-rewind condition). Anything
else taints the body and leaves it to the GC: fan-out, CC/BCC, a second
enqueue or hand-off (requeue), basic.get and non-AMQP consumers, republish
(shovel, federation, trace), forwards (`enqueue`, `callBytes`,
`encodeFrame`, quorum wire records), streams, overflow, paged ("slim")
messages, a closed connection. A missed path can only skip a release. Each
use carries the buffer generation; a read of a reused buffer is logged and
turns reuse off (`queueforge_body_pool_violations_total`). Kill switch
`QUEUEFORGE_BODY_POOL=0`; `QUEUEFORGE_BODY_POOL_CHECK=1` hashes every pooled
body at enqueue, delivery and reuse. Bounds: 16 MiB free, 64 MiB waiting.

Tests: `test/body-pool.test.ts` (state machine, 19 cases) and
`test/body-pool-e2e.test.ts` (unique bodies checked byte for byte through
plain, fan-out with a slow and a late consumer, requeue, dead-letter, a
paused consumer socket, quorum, transient and mixed sizes; check mode on).
With every fan-out guard removed the late fan-out consumer trips the
generation check (4 violations), so the test sees a premature release; with
one layer removed it still passes, since six independent steps each stop a
shared body.

Fresh-VM A/B against `e452756`, 1 CPU, 3 rounds, medians: large-64k 868 ->
936 MB/s (+7.9%), large-64k-quorum 943 -> 1,042 (+10.6%), large-1m-quorum
857 -> 1,010 (+17.9%, anonymous 306 -> 146 MiB), large-1m 1,039 -> 1,027
(within its range), work-queue +9.5% (small bodies, not pooled: noise).

Multi-core confirm stall (fixed): with `QUEUEFORGE_CORES=3`, one confirm
channel mixing fan-out and plain publishes stopped receiving confirms after
150 to 450 publishes (2 of 3 runs, also on `e452756` and with the pool off).
Cause, from an in-memory trace of `sendConfirm`: confirm coalescing
(`a871f55`). Fan-out confirms finish out of order; tag 501 was staged as its
own basic.ack while 473 was still pending, then the in-order run reached 472
and rewrote that still-staged frame in place to `multiple` 472. The ack for
501 was gone and no later frame covered it. A staged ack is now extended
only when the highest tag it already acks (`Ch.ackTag`) is inside the
settled run. `test/confirm-order.test.ts` replays the traced order and 200
random out-of-order settle/flush runs (both fail without the fix); the
stall harness went from about 2 in 3 to 0 in 10, and the 3-core pool e2e
case runs in the suite again.

Not kept (second round): writing adopted sockets through Bun's native
handle, which copies on write and returns short counts (no change in 4 CPU
A/B: the cost is the kernel send); a sync-worker gather for rounds of large
records up to 1 MiB (fsyncs 2,200 -> 760/s but MB/s unchanged and quorum
anonymous memory 145 -> 384 MiB); an adaptive gather after slow rounds (rounds
already ran back to back); a 1 ms keep-awake timer for the latency tails
(p99 of one image spreads 0.49-16.6 ms either way); `MIMALLOC_PURGE_DELAY`
for the 64 KiB body allocations (more memory, no throughput). What is left
at 64 KiB is two body copies Rust does not make (into the log's staging ring
and into the socket gather buffer).

Not kept (2026-10-18): a sync-worker gather delay before each fsync round
(the main thread spends 13% of 1 CPU work-queue in `Atomics.notify`, the
handoff to the worker): 0.1 / 0.25 / 0.5 ms lowered work-queue 229k to
202k / 202k / 187k msg/s. Copying 1 KiB bodies into a bump arena instead of
`slice`: ~25 ns saved per body at 1 KiB and slower at 256 B, not worth the
retention risk.

Evidence: work-queue (one queue) at 4 CPU 165,339 msg/s at 1.3 of 4 CPUs used;
Rust 197,010 at 3 CPUs. The 16-queue shape does scale (522,318 at 4 CPU).

Where: the cores supervisor (`bun/src/main.ts`, `bun/src/supervise.ts`) and
`queueHome()`.

### B10. Quorum in cores mode pays for a Raft group of one

Status: done 2026-10-15 (design and rules in `docs/bun-scaling.md`, B10). A
persistent publish to a one-voter group on its voter skips the Raft log; the
queue's store row, fsynced before the confirm, is the durable copy, and its
ack proposes no `drop`. Transient publishes and multi-voter groups are
unchanged. 2 CPU (broker on 2 CPUs, client on 2 others, three pairs,
medians): quorum 88,134 to 159,557 msg/s, p99 12.75 to 10.12 ms; work-queue
194,225 to 201,493. The published 2 and 4 CPU quorum cells change at the next
full run.

### B5. Large messages: CPU per byte

Status: mostly done 2026-10-16 (`6bae16d`). What was found and changed:

- The durable log rotated a 4 MiB chunk every ~4 ms at 1 GB/s; each rotation
  adds a directory fsync and the old chunk's fsync to a commit round, which
  doubled the fsync time of a 64 KiB confirm (1.0 against 0.4 ms). Chunks now
  double up to 32 MiB while rotations come under 250 ms apart and shrink
  after 2 s (`fast-log.ts`, `adaptChunkSize`); compaction keeps ~64 MiB behind.
- `absorbFresh` copied every socket read into a fresh buffer, a body frame
  that spanned reads was concatenated again per read, and the body was then
  sliced out: three fresh 64 KiB allocations per publish, each ~10 us in the
  broker against ~1.6 us warm. A lone read is now parsed in place, a body of
  16 KiB or more gets a buffer of its declared size (at most 64 MiB reserved
  per connection), and a spanning body frame streams into it.
- Deliveries of 16 KiB or more are the frame headers plus views of the stored
  body (`encodeDeliverParts`); writes past the 1 MiB gather buffer go through
  it in pieces instead of a fresh buffer of the whole size.

Interleaved pairs against Rust, 1 CPU, fresh VM (MB/s, Bun / Rust): large-1m
1,600 / 1,308 and 1,582 / 1,074; large-1m-quorum 1,453 / 1,173 and 1,269 /
1,296; large-64k-quorum 1,069 / 1,233 and 967 / 764; large-64k 1,003 / 1,175,
then 686 / 682 on a slower disk. Before (2026-10-16 run): 64k 797.6, 1m 810.5.

What is left at 64 KiB: Bun is CPU-bound (87–92%). The profile's largest
costs are the socket write, `Atomics.notify` (the sync worker taking the one
CPU) and the body allocation. A 64 KiB body allocation is external memory to
JavaScriptCore: 1 GB/s of bodies runs ~80 collections a second, half of them
full (~5 s of collector time in a 30 s cell). Tried: carving bodies out of
1 MiB slabs (no consistent gain, pins a slab per straggler, not kept);
`BUN_JSC_largeHeapSize=64 MiB` (half the collections, no throughput change,
+100 MiB, not kept); a 32 MiB staging ring (inside the noise, not kept).

Earlier notes, before 2026-10-16: What the 1 CPU 64 KiB profile shows now, main thread: socket
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

2026-10-16: an idle 1 ms Bun timer and a Python sleep loop, each alone on one
CPU of the 8 CPU VM, see the same gaps over 20 s: p999 5.4 / 5.3 ms, max 17.6
/ 17.8 ms, 145 / 136 gaps past 2 ms. Interleaved pairs (Bun / Rust p99):
latency-10k 3.25 / 0.59, 0.40 / 0.22, 1.98 / 1.16 ms; slow-consumers 0.40 /
0.90 and 0.60 / 7.74 ms (Bun ahead since the per-message work of 2026-10-16).
Full run 2026-10-18: latency-10k p99 1.10 ms (Rust 0.84), slow-consumers
8.54 (Rust 3.58), latency-1k 1.64 (Rust 1.70), many-queues 2.88 (Rust
6.13). latency-10k is the one cell left where one thread waiting out a VM stall
shows; it moves between 0.4 and 3.3 ms run to run.

Where: the lone-publish path (`SOCKET_WAIT_MS`, lone mode in `driveInbound`,
`bun/src/amqp/frames.ts`; `armLoneFlush`, `SMALL_GROUP` in `bun/src/store.ts`).
Anything that shortens the work done per event-loop turn helps; the VM stall
itself can't be fixed in the broker.

### B6. Peak memory and latency vary between runs

Status: open, cause unknown. Five fixed-image repeats did not tie high cgroup
peaks to process RSS or heap. Hypothesis, not a diagnosis: JavaScriptCore GC
timing. Freeze one image and run a cell five times, logging
`process.memoryUsage()` and `bun:jsc` `heapStats()` over time.

2026-10-18, 4 CPU fanout-20 (anonymous 548 MiB; Rust 56): each of the four
workers holds 115-140 MiB resident in one 1 GiB anonymous mapping while its
live JavaScript heap is 16-34 MiB (capacity 54-78) and its ArrayBuffers
9-14 MiB; no connection or queue holds a backlog (ready 0, nothing staged).
The rest is allocator slack inside the runtime, not data the broker keeps.
Tried, none moved it by more than ~5%: `BUN_OPTIONS=--smol`,
`BUN_GARBAGE_COLLECTOR_LEVEL=1/2`, `BUN_JSC_forceRAMSize=256/512 MiB`,
`MIMALLOC_PURGE_DECOMMITS=1` / `MIMALLOC_PURGE_DELAY=0` (mimalloc already
decommits after 100 ms). Fewer allocations per message (the frame arena)
is the lever left; the per-process floor (~30 MiB idle per worker) is
B16.

### B9. Fanout stall at 1 CPU

Status: watch. Not reproduced since 2026-10-12 (22 repeats passed). Once, one
fanout-20 repeat stopped about 2 s in at 2% CPU. If it stalls again, rerun with
`QF_BENCH_PROFILE=1` and read the per-connection state in the broker log before
changing code.

---

## Rust

### R1. A durable classic queue can stall from its first message

Status: open, correctness, predates 2026-10-16. Under the growing durable load
(`bench/scaling.py --scenarios weak`), now and then one loadgen process gets
no deliveries at all: both its publishers stop at a full confirm window
(`inflight=1024`, 170 to 16,303 confirmed) and both its consumers receive 0,
while the other processes on the same broker run normally. loadgen reports
`err=no_consume`. Seen in 7 of 160 Rust `weak` cells (2, 4 and 5 CPUs), never in
`weak-t`, and in 0 of 193 Bun cells on the same loadgen, so the client is not
the cause. The original image shows it (`weak-before`, 2 CPUs, repeat 2; and a
local rerun). mimalloc made it more frequent at 4 CPUs (3 of 3 cells against 0
of 12 for the other images), so it is timing: something lets a queue's
confirms and deliveries stop together. Ten instrumented reruns polling
`/api/queues/%2F` every 6 s did not reproduce it. Reading the actor loop, the
interval tick re-arms whenever no fsync is in flight, so the likely shape is an
in-flight fsync (`spawn_blocking`) that does not return, or a path that leaves
`interval_fsync` set. It is not memory flow control: the load sweep's spread
cell (4 CPU / 4 GiB) stalled the same way once, and six reruns sampling the
alarm gauges never saw the soft or hard alarm on (tracked bytes peaked at
3-169 MiB of a 2 GiB soft mark). Next: log the actor's fsync state on a timer
while a cell runs, and capture a stalled process.

### R2. Shared counters cap scaling near 7 CPUs

Status: open. After the 2026-10-16 work (ledger counts, gauges written on
change, cached counter handles, mimalloc, Fx hasher for broker-assigned keys)
the growing transient load is 281,903 msg/s at 1 CPU and 1,010,452 at 6
(0.60), against 199,566 and 560,500 before. The USL fit now has a coherency
term, k = 0.018, peak near N = 7.4: every message still increments about six
global Prometheus counters, one atomic each on lines all threads share, and
per-queue depth gauges are written on every actor pass. Per-thread counters
summed at scrape time would remove the shared lines. `prom::queue_depth` also
writes `rabbitmq_queue_messages_ready` for the same labels as the actor, so
writing the actor's gauges only on change needs that writer settled first.
Tried and not kept: `lto = "fat"` with one codegen unit (inside the noise).

---

## PHP

### P8. One process, one core

Status: open, architectural; the default needs a decision. PHP runs one
process unless `QUEUEFORGE_CORES` is set. Under the growing transient load
(independent queues, so each connection migrates to its queue's process) the
per-core mode scales: 81,377 msg/s at 1 CPU; at 2 CPUs 82,090 one process vs
159,185 per core; at 4 CPUs 77,500 vs 277,345 (0.85), and after the deferred
delivery writes 520,170 at 4 CPUs. The full curve per core
(`bench/results/scaling-2026-10-15/php-cores/`) is 153,099 at 1 CPU and
697,252 at 6 transient (0.76), 118,572 and 510,092 durable (0.72), with no
coherency term in the fit and every repeat passing. It lost in the fixed-load PerfTest cells
(work-queue 36.9k vs 10.4k at 4 CPU), where most traffic crosses processes,
and those cells are partly client-bound (H8). Following the cgroup by default
would trade one shape for the other; measure the PerfTest cells again with a
larger client before switching.

### P9. Per-frame cost

Status: improved, open. Each delivery was its own socket write, 43% of the CPU
under the growing transient load; deliveries are now written once per
connection per loop pass (1 CPU 82,564 -> 153,366 msg/s). What remains is
spread over userland: `Server::pump` 12%, `onData` 9%, `brokerFor` 8% (a vhost
lookup per frame), `Broker::publish` 7%. With Raft active, `awaitMetadata`
parses every method frame for a queue name (6% inclusive) even when the
connection already waited for that queue; remembering the last name per
connection would skip the parse. The large-message cells swing by a factor of
two between repeats on one image (large-64k 142-290 MB/s), which needs a look
of its own.

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

### H9. Docker Desktop fails past ~300k connections in one VM

Status: open (2026-10-18). The connections step's Bun 4 CPU / 8 GiB search
asks for 320,000 connections; three times in a row Docker's engine then
answered 500 to every request (the client container could not start, and
the broker container was gone), on a fresh VM too. 240,000-260,000 held
in earlier runs. The published row is the last step held (160,000). Cap
the search below the VM's limit, or run it on a Linux host.

### H5. Throughput through a failover

Status: open. No harness measures any engine while a quorum leader dies.
`bun/test/quorum-failover.test.ts` and `php/test/quorum-failover.test.php`
prove correctness (every confirmed body delivered once), not rate.

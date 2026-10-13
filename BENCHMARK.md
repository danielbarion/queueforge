# QueueForge vs RabbitMQ 4

One client, `queueforge-compare`. Each container is 1 CPU and 512 MiB (`docker_ncpu=2` on the host). Ports are 35672 (RabbitMQ 4.3), 35673 (Rust), 35674 (Bun), and 35675 (PHP), one container at a time. From 2026-10-04T21:30:56Z on, the compare binary mtime is 2026-10-04T16:16:23-0300, `QUEUEFORGE_COMPARE_RATES` is unset, and the slot wait stays on.

Message k is published at `k/rate`. In `fan-2x2`, two producers run together, each at half the labeled rate. A step is kept when confirms and acks both reach 95% of the offer. `messages/s` equals `pace_messages_per_sec`: acked deliveries over the counted windows. The report line is `throughput=paced`.

**First miss** is `saturation_load`, the first offered rate that missed 95%. **kept** means every step held (`kept_up=true`), and the number is the last offer. `durable-256` offers 200, 1000, 2000, 4000, 8000, 16000. `fan-2x2` offers 200, 800, 1600, 3200, 6400, 12800. With 128 confirms in flight, both also offer 32000, 48000, 64000, and 96000. The short scenarios offer 200 and 1000 (`size-64`, `prefetch-1`, `prefetch-128`), 100 and 400 (`size-4096`), or 500 and 2000 (`transient-256`). Keeping both steps scores exactly 600.00, 250.00, or 1250.00.

RabbitMQ confirms before its flush in every session. From 2026-10-04T21:30:56Z on, a QueueForge durable confirm returns after the covering fsync, and `queueforge_confirm_before_fsync_total` stays 0. The [historical run](#historical-confirm-before-the-fsync) is the older path, where QueueForge also confirmed before the 10 ms fsync.

The [2026-10-12 session](#session-2026-10-12) is the current result for all four brokers: paced, the load sweep, 16 application scenarios driven by RabbitMQ PerfTest at two container sizes with two repeats per cell, and the connection hold, all in Docker with the harness in `bench/`. Bun's rows there are an evening re-run of the same day, [Bun re-measured that evening](#bun-re-measured-that-evening). The [2026-10-11 session](#session-2026-10-11) is the previous one, then the [2026-10-09 session](#session-2026-10-09), and the [2026-10-08 session](#session-2026-10-08) the one before, with a local Raft latency check. The [paced run](#paced-2026-10-07t213657z) is the previous `queueforge-compare` session. The [latest run](#latest-2026-10-05t090342z) and [PHP parity paced](#php-parity-paced-2026-10-06t181520z) are the earlier sessions. Older paced sessions use the same columns. The [load sweep](#load-2026-10-05t212906z) is a separate unpaced run: messages per second and cgroup memory at five container sizes. The [load compare](#load-compare) table is one row per broker and size. RabbitMQ, Rust, and Bun message rates there are [Load remeasure](#load-2026-10-07t210340z). PHP rows at 1 CPU are that same run. PHP rows above 1 CPU stay [PHP cores](#php-cores-2026-10-07t034705z), because this remeasure's PHP containers with more than one CPU exited before a rate.

The PHP rows are the parity build, the one with client-visible parity with Bun and Rust. The earlier PHP image is kept in [PHP paced](#php-paced-2026-10-06t161324z) for comparison.

## Session (2026-10-12)

Except Bun (below), this section was measured on 2026-10-12 (10:59Z–14:14Z) in one pass with
`python3 bench/run.py`, from images built from the tree at `aaacc5d` plus the
uncommitted work of that morning, with two repeats per scenario cell. Results:
`bench/results/2026-10-12/`. Bun was measured again that evening; the tables
at the end of this section carry that run (see below).

Images (`broker_image` in `bench/results/2026-10-12/scenarios.tsv`):

- `rabbitmq:4.3-management` `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`
- `queueforge-rust:bench` `sha256:920cf3020b39f0bc1fb6ea1e4d02fda4301342a01a7acd5f0a0418aaf7f87637`
- `queueforge-bun:bench` `sha256:21edf8e6606646f27ce8d72d71dedaf464e4a5f0df9b44702f297077ea251f20`
- `queueforge-php:bench` `sha256:50c130c5cee8a317e48bb4dbd41585f7dd6d2ebbc7f973d10b744946cf99968a`

### What changed since 2026-10-11

Each before → after pair compares the published 2026-10-11 cell with this run, at
1 CPU / 1 GiB.

- Rust: stream appends share one fsync, and readers stop at the synced offset.
  Synced bodies past 64 MiB are read back from the WAL. Stream 11,528 → 101,429
  msg/s (R3).
- PHP: `Server::pump` copied the queue rows, so every delivery copied the ready
  list. Backlog drain 5,884 → 99,960 msg/s (P3).
- PHP: an `ev` (epoll) poller replaces `stream_select`, which stops at 1,024
  descriptors. Connections held at 1 CPU / 512 MiB: 1,000 → 28,000; 40,000 at
  2 CPU / 2 GiB and at 4 CPU / 4 GiB (P5).
- PHP: transactions 15,191 → 26,927 msg/s.

At 4 CPU / 8 GiB PHP held 20,000: at 40,000 its logins took more than the 180 s
login window, and the search stopped there. That is a limit on how fast one PHP
process accepts connections, not on how many it holds.

A quick run that afternoon (`python3 bench/run.py --quick`, one repeat, Rust, Bun
and PHP only; `bench/results-quick/`, not committed) measured the work that
followed:

- Bun's single-voter quorum groups (B3): quorum at 4 CPU 11.4k → 16.8k msg/s.
- PHP's classic paging: backlog drain 100k → 60k msg/s, anonymous memory 648 →
  352 MiB. A read-ahead for paged bodies was added afterwards. A second quick
  run with two repeats measured 87k msg/s median (one repeat 99.7k) at 352–462 MiB.

An A/B run that evening (`python3 bench/ab.py`, Bun, 1 CPU / 1 GiB, three repeats
interleaved, `bench/results-quick/ab-2026-10-12T181818Z/ab.md`) compared three
images: this run's code on Bun 1.4.2 (`old`), the code after the work above on
Bun 1.4.2 (`today`), and that code on Bun 1.4.3 (`new`). Medians, MB/s or msg/s:

| Scenario | old | today | new |
| --- | ---: | ---: | ---: |
| large-1m | 380.4 (368.3–416.2) | 394.6 (350.9–434.2) | 527.0 (505.2–532.2) |
| work-queue | 112,779 (109,318–120,899) | 106,794 (105,991–119,803) | 124,722 (119,630–136,764) |
| telemetry | 440,633 (385,481–450,626) | 398,195 (395,700–457,037) | 441,024 (427,006–448,338) |
| backlog drain | 149,681 (100,033–150,000) | 100,051 (99,975–149,681) | 149,837 (100,057–149,846) |

Bun 1.4.3 is +34% on 1 MiB messages against the same code on 1.4.2, with no
overlap between the ranges. The other cells overlap and show no clear change.
`old` and `today` agree on large-1m. The 445 MB/s this session measured that
morning was the machine, not the code. The backlog drain is 300,000 messages over
2 or 3 seconds, so it reads as 150k or 100k and its medians do not separate
images.

See PENDING.md.

### Bun re-measured that evening

Bun's rows in the tables below are a second run, the same evening
(20:35Z–21:38Z), from `queueforge-bun:bench`
`sha256:73058cfe8eb696eccf6dc245b929581c31896259bf0df0dd628cf3ff6c95594a` (tree at `08d1bb5`, Bun 1.4.3):
`python3 bench/run.py --kinds bun --baseline bench/results/2026-10-12 --out bench/results/2026-10-12c`.
RabbitMQ, Rust and PHP are the morning run above, copied from that baseline.
Results: `bench/results/2026-10-12c/`.

What changed in Bun:

- Bun 1.4.2 → 1.4.3 (`e3054c9`).
- A message forwarded to one queue on another Bun process arrived without an id
  and got `""`. Every such message in that queue shared it, so a delivery
  replaced the last one in the unacked set, and its acks deleted nothing: the
  rows stayed in the store. Each message now gets its own id (`7b571bf`).
- Stream appends share one sqlite transaction per event-loop task. A reader
  behind the 16 MiB kept in memory reads the store in ranges of up to 512
  entries or 4 MiB, not one query per delivery. Entries that have left memory
  keep only a timestamp and a size, not an object each (`08d1bb5`).
- The disk alarm samples free space every 5 s, or on every sweep when it is
  near the limit. `statfs` took about 17 ms a call in Docker Desktop, 9% of a
  1 CPU broker's time at the 200 ms sweep (`08d1bb5`).

Bun before → after, the morning cell against this run (median, msg/s unless
marked; process memory in MiB):

| Cell | Before | After | Memory before → after |
| --- | ---: | ---: | ---: |
| 1 CPU stream | 26,224 | 81,878 | 222 → 216 |
| 1 CPU large-1m | 444.0 MB/s | 532.5 MB/s | 91 → 98 |
| 1 CPU large-64k | 470.8 MB/s | 523.1 MB/s | 92 → 106 |
| 1 CPU priority | 104,613 | 129,451 | 95 → 89 |
| 1 CPU transactions | 47,953 | 57,596 | 70 → 62 |
| 1 CPU telemetry | 456,653 | 481,848 | 60 → 58 |
| 4 CPU work-queue | 116,538 | 153,431 | 202 → 196 |
| 4 CPU telemetry | 353,953 | 425,089 | 168 → 158 |
| 4 CPU fanout-20 | 149,965 | 171,087 | 2,050 → 372 |
| 4 CPU quorum | unscored | 17,131 | 750 → 328 |
| 4 CPU heavy | 310,204 | 266,956 | 602 → 530 |
| 1 CPU many-queues p99 | 2.13 ms | 6.55 ms | 95 → 86 |
| 1 CPU slow-consumers p99 | 5.38 ms | 9.34 ms | 66 → 62 |

Interleaved A/B runs (`python3 bench/ab.py`, `bench/results-quick/`, not
committed) checked each change against the code before it:

- 4 CPU fanout-20: 1,945 → 368 MiB process memory, 3,933 → 417 MiB container
  memory, rate unchanged (151,559 → 153,096). The id fix alone gives the same
  memory.
- 1 CPU stream: 24,430 (22,733–24,584) → 78,992 (77,114–84,002) msg/s at
  217–219 MiB. Without the compact index the same rate held 651–737 MiB.
- Disk sampling, 1 CPU: telemetry +2% and priority +3% with no overlap;
  latency-10k p99 2.27 → 0.91 ms (median of three).
- 4 CPU heavy and routing-64 do not separate from the code before them
  (heavy 329,320 vs 329,140; routing-64 drifted from 296k to 145k in both
  images within one run). The lower heavy cell above is the machine.
- 1 CPU many-queues and slow-consumers p99, published code on Bun 1.4.2
  against this code, both images run now: 3.3–6.2 vs 4.0–7.7 ms and 0.3–15.5
  vs 9.7–16.6 ms. The ranges overlap; this run does not show a change from
  the code. The morning's 1.97 and 2.30 ms for many-queues were on Bun 1.4.2.

Tried and not kept: credit and backpressure for consumers on another Bun
process, and one body copy per remote delivery, are kept for correctness (the
copy also stopped dropping message properties between Bun processes) but did
not change any cell here, because every benchmark consumer reads a queue on its
own process. Batching cluster socket writes per task was neutral and was
reverted. `BUN_OPTIONS=--smol` saved no memory and cost 4 CPU work-queue 13%.

At 1 CPU most of Bun's ~90 MiB is the runtime: 61 MiB before the first
connection, with a live JavaScript heap of 10–20 MiB under work-queue.

### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 9,310.34 | 32000 | 4.41 | 7,022.51 | 32000 |
| Rust | 19,026.76 | 48000 | 2.07 | 23,775.86 | 96000 |
| Bun | 20,437.59 | 64000 | 0.97 | 23,845.48 | 96000 |
| PHP | 6,695.41 | 16000 | 9.90 | 10,436.78 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,604.59 | 2000 | 0.45 | 2,202.27 | 6400 |
| Rust | 1,518.57 | 1000 | 0.33 | 3,204.46 | 800 |
| Bun | 2,704.95 | 8000 | 0.21 | 3,398.79 | 12800 |
| PHP | 1,432.29 | 2000 | 0.36 | 2,142.75 | 6400 |

### Load

| Container | Broker | single confirm/s | single consume/s | shared confirm/s | shared consume/s | spread confirm/s | spread consume/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 55,637.0 | 55,632.2 | 40,960.0 | 40,959.9 | 41,172.8 | 41,239.8 | 180 |
| 1 CPU / 512 MiB | Rust | 151,312.0 | 151,317.4 | 100,552.1 | 62,989.5 | 151,524.4 | 151,546.2 | 365 |
| 1 CPU / 512 MiB | Bun | 162,160.0 | 162,168.0 | 74,481.4 | 74,945.5 | 199,632.0 | 200,344.0 | 195 |
| 1 CPU / 512 MiB | PHP | 11,840.0 | 11,840.0 | 81,814.0 | 81,814.0 | 75,295.9 | 75,506.8 | 184 |
| 1 CPU / 1 GiB | RabbitMQ | 53,795.9 | 53,804.6 | 38,629.5 | 38,811.0 | 40,813.8 | 40,854.9 | 181 |
| 1 CPU / 1 GiB | Rust | 163,130.8 | 163,130.9 | 105,525.9 | 61,090.6 | 143,286.2 | 143,230.4 | 362 |
| 1 CPU / 1 GiB | Bun | 97,816.0 | 97,824.0 | 158,672.0 | 158,583.0 | 188,969.5 | 189,377.5 | 210 |
| 1 CPU / 1 GiB | PHP | 11,808.0 | 11,808.0 | 76,244.0 | 76,244.0 | 72,301.5 | 72,413.1 | 177 |
| 2 CPU / 2 GiB | RabbitMQ | 67,773.8 | 67,769.0 | 65,547.4 | 65,825.5 | 81,053.1 | 81,258.8 | 199 |
| 2 CPU / 2 GiB | Rust | 198,000.0 | 197,997.5 | 183,290.0 | 85,598.5 | 250,681.1 | 250,649.5 | 538 |
| 2 CPU / 2 GiB | Bun | 225,340.0 | 225,352.0 | 65,886.2 | 66,558.2 | 360,344.0 | 360,240.0 | 282 |
| 2 CPU / 2 GiB | PHP | 11,888.0 | 11,888.0 | 76,672.0 | 76,672.0 | 78,290.0 | 78,134.2 | 190 |
| 4 CPU / 4 GiB | RabbitMQ | 76,623.0 | 76,630.5 | 67,982.0 | 67,918.0 | 159,719.5 | 159,492.4 | 245 |
| 4 CPU / 4 GiB | Rust | 203,650.8 | 203,649.4 | 245,078.6 | 124,121.1 | 384,363.2 | 384,290.6 | 701 |
| 4 CPU / 4 GiB | Bun | 175,048.0 | 175,048.0 | 202,737.1 | 202,474.6 | 356,653.8 | 356,797.8 | 447 |
| 4 CPU / 4 GiB | PHP | 11,760.0 | 11,751.1 | 76,672.1 | 76,254.0 | 82,567.5 | 82,386.8 | 205 |
| 4 CPU / 8 GiB | RabbitMQ | 77,608.0 | 77,596.2 | 66,674.9 | 65,965.4 | 156,266.4 | 156,328.1 | 228 |
| 4 CPU / 8 GiB | Rust | 199,301.2 | 199,296.0 | 216,175.0 | 109,002.2 | 361,168.5 | 361,130.1 | 722 |
| 4 CPU / 8 GiB | Bun | 158,304.0 | 158,308.0 | 206,693.9 | 206,959.9 | 437,701.2 | 437,725.2 | 437 |
| 4 CPU / 8 GiB | PHP | 11,920.0 | 11,920.0 | 81,813.9 | 81,813.9 | 77,862.8 | 77,807.4 | 196 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 43,341 [43,074–43,608; n=2] | 130,415 [129,918–130,911; n=2] | 125,690 [123,419–127,962; n=2] | 39,139 [38,774–39,505; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 104,101 [103,745–104,457; n=2] | 94,260 [94,143–94,378; n=2] | 481,848 [478,839–484,856; n=2] | 90,752 [89,844–91,659; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,547 [47,106–47,988; n=2] | 135,620 [134,244–136,995; n=2] | 264,769 [264,298–265,241; n=2] | 84,598 [81,360–87,836; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,298 [41,914–42,682; n=2] | 91,002 [87,815–94,189; n=2] | 142,215 [141,320–143,110; n=2] | 52,064 [51,253–52,874; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 778.0 MB/s [776.9–779.1; n=2] | 252.3 MB/s [252.2–252.5; n=2] | 523.0 MB/s [514.9–531.2; n=2] | 230.6 MB/s [215.2–246.1; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,345.8 MB/s [1,336.2–1,355.4; n=2] | 573.9 MB/s [554.9–592.9; n=2] | 532.5 MB/s [530.0–534.9; n=2] | 605.0 MB/s [590.1–619.8; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 1.73 ms [1.30–2.16; n=2] | 1.08 ms [0.79–1.36; n=2] | 2.35 ms [1.86–2.84; n=2] | 20.69 ms [15.57–25.80; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 12.80 ms [12.07–13.54; n=2] | 1.07 ms [1.00–1.14; n=2] | 2.99 ms [1.84–4.14; n=2] | 53.74 ms [40.93–66.55; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 16.53 ms [15.29–17.77; n=2] | 6.45 ms [2.14–10.75; n=2] | 6.55 ms [6.30–6.80; n=2] | 24.16 ms [23.66–24.67; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 10.72 ms [10.44–11.01; n=2] | 2.25 ms [1.01–3.48; n=2] | 9.34 ms [4.87–13.81; n=2] | 260.07 ms [257.89–262.25; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 96,107 [95,295–96,920; n=2] | 8,414 [8,304–8,524; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 101,429 [98,823–104,035; n=2] | 81,878 [78,292–85,464; n=2] | 33,846 [33,535–34,156; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 32,635 [32,588–32,682; n=2] | 112,740 [107,073–118,408; n=2] | 129,451 [128,394–130,507; n=2] | 17,859 [17,847–17,871; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 21,134 [20,235–22,033; n=2] | 35,336 [32,090–38,583; n=2] | 57,596 [57,268–57,924; n=2] | 26,927 [26,848–27,006; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 34,843 [34,829–34,858; n=2] | 95,672 [93,922–97,422; n=2] | 122,937 [122,911–122,963; n=2] | 59,746 [59,256–60,236; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,235 / 50,007 [50,002–50,012; n=2] | 146,492 / 150,000 [150,000–150,000; n=2] | 149,614 / 149,448 [149,338–149,558; n=2] | 66,529 / 99,960 [99,931–99,989; n=2] |

Peak memory, 1 CPU / 1 GiB (anonymous / cgroup total MiB):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 122 / 194 | 9 / 93 | 92 / 147 | 12 / 108 |
| Fire-and-forget telemetry | 193 / 254 | 64 / 186 | 58 / 94 | 12 / 25 |
| Broadcast to 20 services | 144 / 186 | 12 / 1,024 | 106 / 120 | 18 / 350 |
| 64 queues, own publisher each | 190 / 220 | 26 / 1,024 | 106 / 121 | 22 / 243 |
| 64 KiB messages | 266 / 368 | 10 / 82 | 106 / 116 | 12 / 54 |
| 1 MiB messages | 240 / 335 | 13 / 82 | 98 / 109 | 16 / 157 |
| Latency at 1,000 msg/s | 120 / 152 | 6 / 42 | 47 / 76 | 10 / 27 |
| Latency at 10,000 msg/s | 122 / 153 | 6 / 74 | 70 / 80 | 11 / 34 |
| 500 queues, 1,000 connections | 376 / 422 | 110 / 1,024 | 86 / 125 | 44 / 66 |
| Slow workers, prefetch 1 | 132 / 176 | 8 / 76 | 62 / 90 | 19 / 38 |
| Quorum queue | 578 / 971 | 791 / 1,024 | 660 / 672 | 33 / 304 |
| Stream queue | 120 / 1,024 | 230 / 1,024 | 216 / 1,024 | 261 / 758 |
| Priority queue | 120 / 152 | 7 / 77 | 89 / 100 | 12 / 50 |
| Transactions | 121 / 151 | 7 / 75 | 62 / 75 | 12 / 34 |
| Heavy mixed load | 202 / 234 | 62 / 1,024 | 292 / 340 | 50 / 932 |
| Backlog fill and drain | 162 / 534 | 465 / 813 | 632 / 852 | 646 / 1,016 |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 78,439 [76,724–80,154; n=2] | 197,010 [190,760–203,260; n=2] | 153,431 [152,645–154,216; n=2] | 36,624 [35,842–37,406; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 326,615 [325,030–328,199; n=2] | 175,799 [175,792–175,806; n=2] | 425,089 [422,507–427,672; n=2] | 83,247 [82,634–83,860; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 161,952 [161,576–162,329; n=2] | 183,530 [182,793–184,268; n=2] | 171,087 [170,235–171,940; n=2] | 84,941 [83,473–86,409; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 139,264 [137,458–141,070; n=2] | 123,730 [123,030–124,430; n=2] | 269,626 [252,508–286,745; n=2] | 48,245 [48,041–48,449; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.29 ms [3.01–3.56; n=2] | 2.56 ms [1.63–3.50; n=2] | 2.01 ms [1.29–2.73; n=2] | 51.02 ms [40.79–61.26; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 85,414 [85,270–85,557; n=2] | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 17,131 [17,075–17,187; n=2] | 7,749 [7,569–7,930; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 122,603 [122,138–123,069; n=2] | 252,798 [249,582–256,015; n=2] | 266,956 [235,385–298,526; n=2] | 58,668 [55,476–61,860; n=2] |

Peak memory, 4 CPU / 4 GiB (anonymous / cgroup total MiB):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 142 / 172 | 11 / 79 | 196 / 311 | 12 / 79 |
| Fire-and-forget telemetry | 184 / 238 | 110 / 230 | 158 / 298 | 11 / 22 |
| Broadcast to 20 services | 178 / 214 | 14 / 1,336 | 372 / 416 | 21 / 366 |
| 64 queues, own publisher each | 236 / 299 | 40 / 1,892 | 415 / 487 | 22 / 244 |
| 500 queues, 1,000 connections | 432 / 480 | 136 / 2,224 | 316 / 460 | 44 / 67 |
| Quorum queue | 313 / 770 | 3,078 / 4,096 | 328 / 448 | 33 / 288 |
| Heavy mixed load | 300 / 410 | 108 / 1,124 | 530 / 602 | 49 / 1,106 |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 454.7 MiB |
| RabbitMQ | 2 CPU / 2g | 18,750 | 1.93 GiB |
| RabbitMQ | 4 CPU / 4g | 41,622 | 3.579 GiB |
| RabbitMQ | 4 CPU / 8g | 62,065 | 5.55 GiB |
| Rust | 1 CPU / 512m | 8,980 | 503.4 MiB |
| Rust | 2 CPU / 2g | 34,600 | 1.876 GiB |
| Rust | 4 CPU / 4g | 67,086 | 3.634 GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.126 GiB |
| Bun | 1 CPU / 512m | 43,875 | 385.6 MiB |
| Bun | 2 CPU / 2g | 116,692 | 1.329 GiB |
| Bun | 4 CPU / 4g | 249,230 | 2.974 GiB |
| Bun | 4 CPU / 8g | 260,000 | 3.032 GiB |
| PHP | 1 CPU / 512m | 28,000 | 210.9 MiB |
| PHP | 2 CPU / 2g | 40,000 | 296.4 MiB |
| PHP | 4 CPU / 4g | 40,000 | 298.6 MiB |
| PHP | 4 CPU / 8g | 20,000 | 159.5 MiB |

## Session (2026-10-11)

Everything in this section was measured on 2026-10-11 (03:15Z–07:24Z) in one pass, from images built from the tree of that day, with the harness in [`bench/`](bench/). The scenario matrix ran first with two repeats per cell (`bench/scenarios.py matrix --repeat 2`, Docker Desktop at 8 CPUs and 25159827456 bytes); a cell shows the median and the `[min–max; n]` range, and a cell with a failed or missed repeat is shown but not scored. `bench/run-all.py --skip scenarios` then ran the load sweep, the connection hold, and the paced ladder (back at 2 CPUs and 8320565248 bytes). `bench/report.py` turned `bench/results/2026-10-11/` into the tables below and into `site/app/bench-data.ts`. The text uses UTC; `bench.log` records local UTC−03:00 times.

Images (`bench/results/2026-10-11/run-meta.json`):

- `rabbitmq:4.3-management` `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`
- `queueforge-rust:bench` `sha256:c2f9c82fb82d9a29b5f3d81c5871e075d7d1fd1425941fe5fdb06ab20124bc8c`
- `queueforge-bun:bench` `sha256:3f876ad3a2fbd919b50873c426810a9b6c055f8dbda3b393328afee64ae15ba2`
- `queueforge-php:bench` `sha256:0ea572b4ad6c7514317241088ff1eee33436625e80424c1fdb2ea6d539d07593`

Tests on that tree: Bun 194/194, Rust core, parity and broker tests pass, PHP 35/35 test files (native PHP 8.5). Conformance with the LDAP, OAuth and PKI fixtures: Bun 135/135, Rust 135/135, PHP 135/135, RabbitMQ 134/135 (no delayed-message plugin).

A first attempt at this run was stopped after 24 cells and discarded: a stray container from an earlier test run was using 1.2 CPUs of the VM. Its cells are kept apart in `/tmp/qf-ab/coco/results-2026-10-11-perturbed`, not in `bench/results`.

### What changed since 2026-10-09

A before → after pair is the published 2026-10-09 cell against this run, at 1 CPU / 1 GiB unless noted; pairs marked A/B are a Docker check of one image pair on the same day.

- Bun: the durable log's `fdatasync` runs on a worker thread, so a group commit no longer stops every socket. 500-queue p99 38 → 3.2 ms (4 CPU 258 → 1.8 ms); 10,000 msg/s p99 14.7 → 4.0 ms.
- Bun: a named exchange that routes to one local classic queue takes the fast publish path (PerfTest publishes through `amq.direct`), routes are cached, and the delivery encoder keeps the encoded tag, exchange and key. Work queue 97.6k → 115.6k, priority 95.9k → 106.7k, heavy 95k → 121.5k (A/B).
- Bun: a backlog past 64 MiB of synced bodies stays in the durable log and is read back on demand, instead of being copied into SQLite in 80,000-row transactions. Backlog fill 30.0k → 141.4k msg/s, drain 74.8k → 149.5k.
- Rust: `tx.commit` flushes each queue once and awaits them together. Transactions 199 → 41,297 msg/s. Two or three lone confirms no longer wait out the 10 ms group interval: paced fan-2x2 at one confirm 197.87 → 3,454.48 msg/s.
- PHP: the 1 MiB OOM was a protocol bug: a body went out as one frame past the negotiated `frame_max`, and the Java client dropped the connection and the messages were redelivered forever. Deliveries are now split at the client's `frame_max` (large-1m: failed (oom) → 610 MB/s). Quorum queues no longer rewrite `topology.json` and fsync per message, nor scan every message per append: 589 → 8,478 msg/s. Streams sync once per loop pass and page old records to disk: 791 → 32,330 msg/s. PHP now runs one process unless `QUEUEFORGE_CORES` asks for more; it delivered more in every 4 CPU scenario than one process per core (work queue 10,050 → 32,560, heavy 2,802 → 65,603), but see the load and connection rows below. TLS now accepts PLAIN without a client certificate, as RabbitMQ does (conformance 134 → 135).

### Paced

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 11,502.52 | 32000 | 3.94 | 11,538.30 | 32000 |
| Rust | 20,825.92 | 64000 | 1.80 | 24,621.59 | 96000 |
| Bun | 20,375.59 | 64000 | 1.10 | 23,325.64 | 96000 |
| PHP | 6,599.79 | 16000 | 10.29 | 11,403.55 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,823.69 | 4000 | 0.36 | 2,289.92 | 6400 |
| Rust | 2,032.03 | 8000 | 0.27 | 3,454.48 | 12800 |
| Bun | 2,687.11 | 8000 | 0.20 | 3,344.90 | 12800 |
| PHP | 1,781.62 | 4000 | 0.32 | 2,291.39 | 6400 |

### Load

| Container | Broker | single confirm/s | single consume/s | shared confirm/s | shared consume/s | spread confirm/s | spread consume/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 57,041.2 | 57,035.6 | 41,984.0 | 42,694.0 | 44,294.2 | 44,301.4 | 190 |
| 1 CPU / 512 MiB | Rust | 169,909.4 | 169,914.9 | 108,121.9 | 63,683.4 | 151,411.5 | 151,385.5 | 368 |
| 1 CPU / 512 MiB | Bun | 192,388.0 | 192,388.0 | 165,168.0 | 165,208.0 | 211,872.0 | 211,928.0 | 202 |
| 1 CPU / 512 MiB | PHP | 10,272.0 | 10,272.0 | 80,528.0 | 80,528.0 | 82,568.8 | 82,568.8 | 210 |
| 1 CPU / 1 GiB | RabbitMQ | 57,601.1 | 57,587.6 | 44,032.0 | 43,681.8 | 44,412.5 | 44,444.5 | 194 |
| 1 CPU / 1 GiB | Rust | 173,158.9 | 173,153.6 | 106,040.1 | 66,281.6 | 152,380.6 | 152,375.6 | 368 |
| 1 CPU / 1 GiB | Bun | 197,688.0 | 197,672.0 | 148,464.0 | 149,000.0 | 184,464.0 | 184,904.0 | 212 |
| 1 CPU / 1 GiB | PHP | 10,352.0 | 10,352.0 | 77,528.1 | 77,537.9 | 86,845.5 | 87,209.8 | 214 |
| 2 CPU / 2 GiB | RabbitMQ | 75,097.2 | 75,103.2 | 73,728.0 | 73,516.9 | 86,539.4 | 86,501.1 | 221 |
| 2 CPU / 2 GiB | Rust | 202,128.1 | 202,124.0 | 193,175.1 | 93,385.8 | 274,706.6 | 274,661.0 | 565 |
| 2 CPU / 2 GiB | Bun | 207,380.0 | 207,380.0 | 172,916.0 | 172,796.0 | 329,948.0 | 330,148.0 | 297 |
| 2 CPU / 2 GiB | PHP | 10,144.0 | 10,144.0 | 87,382.0 | 87,382.0 | 88,557.2 | 88,557.2 | 207 |
| 4 CPU / 4 GiB | RabbitMQ | 80,289.0 | 80,284.0 | 76,978.6 | 76,258.5 | 167,793.8 | 167,964.6 | 287 |
| 4 CPU / 4 GiB | Rust | 200,590.4 | 200,592.8 | 240,802.8 | 118,389.0 | 406,018.0 | 406,040.6 | 833 |
| 4 CPU / 4 GiB | Bun | 209,088.0 | 209,096.0 | 171,984.0 | 171,792.0 | 585,756.0 | 586,020.0 | 534 |
| 4 CPU / 4 GiB | PHP | 9,840.0 | 9,840.0 | 87,810.0 | 87,548.2 | 88,558.1 | 88,578.8 | 216 |
| 4 CPU / 8 GiB | RabbitMQ | 79,233.8 | 79,219.5 | 76,952.6 | 76,964.1 | 167,304.1 | 167,345.5 | 247 |
| 4 CPU / 8 GiB | Rust | 204,938.6 | 204,939.4 | 239,406.4 | 123,882.0 | 390,902.2 | 391,099.1 | 714 |
| 4 CPU / 8 GiB | Bun | 227,064.0 | 227,064.0 | 176,656.0 | 176,884.0 | 591,260.0 | 591,144.0 | 531 |
| 4 CPU / 8 GiB | PHP | 9,840.0 | 9,840.0 | 86,524.0 | 86,564.4 | 88,130.0 | 88,130.0 | 215 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 44,423 [44,190–44,657; n=2] | 127,384 [126,525–128,244; n=2] | 117,618 [116,988–118,249; n=2] | 33,013 [32,980–33,045; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 102,552 [102,545–102,559; n=2] | 97,287 [94,053–100,520; n=2] | 450,079 [450,045–450,114; n=2] | 91,230 [90,498–91,961; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 47,742 [47,648–47,835; n=2] | 134,809 [134,316–135,302; n=2] | unscored (1/2 complete; 1 failed, 0 incomplete, 0 missed rate) | 76,247 [66,960–85,534; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 42,421 [42,126–42,716; n=2] | 91,608 [89,502–93,714; n=2] | 138,157 [137,266–139,048; n=2] | 54,315 [54,263–54,366; n=2] |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 775.0 MB/s [773.2–776.8; n=2] | 255.3 MB/s [254.0–256.6; n=2] | 452.4 MB/s [447.8–456.9; n=2] | 290.9 MB/s [281.2–300.7; n=2] |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,333.4 MB/s [1,320.0–1,346.8; n=2] | 566.0 MB/s [552.3–579.8; n=2] | 433.8 MB/s [433.4–434.2; n=2] | 610.0 MB/s [605.2–614.9; n=2] |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 3.75 ms [3.22–4.27; n=2] | 2.14 ms [1.74–2.54; n=2] | 2.60 ms [2.11–3.10; n=2] | 28.23 ms [25.85–30.61; n=2] |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 14.99 ms [14.37–15.62; n=2] | 2.46 ms [1.96–2.96; n=2] | 4.04 ms [3.50–4.59; n=2] | 26.62 ms [21.75–31.48; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 10.90 ms [7.65–14.15; n=2] | 6.18 ms [4.71–7.65; n=2] | 3.20 ms [2.19–4.21; n=2] | 36.19 ms [34.70–37.67; n=2] |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 14.29 ms [14.09–14.48; n=2] | 6.44 ms [3.56–9.32; n=2] | 4.76 ms [0.59–8.94; n=2] | 35.38 ms [34.20–36.55; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 88,427 [87,014–89,840; n=2] | 90,750 [90,227–91,273; n=2] | 8,478 [8,346–8,609; n=2] |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 11,528 [11,504–11,552; n=2] | 24,868 [22,577–27,158; n=2] | 32,330 [32,247–32,413; n=2] |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 34,187 [34,134–34,239; n=2] | 124,284 [124,095–124,473; n=2] | 112,213 [110,805–113,621; n=2] | 15,906 [15,814–15,998; n=2] |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 20,523 [20,002–21,045; n=2] | 41,297 [40,620–41,974; n=2] | 53,633 [50,989–56,277; n=2] | 15,191 [8,266–22,115; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 35,689 [35,575–35,803; n=2] | 98,535 [98,322–98,749; n=2] | 121,853 [121,539–122,167; n=2] | 65,431 [64,914–65,948; n=2] |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 74,543 / 50,007 [50,001–50,012; n=2] | 156,186 / 149,919 [149,838–150,000; n=2] | 141,357 / 149,456 [149,241–149,672; n=2] | 64,120 / 5,884 [5,884–5,884; n=2] |

Peak memory, 1 CPU / 1 GiB (anonymous / cgroup total MiB):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 122 / 152 | 8 / 78 | 102 / 112 | 12 / 78 |
| Fire-and-forget telemetry | 194 / 255 | 65 / 186 | 59 / 95 | 12 / 22 |
| Broadcast to 20 services | 144 / 184 | 12 / 1,024 | 104 / 114 | 19 / 368 |
| 64 queues, own publisher each | 188 / 224 | 26 / 1,024 | 102 / 116 | 22 / 232 |
| 64 KiB messages | 264 / 362 | 10 / 79 | 95 / 106 | 12 / 52 |
| 1 MiB messages | 240 / 330 | 14 / 89 | 95 / 106 | 17 / 154 |
| Latency at 1,000 msg/s | 120 / 152 | 6 / 42 | 54 / 86 | 10 / 27 |
| Latency at 10,000 msg/s | 120 / 151 | 6 / 74 | 74 / 86 | 12 / 30 |
| 500 queues, 1,000 connections | 372 / 416 | 110 / 1,024 | 98 / 139 | 42 / 64 |
| Slow workers, prefetch 1 | 124 / 160 | 10 / 78 | 68 / 92 | 12 / 29 |
| Quorum queue | 667 / 1,024 | 792 / 1,024 | 604 / 618 | 32 / 306 |
| Stream queue | 120 / 1,024 | 404 / 707 | 232 / 1,024 | 251 / 728 |
| Priority queue | 120 / 152 | 8 / 78 | 98 / 108 | 11 / 49 |
| Transactions | 115 / 144 | 6 / 72 | 70 / 82 | 10 / 31 |
| Heavy mixed load | 200 / 234 | 63 / 1,024 | 288 / 333 | 51 / 1,024 |
| Backlog fill and drain | 169 / 539 | 464 / 812 | 524 / 778 | 706 / 1,012 |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 79,742 [79,149–80,335; n=2] | 224,839 [222,658–227,019; n=2] | 143,478 [143,040–143,916; n=2] | 32,560 [32,353–32,767; n=2] |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 365,398 [364,367–366,428; n=2] | 190,163 [189,721–190,605; n=2] | 400,702 [396,624–404,780; n=2] | 92,435 [92,289–92,582; n=2] |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 182,490 [182,176–182,804; n=2] | 211,845 [210,683–213,007; n=2] | 157,279 [152,907–161,651; n=2] | 79,375 [67,814–90,936; n=2] |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 156,559 [155,072–158,046; n=2] | 124,678 [123,519–125,838; n=2] | 282,647 [279,011–286,284; n=2] | 54,222 [54,199–54,246; n=2] |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 3.56 ms [2.58–4.55; n=2] | 2.42 ms [1.97–2.86; n=2] | 1.81 ms [1.67–1.96; n=2] | 31.82 ms [24.68–38.97; n=2] |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 90,123 [86,136–94,110; n=2] | unscored (0/2 complete; 0 failed, 0 incomplete, 2 missed rate) | 10,022 [8,197–11,847; n=2] | 8,775 [8,756–8,794; n=2] |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 139,106 [137,587–140,624; n=2] | 237,896 [213,337–262,455; n=2] | 318,865 [315,489–322,241; n=2] | 65,603 [65,095–66,111; n=2] |

Peak memory, 4 CPU / 4 GiB (anonymous / cgroup total MiB):

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 138 / 170 | 12 / 78 | 208 / 323 | 12 / 80 |
| Fire-and-forget telemetry | 188 / 242 | 118 / 240 | 170 / 310 | 12 / 23 |
| Broadcast to 20 services | 174 / 210 | 14 / 1,336 | 2,198 / 4,066 | 21 / 362 |
| 64 queues, own publisher each | 238 / 310 | 36 / 1,904 | 416 / 468 | 22 / 236 |
| 500 queues, 1,000 connections | 420 / 467 | 136 / 2,220 | 345 / 482 | 43 / 66 |
| Quorum queue | 348 / 786 | 3,082 / 4,096 | 782 / 834 | 34 / 314 |
| Heavy mixed load | 342 / 430 | 153 / 1,220 | 608 / 684 | 44 / 1,065 |

### Connections

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 481.6MiB |
| RabbitMQ | 2 CPU / 2g | 18,750 | 1.831GiB |
| RabbitMQ | 4 CPU / 4g | 38,040 | 3.35GiB |
| RabbitMQ | 4 CPU / 8g | 61,562 | 5.088GiB |
| Rust | 1 CPU / 512m | 8,000 | 448.8MiB |
| Rust | 2 CPU / 2g | 34,600 | 1.875GiB |
| Rust | 4 CPU / 4g | 67,086 | 3.637GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.126GiB |
| Bun | 1 CPU / 512m | 36,777 | 379.3MiB |
| Bun | 2 CPU / 2g | 123,636 | 1.609GiB |
| Bun | 4 CPU / 4g | 253,332 | 3.136GiB |
| Bun | 4 CPU / 8g | 260,000 | 3.337GiB |
| PHP | 1 CPU / 512m | 1,000 | 23.91MiB |
| PHP | 2 CPU / 2g | 1,000 | 23.74MiB |
| PHP | 4 CPU / 4g | 1,000 | 23.68MiB |
| PHP | 4 CPU / 8g | 1,000 | 23.7MiB |

### Reading this run

**Site average.** The `/benchmark` page scores each broker as the geometric mean of its ratio to RabbitMQ over the scenario cells where both have a scored value (latency inverted). From this run: Bun 1.72× RabbitMQ over 20 of 21 cells, Rust 1.64× over 20, PHP 0.42× over 21. On the 2026-10-09 data the same formula gave Bun 0.73× (19 cells), Rust 1.54×, PHP 0.73× (12 cells). Bun's unscored cell is fanout at 1 CPU; Rust's is quorum at 4 CPU (it published faster than it delivered: 156,908 and 152,171 sent against 92,670 and 98,552 received).

**Where Bun leads Rust.** Telemetry (450,079 against 97,287 at 1 CPU, where Rust's p50 is 3.3 s; 400,702 against 190,163 at 4 CPU), streams (24,868 against 11,528), 64 queues (138,157 against 91,608; 282,647 against 124,678 at 4 CPU), heavy (121,853 against 98,535; 318,865 against 237,896), transactions (53,633 against 41,297), 64 KiB messages (452 against 255 MB/s), the 500-queue and slow-worker latencies, and the connection hold (260,000 against 95,000). The backlog drain and the 1 CPU quorum cell are even.

**Where Rust leads Bun.** The single work queue (127,384 against 117,618; 224,839 against 143,478 at 4 CPU), priority (124,284 against 112,213), 1 MiB messages (566 against 434 MB/s), the 1,000 and 10,000 msg/s latencies (p99 2.14 and 2.46 ms against 2.60 and 4.04), fanout at 4 CPU (211,845 against 157,279), and quorum at 4 CPU, where Bun delivers 10,022 msg/s: each queue's Raft group spans Bun's four sibling processes on one disk. Forced to one process on the same image, Bun's 4 CPU quorum cell delivered 97.9k msg/s (A/B, one repeat).

**Open problems this run found.**

- Bun: one of two fanout repeats at 1 CPU stalled about two seconds in: publishes, confirms and deliveries all stopped at 2% CPU for the rest of the cell. The other repeat delivered 233,672/s, and six more repeats on the same image afterwards all passed (249k–260k). The cause is not found; the cell stays unscored.
- PHP: with one process by default, the load sweep no longer scales past 1 CPU (spread at 4 CPU / 4 GiB 88,558 msg/s, against 288,669 on 2026-10-09 with one process per core, when the shared shape failed with `404 NOT_FOUND`), and the connection hold is 1,000 at every size (was 2,500 at 4 CPU). `QUEUEFORGE_CORES=cgroup` restores the per-core mode. The backlog drain is still 5,884 msg/s.
- PHP's 10,000 msg/s and 1,000 msg/s p99 (26.62 and 28.23 ms) are its slowest latency cells; RabbitMQ's quorum and stream cells missed their rate in both repeats, as in earlier sessions.

## Session (2026-10-09)

Everything in this section was measured on 2026-10-09 with the harness in [`bench/`](bench/), from images built from the tree of that day. `bench/run-all.py` runs it all: Docker Desktop goes to 8 CPUs and 25159827456 bytes for the load sweep, the scenarios and the connection hold, then back to 2 CPUs and 8320565248 bytes for the paced ladder. `bench/report.py` turns `bench/results/2026-10-09/` into the tables below and into `site/app/bench-data.ts`. Rust and RabbitMQ are the first pass (11:45Z–14:26Z). PHP's load and scenario cells are the second pass (14:37Z–15:16Z), because the PHP image of the first pass caught another session's half-written `Protocols.php` and did not boot. Bun's load, scenario and paced cells are the third pass (15:35Z on), on the final Bun image. Bun's connection hold is from the first pass on image `sha256:2a9ea15e5eabb3dac277ec1189b120dd23b28042bbab5a6980a8d845595493af`; it has not been remeasured on the final image. The text uses UTC; `bench.log` records local UTC−03:00 times. These measurements describe the recorded images, not every later change in the source tree.

Load/scenario images (`bench/results/2026-10-09/run-meta.json` records earlier passes too):

- `rabbitmq:4.3-management` `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`
- `queueforge-rust:bench` `sha256:38137db59067dd0cb8ca1c369068c6051c7f5d5aa6f432f001d51de5d4a5aa9e`
- `queueforge-php:bench` `sha256:ef6e84c66cba7abb25b614b05d61d6fbe21afaa396e6af9c390ed2fa7bc90acd`
- `queueforge-bun:bench` `sha256:3fbdcbbaf7ff0cd87b78966085cda98bd2c65459edf7a7d0c14273cf1b1c4b82`

### What is new

**Application scenarios.** `bench/scenarios.py` runs RabbitMQ's own load tool, PerfTest 2.25.0 (`pivotalrabbitmq/perf-test:2.25.0`), against each broker: a fresh broker container per cell, pinned to CPU 0 (1 CPU / 1 GiB) or CPUs 0–3 (4 CPU / 4 GiB), and the client on CPUs 4–6 with 3 CPUs and 4 GiB. Every cell runs 25 s and drops the first 5 s and the last partial second; rates are the mean of PerfTest's per-second samples, and a p50 or p99 is the median of the per-second percentiles. `many-queues` runs 50 s and drops 20. The backlog cell fills 300,000 messages with no consumer, then drains them with four. Memory is the cgroup peak, sampled once a second: `anon` is the process heap, `cgroup` adds page cache. The 1 CPU size runs all 16 scenarios; the 4 CPU size runs the seven that are about load. A cell fails when the broker is OOM-killed or exits, when PerfTest exits non-zero, or when it logs an error that is not its own teardown.

**Fixes the scenarios forced.** Before these runs PerfTest could not drive Bun at all, and several scenarios broke or stalled QueueForge:

- No engine sent the RabbitMQ `version` its clients key on. Bun sent none, and PerfTest stopped with a NullPointerException; Rust and PHP sent `0.1.0`, so PerfTest took them for a pre-4.3 broker and declared transient non-exclusive queues, which 4.3 refuses (`transient_nonexcl_queues`, 541). All three now send `version` 4.3.0 and their own build as `queueforge_version`.
- Bun: a confirm window of 17 to 127 (PerfTest uses 50) waited for the 10 ms fsync timer before its flush. It now flushes once the reads that are ready are handled.
- Bun: a publish through a named exchange on more than one core was forwarded between processes for every message. A connection now moves to the process that homes every queue its exchange routes to.
- Bun: a message body over 131,072 bytes went out as one frame larger than the negotiated `frame_max`; Java and other clients close the connection. 1 MiB messages were undeliverable. Body frames are now split at the negotiated size.
- Bun: three ways to exit or exhaust memory under a backlog. A log batch larger than one 4 MiB chunk reused the preallocated chunk and threw; a relocated row hit `UNIQUE constraint failed` in the SQLite catch-up; both threw from a timer and killed the process. Every queued body and every stream entry stayed in memory, and each stored message pinned its whole socket read buffer through a 20-byte property view. A 300,000-message backlog was OOM-killed in 1 GiB, and a stream with no retention grew until it was. Classic messages behind 2,048 ready (or 64 MiB) and stream bodies past 16 MiB per stream are now read back from the store, as RabbitMQ pages to disk.
- Bun: `tx.commit` awaited each publish's fsync in turn; the publishes of one commit now share one (12,921 msg/s at p50 1.12 ms before, 46,479 at p50 0.35 ms after, in a two-image check on the 2 CPU host). A fanout publish awaited each remote queue in turn, and now sends one forward per process: the 4 CPU fanout cell went from 6,917 deliveries/s in the first pass to 28,641–30,128.

### Paced

The ladder of the earlier sessions: `queueforge-compare`, Mac client through the published port, one broker at a time at 1 CPU / 512 MiB.

#### 128 confirms in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 11,545.89 | 32000 | 3.84 | 11,855.48 | 32000 |
| Rust | 21,032.90 | 64000 | 1.75 | 23,993.55 | 96000 |
| Bun | 20,357.20 | 64000 | 0.61 | 24,306.63 | 96000 |
| PHP | 7,004.91 | 16000 | 9.84 | 11,124.00 | 32000 |

#### One confirm in flight

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1,827.86 | 4000 | 0.37 | 2,273.10 | 6400 |
| Rust | 2,086.78 | 8000 | 0.28 | 197.87 | 800 |
| Bun | 2,779.87 | 8000 | 0.19 | 3,678.05 | 12800 |
| PHP | 1,146.85 | 4000 | 0.42 | 2,188.24 | 6400 |

### Load sweep

The shapes and client of [Load (2026-10-05T21:29:06Z)](#load-2026-10-05t212906z): `qf-loadgen:linux` (`bench/loadgen`), 2 s warmup, 8 s measure. A failed cell shows its error.

| Container | Broker | single confirm/s | single consume/s | shared confirm/s | shared consume/s | spread confirm/s | spread consume/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 53,568.0 | 53,567.4 | 41,984.0 | 41,984.0 | 45,760.2 | 45,789.2 | 190 |
| 1 CPU / 512 MiB | Rust | 175,360.1 | 175,360.1 | 59,836.4 | 34,719.8 | 157,075.1 | 157,219.5 | 368 |
| 1 CPU / 512 MiB | Bun | 205,140.0 | 205,140.0 | 162,848.0 | 162,824.0 | 146,304.0 | 146,304.0 | 159 |
| 1 CPU / 512 MiB | PHP | 11,232.0 | 11,232.0 | 51,401.9 | 51,278.2 | 81,285.0 | 81,383.0 | 207 |
| 1 CPU / 1 GiB | RabbitMQ | 58,114.6 | 58,129.6 | 45,056.0 | 45,198.1 | 44,635.6 | 44,616.4 | 196 |
| 1 CPU / 1 GiB | Rust | 174,773.5 | 174,779.1 | 116,388.6 | 68,803.4 | 153,490.6 | 153,455.0 | 367 |
| 1 CPU / 1 GiB | Bun | 180,636.0 | 180,644.0 | 161,520.0 | 161,504.0 | 205,584.0 | 205,600.0 | 157 |
| 1 CPU / 1 GiB | PHP | 11,152.0 | 11,152.0 | 49,688.0 | 49,835.0 | 77,007.4 | 76,805.5 | 181 |
| 2 CPU / 2 GiB | RabbitMQ | 78,934.0 | 78,943.9 | 74,752.0 | 75,338.5 | 89,170.8 | 89,523.0 | 245 |
| 2 CPU / 2 GiB | Rust | 189,818.4 | 189,817.9 | 189,525.0 | 93,801.8 | 250,179.1 | 250,176.8 | 502 |
| 2 CPU / 2 GiB | Bun | 170,456.0 | 170,452.0 | 144,472.1 | 144,464.1 | 277,712.0 | 277,688.0 | 248 |
| 2 CPU / 2 GiB | PHP | 32,048.0 | 32,048.0 | c17_404_NOT_FOUND_-_no_queue__q0_ |  | 170,484.2 | 170,507.5 | 417 |
| 4 CPU / 4 GiB | RabbitMQ | 83,586.1 | 83,592.2 | 80,154.2 | 80,820.8 | 172,532.1 | 172,583.6 | 274 |
| 4 CPU / 4 GiB | Rust | 177,606.4 | 177,595.8 | 244,256.8 | 120,327.6 | 381,878.9 | 381,938.6 | 700 |
| 4 CPU / 4 GiB | Bun | 189,068.0 | 189,068.0 | 155,968.0 | 155,992.0 | 411,904.0 | 411,888.0 | 398 |
| 4 CPU / 4 GiB | PHP | 31,296.0 | 31,296.0 | 47,546.0 | 47,576.4 | 288,669.0 | 288,512.6 | 332 |
| 4 CPU / 8 GiB | RabbitMQ | 83,574.6 | 83,571.1 | 82,234.2 | 82,252.1 | 173,112.8 | 173,110.0 | 241 |
| 4 CPU / 8 GiB | Rust | 195,854.9 | 195,861.5 | 239,698.4 | 123,188.1 | 399,582.0 | 399,594.9 | 792 |
| 4 CPU / 8 GiB | Bun | 165,220.0 | 165,216.0 | 149,984.0 | 149,974.0 | 461,888.0 | 461,936.0 | 384 |
| 4 CPU / 8 GiB | PHP | c2_404_NOT_FOUND_-_no_queue__q0_ |  | 50,116.0 | 50,116.0 | 292,520.1 | 292,480.2 | 334 |

### Scenarios, 1 CPU / 1 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 41,976 | 124,898 | 92,563 | 35,518 |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 94,088 | 88,042 | 69,776 | 88,917 |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 42,192 | 117,624 | 214,913 | 75,801 |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 34,480 | 73,944 | 92,925 | 51,665 |
| 64 KiB messages | Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s. | delivered MB/s | 634.3 MB/s | 255.0 MB/s | 312.5 MB/s | 245.9 MB/s |
| 1 MiB messages | Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s. | delivered MB/s | 1,240.5 MB/s | 390.3 MB/s | 362.8 MB/s | failed (oom) |
| Latency at 1,000 msg/s | A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency. | p99 latency at the offered rate | 4.23 ms | 1.46 ms | 6.50 ms | 13.45 ms |
| Latency at 10,000 msg/s | A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each. | p99 latency at the offered rate | 14.91 ms | 1.64 ms | 14.69 ms | 28.87 ms |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 13.35 ms | 7.43 ms | 38.42 ms | 33.97 ms |
| Slow workers, prefetch 1 | 20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s. | p99 latency at the offered rate | 12.53 ms | 0.67 ms | 22.82 ms | 37.66 ms |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 26,692 | 76,544 | 88,158 | 589 |
| Stream queue | An append-only log read by 2 consumers from the start, over AMQP 0-9-1. | delivered msg/s | 26,688 | 10,442 | 27,171 | 791 |
| Priority queue | x-max-priority=10, every message at priority 5, 2 producers and 2 consumers. | delivered msg/s | 31,130 | 113,478 | 79,705 | 16,991 |
| Transactions | tx.select publishers committing every 10 messages, transactional consumers acking every 10. | delivered msg/s | 19,233 | 199 | 17,704 | 21,550 |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 29,258 | 90,035 | 96,051 | 61,762 |
| Backlog fill and drain | A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them. | fill / drain msg/s | 71,864 / 50,009 | 145,338 / 149,850 | 29,988 / 74,814 | 60,000 / 811 (incomplete: sent 300000 got 76200) |

Peak memory, anonymous / cgroup MiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 125 / 161 | 15 / 83 | 798 / 840 | 20 / 88 |
| Fire-and-forget telemetry | 195 / 256 | 66 / 189 | 790 / 909 | 20 / 31 |
| Broadcast to 20 services | 151 / 193 | 18 / 1024 | 88 / 145 | 26 / 367 |
| 64 queues, own publisher each | 200 / 236 | 34 / 1024 | 96 / 126 | 28 / 255 |
| 64 KiB messages | 268 / 366 | 16 / 88 | 960 / 1024 | 18 / 61 |
| 1 MiB messages | 241 / 324 | 20 / 97 | 204 / 438 | 993 / 1022 |
| Latency at 1,000 msg/s | 127 / 159 | 14 / 48 | 54 / 84 | 18 / 35 |
| Latency at 10,000 msg/s | 127 / 159 | 12 / 83 | 68 / 91 | 16 / 35 |
| 500 queues, 1,000 connections | 392 / 441 | 111 / 1024 | 91 / 127 | 50 / 73 |
| Slow workers, prefetch 1 | 128 / 161 | 18 / 85 | 70 / 95 | 22 / 38 |
| Quorum queue | 573 / 954 | 794 / 1024 | 569 / 595 | 38 / 84 |
| Stream queue | 126 / 1024 | 366 / 634 | 241 / 1024 | 42 / 70 |
| Priority queue | 127 / 158 | 15 / 83 | 90 / 110 | 20 / 58 |
| Transactions | 126 / 157 | 14 / 23 | 295 / 1024 | 18 / 38 |
| Heavy mixed load | 214 / 248 | 68 / 1024 | 314 / 1024 | 53 / 1024 |
| Backlog fill and drain | 169 / 545 | 473 / 820 | 556 / 1024 | 763 / 1024 |

### Scenarios, 4 CPU / 4 GiB

| Scenario | What it stands for | Score | RabbitMQ | Rust | Bun | PHP |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Work queue | Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one. | delivered msg/s | 68,452 | 181,079 | 101,532 | 10,050 |
| Fire-and-forget telemetry | Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers. | delivered msg/s | 274,151 | 143,620 | 97,339 | 50,305 |
| Broadcast to 20 services | Domain events on a fanout exchange, each copied to 20 durable subscriber queues. | deliveries/s (20 per publish) | 144,495 | 189,063 | 28,641 | 7,269 |
| 64 queues, own publisher each | Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B. | delivered msg/s | 132,621 | 125,354 | 208,309 | 36,746 |
| 500 queues, 1,000 connections | Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer. | p99 latency at the offered rate | 2.17 ms | 0.80 ms | 258.10 ms | failed (client-exit-124) |
| Quorum queue | The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB. | delivered msg/s | 89,750 | 73,562 | 12,034 | failed (no-delivery) |
| Heavy mixed load | 64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each. | delivered msg/s | 123,802 | 240,587 | 234,593 | 2,802 |

Peak memory, anonymous / cgroup MiB:

| Scenario | RabbitMQ | Rust | Bun | PHP |
| --- | ---: | ---: | ---: | ---: |
| Work queue | 146 / 180 | 18 / 85 | 2147 / 2286 | 57 / 106 |
| Fire-and-forget telemetry | 203 / 268 | 100 / 223 | 1015 / 1158 | 378 / 446 |
| Broadcast to 20 services | 176 / 216 | 21 / 1343 | 1002 / 2949 | 61 / 169 |
| 64 queues, own publisher each | 247 / 307 | 39 / 2149 | 345 / 404 | 65 / 279 |
| 500 queues, 1,000 connections | 413 / 461 | 144 / 2231 | 344 / 498 | 67 / 107 |
| Quorum queue | 337 / 810 | 3082 / 4096 | 2209 / 2281 | 2340 / 2405 |
| Heavy mixed load | 323 / 506 | 143 / 1139 | 2098 / 3951 | 105 / 237 |

### Connections

The hold sweep of [Load compare](#load-compare) (`bench/connections.py`, `bench/connscale`): the largest number of simultaneous AMQP connections that stayed up and then passed 40 confirms. Memory is the container's use at that hold.

| Broker | Size | Held | Memory at hold |
| --- | --- | ---: | ---: |
| RabbitMQ | 1 CPU / 512m | 3,500 | 440.5MiB |
| RabbitMQ | 2 CPU / 2g | 20,000 | 1.862GiB |
| RabbitMQ | 4 CPU / 4g | 41,083 | 3.801GiB |
| RabbitMQ | 4 CPU / 8g | 64,370 | 5.676GiB |
| Rust | 1 CPU / 512m | 8,980 | 502.2MiB |
| Rust | 2 CPU / 2g | 34,600 | 1.873GiB |
| Rust | 4 CPU / 4g | 67,208 | 3.639GiB |
| Rust | 4 CPU / 8g | 95,000 | 5.121GiB |
| Bun | 1 CPU / 512m | 42,965 | 426.7MiB |
| Bun | 2 CPU / 2g | 117,420 | 1.506GiB |
| Bun | 4 CPU / 4g | 247,316 | 3.03GiB |
| Bun | 4 CPU / 8g | 260,000 | 3.121GiB |
| PHP | 1 CPU / 512m | 1,000 | 20.99MiB |
| PHP | 2 CPU / 2g | 1,250 | 52.99MiB |
| PHP | 4 CPU / 4g | 2,500 | 89.01MiB |
| PHP | 4 CPU / 8g | 2,500 | 88.89MiB |

### Reading the scenarios

These are the reported cells; Bun's three passes are in `bench/results/2026-10-09/bench.log`. Where they disagree by more than a tenth, both ends are given.

**Where QueueForge is ahead.** Bun leads fanout at 1 CPU (197,637 to 235,556 deliveries/s across passes, against Rust 117,624 and RabbitMQ 42,192), the 64-queue routing cell at both sizes (1 CPU 86,468–107,347; 4 CPU 208,309–236,174, against RabbitMQ 132,621 and Rust 125,354), quorum queues at 1 CPU (80,987–88,158, against Rust 76,544 and RabbitMQ 26,692), the heavy mixed cell at 1 CPU (91,083–96,051, against Rust 90,035 and RabbitMQ 29,258; at 4 CPU Bun's 202,133–249,098 brackets Rust's 240,587), and the connection hold (260,000 at 4 CPU / 8 GiB, against Rust 95,000 and RabbitMQ 64,370). Rust leads the work queue, priority, the backlog, and every latency row: at 1 CPU its p99 is 0.67 to 7.43 ms where RabbitMQ's is 4.23 to 14.91 ms. Both QueueForge engines pass 300,000 messages through the backlog, and Rust fills it at 145,338 msg/s, twice RabbitMQ.

**Where RabbitMQ is ahead.** Large messages: RabbitMQ delivers 634.3 MB/s of 64 KiB bodies and 1,240.5 MB/s of 1 MiB bodies, against 255.0–406.6 MB/s for Rust and Bun. RabbitMQ confirms before its flush and QueueForge after the fsync, and a 1 MiB body makes that wait the largest share of each confirm. Fire-and-forget telemetry at 4 CPU: RabbitMQ 274,151, Rust 143,620, Bun 96,887. Bun's work queue does not scale with cores (one queue lives on one process: 92,563–111,883 at 1 CPU, 95,350–101,532 at 4 CPU), nor do its fanout (28,641–30,128 at 4 CPU, every copy crosses processes) or its quorum queues (12,034–12,517 at 4 CPU: each queue's Raft group spans the four sibling processes on one disk, where RabbitMQ on one node runs a group of one).

**Open problems this run found.**

- Bun has no publisher flow control. Without confirms (telemetry) or inside transactions, publishers outrun consumers and the queue grows: telemetry p50 54 to 472 ms and 790+ MiB, transactions 10,786 to 37,052 msg/s with p50 0.67 to 2.6 s. RabbitMQ's credit flow holds the publisher back. In a two-image check on the 2 CPU host, the transactions cell went from 12,921 msg/s (p50 1.12 ms) before the `tx.commit` change to 46,479 (p50 0.35 ms) after; in the full passes the queue grew instead.
- Bun's peak memory varies across recorded builds and passes: work queue at 1 CPU 77 to 798 MiB anonymous, 64 KiB messages 90 to 960, 10,000 msg/s 61 to 883 (two reruns of that cell after the run: 54 and 60). The second and third passes use different image hashes, so these ranges do not isolate collector timing. The cause needs profiling and repeated runs on one frozen image.
- Bun's low-rate latency is noisy: at 1,000 msg/s the p50 was 0.15, 0.67 and 1.05 ms in three passes, the p99 3.42 to 6.50 ms.
- Rust: transactions run at 199 msg/s with a 45 ms p50; the cause is not traced yet. With one confirm in flight, paced fan-2x2 keeps 197.87 msg/s. The stream cell delivers 10,442 msg/s, under RabbitMQ's 26,688.
- PHP: 1 MiB messages are OOM-killed in 1 GiB, and the backlog drains at 811 msg/s, with only 76,200 of 300,000 messages received during the 90 s drain window; the incomplete cell is excluded from website score comparisons. Above 1 CPU, a connection can reach a process that has not seen its queue's declare, so the load sweep shows `404 NOT_FOUND - no queue 'q0'` and several 4 CPU scenarios deliver nothing or exit. Quorum queues at 1 CPU deliver 589 msg/s.
- Memory is not comparable from the cgroup column alone: SQLite and the log files fill page cache, so Rust and Bun show 1,024 MiB on cells where their heap is 18 to 314 MiB. The anonymous column is the process.

### Run it

```
docker compose -f docker-compose.bench.yml build
docker build -t qf-loadgen:linux bench/loadgen
docker build -t qf-connscale:linux bench/connscale
(cd rust && cargo build --release -p queueforge-compare)
QF_BENCH_SIDECARS=<containers to keep up> python3 bench/run-all.py
python3 bench/report.py
```

`bench/run-all.py --kinds bun --skip connections` reruns one broker and replaces its rows. `bench/scenarios.py one <scenario> [broker]` runs one cell at 1 CPU / 1 GiB.

## Session (2026-10-08)

Everything in this section was measured after the Raft and PHP work of 2026-10-08, on images built from that tree at 12:34Z–12:50Z. Docker Desktop was at 2 CPUs and 8320565248 bytes for the paced runs. The load sweeps raised it to 8 CPUs and 25159827456 bytes and restored it afterwards; Postgres was restarted and accepting connections both times.

Images:

- `queueforge-rust:bench` `sha256:4bb0b6352932242d4564a5793446259c71ce3d0d79c0065f09093bd9ec69920c`
- `queueforge-bun:bench` `sha256:e4c63bb2ba9ec3992e6b40a1c4eadd18c3e02ea9d9a376c1bd1b12454a2de958`
- `queueforge-php:bench` `sha256:9c7e5e910086fdb3233a797fa6815312871142117ea9d80c61d0a502446e7a62`
- `rabbitmq:4.3-management` `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`
- `queueforge-php:bench` after the handoff fix, for the PHP load rerun: `sha256:e9c2ecec344c115a7a69ef9e758e774b39f4484196022fa555b7455bc6f8c176`

### Paced, 128 confirms (2026-10-08T12:50:37Z)

Same client and scenarios as [Paced](#paced-2026-10-07t213657z): `rust/target/release/queueforge-compare`, rebuilt from unchanged source. One container at a time, 1 CPU and 512 MiB, host ports 35672–35675. A Rust build overlapped the first Rust cells, so Rust was rerun alone at 13:43Z; the rerun is below and the overlapped run had durable-256 18396.03 and fan-2x2 20111. Every scenario is `declare=ok publish=ok consume=ok ack=ok confirms=ok`.

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 10110.95 | 32000 | 4.13 | 10602.10 | 32000 |
| Rust | 20377.46 | 64000 | 1.85 | 24530.32 | 96000 |
| Bun | 17926.84 | 48000 | 2.16 | 23189.31 | 96000 |
| PHP | 6579.89 | 16000 | 10.60 | 10974.87 | 32000 |

### Paced, one confirm

| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |
| --- | ---: | ---: | ---: | ---: | ---: |
| RabbitMQ | 1633.88 | 4000 | 0.41 | 2095.87 | 6400 |
| Rust | 2491.43 | 4000 | 0.24 | 198.15 | 800 |
| Bun | 2318.39 | 8000 | 0.23 | 172.14 | 200 |
| PHP | 1636.71 | 4000 | 0.38 | 1912.44 | 6400 |

The short scenarios (`size-64`, `size-4096`, `transient-256`, `prefetch-1`, `prefetch-128`) kept every step on all four brokers in both runs, scoring 600.00, 250.00 or 1250.00, except Rust `size-64` at one confirm (595.25 on the overlapped run, 600.00 on the rerun).

### Load (2026-10-08T13:00:41Z)

Same client (`qf-loadgen:linux`), shapes, sizes, pins and 2 s warmup / 8 s measure as [Load remeasure](#load-2026-10-07t210340z). RabbitMQ, Rust and Bun are the 13:00:41Z sweep; all 45 of their cells are `ok=1`, `oom=false`, with no blocked connections, nacks, misses or returns, and `queueforge_confirm_before_fsync_total` 0 on every QueueForge cell. PHP is the 13:31Z rerun after the handoff fix, 15 of 15 cells `ok=1`. In the 13:00 sweep, PHP above 1 CPU passed the one-connection shape (about 33k/s, which failed with `no_consume` before the homes work) and failed both 16-connection shapes with `reset`/`eof`. The cause was the move of a connection to its queue's home process: `socket_recvmsg` sized its buffer from `buffer_size`, not from the `iov` the code passed, so every handoff datagram was cut to 8192 bytes and a connection that arrived with a deep publish window was lost. `php/src/Handoff.php` now sets `buffer_size`.

`confirm/s` and `consume/s` are both shown. On one shared queue Rust confirms run ahead of deliveries and the queue grows; the table on the site uses the lower of the two.

| Container | Broker | single confirm/s | single consume/s | shared confirm/s | shared consume/s | spread confirm/s | spread consume/s | spread MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 54418.2 | 54413.9 | 41963.4 | 42296.2 | 43022.9 | 43068.8 | 183 |
| 1 CPU / 512 MiB | Rust | 165861.2 | 165856.0 | 106618.1 | 64029.5 | 150341.5 | 150311.5 | 363 |
| 1 CPU / 512 MiB | Bun | 219556.0 | 219556.0 | 161088.0 | 161097.0 | 152448.0 | 152448.0 | 145 |
| 1 CPU / 512 MiB | PHP | 10704.0 | 10704.0 | 84384.0 | 84384.0 | 100537.0 | 100608.1 | 208 |
| 1 CPU / 1 GiB | RabbitMQ | 56560.0 | 56563.8 | 40483.1 | 40559.2 | 41014.6 | 41088.1 | 194 |
| 1 CPU / 1 GiB | Rust | 163104.0 | 163109.2 | 105064.2 | 64015.2 | 153594.0 | 153632.0 | 367 |
| 1 CPU / 1 GiB | Bun | 229916.0 | 229916.0 | 181136.0 | 181141.5 | 194000.0 | 193992.0 | 153 |
| 1 CPU / 1 GiB | PHP | 10720.0 | 10720.0 | 92092.0 | 92092.0 | 105243.0 | 105293.0 | 245 |
| 2 CPU / 2 GiB | RabbitMQ | 71457.8 | 71453.1 | 73728.0 | 73688.0 | 81944.5 | 81863.2 | 245 |
| 2 CPU / 2 GiB | Rust | 201546.6 | 201552.0 | 192043.2 | 95089.6 | 269472.9 | 269444.0 | 565 |
| 2 CPU / 2 GiB | Bun | 183544.0 | 183544.0 | 147696.0 | 147680.0 | 313856.0 | 313816.0 | 251 |
| 2 CPU / 2 GiB | PHP | 35008.0 | 35008.0 | 92092.1 | 92289.5 | 206849.8 | 206664.0 | 140 |
| 4 CPU / 4 GiB | RabbitMQ | 78450.9 | 78457.8 | 67762.5 | 67505.4 | 163720.0 | 163281.1 | 252 |
| 4 CPU / 4 GiB | Rust | 201461.4 | 201456.8 | 225623.5 | 110926.5 | 417402.2 | 417449.9 | 793 |
| 4 CPU / 4 GiB | Bun | 173436.0 | 173436.0 | 166752.0 | 166776.0 | 519616.0 | 519568.0 | 384 |
| 4 CPU / 4 GiB | PHP | 33072.0 | 33072.0 | 92950.0 | 92640.8 | 379881.0 | 380125.9 | 318 |
| 4 CPU / 8 GiB | RabbitMQ | 80848.9 | 80855.2 | 75887.1 | 75888.1 | 163131.8 | 162923.1 | 250 |
| 4 CPU / 8 GiB | Rust | 200998.9 | 201002.6 | 242289.1 | 118865.5 | 410258.1 | 410261.5 | 810 |
| 4 CPU / 8 GiB | Bun | 196044.0 | 196044.0 | 176320.0 | 176320.0 | 501680.0 | 501696.0 | 398 |
| 4 CPU / 8 GiB | PHP | 33776.0 | 33767.0 | 86524.0 | 86273.5 | 380481.8 | 380544.6 | 321 |

### Quorum queues with Raft (local, not a container run)

Three Rust debug builds on the Mac host, every member on one APFS disk, `fsync_interval_ms=10`, a 1-publisher probe (`conformance/raft-probe.ts`). On macOS each Raft log write and each queue log write is an `F_FULLFSYNC`, so these are latency figures for one laptop disk, not a capacity claim.

| Path | 1 in flight p50 ms | 1 in flight msg/s | 128 in flight p50 ms | 128 in flight msg/s |
| --- | ---: | ---: | ---: | ---: |
| Version 1 (majority ack, `QUEUEFORGE_RAFT=0`) | 10.3 | 88 | 26.0 | 4707 |
| Raft (commit = confirm) | 23.5 | 42 | 45.7 | 2558 |

Raft adds a log fsync on the leader and on a follower before every confirm. The numbers above are after pipelined appends and the leader's early `append` (docs/raft.md, section 7); before those, 128 in flight ran at about 1800/s.

## Load (2026-10-05T21:29:06Z)

Unpaced messages per second. This is a different client and a different definition from the paced ladder in [Latest](#latest-2026-10-05t090342z): the publisher fills a fixed confirm window, and `confirm/s` is confirms over the 8 s measure. The 09:03 paced rows stay the `queueforge-compare` score.

One broker at a time, on Docker network `qf-load`, with no host port. The client is a Linux epoll process (`qf-loadgen:linux`) pinned to CPUs 4–6. The broker is pinned to CPU 0, CPUs 0–1, or CPUs 0–3. Docker Desktop was at 8 CPUs and 25159827456 bytes for the sweep, then returned to 2 CPUs and 8320565248 bytes. Postgres stayed up.

Images: Bun `queueforge-bun:bench` `sha256:db73aeebf27f09274ed87a6fe62af45ae40bfb3037ca9aac1a897bddf1a64dfc` (2026-10-05T16:31:38Z), Rust `queueforge-rust:bench` `sha256:ceff58723095f58ca73335d32262e4d87dcdb156b04fbf9c0adbe8f8e662ecd2` (2026-10-05T16:34:19Z), RabbitMQ `rabbitmq:4.3-management` `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`. These QueueForge images are newer than the 09:03:42Z paced images. `password.rs` and `bun/src/broker/auth.ts` were edited after these images were built.

Each container has `--memory` and `--memory-swap` set to the same value: 1 CPU / 512 MiB, 1 CPU / 1 GiB, 2 CPU / 2 GiB, 4 CPU / 4 GiB, 4 CPU / 8 GiB. The queue is durable classic, declared with `x-queue-type=classic`. RabbitMQ 4.3 listed `q0 true classic [{"x-queue-type","classic"}]`. Body is 256 bytes, `delivery_mode=2`, publisher confirms, manual consumer acks. User is `admin` / `devpassword12`. Warmup is 2 s and the measure is 8 s (`elapsed_s=8.000` on every cell).

Shapes:

| Shape | Publishers | Consumers | Queues | Window | Prefetch | In flight |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single | 1 | 1 | 1 | 128 | 256 | 128 |
| shared | 16 | 16 | 1 | 512 | 1024 | 8192 |
| spread | 16 | 16 | 16 | 512 | 1024 | 8192 |

All 45 cells from 2026-10-05T21:29:06Z are `ok=1`, `oom=false`, `blocked=0`, `nacks=0`, `missed=0`, `returns=0`. Inflight ended on the window cap except Rust at 4 CPU / 8 GiB single, which ended at 117. PHP was measured on 2026-10-06T12:05:24Z with the same client, shapes, sizes, and pins. Those 15 cells are also `ok=1`, with no OOM, blocked connections, nacks, misses, or returns. Image `queueforge-php:bench` `sha256:f3113acd45996ecb624cc32c5d4b252ddf8f957d44f70a3737613d0c6f7d4cc1`. Docker Desktop was raised to 8 CPUs and 24576 MiB for that run, then returned to 2 CPUs and 8320565248 bytes. Postgres was accepting connections again. PHP does not expose `queueforge_confirm_before_fsync_total`. A durable confirm is released only after the covering fsync. These 15 cells predate the small-batch flush described in [PHP paced](#php-paced-2026-10-06t161324z), which only changes behaviour below 8 outstanding confirms; the single shape holds a 128 window, so it is the least affected.

`MiB` is cgroup `memory.current` during a 2 s sample that starts 0.5 s after the measure mark. It includes page cache. `CPU` is cgroup `usage_usec` over that same 2 s, in percent of one core. The client sat at 97–98 on every cell because the measure loop spins. p50 and p99 are the high edge of a 100 µs bucket. A printed 200.00 ms is the overflow bucket, so that sample is at least 200 ms.

QueueForge bench images use `fsync_policy=every_n_ms` and `fsync_interval_ms=10`. A durable confirm returns after the covering fsync. `queueforge_confirm_before_fsync_total` was 0 on every Bun cell and on every Rust cell whose scrape returned. The Rust 1 CPU / 512 MiB shared scrape timed out while that queue was growing; the same binary returned 0 on the neighboring cells. RabbitMQ classic confirms before its flush.

On the shared shape, Rust confirms run ahead of deliveries, so that queue is growing through the sample. The MiB there is early in the 8 s window. On the other shapes, confirms and deliveries stay together.

### One publisher, one consumer

| Container | Broker | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 51505.4 | 51507.2 | 2.40 | 5.10 | 97 | 219 |
| 1 CPU / 512 MiB | Rust | 141278.2 | 141278.4 | 0.80 | 2.70 | 79 | 92 |
| 1 CPU / 512 MiB | Bun | 140988.0 | 140980.0 | 0.60 | 3.40 | 83 | 427 |
| 1 CPU / 512 MiB | PHP | 9968.0 | 9968.0 | 12.80 | 25.10 | 13 | 85 |
| 1 CPU / 1 GiB | RabbitMQ | 53602.4 | 53602.4 | 2.30 | 3.90 | 97 | 149 |
| 1 CPU / 1 GiB | Rust | 161930.6 | 161930.6 | 0.80 | 1.80 | 87 | 35 |
| 1 CPU / 1 GiB | Bun | 135216.0 | 135208.0 | 0.60 | 4.30 | 85 | 371 |
| 1 CPU / 1 GiB | PHP | 10000.0 | 10000.0 | 12.90 | 17.00 | 13 | 31 |
| 2 CPU / 2 GiB | RabbitMQ | 71411.1 | 71407.0 | 1.70 | 4.90 | 161 | 168 |
| 2 CPU / 2 GiB | Rust | 187547.9 | 187546.8 | 0.70 | 2.00 | 126 | 82 |
| 2 CPU / 2 GiB | Bun | 165656.0 | 165648.0 | 0.60 | 2.70 | 97 | 430 |
| 2 CPU / 2 GiB | PHP | 9824.0 | 9824.0 | 13.10 | 16.30 | 13 | 32 |
| 4 CPU / 4 GiB | RabbitMQ | 78982.8 | 78993.9 | 1.60 | 2.80 | 202 | 173 |
| 4 CPU / 4 GiB | Rust | 188426.8 | 188426.8 | 0.70 | 1.90 | 133 | 29 |
| 4 CPU / 4 GiB | Bun | 170184.0 | 170192.0 | 0.60 | 2.40 | 111 | 459 |
| 4 CPU / 4 GiB | PHP | 9744.0 | 9744.0 | 13.00 | 22.00 | 13 | 28 |
| 4 CPU / 8 GiB | RabbitMQ | 77459.4 | 77448.5 | 1.60 | 3.00 | 202 | 164 |
| 4 CPU / 8 GiB | Rust | 193370.8 | 193360.0 | 0.70 | 1.80 | 133 | 82 |
| 4 CPU / 8 GiB | Bun | 170488.0 | 170496.0 | 0.60 | 2.40 | 112 | 458 |
| 4 CPU / 8 GiB | PHP | 9776.0 | 9776.0 | 13.10 | 17.30 | 12 | 30 |

### 16 connections, one queue

| Container | Broker | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 36394.2 | 37190.1 | 200.00 | 200.00 | 97 | 235 |
| 1 CPU / 512 MiB | Rust | 104653.0 | 60154.2 | 68.30 | 200.00 | 95 | 326 |
| 1 CPU / 512 MiB | Bun | 118928.0 | 118944.0 | 68.50 | 106.80 | 86 | 413 |
| 1 CPU / 512 MiB | PHP | 61680.1 | 61326.2 | 116.60 | 200.00 | 94 | 120 |
| 1 CPU / 1 GiB | RabbitMQ | 39664.8 | 39660.9 | 200.00 | 200.00 | 96 | 256 |
| 1 CPU / 1 GiB | Rust | 106388.5 | 62948.4 | 67.10 | 200.00 | 96 | 327 |
| 1 CPU / 1 GiB | Bun | 120608.0 | 120592.5 | 67.50 | 115.00 | 84 | 389 |
| 1 CPU / 1 GiB | PHP | 66711.9 | 66571.1 | 105.80 | 191.40 | 94 | 124 |
| 2 CPU / 2 GiB | RabbitMQ | 66565.4 | 67480.6 | 120.50 | 163.10 | 167 | 244 |
| 2 CPU / 2 GiB | Rust | 163821.6 | 78149.5 | 47.30 | 122.60 | 181 | 582 |
| 2 CPU / 2 GiB | Bun | 131344.0 | 131344.0 | 59.10 | 153.50 | 92 | 430 |
| 2 CPU / 2 GiB | PHP | 66775.0 | 66820.4 | 104.60 | 197.20 | 93 | 125 |
| 4 CPU / 4 GiB | RabbitMQ | 66855.4 | 67820.9 | 117.40 | 200.00 | 220 | 253 |
| 4 CPU / 4 GiB | Rust | 224816.2 | 110058.8 | 35.20 | 72.60 | 331 | 729 |
| 4 CPU / 4 GiB | Bun | 136640.0 | 136616.0 | 56.40 | 149.20 | 104 | 448 |
| 4 CPU / 4 GiB | PHP | 67678.0 | 67678.0 | 103.00 | 186.90 | 96 | 124 |
| 4 CPU / 8 GiB | RabbitMQ | 71935.4 | 71467.6 | 112.60 | 179.50 | 221 | 265 |
| 4 CPU / 8 GiB | Rust | 239222.9 | 120737.6 | 33.10 | 72.20 | 339 | 791 |
| 4 CPU / 8 GiB | Bun | 140432.0 | 140456.0 | 58.50 | 99.30 | 105 | 472 |
| 4 CPU / 8 GiB | PHP | 66009.9 | 65964.0 | 109.30 | 180.30 | 96 | 125 |

### 16 queues

| Container | Broker | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB | RabbitMQ | 40603.8 | 40825.0 | 198.70 | 200.00 | 97 | 187 |
| 1 CPU / 512 MiB | Rust | 135840.6 | 135805.1 | 56.20 | 117.90 | 95 | 348 |
| 1 CPU / 512 MiB | Bun | 148240.0 | 148256.0 | 53.60 | 99.50 | 81 | 384 |
| 1 CPU / 512 MiB | PHP | 64600.5 | 64600.5 | 113.10 | 200.00 | 94 | 115 |
| 1 CPU / 1 GiB | RabbitMQ | 41877.0 | 41937.1 | 192.20 | 200.00 | 97 | 190 |
| 1 CPU / 1 GiB | Rust | 144892.1 | 144821.2 | 53.10 | 99.40 | 96 | 344 |
| 1 CPU / 1 GiB | Bun | 147968.0 | 147976.0 | 55.50 | 95.60 | 85 | 447 |
| 1 CPU / 1 GiB | PHP | 66739.4 | 66739.4 | 106.60 | 190.70 | 93 | 125 |
| 2 CPU / 2 GiB | RabbitMQ | 86316.0 | 86039.0 | 96.80 | 144.10 | 193 | 216 |
| 2 CPU / 2 GiB | Rust | 249498.5 | 249409.1 | 31.90 | 61.60 | 187 | 469 |
| 2 CPU / 2 GiB | Bun | 165440.0 | 165440.0 | 47.50 | 97.30 | 93 | 491 |
| 2 CPU / 2 GiB | PHP | 68165.4 | 68022.8 | 101.20 | 189.60 | 93 | 122 |
| 4 CPU / 4 GiB | RabbitMQ | 159453.6 | 159438.0 | 51.00 | 99.50 | 379 | 262 |
| 4 CPU / 4 GiB | Rust | 339656.9 | 339265.2 | 22.40 | 52.70 | 346 | 700 |
| 4 CPU / 4 GiB | Bun | 169120.0 | 169144.0 | 48.70 | 93.90 | 104 | 505 |
| 4 CPU / 4 GiB | PHP | 67594.8 | 67878.8 | 101.60 | 188.00 | 94 | 126 |
| 4 CPU / 8 GiB | RabbitMQ | 151569.8 | 151802.4 | 52.50 | 118.30 | 372 | 240 |
| 4 CPU / 8 GiB | Rust | 344963.6 | 345357.8 | 17.80 | 42.60 | 363 | 682 |
| 4 CPU / 8 GiB | Bun | 174560.0 | 174584.0 | 47.10 | 82.90 | 106 | 535 |
| 4 CPU / 8 GiB | PHP | 67545.8 | 67594.4 | 104.20 | 186.90 | 94 | 125 |

Same CPU count with more RAM leaves confirm/s in the same band. Bun stays near one core (CPU 81–112). A second CPU lifts the single shape from about 141k to about 166k. Four CPUs stay near 170k. Rust on one connection uses about 0.8–1.3 cores and reaches about 141k–193k. On one shared queue the delivery rate is the rate that keeps the queue from growing: about 60k, 63k, 78k, 110k, and 121k. Across 16 queues Rust keeps up and follows the cores: about 136k, 145k, 249k, 340k, and 345k, at CPU 95, 96, 187, 346, and 363. RabbitMQ on one connection is about 52k–54k on 1 CPU and about 71k–79k with more CPUs. One queue with 16 connections stays near 36k–72k and about 1–2.2 cores. Sixteen queues use the cores: about 41k, 42k, 86k, 159k, and 152k, at CPU 97, 97, 193, 379, and 372. PHP does not follow the cores. One connection stays near 10k at every size, with p50 about 13 ms. Sixteen connections and sixteen queues stay near 62k–68k at about one core (CPU 93–96). Extra RAM does not move it. Confirms and deliveries stay together.

Drained cgroup memory stays a few hundred MiB: Bun about 371–535, RabbitMQ about 149–265, Rust on one connection about 29–92. Rust across 16 queues is about 344–700. The shared Rust queue is the row that accumulates, from 326 MiB on 1 CPU to 791 MiB on 4 CPU / 8 GiB at the early sample. No cell approached its memory cap.

The wide-row p50 is mostly wait inside the 8192-deep window. On one connection that wait is under a millisecond for Rust and Bun, 1.6–2.4 ms for RabbitMQ, and about 13 ms for PHP.

The paced ladder on 2026-10-07T21:36:57Z, from the Mac through the published port, kept 10996.58 / 19221.45 / 18452.24 / 6505.41 messages/s at 128 in flight on 1 CPU / 512 MiB (RabbitMQ / Rust / Bun / PHP). That ladder counts a step only when 95% of an offered rate is kept. The numbers in this section are the full-window push from inside the Docker network.

## Bun homes (2026-10-06T22:33:30Z)

Same client, shapes, sizes, and pins as [Load](#load-2026-10-05t212906z). Only Bun was remeasured. Rust, RabbitMQ, and PHP rows above are unchanged. Image `queueforge-bun:bench` `sha256:5989731ec1fc37e43d097497e5e38fa56cec5ed35806a8f942e9b155fe175936` (2026-10-06T22:32:48Z). Docker Desktop was at 8 CPUs and 25159827456 bytes, then returned to 2 CPUs and 8320565248 bytes. Postgres was accepting connections again.

A container whose cgroup grants one CPU stays one process. Two or four CPUs start that many children. The parent accepts the TCP connection and hands the socket to the child that owns the queue, once, after the queue name is known. That child stores and delivers. It does not forward the message. An earlier image shared the port and forwarded every publish; that run is not this table. Confirms and deliveries stayed apart there.

All 15 cells are `ok=1`. No OOM, blocked connections, nacks, misses, or returns. `queueforge_confirm_before_fsync_total` was 0 on every cell.

One connection stays on one child, so extra CPUs do not raise it: about 173k–180k. Sixteen connections on one queue also stay on that queue's home: about 130k–152k, and confirm/s matches consume/s. Sixteen queues spread across the children. Confirm/s and consume/s stay together: 159952.0, 175792.0, 286304.0, 413888.0, and 446832.0, at CPU 76, 83, 165, 283, and 304. At 4 CPU / 4 GiB that is 413888.0 confirm/s and 413896.0 consume/s. The 2026-10-05 Rust row at that size is 339656.9. The 2026-10-05 RabbitMQ row is 159453.6. Extra RAM at 4 CPUs raises the 16-queue rate from 413888.0 to 446832.0. It does not raise the one-queue rates.

| Container | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB, 1 conn | 174808.0 | 174812.0 | 0.50 | 2.40 | 84 | 512 |
| 1 CPU / 1 GiB, 1 conn | 180432.0 | 180428.0 | 0.50 | 2.30 | 83 | 454 |
| 2 CPU / 2 GiB, 1 conn | 177380.0 | 177376.0 | 0.60 | 2.20 | 93 | 533 |
| 4 CPU / 4 GiB, 1 conn | 175700.0 | 175708.0 | 0.60 | 2.00 | 109 | 632 |
| 4 CPU / 8 GiB, 1 conn | 172680.0 | 172680.0 | 0.60 | 2.30 | 107 | 650 |
| 1 CPU / 512 MiB, 16 conn | 130400.0 | 130424.0 | 59.20 | 165.60 | 85 | 418 |
| 1 CPU / 1 GiB, 16 conn | 149936.0 | 149931.0 | 54.50 | 105.60 | 85 | 456 |
| 2 CPU / 2 GiB, 16 conn | 151008.0 | 151020.5 | 51.70 | 99.60 | 97 | 528 |
| 4 CPU / 4 GiB, 16 conn | 151920.0 | 151920.0 | 52.10 | 98.70 | 104 | 650 |
| 4 CPU / 8 GiB, 16 conn | 149536.0 | 149544.0 | 52.80 | 98.60 | 102 | 639 |
| 1 CPU / 512 MiB, 16 queues | 159952.0 | 159952.0 | 48.30 | 105.80 | 76 | 423 |
| 1 CPU / 1 GiB, 16 queues | 175792.0 | 175792.0 | 46.50 | 86.00 | 83 | 507 |
| 2 CPU / 2 GiB, 16 queues | 286304.0 | 286288.0 | 27.90 | 67.30 | 165 | 847 |
| 4 CPU / 4 GiB, 16 queues | 413888.0 | 413896.0 | 19.00 | 41.90 | 283 | 1286 |
| 4 CPU / 8 GiB, 16 queues | 446832.0 | 446832.0 | 18.10 | 45.00 | 304 | 1302 |

## PHP cores (2026-10-07T03:47:05Z)

Same client, shapes, sizes, and pins. Only PHP was remeasured. Image `queueforge-php:bench` `sha256:53c4efbd34ae221dfa303298450299d09d14689ec3e8741019aa882872ab3bbf` (2026-10-07T03:47:05Z). Docker Desktop was at 8 CPUs and 25159827456 bytes, then returned to 2 CPUs and 8320565248 bytes. Postgres was accepting connections again.

One CPU stays one process. More CPUs start one child per core. The parent accepts AMQP and hands each connection to a child. A child keeps the messages from the connections it was given; it does not forward them. The benchmark opens every publisher before every consumer, and the publisher count divides the process count, so a queue's publisher and its consumer land on the same child. One publisher and one consumer do not. Those cells confirm and then deliver nothing.

Twelve cells are `ok=1`. The three one-connection cells with more than one CPU are `ok=0`, `err=no_consume`, consume/s 0. They are not a rate. No OOM, blocked connections, nacks, misses, or returns on the cells that passed. Where confirm/s and consume/s are both reported below, they stay together.

Sixteen queues at 4 CPU / 4 GiB are 387660.0 confirm/s and 387562.0 consume/s, at CPU 363. The 2026-10-05 Rust row at that size is 339656.9. The RabbitMQ row is 159453.6. Sixteen connections on one queue at that size are 378086.0 confirm/s and 377935.8 consume/s.

| Container | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB | note |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 CPU / 512 MiB, 1 conn | 9760.0 | 9760.0 | 13.60 | 19.70 | 9 | 31 | |
| 1 CPU / 1 GiB, 1 conn | 9744.0 | 9744.0 | 13.60 | 17.80 | 8 | 29 | |
| 2 CPU / 2 GiB, 1 conn | 40576.0 | 0.0 | 3.00 | 3.80 | 13 | 253 | not a rate |
| 4 CPU / 4 GiB, 1 conn | 36351.9 | 0.0 | 3.20 | 14.60 | 18 | 269 | not a rate |
| 4 CPU / 8 GiB, 1 conn | 38144.0 | 0.0 | 3.20 | 7.60 | 16 | 273 | not a rate |
| 1 CPU / 512 MiB, 16 conn | 89095.9 | 89373.4 | 79.40 | 147.80 | 92 | 211 | |
| 1 CPU / 1 GiB, 16 conn | 99376.0 | 99024.4 | 70.10 | 135.40 | 94 | 225 | |
| 2 CPU / 2 GiB, 16 conn | 210743.1 | 210728.8 | 33.20 | 66.60 | 186 | 139 | |
| 4 CPU / 4 GiB, 16 conn | 378086.0 | 377935.8 | 16.70 | 47.30 | 353 | 247 | |
| 4 CPU / 8 GiB, 16 conn | 379552.5 | 379477.8 | 16.60 | 51.20 | 355 | 242 | |
| 1 CPU / 512 MiB, 16 queues | 101392.4 | 101392.4 | 72.50 | 140.30 | 92 | 207 | |
| 1 CPU / 1 GiB, 16 queues | 108238.0 | 108246.6 | 64.10 | 119.60 | 94 | 247 | |
| 2 CPU / 2 GiB, 16 queues | 211555.1 | 211735.8 | 33.10 | 75.60 | 186 | 131 | |
| 4 CPU / 4 GiB, 16 queues | 387660.0 | 387562.0 | 16.50 | 47.70 | 363 | 275 | |
| 4 CPU / 8 GiB, 16 queues | 389549.2 | 389758.0 | 16.20 | 51.80 | 360 | 280 | |

## Load (2026-10-07T21:03:40Z)

Same client, shapes, sizes, and pins as [Load](#load-2026-10-05t212906z). All four brokers were remeasured. Images: Rust `queueforge-rust:bench` `sha256:29d17851d2746e41226d188251a3787abbec8c0386b78679409992a5ba874957` (2026-10-07T21:03:31Z), Bun `queueforge-bun:bench` `sha256:665787b123a4cde32631adab0576287843ceb43fc187fd19da3f47a5a9f35ed9` (2026-10-07T21:01:11Z), PHP `queueforge-php:bench` `sha256:c0c2ce662bc940f7c21ecbc2b4740f52819788c8c56e86742c94bcb72952c296` (2026-10-07T21:01:10Z), RabbitMQ `rabbitmq:4.3-management` `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`. Docker Desktop was raised to 8 CPUs for the sweep and returned to 2 CPUs and 8320565248 bytes. Postgres was accepting connections again. TSV `/tmp/qf-load/load-all.tsv`.

Fifty-one cells are `ok=1`. The nine PHP cells with more than one CPU are `ok=0`, `err=c1_eof` or `err=c2_eof`, confirm/s 0, and they exited in under a second. They are not a rate. No OOM on any cell. Where a p99 below is 200.00 ms, that is the histogram overflow bucket.

Bun confirms and deliveries stay together on every cell. `queueforge_confirm_before_fsync_total` was 0 on every Bun cell. Sixteen queues at 4 CPU / 4 GiB are 555216.0 confirm/s and 555184.0 consume/s, at CPU 308 and 362 MiB. At 4 CPU / 8 GiB they are 547216.0 and 547264.0. One connection stays on one child: 224036.0 down to 185652.0, then 198988.0. Sixteen connections on one queue also stay on that home: 162400.0 to 178496.0, and extra CPUs do not raise it.

| Container | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB, 1 conn | 224036.0 | 224032.0 | 0.40 | 2.30 | 79 | 168 |
| 1 CPU / 1 GiB, 1 conn | 229504.0 | 229492.0 | 0.40 | 2.30 | 75 | 113 |
| 2 CPU / 2 GiB, 1 conn | 186400.0 | 186396.0 | 0.50 | 3.30 | 83 | 169 |
| 4 CPU / 4 GiB, 1 conn | 185652.0 | 185648.0 | 0.50 | 2.30 | 105 | 293 |
| 4 CPU / 8 GiB, 1 conn | 198988.0 | 198980.0 | 0.50 | 2.10 | 102 | 290 |
| 1 CPU / 512 MiB, 16 conn | 162400.0 | 162376.0 | 45.30 | 181.60 | 82 | 149 |
| 1 CPU / 1 GiB, 16 conn | 178496.0 | 178496.0 | 42.30 | 140.30 | 76 | 138 |
| 2 CPU / 2 GiB, 16 conn | 168704.0 | 168692.0 | 45.90 | 108.70 | 96 | 208 |
| 4 CPU / 4 GiB, 16 conn | 164672.0 | 164656.0 | 48.10 | 93.10 | 103 | 337 |
| 4 CPU / 8 GiB, 16 conn | 166176.0 | 166192.0 | 47.40 | 93.00 | 106 | 339 |
| 1 CPU / 512 MiB, 16 queues | 174912.0 | 174912.0 | 42.00 | 173.90 | 60 | 144 |
| 1 CPU / 1 GiB, 16 queues | 208896.0 | 208896.0 | 34.80 | 197.40 | 80 | 150 |
| 2 CPU / 2 GiB, 16 queues | 334416.0 | 334408.0 | 23.70 | 61.70 | 160 | 235 |
| 4 CPU / 4 GiB, 16 queues | 555216.0 | 555184.0 | 14.10 | 39.30 | 308 | 362 |
| 4 CPU / 8 GiB, 16 queues | 547216.0 | 547264.0 | 14.10 | 52.90 | 290 | 381 |

Rust confirms and deliveries stay together on one connection and on sixteen queues. On one shared queue, confirms run ahead of deliveries, so the compare table uses consume/s there. Rust confirm/s on that shape, in size order, is 101157.4, 113040.0, 185820.9, 238494.2, and 242481.1. Sixteen queues at 4 CPU / 4 GiB are 402591.5 confirm/s and 402628.9 consume/s. `queueforge_confirm_before_fsync_total` was 0 on the one-connection and sixteen-queue cells. Two shared cells (1 CPU) left that scrape blank; the other shared cells were 0.

| Container | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB, 1 conn | 168320.2 | 168320.2 | 0.80 | 1.70 | 89 | 60 |
| 1 CPU / 1 GiB, 1 conn | 158330.8 | 158325.4 | 0.80 | 2.00 | 83 | 72 |
| 2 CPU / 2 GiB, 1 conn | 199800.5 | 199800.5 | 0.60 | 1.50 | 129 | 30 |
| 4 CPU / 4 GiB, 1 conn | 206490.6 | 206496.0 | 0.60 | 1.10 | 139 | 50 |
| 4 CPU / 8 GiB, 1 conn | 201626.6 | 201626.2 | 0.60 | 1.20 | 139 | 41 |
| 1 CPU / 512 MiB, 16 conn | 101157.4 | 62325.1 | 70.00 | 200.00 | 96 | 334 |
| 1 CPU / 1 GiB, 16 conn | 113040.0 | 68226.9 | 63.50 | 200.00 | 95 | 363 |
| 2 CPU / 2 GiB, 16 conn | 185820.9 | 95306.0 | 41.80 | 100.40 | 185 | 587 |
| 4 CPU / 4 GiB, 16 conn | 238494.2 | 116860.6 | 33.60 | 54.90 | 333 | 789 |
| 4 CPU / 8 GiB, 16 conn | 242481.1 | 119871.9 | 32.50 | 73.00 | 338 | 833 |
| 1 CPU / 512 MiB, 16 queues | 126972.5 | 127012.2 | 55.50 | 200.00 | 96 | 300 |
| 1 CPU / 1 GiB, 16 queues | 152235.4 | 152243.1 | 50.50 | 98.00 | 96 | 365 |
| 2 CPU / 2 GiB, 16 queues | 259525.0 | 259590.0 | 30.80 | 56.10 | 188 | 562 |
| 4 CPU / 4 GiB, 16 queues | 402591.5 | 402628.9 | 18.10 | 118.00 | 329 | 831 |
| 4 CPU / 8 GiB, 16 queues | 408183.8 | 408089.6 | 17.20 | 46.50 | 369 | 861 |

RabbitMQ confirms and deliveries stay together. Sixteen queues at 4 CPU / 4 GiB are 169979.2 confirm/s and 169788.9 consume/s. The early-confirm counter is a QueueForge metric; these rows do not report it.

| Container | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 CPU / 512 MiB, 1 conn | 55009.2 | 55011.5 | 2.30 | 3.60 | 97 | 224 |
| 1 CPU / 1 GiB, 1 conn | 57424.0 | 57424.0 | 2.20 | 3.10 | 96 | 156 |
| 2 CPU / 2 GiB, 1 conn | 78327.1 | 78331.4 | 1.60 | 4.00 | 157 | 152 |
| 4 CPU / 4 GiB, 1 conn | 79349.9 | 79345.0 | 1.60 | 2.80 | 202 | 175 |
| 4 CPU / 8 GiB, 1 conn | 80578.0 | 80578.0 | 1.60 | 2.70 | 199 | 175 |
| 1 CPU / 512 MiB, 16 conn | 40549.8 | 40346.1 | 200.00 | 200.00 | 97 | 226 |
| 1 CPU / 1 GiB, 16 conn | 42988.5 | 42542.8 | 191.80 | 200.00 | 97 | 225 |
| 2 CPU / 2 GiB, 16 conn | 77824.0 | 77261.0 | 108.20 | 155.90 | 170 | 243 |
| 4 CPU / 4 GiB, 16 conn | 71813.9 | 72143.0 | 110.10 | 196.50 | 229 | 249 |
| 4 CPU / 8 GiB, 16 conn | 78019.0 | 79043.0 | 101.40 | 174.60 | 237 | 249 |
| 1 CPU / 512 MiB, 16 queues | 43926.6 | 43797.4 | 184.10 | 200.00 | 97 | 190 |
| 1 CPU / 1 GiB, 16 queues | 43886.4 | 43871.9 | 182.00 | 200.00 | 97 | 190 |
| 2 CPU / 2 GiB, 16 queues | 86072.8 | 86202.0 | 95.10 | 136.80 | 194 | 204 |
| 4 CPU / 4 GiB, 16 queues | 169979.2 | 169788.9 | 47.50 | 99.40 | 381 | 227 |
| 4 CPU / 8 GiB, 16 queues | 171022.1 | 171028.4 | 47.40 | 95.80 | 383 | 239 |

PHP at 1 CPU is `ok=1`. Confirms and deliveries stay together. One connection is 10720.0 and 10816.0 confirm/s. Sixteen connections on one queue are 94662.0 confirm/s and 94290.0 consume/s at 512 MiB, and 92950.0 both ways at 1 GiB. Sixteen queues are 102676.6 and 104815.6 confirm/s. The nine cells above 1 CPU never produced a rate. The last PHP rates for those sizes remain the cores run above.

| Container | confirm/s | consume/s | p50 ms | p99 ms | CPU | MiB | note |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 CPU / 512 MiB, 1 conn | 10720.0 | 10720.0 | 12.10 | 14.60 | 9 | 85 | |
| 1 CPU / 1 GiB, 1 conn | 10816.0 | 10816.0 | 12.10 | 16.10 | 9 | 31 | |
| 2 CPU / 2 GiB, 1 conn | | | | | | | not a rate, c1_eof |
| 4 CPU / 4 GiB, 1 conn | | | | | | | not a rate, c1_eof |
| 4 CPU / 8 GiB, 1 conn | | | | | | | not a rate, c2_eof |
| 1 CPU / 512 MiB, 16 conn | 94662.0 | 94290.0 | 75.00 | 142.90 | 95 | 219 | |
| 1 CPU / 1 GiB, 16 conn | 92950.0 | 92950.0 | 76.60 | 144.60 | 94 | 226 | |
| 2 CPU / 2 GiB, 16 conn | | | | | | | not a rate, c1_eof |
| 4 CPU / 4 GiB, 16 conn | | | | | | | not a rate, c2_eof |
| 4 CPU / 8 GiB, 16 conn | | | | | | | not a rate, c2_eof |
| 1 CPU / 512 MiB, 16 queues | 102676.6 | 102676.6 | 69.80 | 125.00 | 94 | 242 | |
| 1 CPU / 1 GiB, 16 queues | 104815.6 | 104815.6 | 66.90 | 122.00 | 95 | 246 | |
| 2 CPU / 2 GiB, 16 queues | | | | | | | not a rate, c1_eof |
| 4 CPU / 4 GiB, 16 queues | | | | | | | not a rate, c1_eof |
| 4 CPU / 8 GiB, 16 queues | | | | | | | not a rate, c1_eof |

### Load compare

One row per broker and container. `1 conn` and `16 queues` are confirm/s. Confirms and deliveries stay together on those shapes, except Rust on one shared queue, where confirms run ahead. `16 conn` is consume/s on that queue, the rate that does not grow it. Rust confirm/s on that shape, in the same size order, is 101157.4, 113040.0, 185820.9, 238494.2, and 242481.1. `MiB` is the 16-queue load sample from the remeasure, except the PHP rows above 1 CPU, whose message rates and MiB stay the cores run. Connections and login memory are the 2026-10-07T21:47:10Z hold sweep.

`Connections` and `Login memory` are a different run from the message rates: simultaneous logins, client in a container, no swap, one broker at a time, 2026-10-07T21:47:10Z through 23:20:51Z. The number is the last hold that stayed up and passed 40 confirms. Docker Desktop was at 8 CPUs and 25159827456 bytes, then returned to 2 CPUs and 8320565248 bytes. Postgres was accepting connections again. There is no 1 CPU / 1 GiB login cell. PHP at 1 CPU / 512 MiB held 1000. Asks of 1750 and above were cut by the login window near 1010 connections and did not pass. PHP above 1 CPU never passed: the large asks were cut by that window, and 1000 then held nothing. Those cells are not a hold. Bun at 4 CPU / 4 GiB held 251968. At 4 CPU / 8 GiB it held 260000; 280000 and 320000 exited before a probe. Rust at 4 CPU / 8 GiB connected 100000, and the 40 confirms failed; 95000 passed. RabbitMQ at 4 CPU / 8 GiB held 65449. An ask of 69691 stopped at 65527 sockets and the probe did not pass.

| App | Size | 1 conn msg/s | 16 conn msg/s | 16 queues msg/s | Connections | MiB | Login memory |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| RabbitMQ | 1 CPU / 512 MiB | 55009.2 | 40346.1 | 43926.6 | 3500 | 190 | 477.8 MiB |
| Rust | 1 CPU / 512 MiB | 168320.2 | 62325.1 | 126972.5 | 8980 | 300 | 492.7 MiB |
| Bun | 1 CPU / 512 MiB | 224036.0 | 162376.0 | 174912.0 | 43828 | 144 | 442.3 MiB |
| PHP | 1 CPU / 512 MiB | 10720.0 | 94290.0 | 102676.6 | 1000 | 242 | 18.75 MiB |
| RabbitMQ | 1 CPU / 1 GiB | 57424.0 | 42542.8 | 43886.4 | n/a | 190 | n/a |
| Rust | 1 CPU / 1 GiB | 158330.8 | 68226.9 | 152235.4 | n/a | 365 | n/a |
| Bun | 1 CPU / 1 GiB | 229504.0 | 178496.0 | 208896.0 | n/a | 150 | n/a |
| PHP | 1 CPU / 1 GiB | 10816.0 | 92950.0 | 104815.6 | n/a | 246 | n/a |
| RabbitMQ | 2 CPU / 2 GiB | 78327.1 | 77261.0 | 86072.8 | 18750 | 204 | 1.775 GiB |
| Rust | 2 CPU / 2 GiB | 199800.5 | 95306.0 | 259525.0 | 34600 | 562 | 1.838 GiB |
| Bun | 2 CPU / 2 GiB | 186400.0 | 168692.0 | 334416.0 | 121284 | 235 | 1.467 GiB |
| PHP | 2 CPU / 2 GiB | 0 | 210728.8 | 211555.1 | n/a | 131 | n/a |
| RabbitMQ | 4 CPU / 4 GiB | 79349.9 | 72143.0 | 169979.2 | 38217 | 227 | 3.884 GiB |
| Rust | 4 CPU / 4 GiB | 206490.6 | 116860.6 | 402591.5 | 68467 | 831 | 3.636 GiB |
| Bun | 4 CPU / 4 GiB | 185652.0 | 164656.0 | 555216.0 | 251968 | 362 | 2.934 GiB |
| PHP | 4 CPU / 4 GiB | 0 | 377935.8 | 387660.0 | n/a | 275 | n/a |
| RabbitMQ | 4 CPU / 8 GiB | 80578.0 | 79043.0 | 171022.1 | 65449 | 239 | 6.34 GiB |
| Rust | 4 CPU / 8 GiB | 201626.6 | 119871.9 | 408183.8 | 95000 | 861 | 5.025 GiB |
| Bun | 4 CPU / 8 GiB | 198988.0 | 166192.0 | 547216.0 | 260000 | 381 | 3.217 GiB |
| PHP | 4 CPU / 8 GiB | 0 | 379477.8 | 389549.2 | n/a | 280 | n/a |

## Paced (2026-10-07T21:36:57Z)

Same client as [Latest](#latest-2026-10-05t090342z): `rust/target/release/queueforge-compare`, binary mtime 2026-10-04T16:16:23-0300, not rebuilt. `QUEUEFORGE_COMPARE_RATES` unset. One container at a time, 1 CPU and 512 MiB, host ports 35672, 35673, 35674, and 35675. Images: RabbitMQ `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`, Rust `sha256:29d17851d2746e41226d188251a3787abbec8c0386b78679409992a5ba874957` (2026-10-07T21:03:31Z), Bun `sha256:665787b123a4cde32631adab0576287843ceb43fc187fd19da3f47a5a9f35ed9` (2026-10-07T21:01:11Z), PHP `sha256:c0c2ce662bc940f7c21ecbc2b4740f52819788c8c56e86742c94bcb72952c296` (2026-10-07T21:01:10Z). Docker Desktop stayed at 2 CPUs and 8320565248 bytes. RabbitMQ started at 21:36:57Z, Rust at 21:38:42Z, Bun at 21:40:27Z, PHP at 21:42:16Z. Every scenario is `declare=ok publish=ok consume=ok ack=ok confirms=ok`.

QueueForge disk line: `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm after that fsync`. RabbitMQ disk line: `publisher confirms before fsync`.

### One confirm, durable-256

| Broker | p50 ms | p99 ms | messages/s | First miss |
| --- | ---: | ---: | ---: | ---: |
| RabbitMQ | 0.48 | 1.85 | 1569.22 | 4000 |
| Rust | 0.24 | 1.06 | 2555.27 | 8000 |
| Bun | 0.19 | 1.06 | 2837.72 | 8000 |
| PHP | 0.34 | 1.64 | 1924.02 | 4000 |

### 128 confirms

| Scenario | Broker | messages/s | × Rabbit | p50 ms | p99 ms | First miss | Wall s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| durable-256 | RabbitMQ | 10996.58 | | 4.23 | 19.90 | 32000 | 20.26 |
| durable-256 | Rust | 19221.45 | 1.748× (1.74795) | 2.02 | 11.11 | 48000 | 20.16 |
| durable-256 | Bun | 18452.24 | 1.678× (1.67800) | 2.10 | 10.56 | 48000 | 20.16 |
| durable-256 | PHP | 6505.41 | 0.592× (0.59158) | 10.79 | 18.39 | 16000 | 20.18 |
| fan-2x2 | RabbitMQ | 11983.62 | | 8.42 | 19.99 | 32000 | 20.14 |
| fan-2x2 | Rust | 23962.66 | 2.000× (1.99962) | 2.70 | 11.30 | 96000 | 20.29 |
| fan-2x2 | Bun | 23653.92 | 1.974× (1.97385) | 2.23 | 10.78 | 96000 | 20.13 |
| fan-2x2 | PHP | 11078.05 | 0.924× (0.92443) | 9.80 | 15.72 | 32000 | 20.15 |

The site chart is the durable-256 row. On one confirm, the fan-2x2 ladder is not that chart. Bun's first miss there is 200, at 175.56 messages/s. Rust keeps 200 and misses 800, at 198.11 messages/s. RabbitMQ keeps through 3200 and misses 6400, at 2188.35 messages/s. PHP keeps through 3200 and misses 6400, at 2314.94 messages/s.

### Short scenarios

On both ladders, `size-4096` scores 250.00 and `transient-256` scores 1250.00, and the top step is kept. `prefetch-1` and `prefetch-128` score 600.00 with the 1000 step kept. `size-64` scores 600.00 on the one-confirm ladder for all four, and on the 128 ladder for Rust, Bun, and PHP. RabbitMQ's 128-confirm `size-64` scores 597.75 with the 1000 step still kept.

The remote one-confirm check and the login hold were not part of this run.

## Latest (2026-10-05T09:03:42Z)

Rust `sha256:913d67ca03332153548bba73f06e82d842825a50993a98be529bda5858241418` (2026-10-05T07:51:31Z). Bun `sha256:002d75f424ccfa8d3aab05a90143e7b2b696d8c3ff3306564128fbe11611308b` (2026-10-05T09:01:45Z). RabbitMQ `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`, startup complete on the first start. RabbitMQ ran at 09:03:42Z, Rust at 09:05:31Z, Bun at 09:07:17Z. A pre-flush consume keeps the insert in the confirm's record. The delete is the next group commit: a crash after the confirm still has the body, and a clean shutdown does not replay it. Every local scenario is `declare=ok publish=ok consume=ok ack=ok confirms=ok`.

QueueForge disk line: `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm after that fsync`. RabbitMQ disk line: `publisher confirms before fsync`.

| Ladder | Rust wal fsyncs | Bun wal fsyncs | Bun full flushes | confirm_before |
| --- | ---: | ---: | ---: | ---: |
| 1 in flight | 36339 | 43441 | 43441 | 0 |
| 128 in flight | 11454 | 14224 | 14224 | 0 |

### One confirm, durable-256

| Broker | p50 ms | p99 ms | messages/s | First miss |
| --- | ---: | ---: | ---: | ---: |
| RabbitMQ | 0.46 | 0.83 | 1678.69 | 4000 |
| Rust | 0.28 | 0.61 | 2229.28 | 4000 |
| Bun | 0.20 | 0.60 | 2710.94 | 8000 |
| PHP | 0.31 | 1.57 | 2027.72 | 4000 |

Rust and Bun p50 are under 0.42 ms and under this session's 0.46 ms. p99 is under 0.88 ms and under this session's 0.83 ms. messages/s is over 1616.44 and over this session's 1678.69. The 01:06:02Z row in the [session table](#one-confirm-across-paced-sessions) is the floor this run replaced: Rust 3.18 / 5.43 / 304.33 and Bun 3.67 / 10.72 / 241.77.

### 128 confirms

| Scenario | Broker | messages/s | × Rabbit | p50 ms | p99 ms | First miss | Wall s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| durable-256 | RabbitMQ | 13139.01 | | 3.19 | 8.58 | 32000 | 20.93 |
| durable-256 | Rust | 19230.10 | 1.464× (1.46359) | 2.03 | 11.15 | 48000 | 20.19 |
| durable-256 | Bun | 18396.86 | 1.400× (1.40017) | 2.18 | 11.25 | 48000 | 20.17 |
| durable-256 | PHP | 6114.63 | 0.465× (0.46538) | 12.47 | 27.77 | 16000 | 20.20 |
| fan-2x2 | RabbitMQ | 14512.22 | | 6.81 | 16.77 | 32000 | 20.14 |
| fan-2x2 | Rust | 24005.72 | 1.654× (1.65417) | 2.41 | 11.56 | 96000 | 20.25 |
| fan-2x2 | Bun | 23744.97 | 1.636× (1.63621) | 2.46 | 11.25 | 96000 | 20.15 |
| fan-2x2 | PHP | 9974.63 | 0.687× (0.68733) | 11.41 | 26.69 | 32000 | 20.24 |

The 1.3× bars are 17080.71 (durable) and 18865.89 (fan). Both brokers clear those bars, and they clear the 01:06 floors: durable 16936.46 (Rust) and 17813.55 (Bun), fan 23372.78 (Rust) and 23203.75 (Bun). Durable p50 is inside 2.58 ms (Rust) and 2.32 ms (Bun). Fan p50 is inside 3.01 ms (Rust) and 2.98 ms (Bun). Offer-by-offer rates are in [Step rates](#step-rates-128-confirms). On the durable steps cited there, confirms match consumed.

### Short scenarios

All four brokers keep the same top saturation, and all four score the same `messages/s` with one exception: PHP's `size-64` on the one-confirm ladder scored 597.25 against 600.00, with a top step of 975.76. On the 128 ladder the same scenario scored 600.00 with a top step of 999.18. The PHP column is the one-confirm ladder.

| Scenario | messages/s | Saturation | Rabbit top step | Rust top step | Bun top step | PHP top step |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| size-64 | 600.00 | 1000 kept | 1000.0 | 996.3 | 995.2 | 975.8 |
| prefetch-1 | 600.00 | 1000 kept | 999.3 | 998.8 | 996.7 | 1000.0 |
| prefetch-128 | 600.00 | 1000 kept | 999.5 | 998.5 | 999.1 | 1000.0 |
| size-4096 | 250.00 | 400 kept | 400.0 | 400.0 | 399.6 | 400.0 |
| transient-256 | 1250.00 | 2000 kept | 2000.0 | 2000.0 | 1998.8 | 1999.4 |

### Remote one confirm

Home is the peer: the peer's `queueforge_wal_fsync_seconds_count` moved, and the client's stayed at 0. `confirm_before_delta=0/0`. Disk line: `publisher confirm after that fsync`. Listeners are `172.30.220.10:25672` and `172.30.220.11:25672`. Client ports are 35773 (Rust) and 35774 (Bun). `REMOTE_DONE` is 2026-10-05T09:11:07Z. Three-node messages/s are off this score.

| Path | p50 ms | p99 ms | Peer fsync | Client fsync | Accepted |
| --- | ---: | ---: | ---: | ---: | --- |
| Rust client, Rust home | 0.32 | 0.62 | 24914 | 0 | attempt 2 at 09:10:39Z (`REMOTE_ACCEPT:rust:attempt=2`) |
| Bun client, Bun home | 0.29 | 0.81 | 26870 | 0 | attempt 1 at 09:10:55Z (`REMOTE_ACCEPT:bun:attempt=1`) |

Rust attempt 1 had the home on the client (`client_fsync_delta=25571`, `peer_fsync_delta=0`, p50 0.29, p99 0.65). Caps for this check are Rust p50 3.23 / p99 4.29 and Bun p50 1.71 / p99 3.34.

Trust for this image is the 09:03 rows in [Trust](#trust).

## PHP parity paced (2026-10-06T18:15:20Z)

The PHP broker was brought to client-visible parity with Bun and Rust after the
16:13:24Z run: policy resolution into queue arguments, alternate exchanges,
`x-death`, `x-expires` and a TTL sweep, consumer priority and
single-active-consumer, `x-delivery-limit`, CC/BCC routing, exclusive
consumers, the five permission refusals, the full Prometheus series, classic
queue home forwarding, the quorum confirm gate, the consumed set, the
remaining management routes, and the MQTT, STOMP, stream and AMQP 1.0 gaps.

PHP `queueforge-php:bench`
`sha256:bedc046ae885f0226775e48789761147622b163fb59046ab1b22a9a2aaf9c62a`
(2026-10-06T17:19:37Z, commit `bca7560`). A freshly created container for each
ladder, host ports 35675 and 36675, 1 CPU and 512 MiB. Same client,
`rust/target/release/queueforge-compare`, binary mtime
2026-10-04T16:16:23-0300, not rebuilt. `QUEUEFORGE_COMPARE_RATES` unset. Docker
Desktop at 2 CPUs and 8320565248 bytes. The one-confirm ladder started at
18:15:20Z and the 128 ladder at 18:17:14Z. Every scenario is
`declare=ok publish=ok consume=ok ack=ok confirms=ok`, and
`queueforge_confirm_before_fsync_total` was 0 after the 128 ladder.

These rows replace the 16:13 rows in [Latest](#latest-2026-10-05t090342z).

### One confirm in flight

| Scenario | messages/s | First miss / kept | p50 ms | p99 ms | Wall s |
| --- | ---: | --- | ---: | ---: | ---: |
| durable-256 | 2027.72 | 4000 miss | 0.31 | 1.57 | 12.26 |
| size-64 | 597.25 | 1000 kept | 0.50 | 2.61 | 4.09 |
| size-4096 | 250.00 | 400 kept | 1.03 | 4.80 | 4.04 |
| transient-256 | 1250.00 | 2000 kept | 0.28 | 1.21 | 3.05 |
| prefetch-1 | 600.00 | 1000 kept | 0.50 | 5.37 | 4.05 |
| prefetch-128 | 600.00 | 1000 kept | 0.53 | 3.55 | 4.05 |
| fan-2x2 | 2004.22 | 3200 miss | 0.52 | 2.58 | 12.10 |

### 128 confirms in flight

| Scenario | messages/s | First miss / kept | p50 ms | p99 ms | Wall s |
| --- | ---: | --- | ---: | ---: | ---: |
| durable-256 | 6114.63 | 16000 miss | 12.47 | 27.77 | 20.20 |
| size-64 | 600.00 | 1000 kept | 2.41 | 17.87 | 4.05 |
| size-4096 | 250.00 | 400 kept | 2.38 | 21.78 | 4.05 |
| transient-256 | 1250.00 | 2000 kept | 1.77 | 21.11 | 3.05 |
| prefetch-1 | 600.00 | 1000 kept | 2.27 | 37.52 | 4.05 |
| prefetch-128 | 600.00 | 1000 kept | 2.13 | 28.35 | 4.05 |
| fan-2x2 | 9974.63 | 32000 miss | 11.41 | 26.69 | 20.24 |

### Against the 16:13 image

| Ladder | Scenario | 16:13 | Parity | Repeat | × Rabbit (parity) |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 in flight | durable-256 | 1938.15 | 2027.72 | 1847.96 | 1.208× (1.20792) |
| 1 in flight | fan-2x2 | 2233.16 | 2004.22 | 2354.13 | |
| 128 in flight | durable-256 | 6324.00 | 6114.63 | 6194.60 | 0.465× (0.46538) |
| 128 in flight | fan-2x2 | 10396.62 | 9974.63 | 9413.85 | 0.687× (0.68733) |

`Repeat` is a second run of only the two laddered scenarios
(`QUEUEFORGE_COMPARE_ONLY=durable-256,fan-2x2`), each ladder on another fresh
container. It is not a score; it shows the spread. At one in flight the two
parity runs straddle the 16:13 value on both scenarios: durable-256 moved by
180 messages/s between runs, and fan-2x2's first miss was 3200 in one run and
6400 in the other. The 16:13 numbers are single runs inside that spread, so no
change is claimed there.

At 128 in flight both parity runs came in below the 16:13 value: durable-256 by
2–3% and fan-2x2 by 4–9%. Two samples are too few to call that a regression,
and the 16:13 image had only one, but the direction was the same both times.
p99 is also higher, 27.77 ms against 16.44 ms on durable-256. The parity build
does more on the publish path: counters, a CC/BCC header scan, the alternate
and internal exchange checks, and a guarded expiry check per destination.

The relative picture is unchanged. At one confirm in flight PHP is ahead of
RabbitMQ and behind Rust and Bun. At 128 it is a little under half of RabbitMQ
on durable and about seven tenths on fan, where Rust and Bun are 1.4× to 1.65×.

### Same-probe check

Before the paced ladder was run, the publish path was also checked with a small
confirm-rate probe (`php/test/probe.php`). It ran against the parity image and
against an image built from the pre-parity commit `4cad540`, each in a freshly
created container at the same limits, alternating, 256-byte persistent bodies,
6 s per run.

| Build | In flight | messages/s | p50 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| pre-parity `4cad540` | 1 | 3221.7 | 0.27 | 0.53 |
| parity | 1 | 3221.6 | 0.27 | 0.53 |
| pre-parity `4cad540` | 128 | 8504.9 | 15.10 | 21.05 |
| parity | 128 | 8716.2 | 14.79 | 19.01 |

The probe saw no difference. Its numbers are **not** comparable to the paced
ladder: it is a single-threaded PHP client that fills a fixed confirm window
and never consumes, while `queueforge-compare` paces offered load and consumes.

The probe exposed one limit, present in both builds: with no consumer attached
the broker holds every message in memory, and log compaction briefly needs a
second copy. An unbounded queue therefore exhausts PHP's 128 MiB `memory_limit`
at about 100,000 queued 256-byte messages. Bounding the queue with
`x-max-length` keeps memory flat.

## PHP paced (2026-10-06T16:13:24Z)

PHP `queueforge-php:bench` `sha256:8b51cf8f616f90eb4ef6e71b82c583a7ffdaa42c8f16f7b0b6af6f418cdebf90` (2026-10-06T16:09:36Z), one container at a time on host ports 35675 and 36675, 1 CPU and 512 MiB, the same `docker-compose.bench.yml` limits as the other three. Same client, `queueforge-compare`, same binary mtime 2026-10-04T16:16:23-0300, `QUEUEFORGE_COMPARE_RATES` unset. Docker Desktop was at 2 CPUs and 8320565248 bytes. Every scenario is `declare=ok publish=ok consume=ok ack=ok confirms=ok`. Disk line: `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm after that fsync`.

This image is newer than the 09:03:42Z Rust and Bun images, so the PHP rows are not from the same session as the other three. They were placed in the 09:03 tables because the client, the offers, the container limits, and the host settings match; the images do not. They have since been replaced there by [PHP parity paced](#php-parity-paced-2026-10-06t181520z) and are kept here as the pre-parity record.

PHP matches the other three exactly on all five short scenarios. It differs on the two laddered ones, and the direction depends on the ladder:

| Ladder | Scenario | PHP messages/s | × Rabbit | First miss | Rabbit first miss |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 in flight | durable-256 | 1938.15 | 1.155× | 4000 | 4000 |
| 1 in flight | fan-2x2 | 2233.16 | | 6400 | |
| 128 in flight | durable-256 | 6324.00 | 0.481× | 16000 | 32000 |
| 128 in flight | fan-2x2 | 10396.62 | 0.716× | 32000 | 32000 |

At one confirm in flight the score is latency-bound, and PHP sits between RabbitMQ and Rust while matching RabbitMQ's first miss. At 128 in flight the score is throughput-bound, and the single thread shows: roughly half of RabbitMQ on durable and about seven tenths on fan, against Rust and Bun at 1.4× to 1.65× of RabbitMQ.

### The flush rule this run required

The first paced attempt scored 77.36 messages a second on `size-64`, against 600.00 for the other three. That was not the broker: with one confirm in flight and a 10 ms group-commit timer, a publisher can complete at most one message per tick, which is 79 a second. The confirm path waited for the tick unconditionally while Bun flushes early for a lone waiter (`bun/src/store.ts:69-82`), so the ladder was measuring the timer rather than the broker.

The fix flushes at once while eight or fewer confirms are outstanding, and keeps waiting for the tick above that, where there is a batch worth forming. Confirm p50 on `size-64` went from 12.66 ms to 0.56 ms and the score from 77.36 to 600.00, matching the other three. `fan-2x2` at one in flight went from 254.77 with a first miss at 200 to 2233.16 with a first miss at 6400.

The durability rule is unchanged: a confirm is released only after the fsync that covers its append. `php/test/roundtrip.php` asserts this by `SIGKILL`ing the broker and requiring a confirmed but unacked durable message to come back, and it passes before and after.

## One confirm across paced sessions

Same `durable-256` ladder, one confirm in flight. From 21:30:56Z on, the Rust and Bun confirm is after the fsync.

| Session | Broker | p50 ms | p99 ms | messages/s | First miss |
| --- | --- | ---: | ---: | ---: | ---: |
| 21:30:56Z | RabbitMQ | 0.46 | 0.77 | 1646.12 | 4000 |
| 21:30:56Z | Rust | 10.01 | 14.26 | 99.16 | 200 |
| 21:30:56Z | Bun | 12.40 | 15.02 | 80.73 | 200 |
| 01:06:02Z | RabbitMQ | 0.42 | 0.88 | 1616.44 | 4000 |
| 01:06:02Z | Rust | 3.18 | 5.43 | 304.33 | 1000 |
| 01:06:02Z | Bun | 3.67 | 10.72 | 241.77 | 1000 |
| 04:59:08Z | RabbitMQ | 0.39 | 0.68 | 1725.53 | 4000 |
| 04:59:08Z | Rust | 0.28 | 0.61 | 2279.90 | 4000 |
| 04:59:08Z | Bun | 0.20 | 0.64 | 2661.80 | 8000 |
| 07:55:31Z | RabbitMQ | 0.47 | 0.86 | 1580.97 | 4000 |
| 07:55:31Z | Rust | 0.27 | 0.54 | 2286.57 | 4000 |
| 07:55:31Z | Bun | 0.25 | 0.82 | 2214.40 | 8000 |
| 09:03:42Z | RabbitMQ | 0.46 | 0.83 | 1678.69 | 4000 |
| 09:03:42Z | Rust | 0.28 | 0.61 | 2229.28 | 4000 |
| 09:03:42Z | Bun | 0.20 | 0.60 | 2710.94 | 8000 |

On 21:30 each confirm waited out its own flush, so the paced rate stayed near 100 messages/s and the 200/s step is the first miss. p50 and p99 sit inside 15 ms and 25 ms. On 01:06, p50 and p99 sit inside 5 ms and 12 ms, and RabbitMQ's messages/s stays higher because that confirm returns before the flush. From 04:59 on, Rust and Bun are ahead of the same session's RabbitMQ on p50, p99, and messages/s.

## 128 confirms across paced sessions

`× Rabbit` is the ratio printed in that session (three decimals) and the exact quotient.

### durable-256

| Session | Broker | messages/s | × Rabbit | p50 ms | p99 ms | First miss | Wall s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 21:30:56Z | RabbitMQ | 12935.91 | | 3.23 | 8.83 | 32000 | 21.00 |
| 21:30:56Z | Rust | 18013.42 | 1.393× (1.39251) | 2.35 | 11.45 | 48000 | 20.21 |
| 21:30:56Z | Bun | 18728.37 | 1.448× (1.44778) | 2.09 | 10.74 | 48000 | 20.18 |
| 01:06:02Z | RabbitMQ | 12799.52 | | 3.28 | 9.07 | 32000 | 21.01 |
| 01:06:02Z | Rust | 16936.46 | 1.323× (1.32321) | 2.58 | 11.46 | 48000 | 20.19 |
| 01:06:02Z | Bun | 17813.55 | 1.392× (1.39174) | 2.32 | 11.15 | 48000 | 20.20 |
| 04:59:08Z | RabbitMQ | 12857.96 | | 3.45 | 8.50 | 32000 | 20.68 |
| 04:59:08Z | Rust | 19239.16 | 1.496× (1.49628) | 2.02 | 11.45 | 48000 | 20.19 |
| 04:59:08Z | Bun | 18941.29 | 1.473× (1.47312) | 2.03 | 10.87 | 48000 | 20.21 |
| 07:55:31Z | RabbitMQ | 13063.30 | | 3.17 | 8.81 | 32000 | 21.10 |
| 07:55:31Z | Rust | 19441.91 | 1.488× (1.48828) | 2.00 | 11.19 | 48000 | 20.16 |
| 07:55:31Z | Bun | 18363.52 | 1.406× (1.40573) | 2.17 | 11.09 | 48000 | 20.17 |
| 09:03:42Z | RabbitMQ | 13139.01 | | 3.19 | 8.58 | 32000 | 20.93 |
| 09:03:42Z | Rust | 19230.10 | 1.464× (1.46359) | 2.03 | 11.15 | 48000 | 20.19 |
| 09:03:42Z | Bun | 18396.86 | 1.400× (1.40017) | 2.18 | 11.25 | 48000 | 20.17 |

### fan-2x2

| Session | Broker | messages/s | × Rabbit | p50 ms | p99 ms | First miss | Wall s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 21:30:56Z | RabbitMQ | 14329.30 | | 6.93 | 17.32 | 48000 | 20.15 |
| 21:30:56Z | Rust | 23786.62 | 1.660× (1.66000) | 2.86 | 11.48 | 96000 | 20.16 |
| 21:30:56Z | Bun | 23932.61 | 1.670× (1.67019) | 2.50 | 11.02 | 96000 | 20.16 |
| 01:06:02Z | RabbitMQ | 14331.58 | | 6.80 | 16.81 | 32000 | 20.14 |
| 01:06:02Z | Rust | 23372.78 | 1.631× (1.63086) | 3.01 | 13.27 | 96000 | 20.15 |
| 01:06:02Z | Bun | 23203.75 | 1.619× (1.61906) | 2.98 | 11.43 | 96000 | 20.17 |
| 04:59:08Z | RabbitMQ | 14837.75 | | 6.64 | 16.96 | 48000 | 20.15 |
| 04:59:08Z | Rust | 24002.12 | 1.618× (1.61764) | 2.39 | 11.79 | 96000 | 20.22 |
| 04:59:08Z | Bun | 23410.67 | 1.578× (1.57778) | 2.64 | 11.34 | 96000 | 20.17 |
| 07:55:31Z | RabbitMQ | 14465.24 | | 6.90 | 16.59 | 32000 | 20.14 |
| 07:55:31Z | Rust | 23966.65 | 1.657× (1.65684) | 2.35 | 11.42 | 96000 | 20.22 |
| 07:55:31Z | Bun | 23792.70 | 1.645× (1.64482) | 2.43 | 11.08 | 96000 | 20.13 |
| 09:03:42Z | RabbitMQ | 14512.22 | | 6.81 | 16.77 | 32000 | 20.14 |
| 09:03:42Z | Rust | 24005.72 | 1.654× (1.65417) | 2.41 | 11.56 | 96000 | 20.25 |
| 09:03:42Z | Bun | 23744.97 | 1.636× (1.63621) | 2.46 | 11.25 | 96000 | 20.15 |

| Session | durable 1.3× bar | fan 1.3× bar |
| --- | ---: | ---: |
| 21:30:56Z | 16816.68 | |
| 01:06:02Z | 16639.38 | 18631.05 |
| 04:59:08Z | 16715.35 | 19289.08 |
| 07:55:31Z | 16982.29 | 18804.81 |
| 09:03:42Z | 17080.71 | 18865.89 |

The 21:30 section printed the durable 1.3× line and left the fan 1.3× line out. On 21:30 and 01:06, Rust and Bun p50 stay inside 15 ms. The 21:30 steps ran between 1.991 s and 2.275 s. Early absolute floors cited with 21:30 were durable Rust 2,800 and Bun 3,600, fan Rust 3,200 and Bun 3,800, against the historical file at Rust 2768.62 / 3169.15 and Bun 3627.59 / 3829.11.

### Short scenarios

From 21:30 through 09:03, all three brokers: `size-64`, `prefetch-1`, and `prefetch-128` at 600.00, saturation 1000 kept; `size-4096` at 250.00, saturation 400 kept; `transient-256` at 1250.00, saturation 2000 kept.

Top-step consumed/s. 21:30 recorded the scores and the kept saturation, without a separate top-step line.

| Session | Scenario | Rabbit | Rust | Bun |
| --- | --- | ---: | ---: | ---: |
| 01:06:02Z | size-64 | 1000.0 | 994.3 | 997.1 |
| 01:06:02Z | prefetch-1 | 1000.0 | 997.9 | 998.6 |
| 01:06:02Z | prefetch-128 | 999.1 | 997.3 | 996.4 |
| 01:06:02Z | size-4096 | 400.0 | 399.8 | 399.8 |
| 01:06:02Z | transient-256 | 1999.4 | 1999.0 | 1998.5 |
| 04:59:08Z | size-64 | 999.9 | 995.4 | 995.7 |
| 04:59:08Z | prefetch-1 | 999.4 | 996.7 | 999.5 |
| 04:59:08Z | prefetch-128 | 999.9 | 996.3 | 995.8 |
| 04:59:08Z | size-4096 | 400.0 | 400.0 | 400.0 |
| 04:59:08Z | transient-256 | 1997.5 | 1999.8 | 1998.8 |
| 07:55:31Z | size-64 | 1000.0 | 997.5 | 997.7 |
| 07:55:31Z | prefetch-1 | 999.8 | 999.6 | 999.9 |
| 07:55:31Z | prefetch-128 | 999.7 | 998.6 | 993.7 |
| 07:55:31Z | size-4096 | 400.0 | 400.0 | 400.0 |
| 07:55:31Z | transient-256 | 1998.4 | 1999.0 | 1998.7 |
| 09:03:42Z | size-64 | 1000.0 | 996.3 | 995.2 |
| 09:03:42Z | prefetch-1 | 999.3 | 998.8 | 996.7 |
| 09:03:42Z | prefetch-128 | 999.5 | 998.5 | 999.1 |
| 09:03:42Z | size-4096 | 400.0 | 400.0 | 399.6 |
| 09:03:42Z | transient-256 | 2000.0 | 2000.0 | 1998.8 |

## Step rates, 128 confirms

Consumed messages/s. A rate under 95% is that broker's first miss. A blank cell has no rate in that session's write-up.

### 21:30:56Z

| Scenario | Offer | Rabbit | Rust | Bun |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 4000 | 3997.1 | 3969.2 | 3992.0 |
| durable-256 | 8000 | 7853.5 (98.17%) | 7965.1 | 7972.6 |
| durable-256 | 16000 | 15990.4 | 15924.3 | 15982.9 |
| durable-256 | 32000 | 23129.4 | 31817.8 | 31944.2 |
| durable-256 | 48000 | | 38433.1 | 41539.1 |
| fan-2x2 | 6400 | 6395.8 | 6384.6 | 6376.6 |
| fan-2x2 | 12800 | 12792.5 | 12707.0 | 12717.2 |
| fan-2x2 | 32000 | 30774.3 (96.17%) | | |
| fan-2x2 | 48000 | 27096.0 | | |
| fan-2x2 | 64000 | | 63690.7 | 63761.9 |
| fan-2x2 | 96000 | | 68486.8 | 69770.0 |

RabbitMQ's durable 8000 step at 98.17% stayed kept. RabbitMQ's fan 32000 step at 96.17% stayed kept, so the fan first miss is 48000.

### 01:06:02Z

| Scenario | Offer | Rabbit | Rust | Bun |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 8000 | 7996.0 (99.95%) | 7981.5 | 7980.6 |
| durable-256 | 16000 | 15987.3 | 15842.7 | 15932.8 |
| durable-256 | 32000 | 23464.0 | 31766.6 | 31833.9 |
| durable-256 | 48000 | | 33602.7 | 38197.1 |
| fan-2x2 | 6400 | 6396.8 | 6362.5 | 6359.6 |
| fan-2x2 | 12800 | 12791.8 | 12732.6 | 12741.5 |
| fan-2x2 | 32000 | 29821.4 (93.19%) | | |
| fan-2x2 | 64000 | | 63887.0 | 62418.3 |
| fan-2x2 | 96000 | | 64177.6 | 63481.9 |

### 04:59:08Z

| Scenario | Offer | Rabbit | Rust | Bun |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 8000 | 7991.3 (99.89%) | 7953.4 (99.42%) | 7954.4 (99.43%) |
| durable-256 | 16000 | 15988.9 | 15923.8 | 15944.4 |
| durable-256 | 32000 | 23626.6 (73.83%) | 31943.2 (99.82%) | 31368.7 (98.03%) |
| durable-256 | 48000 | | 42762.3 (89.09%) | 41609.2 (86.69%) |
| fan-2x2 | 6400 | 6398.7 | 6376.3 | 6356.0 |
| fan-2x2 | 12800 | 12792.7 (99.94%) | 12698.7 (99.21%) | 12756.8 (99.66%) |
| fan-2x2 | 32000 | 31206.2 (97.52%) | | |
| fan-2x2 | 48000 | 31180.3 (64.96%) | | |
| fan-2x2 | 64000 | | 63629.7 (99.42%) | 63952.5 (99.93%) |
| fan-2x2 | 96000 | | 69598.8 (72.50%) | 64718.0 (67.41%) |

RabbitMQ's fan 32000 step at 97.52% stayed kept, so that fan first miss is 48000. On the durable 8000 step, confirms match consumed.

### 07:55:31Z

| Scenario | Offer | Rabbit | Rust | Bun |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 8000 | 7993.3 (99.92%) | 7979.3 (99.74%) | 7953.0 (99.41%) |
| durable-256 | 16000 | 15991.4 | 15967.0 | 15952.1 |
| durable-256 | 32000 | 23917.6 (74.74%) | 31837.7 (99.49%) | 31957.7 (99.87%) |
| durable-256 | 48000 | | 43511.4 (90.65%) | 40094.3 (83.53%) |
| fan-2x2 | 6400 | 6395.1 | 6391.8 | 6376.4 |
| fan-2x2 | 12800 | 12792.7 (99.94%) | 12713.5 (99.32%) | 12757.2 (99.67%) |
| fan-2x2 | 32000 | 30146.6 (94.21%) | | |
| fan-2x2 | 64000 | | 63865.7 (99.79%) | 63888.3 (99.83%) |
| fan-2x2 | 96000 | | 68894.1 (71.76%) | 68352.1 (71.20%) |

On the durable 8000 step, confirms match consumed.

### 09:03:42Z

| Scenario | Offer | Rabbit | Rust | Bun |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 8000 | 7994.4 (99.93%) | 7982.6 (99.78%) | 7993.9 (99.92%) |
| durable-256 | 16000 | 15993.0 (99.96%) | 15913.9 (99.46%) | 15963.5 (99.77%) |
| durable-256 | 32000 | 23629.0 (73.84%) | 31964.8 (99.89%) | 31818.9 (99.43%) |
| durable-256 | 48000 | | 42462.3 (88.46%) | 40059.3 (83.46%) |
| fan-2x2 | 6400 | 6396.8 (99.95%) | 6357.8 (99.34%) | 6382.6 (99.73%) |
| fan-2x2 | 12800 | 12792.1 (99.94%) | 12710.7 (99.30%) | 12710.9 (99.30%) |
| fan-2x2 | 32000 | 30368.7 (94.90%) | | |
| fan-2x2 | 64000 | | 63897.2 (99.84%) | 63803.0 (99.69%) |
| fan-2x2 | 96000 | | 68643.1 (71.50%) | 67857.5 (70.68%) |

On the durable 8000 step, confirms match consumed. The same match holds on each cited durable step in this session.

## Remote one confirm across sessions

Same implementation, client node `a`, home on the peer, 1 CPU and 512 MiB each. Disk line: `publisher confirm after that fsync`. `confirm_before` stayed 0. From 04:59 on, `confirm_before_delta=0/0` on the accepted attempt.

Rust, client on `a`, home on the peer:

| Session | p50 ms | p99 ms | Peer fsync | Client fsync | Accepted |
| --- | ---: | ---: | ---: | ---: | --- |
| 01:06:02Z | 3.23 | 4.29 | 3553 | 0 | attempt 1 (`REMOTE_ACCEPT:rust:attempt=1`) |
| 04:59:08Z | 0.28 | 1.62 | 26836 | 0 | attempt 1 at 05:26:20Z |
| 07:55:31Z | 0.31 | 0.62 | 25158 | 0 | attempt 1 at 08:10:17Z |
| 09:03:42Z | 0.32 | 0.62 | 24914 | 0 | attempt 2 at 09:10:39Z |

Bun, client on `a`, home on the peer:

| Session | p50 ms | p99 ms | Peer fsync | Client fsync | Accepted |
| --- | ---: | ---: | ---: | ---: | --- |
| 01:06:02Z | 1.71 | 3.34 | 6931 | 1 | attempt 3 (`REMOTE_ACCEPT:bun:attempt=3`) |
| 04:59:08Z | 0.25 | 1.23 | 30855 | 0 | attempt 2 at 05:26:48Z |
| 07:55:31Z | 0.29 | 0.78 | 27060 | 0 | attempt 2 at 08:10:45Z |
| 09:03:42Z | 0.29 | 0.81 | 26870 | 0 | attempt 1 at 09:10:55Z |

Attempts where the home was the client:

| Session | Broker | Client fsync | Peer fsync | p50 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 04:59:08Z | Bun attempt 1 | 37613 | 0 | | |
| 07:55:31Z | Bun attempt 1 | 34416 | 0 | | |
| 09:03:42Z | Rust attempt 1 | 25571 | 0 | 0.29 | 0.65 |

`REMOTE_DONE` is 2026-10-05T05:27:01Z, 2026-10-05T08:10:57Z, and 2026-10-05T09:11:07Z. The 01:06 Rust pair listens on `172.30.220.10:25672` and `172.30.220.11:25672`, because that broker's cluster listen is a socket address. Later pairs use the same addresses, client ports 35773 and 35774. The 01:06 accepted rows are inside p50 15 ms and p99 25 ms. From 04:59 on, the caps are the 01:06 accepted latencies: Rust p50 3.23 / p99 4.29 and Bun p50 1.71 / p99 3.34. The 21:30 remote rows are in the [cluster smoke](#cluster-smoke-2026-10-04t215609z).

## Fsync counters

`confirm_before` is 0 on every row.

| Session | After 1 in flight, Rust wal | Bun wal | Bun full flush | After 128, Rust wal | Bun wal | Bun full flush |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 21:30:56Z | 10704 | 11743 | 11743 | | | |
| 01:06:02Z | | | | 19473 | 24324 | 24324 |
| 04:59:08Z | 36952 | 41869 | 41869 | 11437 | 13221 | 13221 |
| 07:55:31Z | 37012 | 37502 | 37502 | 11524 | 14413 | 14413 |
| 09:03:42Z | 36339 | 43441 | 43441 | 11454 | 14224 | 14224 |

21:30 counted fsyncs after the durable and fan run, so those counts sit in the inflight-1 columns. 01:06 published the counts after the 128 ladder.

## Trust

Kill -9 after the confirm, then a restart, delivers that body once. `confirm_before` stays 0 on every row.

| When | Check | ms | Fsync | Result |
| --- | --- | ---: | --- | --- |
| 21:40:23Z | Rust classic every_n_ms | 404 | 0 → 1 | survived, `TRUST_RUST_EXIT:0`, 4 passed |
| 21:40:23Z | Rust classic always | 9 | | survived |
| 21:40:23Z | Rust classic every_n_messages | 9 | | survived |
| 21:40:23Z | Rust forwarded home | 9 | 0 → 1 | survived |
| 21:40:23Z | Bun classic every_n_ms | 402 | | survived, `consumed body=kill9-body`, `TRUST_BUN_EXIT:0`, 6 pass |
| 21:40:23Z | Bun classic always | 1 | | survived |
| 21:40:23Z | Bun classic every_n_messages | 1 | | survived |
| 21:40:23Z | Bun forwarded home | 11 | 0 → 1 | survived kill of the home |
| 21:42:14Z | Quorum second confirm | 254 | | inside the 2000 ms cap |
| 21:42:14Z | Quorum second confirm | 79 | | inside the 2000 ms cap |
| 21:42:14Z | bun-bun-rust | | | survivor delivered once, restart did not duplicate |
| 21:42:14Z | rust-rust-bun | | | survivor delivered once, restart did not duplicate |
| 21:42:14Z | quorum body | | | bun consumed `body-one` from the survivor, rust consumed `body-one` from the survivor, bun confirm before fsync stayed 0, rust confirm before fsync stayed 0, `CLUSTER_TRUST_DONE` |
| 21:42:14Z | Stored home | | | `from-rust` from the Bun home, `from-bun` from the Rust home |
| 21:42:14Z | Membership | | | HTTP 400 on `a node cannot forget itself`, `member b still homes a classic queue`, and `the member list cannot become empty` |
| 21:42:14Z | URI shovel | | | `status=201 delivered shovel-body`, on Rust and on Bun |
| 21:42:14Z | URI federation | | | `upstream=201 policy=201 delivered fed-body`, on Rust and on Bun |
| 01:06:02Z | Rust classic always | 4.709 | 0 → 2 | survived, `CLASSIC_EXIT:0`, 4 passed |
| 01:06:02Z | Rust classic every_n_messages | 5.089 | 0 → 2 | survived |
| 01:06:02Z | Rust classic every_n_ms | 6.403 | 0 → 2 | survived |
| 01:06:02Z | Rust forwarded home | 11 | 0 → 1 | survived |
| 01:06:02Z | Rust 128-confirm batch | 11 | 0 → 1 | one fsync, `BATCH_EXIT:0` |
| 01:06:02Z | Bun 128-confirm batch | 4 | 0 → 1 | one fsync |
| 01:06:02Z | Bun classic and forwarded | under 10 | | each survived, `BUN_EXIT:0`, 14 pass |
| 01:06:02Z | bun-bun-rust | | | survivor delivered once, restart did not duplicate, `BUN_QUORUM_EXIT:0` |
| 01:06:02Z | rust-rust-bun | | | survivor delivered once, restart did not duplicate, `RUST_QUORUM_EXIT:0` |
| 04:59:08Z | Rust classic always | 4.957 | 0 → 2 | survived, `CLASSIC_EXIT:0`, 5 passed |
| 04:59:08Z | Rust classic every_n_messages | 5.352 | 0 → 2 | survived |
| 04:59:08Z | Rust classic every_n_ms | 4.968 | 0 → 2 | survived |
| 04:59:08Z | Rust forwarded home | 12 | 0 → 1 | survived |
| 04:59:08Z | Rust forwarded home, second run | 15 | 0 → 1 | survived, `FORWARDED_EXIT:0`, 2 passed |
| 04:59:08Z | Rust pipelined forwarded | 178 | 0 → 1 | survived |
| 04:59:08Z | Rust 128-confirm batch | 13 | 0 → 1 | one fsync, delta 1, `BATCH_EXIT:0`, 2 passed |
| 04:59:08Z | Rust 128-confirm batch | 81 | 0 → 1 | one fsync, delta 1 |
| 04:59:08Z | Bun 128-confirm batch | 5 | 0 → 1 | one fsync, delta 1 |
| 04:59:08Z | Bun 128-confirm batch | 81 | 0 → 1 | one fsync, delta 1 |
| 04:59:08Z | Bun classic every_n_ms | 2 | | survived, `consumed body=kill9-body` |
| 04:59:08Z | Bun classic always | 2 | | survived |
| 04:59:08Z | Bun classic every_n_messages | 2 | | survived |
| 04:59:08Z | Bun forwarded home | 3 | 0 → 1 | survived kill of the home |
| 04:59:08Z | Bun 128-confirm batch, bun log | 83 | 0 → 1 | full_flush, 6 pass, 0 fail |
| 04:59:08Z | rust-rust-bun | | | survivor delivered `kept-body`, restart returned `still-body`, second restart did not duplicate, 2 pass, 0 fail |
| 04:59:08Z | bun-bun-rust | | | same three lines |
| 04:59:08Z | quorum_majority | | | bun confirm before fsync stayed 0, bun consumed `body-one` from the survivor, bun fsync 2 → 6, rust confirm before fsync stayed 0, rust consumed `body-one` from the survivor, rust fsync 1 → 3, then `body-two` from the restarted node, 2 passed |
| 07:55:31Z | Rust classic always | 4.122 | 0 → 2 | survived, `CARGO_CLASSIC_EXIT:0`, 5 passed, 0 failed |
| 07:55:31Z | Rust classic every_n_messages | 4.375 | 0 → 2 | survived |
| 07:55:31Z | Rust classic every_n_ms | 4.062 | 0 → 2 | survived |
| 07:55:31Z | Rust forwarded home | 12 | 0 → 1 | survived |
| 07:55:31Z | Rust 128-confirm batch | 12 | 0 → 1 | one fsync, delta 1, `CARGO_BATCH_EXIT:0`, 2 passed, 0 failed |
| 07:55:31Z | Rust 128-confirm batch | 81 | 0 → 1 | one fsync, delta 1 |
| 07:55:31Z | Bun 128-confirm batch | 4 | 0 → 1 | one fsync, delta 1 |
| 07:55:31Z | Bun 128-confirm batch | 80 | 0 → 1 | one fsync, delta 1 |
| 07:55:31Z | Bun classic every_n_ms | 2 | | survived, `consumed body=kill9-body` |
| 07:55:31Z | Bun classic always | 2 | | survived |
| 07:55:31Z | Bun classic every_n_messages | 2 | | survived |
| 07:55:31Z | Bun forwarded home | 3 | 0 → 1 | survived kill of the home |
| 07:55:31Z | Bun illegal frame | | | that connection closed, the process stayed up |
| 07:55:31Z | Bun noAck | 3 | 0 → 1 | full_flush, `log_has_body=true`, `delivered=1`, survived kill -9 |
| 07:55:31Z | Bun 128-confirm batch, bun log | 81 | 0 → 1 | full_flush, 9 pass, 0 fail, `TRUST_BUN_EXIT:0` |
| 07:55:31Z | rust-rust-bun | | | survivor delivered `kept-body`, restart returned `still-body`, second restart did not duplicate |
| 07:55:31Z | bun-bun-rust | | | same three lines |
| 07:55:31Z | Bun quorum classic every_n_ms | 2 | | survived |
| 07:55:31Z | Bun quorum classic always | 3 | | survived |
| 07:55:31Z | Bun quorum classic every_n_messages | 3 | | survived, 2 pass, 0 fail, `QUORUM_EXIT:0` |
| 09:03:42Z | Rust classic always | 4.279 | 0 → 2 | survived, `CARGO_CLASSIC_EXIT:0`, 5 passed, 0 failed |
| 09:03:42Z | Rust classic every_n_messages | 4.425 | 0 → 2 | survived |
| 09:03:42Z | Rust classic every_n_ms | 3.992 | 0 → 2 | survived |
| 09:03:42Z | Rust forwarded home | 12 | 0 → 1 | survived |
| 09:03:42Z | Rust 128-confirm batch | 13 | 0 → 1 | one fsync, delta 1, `CARGO_BATCH_EXIT:0`, 2 passed, 0 failed |
| 09:03:42Z | Rust 128-confirm batch | 82 | 0 → 1 | one fsync, delta 1 |
| 09:03:42Z | Bun 128-confirm batch | 5 | 0 → 1 | one fsync, delta 1 |
| 09:03:42Z | Bun 128-confirm batch | 82 | 0 → 1 | one fsync, delta 1 |
| 09:03:42Z | Bun classic every_n_ms | 2 | | survived, `consumed body=kill9-body` |
| 09:03:42Z | Bun classic always | 2 | | survived |
| 09:03:42Z | Bun classic every_n_messages | 2 | | survived |
| 09:03:42Z | Bun forwarded home | 3 | 0 → 1 | survived kill of the home |
| 09:03:42Z | Bun illegal frame | | | that connection closed, the process stayed up |
| 09:03:42Z | Bun noAck | 2 | 0 → 1 | full_flush, `log_has_body=true`, `delivered=1`, survived kill -9 |
| 09:03:42Z | Bun 128-confirm batch, bun log | 83 | 0 → 1 | full_flush, 9 pass, 0 fail, `TRUST_BUN_EXIT:0` |
| 09:03:42Z | rust-rust-bun | | | survivor delivered `kept-body`, restart returned `still-body`, second restart did not duplicate |
| 09:03:42Z | bun-bun-rust | | | same three lines |
| 09:03:42Z | Bun quorum classic every_n_ms | 3 | | survived |
| 09:03:42Z | Bun quorum classic always | 2 | | survived |
| 09:03:42Z | Bun quorum classic every_n_messages | 3 | | survived, 2 pass, 0 fail, `QUORUM_EXIT:0` |

## Builds

RabbitMQ throughout is `rabbitmq:4.3-management` `sha256:ddc75301edf58a8332934cf2d801be7cbf8d65c6458d747364a8046238ff1c89`.

| Session | Rust image | Rust created | Bun image | Bun created |
| --- | --- | --- | --- | --- |
| 21:30:56Z | `sha256:be759722813fde142ab46cc669e2839efe6e5bb1b4ac85c8a59d19b6f39e3662` | 2026-10-04T21:30:42Z | `sha256:a8b506e5e01f84f66993ebcd0c6be0c4bc12df119a7c3bdcd4cbe69775169c73` | 2026-10-04T21:30:45Z |
| 01:06:02Z | `sha256:9def1b9b8dcf4850874a66dc8eb03614e209b4989991143ad1f74a3fa6018ddf` | 2026-10-05T00:58:47Z | `sha256:49d069e3f46755d92bf66f672468f5e94ec45a70f3c5fd8b0782f005e5937ca7` | 2026-10-05T00:56:44Z |
| 04:59:08Z | `sha256:09c41772d329340184f099fbf9b5becd026ec0665ceca5f12218bf8f4e875b97` | 2026-10-05T04:47:19Z | `sha256:a02efd67b25ad4743bf996effb37f52dcba0a8bf34a68c4e4e118fca3634bf6e` | 2026-10-05T05:20:18Z |
| 07:55:31Z | `sha256:913d67ca03332153548bba73f06e82d842825a50993a98be529bda5858241418` | 2026-10-05T07:51:31Z | `sha256:619c7a5c2492ecc534d9590652aacf35b259cd5cd8831419c244c4bb8b7e5c7c` | 2026-10-05T08:06:47Z |
| 09:03:42Z | `sha256:913d67ca03332153548bba73f06e82d842825a50993a98be529bda5858241418` | 2026-10-05T07:51:31Z | `sha256:002d75f424ccfa8d3aab05a90143e7b2b696d8c3ff3306564128fbe11611308b` | 2026-10-05T09:01:45Z |

The 04:59 capture's image list names an earlier Bun image from 2026-10-05T04:42:21Z. The measured Bun id is the 05:20:18Z image, the line immediately before `===== bun =====`. Rust ran at 05:00:57Z and Bun at 05:22:08Z. RabbitMQ reached `Server startup complete` on the first start.

The 01:06 RabbitMQ container first exited while reading `/var/lib/rabbitmq/.erlang.cookie` (`eacces`). Those RabbitMQ rows are the restart at 2026-10-05T01:21:41Z on a fresh volume, same image, same 1 CPU / 512 MiB limit, same `bench/rabbitmq.conf`. Rust ran at 01:08:10Z and Bun at 01:09:55Z, before that restart.

The 07:55 and 09:03 captures record `rabbit startup complete` on the first start. 07:55 times: RabbitMQ 07:55:31Z, Rust 07:57:21Z, Bun 08:07:34Z.

## Cluster smoke (2026-10-04T21:56:09Z)

Same images as the 21:30 single-node run, same 1 CPU / 512 MiB cap, `fsync_interval_ms=10`. The host has 2 CPUs, so three-node messages/s stay smoke. 52 summaries (7 classic and 6 quorum on each of 4 layouts), each `consume=ok` and `confirms=ok`, `throughput=paced`, compare exit 0. Durable confirms print `publisher confirm after that fsync`.

One confirm in flight, classic queue stored on the other node. Each path is inside p50 25 ms and p99 40 ms. The highest p50 is 11.78 ms. The highest p99 is 16.32 ms. The mixed rows consumed `remote-body`.

| Path | p50 ms | p99 ms | consume |
| --- | ---: | ---: | --- |
| Rust client, Rust home | 9.92 | 14.25 | ok |
| Bun client, Bun home | 10.93 | 16.32 | ok |
| Bun client, Rust home | 10.08 | 13.93 | ok |
| Rust client, Bun home | 11.78 | 15.17 | ok |

| Layout | Classic durable msg/s | Durable first miss | Classic fan msg/s | Fan first miss | Quorum durable msg/s | Quorum fan msg/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| rust3 | 11659.52 | 32000 | 12805.03 | 32000 | 4619.06 | 4166.67 |
| bun3 | 9035.20 | 16000 | 23849.34 | 96000 | 4512.95 | 4166.17 |
| Rust-Rust-Bun | 11906.33 | 32000 | 13159.93 | 32000 | 4600.24 | 4166.67 |
| Bun-Bun-Rust | 18776.39 | 48000 | 8151.36 | 32000 | 4597.22 | 4166.67 |

Quorum `durable-256` first miss is 16000 on every layout. Quorum `fan-2x2` saturation is 12800 kept on every layout. The capture listed one wall per layout, in the order the layouts were printed, without repeating the layout name on the wall:

| | 1 | 2 | 3 | 4 |
| --- | ---: | ---: | ---: | ---: |
| Quorum durable wall s | 12.11 | 12.15 | 12.14 | 12.11 |
| Quorum fan wall s | 12.15 | 12.18 | 12.13 | 12.09 |

Quorum durable confirm p50 runs from 9.04 ms to 9.39 ms. The single-node paced rates beside this smoke are the 21:30 rows: durable 12935.91 / 18013.42 (1.393×) / 18728.37 (1.448×) and fan 14329.30 / 23786.62 (1.660×) / 23932.61 (1.670×).

Classic confirm p50, milliseconds:

| Scenario | rust3 | bun3 | Rust-Rust-Bun | Bun-Bun-Rust |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 4.00 | 7.81 | 4.04 | 2.07 |
| size-64 | 7.26 | 6.71 | 7.33 | 7.19 |
| size-4096 | 7.84 | 6.67 | 7.15 | 7.97 |
| transient-256 | 1.82 | 1.57 | 2.01 | 1.78 |
| prefetch-1 | 7.60 | 7.08 | 7.36 | 6.87 |
| prefetch-128 | 7.02 | 7.61 | 7.69 | 7.57 |
| fan-2x2 | 6.96 | 2.56 | 5.72 | 16.69 |

Classic confirm p99, milliseconds:

| Scenario | rust3 | bun3 | Rust-Rust-Bun | Bun-Bun-Rust |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 12.61 | 17.20 | 11.35 | 10.81 |
| size-64 | 34.74 | 23.22 | 16.12 | 27.79 |
| size-4096 | 28.00 | 27.93 | 25.12 | 30.77 |
| transient-256 | 8.86 | 3.09 | 12.08 | 10.82 |
| prefetch-1 | 15.22 | 15.34 | 15.86 | 31.13 |
| prefetch-128 | 17.09 | 28.67 | 27.10 | 33.57 |
| fan-2x2 | 18.78 | 11.05 | 14.53 | 32.23 |

Quorum confirm p50, milliseconds. Two members fsync before the confirm. The highest p99 in the 52 summaries is 36.70 ms, Bun-Bun-Rust quorum `prefetch-128`.

| Scenario | rust3 | bun3 | Rust-Rust-Bun | Bun-Bun-Rust |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 9.39 | 9.04 | 9.39 | 9.23 |
| size-64 | 9.17 | 7.67 | 7.67 | 7.51 |
| size-4096 | 7.78 | 7.70 | 8.11 | 7.53 |
| prefetch-1 | 7.68 | 6.87 | 7.97 | 6.68 |
| prefetch-128 | 9.11 | 7.56 | 7.94 | 7.73 |
| fan-2x2 | 9.23 | 9.08 | 9.14 | 9.38 |

Quorum confirm p99, milliseconds:

| Scenario | rust3 | bun3 | Rust-Rust-Bun | Bun-Bun-Rust |
| --- | ---: | ---: | ---: | ---: |
| durable-256 | 15.38 | 18.27 | 16.50 | 17.06 |
| size-64 | 29.24 | 28.10 | 25.29 | 24.22 |
| size-4096 | 17.46 | 20.36 | 19.00 | 34.85 |
| prefetch-1 | 22.48 | 29.52 | 14.30 | 35.07 |
| prefetch-128 | 20.17 | 21.76 | 13.35 | 36.70 |
| fan-2x2 | 18.22 | 14.33 | 29.28 | 31.96 |

`size-64`, `prefetch-1`, and `prefetch-128` are saturation 1000 kept at 600.00 on every layout. `size-4096` is 400 kept at 250.00. Classic `transient-256` is 2000 kept at 1250.00.

## Captures that are not this score

| Capture | What it printed | Why it is off the tables above |
| --- | --- | --- |
| 2026-10-04T19:16:30Z | durable 12762.92 / 16591.66 / 16356.08 (1.29999× and 1.28153×); fan 13927.56 / 19144.97 / 18681.61 (1.375× and 1.341×) | durable was under 1.3× |
| 2026-10-04T06:27:09Z | sustained headline durable 15986.24 / 31802.97 / 31881.49, fan 12792.38 / 45983.97 / 31843.32; pace lines durable 12920.66 / 17136.55 / 16960.69, fan 13839.21 / 19838.45 / 19137.78 | `throughput=sustained` used the highest kept step |
| 2026-10-04T06:10:15Z | durable 5191.00 / 5200.00 / 5200.00 (1.002×); fan 4166.67 on all three (1.000×) | historical ladder only, same window mean |
| 2026-10-04T03:07:26Z | classic durable and fan as `throughput=consumer` | older compare build |

Order in those triples is RabbitMQ / Rust / Bun.

## Historical confirm before the fsync

One run on one shared disk. Host ports 35672, 35673, and 35674. Images: RabbitMQ `rabbitmq:4.3-management`; Rust `queueforge-rust:bench` built `rust:1.85-bookworm`, runtime `debian:bookworm-slim`; Bun `queueforge-bun:bench` on `oven/bun:1.4.2-alpine`. Each container had `NanoCpus=1000000000` and `Memory=536870912`. The only change between runs was `AMQP_URL`.

Both QueueForge brokers fsync on `fsync_interval_ms=10`. In this run a durable confirm completed after the buffered write and before that fsync. On Rust the interval fsync runs outside the queue-actor command loop, and the actor takes the finished fsync before the next mailbox command. A confirm issued while the fsync is still blocked returns without waiting. On Bun, `every_n_ms` stages durable rows and writes them in one transaction on the group-commit timer. A crash before the interval fsync can drop an acknowledged message. Confirm ms below is the median, `confirm_latency_ms`.

| Scenario | Broker | messages/s | Confirm ms | Saturation | Wall s |
| --- | --- | ---: | ---: | --- | ---: |
| durable-256 | RabbitMQ | 1430.87 | 0.53 | miss 2000 | 12.05 |
| durable-256 | Rust | 2768.62 | 0.21 | miss 8000 | 12.12 |
| durable-256 | Bun | 3627.59 | 0.13 | miss 8000 | 12.12 |
| fan-2x2 | RabbitMQ | 2105.03 | 0.57 | miss 6400 | 12.05 |
| fan-2x2 | Rust | 3169.15 | 0.27 | miss 12800 | 12.16 |
| fan-2x2 | Bun | 3829.11 | 0.18 | miss 12800 | 12.14 |
| size-64 | RabbitMQ | 589.43 | 0.54 | 1000 kept | 4.05 |
| size-64 | Rust | 596.64 | 0.27 | 1000 kept | 4.07 |
| size-64 | Bun | 596.66 | 0.35 | 1000 kept | 4.07 |
| size-4096 | RabbitMQ | 244.88 | 0.82 | 400 kept | 4.09 |
| size-4096 | Rust | 247.47 | 0.68 | 400 kept | 4.07 |
| size-4096 | Bun | 247.84 | 0.60 | 400 kept | 4.07 |
| transient-256 | RabbitMQ | 1233.16 | 0.23 | 2000 kept | 3.07 |
| transient-256 | Rust | 1240.46 | 0.28 | 2000 kept | 3.07 |
| transient-256 | Bun | 1239.90 | 0.20 | 2000 kept | 3.07 |
| prefetch-1 | RabbitMQ | 600.25 | 0.55 | 1000 kept | 4.05 |
| prefetch-1 | Rust | 593.19 | 0.29 | 1000 kept | 4.08 |
| prefetch-1 | Bun | 593.91 | 0.31 | 1000 kept | 4.07 |
| prefetch-128 | RabbitMQ | 599.96 | 0.54 | 1000 kept | 4.05 |
| prefetch-128 | Rust | 596.46 | 0.28 | 1000 kept | 4.07 |
| prefetch-128 | Bun | 593.87 | 0.32 | 1000 kept | 4.07 |

| Broker | Flush path |
| --- | --- |
| RabbitMQ | `classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` |
| Rust | `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` |
| Bun | `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` |

Transient rows print `disk_flush=not-durable`.

On `durable-256` and `fan-2x2`, Rust and Bun are at least 30% higher in messages/s than RabbitMQ and at most 70% of RabbitMQ's confirm time. Bun is the faster of the two on both (3627.59 and 3829.11). Rust clears the same 8000 and 12800 saturation steps. `transient-256` kept 2000/s on every broker.

### Still outside this comparison

| Blocker | Why it still matters |
| --- | --- |
| Joining an existing RabbitMQ cluster | QueueForge does not join an Erlang RabbitMQ cluster. |
| Classic queue mirroring | RabbitMQ 4 rejects `ha-mode` / `ha-params`. QueueForge does not implement mirroring. Quorum is the replicated type. |
| LDAP, OAuth, x509 | Authentication is the local user table. |
| Kubernetes operator and management auth | QueueForge management uses the `queueforge_session` cookie. RabbitMQ uses HTTP basic auth. Permission URLs differ. |
| Confirm before the flush | This historical run only. From 2026-10-04T21:30:56Z, QueueForge confirms after the covering fsync. RabbitMQ classic queues still confirm before a flush of at least 200 ms. |

Mixed Rust and Bun quorum is the [21:56 smoke](#cluster-smoke-2026-10-04t215609z) on a 2 CPU host. Data directories are still not interchangeable, and a QueueForge node still does not join an Erlang cluster.

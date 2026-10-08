# QueueForge vs RabbitMQ 4

One client, `queueforge-compare`. Each container is 1 CPU and 512 MiB (`docker_ncpu=2` on the host). Ports are 35672 (RabbitMQ 4.3), 35673 (Rust), 35674 (Bun), and 35675 (PHP), one container at a time. From 2026-10-04T21:30:56Z on, the compare binary mtime is 2026-10-04T16:16:23-0300, `QUEUEFORGE_COMPARE_RATES` is unset, and the slot wait stays on.

Message k is published at `k/rate`. In `fan-2x2`, two producers run together, each at half the labeled rate. A step is kept when confirms and acks both reach 95% of the offer. `messages/s` equals `pace_messages_per_sec`: acked deliveries over the counted windows. The report line is `throughput=paced`.

**First miss** is `saturation_load`, the first offered rate that missed 95%. **kept** means every step held (`kept_up=true`), and the number is the last offer. `durable-256` offers 200, 1000, 2000, 4000, 8000, 16000. `fan-2x2` offers 200, 800, 1600, 3200, 6400, 12800. With 128 confirms in flight, both also offer 32000, 48000, 64000, and 96000. The short scenarios offer 200 and 1000 (`size-64`, `prefetch-1`, `prefetch-128`), 100 and 400 (`size-4096`), or 500 and 2000 (`transient-256`). Keeping both steps scores exactly 600.00, 250.00, or 1250.00.

RabbitMQ confirms before its flush in every session. From 2026-10-04T21:30:56Z on, a QueueForge durable confirm returns after the covering fsync, and `queueforge_confirm_before_fsync_total` stays 0. The [historical run](#historical-confirm-before-the-fsync) is the older path, where QueueForge also confirmed before the 10 ms fsync.

The [2026-10-08 session](#session-2026-10-08) is the current result for all four brokers: paced, the load sweep, and a local Raft latency check. The [paced run](#paced-2026-10-07t213657z) is the previous `queueforge-compare` session. The [latest run](#latest-2026-10-05t090342z) and [PHP parity paced](#php-parity-paced-2026-10-06t181520z) are the earlier sessions. Older paced sessions use the same columns. The [load sweep](#load-2026-10-05t212906z) is a separate unpaced run: messages per second and cgroup memory at five container sizes. The [load compare](#load-compare) table is one row per broker and size. RabbitMQ, Rust, and Bun message rates there are [Load remeasure](#load-2026-10-07t210340z). PHP rows at 1 CPU are that same run. PHP rows above 1 CPU stay [PHP cores](#php-cores-2026-10-07t034705z), because this remeasure's PHP containers with more than one CPU exited before a rate.

The PHP rows are the parity build, the one with client-visible parity with Bun and Rust. The earlier PHP image is kept in [PHP paced](#php-paced-2026-10-06t161324z) for comparison.

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

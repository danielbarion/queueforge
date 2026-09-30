# Local bench smoke (not the reference SLO)

Recorded on this developer machine after the single-node production fixes. It is
not the 30-second run on the 8-vCPU Linux NVMe host in `docs/PERFORMANCE.md`.
The hypothesis table there is still a hypothesis.

| | |
|---|---|
| Host | macOS 26.6.2, Apple M4 Max, 16 cores, 128 GB |
| Broker RAM probe | 8 GiB non-Linux fallback (`total_ram_bytes=8589934592`) |
| Binary | local `cargo build --release` of `queueforge` and `queueforge-bench` |
| Window | 5s measure + 1s warmup, TLS off, group commit 100 ms, body 1024 B |
| Command | `queueforge-bench --shape all --queue-prefix qf-v5 --duration-secs 5 --warmup-secs 1` |

Rebuilt `queueforge` and `queueforge-bench` from the current tree, then ran shapes A–E. Every shape has a non-zero publish count and a non-zero consume count.

| Shape | Published | Consumed | Ingress | Consume rate | Publish p99 | E2E p99 |
|---|---|---|---|---|---|---|
| A | 99,694 | 5,993 | 19,939 msg/s | 1,199 msg/s | 1.63 ms | 5.64 s |
| B | 423,135 | 405,392 | 84,627 msg/s | 81,078 msg/s | 380 µs | 740 ms |
| C | 39,409 | 394,001 | 7,882 msg/s | 78,800 msg/s | 53 µs | 250 ms |
| D | 50 | 50 | 10 msg/s | 10 msg/s | 104 ms | 104 ms |
| E | 378,148 | 360,995 | 75,630 msg/s | 72,199 msg/s | 586 µs | 809 ms |

Every shape published and consumed. Shape C's consume count is higher than its
ingress because each publish is delivered to 10 fanout queues. Shape D tracks
the 100 ms group-commit interval, so a 5-second window only moves a handful of
persistent confirmed messages. Do not read these rates as the design targets.

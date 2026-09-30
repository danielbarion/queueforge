# Production benchmark: RabbitMQ 4, Rust QueueForge, Bun QueueForge

One client, `queueforge-compare`, ran seven classic-queue scenarios against each broker. The only change between runs was `AMQP_URL`. Each container had `NanoCpus=1000000000` and `Memory=536870912`. Host ports were 35672 (RabbitMQ `rabbitmq:4.3-management`), 35673 (Rust image `queueforge-rust:bench`, build `rust:1.85-bookworm`, runtime `debian:bookworm-slim`), and 35674 (Bun image `queueforge-bun:bench`, base `oven/bun:1.4.2-alpine`).

Each publisher sends message k at `k / rate` for its share of the labeled rate, then waits for that confirm. Two producers in `fan-2x2` run at the same time, each at half the labeled rate, so the pair offers the full rate. A step keeps up when both confirms and acks reach 95% of the labeled rate inside that step. `messages_per_sec` is acked deliveries over the whole scenario. `confirm_latency_ms` is the median confirm time. `saturation_load` is the first offered rate that missed that bar, or the last rate when every step kept up. These figures are one run on one shared disk.

## What changed before this run

Bun was ignoring `fsync_interval_ms` and wrapping every durable insert in `PRAGMA synchronous=FULL`. It now group-commits on that interval and completes the publisher confirm after the fsync, which is the same rule the Rust broker already uses. A durable publish, confirm, and consume of the same body passed on RabbitMQ 4, Rust, and Bun (`durable_group_commit_matches_rabbitmq`).

## Results

| Scenario | RabbitMQ messages/s | Rust messages/s | Bun messages/s | RabbitMQ confirm ms | Rust confirm ms | Bun confirm ms | RabbitMQ saturation | Rust saturation | Bun saturation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| durable-256 | 599.84 | 97.13 | 86.61 | 0.61 | 10.08 | 11.32 | `saturation_load=1000 kept_up=true` | `saturation_load=200 kept_up=false` | `saturation_load=200 kept_up=false` |
| size-64 | 600.26 | 95.34 | 87.26 | 0.58 | 10.08 | 11.16 | `saturation_load=1000 kept_up=true` | `saturation_load=200 kept_up=false` | `saturation_load=200 kept_up=false` |
| size-4096 | 247.64 | 95.38 | 86.60 | 0.89 | 9.50 | 11.24 | `saturation_load=400 kept_up=true` | `saturation_load=400 kept_up=false` | `saturation_load=100 kept_up=false` |
| transient-256 | 1229.94 | 1231.26 | 1231.31 | 0.27 | 0.28 | 0.27 | `saturation_load=2000 kept_up=true` | `saturation_load=2000 kept_up=true` | `saturation_load=2000 kept_up=true` |
| prefetch-1 | 600.15 | 96.27 | 86.62 | 0.56 | 10.00 | 11.09 | `saturation_load=1000 kept_up=true` | `saturation_load=200 kept_up=false` | `saturation_load=200 kept_up=false` |
| prefetch-128 | 600.03 | 96.83 | 87.91 | 0.52 | 10.26 | 11.06 | `saturation_load=1000 kept_up=true` | `saturation_load=200 kept_up=false` | `saturation_load=200 kept_up=false` |
| fan-2x2 | 500.58 | 90.83 | 173.68 | 1.07 | 20.11 | 11.37 | `saturation_load=800 kept_up=true` | `saturation_load=200 kept_up=false` | `saturation_load=200 kept_up=false` |

Exact lines from the logs:

### RabbitMQ

- `scenario=durable-256` `messages_per_sec=599.84` `confirm_latency_ms=0.61 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=size-64` `messages_per_sec=600.26` `confirm_latency_ms=0.58 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=size-4096` `messages_per_sec=247.64` `confirm_latency_ms=0.89 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=400 kept_up=true` `wall_secs=4.07`
- `scenario=transient-256` `messages_per_sec=1229.94` `confirm_latency_ms=0.27 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.08`
- `scenario=prefetch-1` `messages_per_sec=600.15` `confirm_latency_ms=0.56 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=prefetch-128` `messages_per_sec=600.03` `confirm_latency_ms=0.52 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=fan-2x2` `messages_per_sec=500.58` `confirm_latency_ms=1.07 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=800 kept_up=true` `wall_secs=4.05`

### Rust

- `scenario=durable-256` `messages_per_sec=97.13` `confirm_latency_ms=10.08 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.08`
- `scenario=size-64` `messages_per_sec=95.34` `confirm_latency_ms=10.08 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.08`
- `scenario=size-4096` `messages_per_sec=95.38` `confirm_latency_ms=9.50 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=400 kept_up=false` `wall_secs=4.08`
- `scenario=transient-256` `messages_per_sec=1231.26` `confirm_latency_ms=0.28 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.07`
- `scenario=prefetch-1` `messages_per_sec=96.27` `confirm_latency_ms=10.00 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.08`
- `scenario=prefetch-128` `messages_per_sec=96.83` `confirm_latency_ms=10.26 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.09`
- `scenario=fan-2x2` `messages_per_sec=90.83` `confirm_latency_ms=20.11 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.10`

### Bun

- `scenario=durable-256` `messages_per_sec=86.61` `confirm_latency_ms=11.32 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.05`
- `scenario=size-64` `messages_per_sec=87.26` `confirm_latency_ms=11.16 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.05`
- `scenario=size-4096` `messages_per_sec=86.60` `confirm_latency_ms=11.24 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=100 kept_up=false` `wall_secs=4.05`
- `scenario=transient-256` `messages_per_sec=1231.31` `confirm_latency_ms=0.27 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.08`
- `scenario=prefetch-1` `messages_per_sec=86.62` `confirm_latency_ms=11.09 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.05`
- `scenario=prefetch-128` `messages_per_sec=87.91` `confirm_latency_ms=11.06 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.06`
- `scenario=fan-2x2` `messages_per_sec=173.68` `confirm_latency_ms=11.37 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` `saturation_load=200 kept_up=false` `wall_secs=4.05`

## Disk flush beside durable latency

| Broker | Flush path that ran |
| --- | --- |
| RabbitMQ | `classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` |
| Rust | `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` |
| Bun | `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, confirm after fsync` |

Transient scenarios print `disk_flush=not-durable`. RabbitMQ’s `bench/rabbitmq.conf` sets `classic_queue.default_version=2`. Classic queue v2 flushes its write buffer at least every 200ms and sends the confirm before that fsync. On `durable-256` that confirm is `confirm_latency_ms=0.61`. Rust and Bun wait for the 10 ms group commit: `confirm_latency_ms=10.08` and `confirm_latency_ms=11.32`.

## Recommendation

Use RabbitMQ 4 for a durable publisher that waits for each confirm. It kept up through the labeled rates in every scenario, including `saturation_load=1000 kept_up=true` at 256 bytes (`messages_per_sec=599.84`, the average of 200 and 1000) and `saturation_load=2000 kept_up=true` for transient publishes (`messages_per_sec=1229.94`). Both QueueForge brokers miss the first durable step: Rust `saturation_load=200 kept_up=false` at `messages_per_sec=97.13`, Bun `saturation_load=200 kept_up=false` at `messages_per_sec=86.61`. A 10 ms confirm cannot reach 200 messages/s with one confirm in flight. The ceiling is the flush rule, not prefetch: `prefetch-1` and `prefetch-128` stay on the same durable rates.

Transient publishes do not wait for fsync. All three kept up at 2000/s: RabbitMQ `messages_per_sec=1229.94`, Rust `messages_per_sec=1231.26`, Bun `messages_per_sec=1231.31`, each with confirm latency under 0.3 ms. For a transient workload this test does not pick a winner.

A 4096-byte body still fits RabbitMQ’s labeled steps (`saturation_load=400 kept_up=true`, `messages_per_sec=247.64`). Rust misses 400 (`saturation_load=400 kept_up=false`, `messages_per_sec=95.38`). Bun misses the 100/s step (`saturation_load=100 kept_up=false`, `messages_per_sec=86.60`).

`fan-2x2` is two publishers and two consumers running together. RabbitMQ kept up at `saturation_load=800 kept_up=true` (`messages_per_sec=500.58`). Bun’s two publishers share one group commit and nearly double the single-publisher rate (`messages_per_sec=173.68`, `confirm_latency_ms=11.37`) but still miss 200/s (`saturation_load=200 kept_up=false`). Rust’s two publishers do not overlap that way: confirm time rises to `confirm_latency_ms=20.11` and throughput stays `messages_per_sec=90.83`.

Use either QueueForge broker only for a new deployment that can accept confirm-after-fsync at about 10 ms and does not need the blockers below. On one durable publisher, Rust is a few messages per second ahead of Bun. On two concurrent publishers in this run, Bun is ahead. Transient publishes are a tie.

## Production blockers

These are outside this classic-queue comparison. A production move off RabbitMQ still has to account for them.

| Blocker | Why it blocks a replacement |
| --- | --- |
| Joining an existing RabbitMQ cluster | QueueForge nodes cluster only with the same implementation. They do not join an Erlang RabbitMQ cluster. |
| Mixing Rust and Bun nodes | The two brokers do not replicate to each other. Data directories are not interchangeable. |
| Classic queue mirroring | RabbitMQ 4 rejects `ha-mode` / `ha-params`. QueueForge does not implement mirroring either. Quorum queues are the replicated type, and only inside one implementation. |
| LDAP, OAuth, x509 | Not implemented. Authentication is the local user table. |
| Kubernetes operator and management-compatible automation | QueueForge management uses the `queueforge_session` cookie. RabbitMQ uses HTTP basic auth. Permission URLs are not the same shape. |
| Confirm-before-fsync durability | RabbitMQ classic queues confirm before fsync. Both QueueForge brokers confirm after the group-commit fsync. Clients that treat a confirm as “not yet on disk” see a different latency and a different crash window. |

MQTT, STOMP, AMQP 1.0, streams, federation, and shovel are implemented on both brokers and were not part of this classic-queue run.

## Decision

For durable classic publishes that confirm one at a time, stay on RabbitMQ 4. It is the only broker here that kept up with the labeled durable rates, because it confirms before fsync. Move to QueueForge only when a 10 ms confirm is acceptable and the deployment does not join a RabbitMQ cluster or use LDAP or the RabbitMQ management API. Between Rust and Bun, pick Rust for a single durable publisher and Bun when several publishers confirm at once. Neither QueueForge broker matched RabbitMQ on durable throughput in this run.


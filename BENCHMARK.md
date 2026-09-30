# Production benchmark: RabbitMQ 4, Rust QueueForge, Bun QueueForge

One client, `queueforge-compare`, ran seven classic-queue scenarios against each broker. The only change between runs was `AMQP_URL`. Each container had `NanoCpus=1000000000` and `Memory=536870912`. Host ports were 35672 (RabbitMQ `rabbitmq:4.3-management`), 35673 (Rust image `queueforge-rust:bench`, build `rust:1.85-bookworm`, runtime `debian:bookworm-slim`), and 35674 (Bun image `queueforge-bun:bench`, base `oven/bun:1.4.2-alpine`).

Each publisher sends message k at `k / rate` for its share of the labeled rate, then waits for that confirm. Two producers in `fan-2x2` run at the same time, each at half the labeled rate, so the pair offers the full rate. A step keeps up when both confirms and acks reach 95% of the labeled rate inside that step. `messages_per_sec` is acked deliveries over the whole scenario. `confirm_latency_ms` is the median confirm time. `saturation_load` is the first offered rate that missed that bar, or the last rate when every step kept up. These figures are one run on one shared disk.

## What changed before this run

Both QueueForge brokers still fsync on `fsync_interval_ms=10`. A durable publisher confirm now completes after the buffered write, before that fsync, which is the same observable rule as RabbitMQ classic queue v2. A crash before the interval fsync can drop an acknowledged message. A durable publish, confirm, and consume of the same body passed on RabbitMQ 4, Rust, and Bun (`durable_group_commit_matches_rabbitmq`).

## Results

| Scenario | RabbitMQ messages/s | Rust messages/s | Bun messages/s | RabbitMQ confirm ms | Rust confirm ms | Bun confirm ms | RabbitMQ saturation | Rust saturation | Bun saturation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| durable-256 | 600.04 | 586.79 | 593.88 | 0.64 | 0.37 | 0.38 | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` |
| size-64 | 600.25 | 583.66 | 593.86 | 0.56 | 0.36 | 0.34 | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` |
| size-4096 | 249.07 | 243.83 | 247.61 | 0.83 | 0.61 | 0.57 | `saturation_load=400 kept_up=true` | `saturation_load=400 kept_up=true` | `saturation_load=400 kept_up=true` |
| transient-256 | 1231.20 | 1232.23 | 1231.17 | 0.30 | 0.27 | 0.25 | `saturation_load=2000 kept_up=true` | `saturation_load=2000 kept_up=true` | `saturation_load=2000 kept_up=true` |
| prefetch-1 | 593.68 | 596.89 | 593.83 | 0.64 | 0.36 | 0.37 | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` |
| prefetch-128 | 590.64 | 593.79 | 594.14 | 0.63 | 0.33 | 0.38 | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` |
| fan-2x2 | 495.20 | 495.73 | 495.10 | 1.09 | 0.55 | 0.54 | `saturation_load=800 kept_up=true` | `saturation_load=800 kept_up=true` | `saturation_load=800 kept_up=true` |

Exact lines from the logs:

### RabbitMQ

- `scenario=durable-256` `messages_per_sec=600.04` `confirm_latency_ms=0.64 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=size-64` `messages_per_sec=600.25` `confirm_latency_ms=0.56 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=size-4096` `messages_per_sec=249.07` `confirm_latency_ms=0.83 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=400 kept_up=true` `wall_secs=4.07`
- `scenario=transient-256` `messages_per_sec=1231.20` `confirm_latency_ms=0.30 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.08`
- `scenario=prefetch-1` `messages_per_sec=593.68` `confirm_latency_ms=0.64 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=prefetch-128` `messages_per_sec=590.64` `confirm_latency_ms=0.63 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=fan-2x2` `messages_per_sec=495.20` `confirm_latency_ms=1.09 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=800 kept_up=true` `wall_secs=4.09`

### Rust

- `scenario=durable-256` `messages_per_sec=586.79` `confirm_latency_ms=0.37 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=size-64` `messages_per_sec=583.66` `confirm_latency_ms=0.36 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.08`
- `scenario=size-4096` `messages_per_sec=243.83` `confirm_latency_ms=0.61 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=400 kept_up=true` `wall_secs=4.08`
- `scenario=transient-256` `messages_per_sec=1232.23` `confirm_latency_ms=0.27 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.08`
- `scenario=prefetch-1` `messages_per_sec=596.89` `confirm_latency_ms=0.36 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=prefetch-128` `messages_per_sec=593.79` `confirm_latency_ms=0.33 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=fan-2x2` `messages_per_sec=495.73` `confirm_latency_ms=0.55 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=800 kept_up=true` `wall_secs=4.07`

### Bun

- `scenario=durable-256` `messages_per_sec=593.88` `confirm_latency_ms=0.38 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=size-64` `messages_per_sec=593.86` `confirm_latency_ms=0.34 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=size-4096` `messages_per_sec=247.61` `confirm_latency_ms=0.57 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=400 kept_up=true` `wall_secs=4.07`
- `scenario=transient-256` `messages_per_sec=1231.17` `confirm_latency_ms=0.25 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.08`
- `scenario=prefetch-1` `messages_per_sec=593.83` `confirm_latency_ms=0.37 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=prefetch-128` `messages_per_sec=594.14` `confirm_latency_ms=0.38 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=fan-2x2` `messages_per_sec=495.10` `confirm_latency_ms=0.54 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=800 kept_up=true` `wall_secs=4.07`

## Disk flush beside durable latency

| Broker | Flush path that ran |
| --- | --- |
| RabbitMQ | `classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` |
| Rust | `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` |
| Bun | `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` |

Transient scenarios print `disk_flush=not-durable`. RabbitMQ’s `bench/rabbitmq.conf` sets `classic_queue.default_version=2`. Classic queue v2 flushes its write buffer at least every 200ms and sends the confirm before that fsync. On `durable-256` that confirm is `confirm_latency_ms=0.64`. Rust and Bun also confirm before the interval fsync: `confirm_latency_ms=0.37` and `confirm_latency_ms=0.38`. The 10 ms timer still fsyncs. A process crash inside that window can drop a message whose confirm already returned.

## Recommendation

On this shared-disk run, durable classic confirms are in the same band for all three brokers. RabbitMQ `durable-256` is `messages_per_sec=600.04`, `confirm_latency_ms=0.64`, `saturation_load=1000 kept_up=true`. Rust is `messages_per_sec=586.79`, `confirm_latency_ms=0.37`, `saturation_load=1000 kept_up=true`. Bun is `messages_per_sec=593.88`, `confirm_latency_ms=0.38`, `saturation_load=1000 kept_up=true`. Each kept up at the 200 messages/s step and at the 1000 messages/s step. Median confirm time on QueueForge is lower than RabbitMQ in this `durable-256` row. RabbitMQ acked a few more messages per second on that same row.

Transient publishes kept up at 2000/s on every broker: RabbitMQ `messages_per_sec=1231.20`, Rust `messages_per_sec=1232.23`, Bun `messages_per_sec=1231.17`. Confirm latency is `confirm_latency_ms=0.30`, `confirm_latency_ms=0.27`, and `confirm_latency_ms=0.25`.

`size-4096` kept up at `saturation_load=400 kept_up=true` on all three: RabbitMQ `messages_per_sec=249.07`, Rust `messages_per_sec=243.83`, Bun `messages_per_sec=247.61`. `fan-2x2` kept up at `saturation_load=800 kept_up=true`: RabbitMQ `messages_per_sec=495.20` with `confirm_latency_ms=1.09`, Rust `messages_per_sec=495.73` with `confirm_latency_ms=0.55`, Bun `messages_per_sec=495.10` with `confirm_latency_ms=0.54`. Prefetch does not move the durable ceiling: `prefetch-1` is RabbitMQ `messages_per_sec=593.68`, Rust `messages_per_sec=596.89`, Bun `messages_per_sec=593.83`, and `prefetch-128` is RabbitMQ `messages_per_sec=590.64`, Rust `messages_per_sec=593.79`, Bun `messages_per_sec=594.14`.

For a new single-node classic-queue deployment that can accept a confirm before the interval fsync, either QueueForge broker matches this workload. Rust has the lower `durable-256` confirm time in this run (`confirm_latency_ms=0.37`). Bun is within a few messages per second of RabbitMQ on that row (`messages_per_sec=593.88` beside `messages_per_sec=600.04`). Stay on RabbitMQ 4 when the deployment needs any blocker below.

## Production blockers

These are outside this classic-queue comparison. A production move off RabbitMQ still has to account for them.

| Blocker | Why it blocks a replacement |
| --- | --- |
| Joining an existing RabbitMQ cluster | QueueForge nodes cluster only with the same implementation. They do not join an Erlang RabbitMQ cluster. |
| Mixing Rust and Bun nodes | The two brokers do not replicate to each other. Data directories are not interchangeable. |
| Classic queue mirroring | RabbitMQ 4 rejects `ha-mode` / `ha-params`. QueueForge does not implement mirroring either. Quorum queues are the replicated type, and only inside one implementation. |
| LDAP, OAuth, x509 | Not implemented. Authentication is the local user table. |
| Kubernetes operator and management-compatible automation | QueueForge management uses the `queueforge_session` cookie. RabbitMQ uses HTTP basic auth. Permission URLs are not the same shape. |
| Crash before the interval fsync | A publisher confirm can return before the 10 ms fsync. A crash in that window can drop an acknowledged message. RabbitMQ classic queues have the same window, with a 200 ms flush. |

MQTT, STOMP, AMQP 1.0, streams, federation, and shovel are implemented on both brokers and were not part of this classic-queue run.

## Decision

For this classic-queue run, Rust and Bun both kept up with every labeled durable and transient step. `durable-256` is RabbitMQ `messages_per_sec=600.04` / `confirm_latency_ms=0.64`, Rust `messages_per_sec=586.79` / `confirm_latency_ms=0.37`, Bun `messages_per_sec=593.88` / `confirm_latency_ms=0.38`, each with `saturation_load=1000 kept_up=true`. `transient-256` kept up at `saturation_load=2000 kept_up=true` on all three. Treat a confirm as “buffered, fsync still pending” on every broker here. Move off RabbitMQ only when the deployment does not join an Erlang RabbitMQ cluster, mix Rust with Bun, use classic mirroring, LDAP, OAuth, x509, or the Kubernetes operator.

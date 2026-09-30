# Production benchmark: RabbitMQ 4, Rust QueueForge, Bun QueueForge

One client, `queueforge-compare`, ran seven classic-queue scenarios against each broker. The only change between runs was `AMQP_URL`. Each container had `NanoCpus=1000000000` and `Memory=536870912`. Host ports were 35672 (RabbitMQ `rabbitmq:4.3-management`), 35673 (Rust image `queueforge-rust:bench`, build `rust:1.85-bookworm`, runtime `debian:bookworm-slim`), and 35674 (Bun image `queueforge-bun:bench`, base `oven/bun:1.4.2-alpine`).

Each publisher sends message k at `k / rate` for its share of the labeled rate, then waits for that confirm. Two producers in `fan-2x2` run at the same time, each at half the labeled rate, so the pair offers the full rate. A step keeps up when both confirms and acks reach 95% of the labeled rate inside that step. `messages_per_sec` is acked deliveries over the whole scenario. `confirm_latency_ms` is the median confirm time. `saturation_load` is the first offered rate that missed that bar, or the last rate when every step kept up. `durable-256` offers `200,1000,2000,4000,8000,16000`. `fan-2x2` offers `200,800,1600,3200,6400,12800`. These figures are one run on one shared disk.

## What changed before this run

Both QueueForge brokers still fsync on `fsync_interval_ms=10`. A durable publisher confirm completes after the buffered write, before that fsync. On Rust the interval fsync runs outside the queue-actor command loop, and the actor takes that finished fsync before the next mailbox command, so a full mailbox cannot leave the log parked. A confirm issued while the fsync is still blocked returns without waiting for it. On Bun, `every_n_ms` stages durable rows and writes them in one transaction on the group-commit timer, so the confirm is not one synchronous insert and does not wait for `synchronous=FULL`. A crash before the interval fsync can drop an acknowledged message. A durable publish, confirm, and consume of the same body passed on RabbitMQ 4, Rust, and Bun (`durable_group_commit_matches_rabbitmq`).

## Results

| Scenario | RabbitMQ messages/s | Rust messages/s | Bun messages/s | RabbitMQ confirm ms | Rust confirm ms | Bun confirm ms | RabbitMQ saturation | Rust saturation | Bun saturation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| durable-256 | 1350.86 | 2878.88 | 3219.02 | 0.53 | 0.20 | 0.15 | `saturation_load=2000 kept_up=false` | `saturation_load=8000 kept_up=false` | `saturation_load=8000 kept_up=false` |
| size-64 | 600.15 | 593.77 | 593.86 | 0.61 | 0.43 | 0.34 | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` |
| size-4096 | 247.83 | 247.82 | 247.78 | 0.84 | 0.62 | 0.59 | `saturation_load=400 kept_up=true` | `saturation_load=400 kept_up=true` | `saturation_load=400 kept_up=true` |
| transient-256 | 1226.84 | 1234.40 | 1232.06 | 0.28 | 0.30 | 0.25 | `saturation_load=2000 kept_up=true` | `saturation_load=2000 kept_up=true` | `saturation_load=2000 kept_up=true` |
| prefetch-1 | 594.75 | 596.84 | 593.83 | 0.63 | 0.42 | 0.32 | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` |
| prefetch-128 | 598.73 | 593.84 | 593.73 | 0.60 | 0.44 | 0.37 | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` | `saturation_load=1000 kept_up=true` |
| fan-2x2 | 2063.77 | 3000.39 | 3726.37 | 0.59 | 0.31 | 0.19 | `saturation_load=6400 kept_up=false` | `saturation_load=12800 kept_up=false` | `saturation_load=12800 kept_up=false` |

Exact lines from the logs:

### RabbitMQ

- `scenario=durable-256` `messages_per_sec=1350.86` `confirm_latency_ms=0.53 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=2000 kept_up=false` `wall_secs=12.05`
- `scenario=size-64` `messages_per_sec=600.15` `confirm_latency_ms=0.61 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.05`
- `scenario=size-4096` `messages_per_sec=247.83` `confirm_latency_ms=0.84 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=400 kept_up=true` `wall_secs=4.07`
- `scenario=transient-256` `messages_per_sec=1226.84` `confirm_latency_ms=0.28 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.08`
- `scenario=prefetch-1` `messages_per_sec=594.75` `confirm_latency_ms=0.63 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.08`
- `scenario=prefetch-128` `messages_per_sec=598.73` `confirm_latency_ms=0.60 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.06`
- `scenario=fan-2x2` `messages_per_sec=2063.77` `confirm_latency_ms=0.59 disk_flush=classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` `saturation_load=6400 kept_up=false` `wall_secs=12.05`

### Rust

- `scenario=durable-256` `messages_per_sec=2878.88` `confirm_latency_ms=0.20 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=8000 kept_up=false` `wall_secs=12.12`
- `scenario=size-64` `messages_per_sec=593.77` `confirm_latency_ms=0.43 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=size-4096` `messages_per_sec=247.82` `confirm_latency_ms=0.62 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=400 kept_up=true` `wall_secs=4.07`
- `scenario=transient-256` `messages_per_sec=1234.40` `confirm_latency_ms=0.30 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.06`
- `scenario=prefetch-1` `messages_per_sec=596.84` `confirm_latency_ms=0.42 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=prefetch-128` `messages_per_sec=593.84` `confirm_latency_ms=0.44 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=fan-2x2` `messages_per_sec=3000.39` `confirm_latency_ms=0.31 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=12800 kept_up=false` `wall_secs=12.13`

### Bun

- `scenario=durable-256` `messages_per_sec=3219.02` `confirm_latency_ms=0.15 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=8000 kept_up=false` `wall_secs=12.12`
- `scenario=size-64` `messages_per_sec=593.86` `confirm_latency_ms=0.34 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=size-4096` `messages_per_sec=247.78` `confirm_latency_ms=0.59 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=400 kept_up=true` `wall_secs=4.07`
- `scenario=transient-256` `messages_per_sec=1232.06` `confirm_latency_ms=0.25 disk_flush=not-durable` `saturation_load=2000 kept_up=true` `wall_secs=3.07`
- `scenario=prefetch-1` `messages_per_sec=593.83` `confirm_latency_ms=0.32 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=prefetch-128` `messages_per_sec=593.73` `confirm_latency_ms=0.37 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=1000 kept_up=true` `wall_secs=4.07`
- `scenario=fan-2x2` `messages_per_sec=3726.37` `confirm_latency_ms=0.19 disk_flush=fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` `saturation_load=12800 kept_up=false` `wall_secs=12.11`

## Disk flush beside durable latency

| Broker | Flush path that ran |
| --- | --- |
| RabbitMQ | `classic_queue.default_version=2; write-buffer flush at least every 200ms; publisher confirms before fsync` |
| Rust | `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` |
| Bun | `fsync_policy=every_n_ms fsync_interval_ms=10; group-commit timer, publisher confirm before fsync` |

Transient scenarios print `disk_flush=not-durable`. RabbitMQ classic queue v2 flushes its write buffer at least every 200 ms and sends the confirm before that fsync. On `durable-256` that confirm is `confirm_latency_ms=0.53`. Rust and Bun also confirm before the interval fsync: `confirm_latency_ms=0.20` and `confirm_latency_ms=0.15`. The 10 ms timer still fsyncs. A process crash inside that window can drop a message whose confirm already returned.

## Recommendation

On this shared-disk run, `saturation_load` is the ceiling. RabbitMQ `durable-256` is `messages_per_sec=1350.86`, `confirm_latency_ms=0.53`, `saturation_load=2000 kept_up=false`. Rust is `messages_per_sec=2878.88`, `confirm_latency_ms=0.20`, `saturation_load=8000 kept_up=false`. Bun is `messages_per_sec=3219.02`, `confirm_latency_ms=0.15`, `saturation_load=8000 kept_up=false`. Both QueueForge brokers kept the 2000 messages/s step that RabbitMQ missed, and both missed at 8000. `fan-2x2` is RabbitMQ `messages_per_sec=2063.77`, `confirm_latency_ms=0.59`, `saturation_load=6400 kept_up=false`; Rust `messages_per_sec=3000.39`, `confirm_latency_ms=0.31`, `saturation_load=12800 kept_up=false`; Bun `messages_per_sec=3726.37`, `confirm_latency_ms=0.19`, `saturation_load=12800 kept_up=false`.

`transient-256` kept up at 2000/s on every broker: RabbitMQ `messages_per_sec=1226.84`, Rust `messages_per_sec=1234.40`, Bun `messages_per_sec=1232.06`, each `saturation_load=2000 kept_up=true`. Confirm latency is `confirm_latency_ms=0.28`, `confirm_latency_ms=0.30`, and `confirm_latency_ms=0.25`.

`size-4096` kept up at `saturation_load=400 kept_up=true` on all three: RabbitMQ `messages_per_sec=247.83`, Rust `messages_per_sec=247.82`, Bun `messages_per_sec=247.78`. The 1000 messages/s durable steps that were not raised (`size-64`, `prefetch-1`, `prefetch-128`) still kept up on all three.

For a new single-node classic-queue deployment that can accept a confirm before the interval fsync, either QueueForge broker carried a higher durable ceiling than RabbitMQ in this run. Bun had the higher `fan-2x2` rate (`messages_per_sec=3726.37`) and Rust cleared the same 8000 and 12800 saturation steps (`confirm_latency_ms=0.20` on `durable-256`). Stay on RabbitMQ 4 when the deployment needs any blocker below.

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

For this classic-queue run, both QueueForge brokers printed a higher `saturation_load` than RabbitMQ on `durable-256` (`saturation_load=8000 kept_up=false` beside RabbitMQ `saturation_load=2000 kept_up=false`) and on `fan-2x2` (`saturation_load=12800 kept_up=false` beside RabbitMQ `saturation_load=6400 kept_up=false`). `transient-256` kept up at `saturation_load=2000 kept_up=true` on Rust and Bun. Treat a confirm as “buffered, fsync still pending” on every broker here. Move off RabbitMQ only when the deployment does not join an Erlang RabbitMQ cluster, mix Rust with Bun, use classic mirroring, LDAP, OAuth, x509, or the Kubernetes operator.

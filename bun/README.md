# QueueForge (Bun)

The Bun broker lives in this directory. It speaks the same AMQP 0-9-1 surface as the [Rust broker](../rust/README.md). Run `bun` commands from here.

Messages and definitions are stored in SQLite at `<data dir>/bun.sqlite`. A Bun process reads and writes only that data directory. It can share a cluster member list with Rust processes, and both sides speak cluster protocol version 1.

## Quick start

```bash
bun install
bun test
bun run start -- --config config.example.toml --dev-bootstrap
```

AMQP listens on `0.0.0.0:5672`, management on `0.0.0.0:15672`, and metrics on `127.0.0.1:15692` unless `config.example.toml` says otherwise. `--dev-bootstrap` creates `admin` / `devpassword12` when the user table is empty.

When a Rust broker is also running, give this process different AMQP, management, metrics, and cluster ports, and a different `[data].dir`.

## Health and management

| Path | Meaning |
|------|---------|
| `GET /healthz` | Liveness. Body `ok`. |
| `GET /readyz` | Readiness. Body `ready`, or `not ready` with status 503. |
| `GET /metrics` | Prometheus text on the metrics listener. |
| `POST /api/login` | Sets the `queueforge_session` cookie (`HttpOnly; SameSite=Lax`). |
| `GET /api/overview` | Broker snapshot, after login. |
| `GET /api/queues/:vhost` | Queues. The vhost `/` is the path segment `%2F`. |
| `PUT /api/permissions/:user/:vhost` | Grant permissions for that user on that vhost. |

The management listener also serves the shared SPA from `rust/ui/dist` when that build is present. The Rust README has the longer management table.

`basic.qos` prefetch count 0 is unlimited consumer credit.

## Durability

`[data].fsync_policy` accepts `never`, `every_n_ms`, and `always`. The example uses `every_n_ms` with `fsync_interval_ms` of 100.

`every_n_ms` stages durable rows and returns the publisher confirm after the timer transaction runs `PRAGMA synchronous=FULL`. `always` and `every_n_messages` also wait for that flush. A confirmed durable publish is in the SQLite file.

## Cluster

Leave `[cluster].members` empty for a single node. Otherwise every process uses the same static member list and its own `node_id` and `listen` address.

Classic queues have one home node. Peers forward operations there.

A durable quorum queue (`x-queue-type` = `quorum`, durable, non-exclusive) confirms a persistent publish after a majority of the members have fsynced the body into their own store. This process records the body and waits for the flush before it acks the peer. Rust members of the same list fsync their write-ahead log before the copy counts. Once a majority is reachable, the live leader is the lowest member id among the reachable members. Publishing to any member is enough. `POST /api/nodes` adds a member and `DELETE /api/nodes/{id}` removes one that homes no classic queue. The [repository README](../README.md) shows a three-member list.

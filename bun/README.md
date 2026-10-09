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

`every_n_ms` stages durable rows and returns the publisher confirm after the fsync of the message log (`<db>.dlog`) that covers them. SQLite catches up afterwards and is not on the confirm path. The fsync runs when a read has finished and at most 16 confirms are waiting, when 128 are waiting, or on the `fsync_interval_ms` timer, whichever comes first. So a few channels with one confirm each do not wait for the timer, and a deep window still shares one fsync. `always` and `every_n_messages` also wait for their flush. A confirmed durable publish survives `kill -9`.

## Cluster

Leave `[cluster].members` empty for a single node. Otherwise every process uses the same static member list and its own `node_id` and `listen` address.

Classic queues have one home node, the shared home hash of [docs/raft.md](../docs/raft.md) section 9, or the member `x-queue-leader-locator` names: `client-local` is the node the client is connected to, `balanced` the member homing the fewest queues. Peers forward operations there.

Raft ([docs/raft.md](../docs/raft.md)) is a feature flag. A cluster created by this build turns it on by itself once every member supports it. After an upgrade from a build without it, enable it with `PUT /api/feature-flags/raft/enable` once the last node runs the new build; until then the cluster keeps the behaviour below.

With Raft on, every quorum queue and stream has its own Raft group, on Bun and Rust members alike, as RabbitMQ runs one Ra cluster per queue. A publish is confirmed once it commits on a majority. Each queue elects its own leader, so losing a member fails over only the queues it led. A message delivered and not yet acked when its leader dies is delivered again by the next one. A stream is copied to every member with the same offsets, and a consumer reads on whichever member it is connected to.

Without Raft, a durable quorum queue (`x-queue-type` = `quorum`, durable, non-exclusive) confirms a persistent publish after a majority of the members have fsynced the body into their own store. This process records the body and waits for the flush before it acks the peer. Rust members of the same list fsync their write-ahead log before the copy counts. Once a majority is reachable, the live leader is the lowest member id among the reachable members. Publishing to any member is enough.

`POST /api/nodes` adds a member and `DELETE /api/nodes/{id}` removes one that homes no classic queue. With Raft on, the change commits through the metadata log and needs a majority; without one it is refused with 503. The [repository README](../README.md) shows a three-member list.

## Cores and TLS

On a host whose cgroup grants more than one core (or with `QUEUEFORGE_CORES` set), a parent process accepts AMQP and AMQPS sockets and hands each one to a child per core, unread; a child runs the TLS handshake itself. `[listeners]` also takes `mqtts`, `stomps` and `stream_tls`, which use the `[tls]` certificate.

## Stored settings

User and vhost limits, topic permissions, runtime parameters (`/api/parameters`) and global parameters (`/api/global-parameters`, including `cluster_name`) are stored in SQLite, replicated to the other members, and included in the definitions export and import.

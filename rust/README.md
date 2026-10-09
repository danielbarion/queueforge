# QueueForge (Rust)

The Rust broker lives in this directory. The repository root is not a Cargo workspace. Run every `cargo` and `./target/...` command from here. Throughput numbers are measurements, not promises — see [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md).

Single-node message queue broker written in Rust, inspired by RabbitMQ’s conceptual model (exchanges, queues, bindings, acknowledgements, durability).

## Status

**Single-node v0.1 feature set is implemented** in this monorepo: full AMQP 0-9-1 pub/sub (direct / fanout / topic / headers + default exchange), routing and bindings, durable queues with WAL recovery, publisher confirms, priority queues, TTL/DLX and length policies, management HTTP CRUD + embedded React SPA, rustls TLS (AMQPS/HTTPS), Prometheus metrics, and `queueforge-bench`. See [`docs/RELEASE_NOTES.md`](docs/RELEASE_NOTES.md) for the integrated milestone summary.

A config with no `[cluster].members` stays a single node. With members set, every process lists the same static membership and keeps its own data directory (redb metadata plus the write-ahead log). Classic queues have one home node, a stable hash of the vhost and name. Peers replicate topology and credentials and forward queue operations to that home, so a classic queue is available while its home is up.

With Raft on (the `raft` feature flag, see [`docs/raft.md`](../docs/raft.md)), metadata and membership commit through a replicated log, and every quorum queue and stream has its own Raft group, as RabbitMQ runs one Ra cluster per queue. A publish is confirmed once it commits on a majority. Each queue elects its own leader (`x-queue-leader-locator` picks the first one), so losing a member fails over only the queues it led. A message delivered and not yet acked when its leader dies is delivered again by the next one. A stream is copied to every member with the same offsets, and a consumer reads on whichever member it is connected to. A membership change without a majority is refused with `503`. Bun nodes can sit in the same member list and the same groups.

Without Raft, a durable quorum queue (`x-queue-type` = `quorum`) confirms a persistent publish after a majority of the members hold the body in memory. Each member appends that body to its own write-ahead log before it acks the peer. The client can publish to any member. Once a majority is reachable, the live leader is the lowest member id among the reachable members. See the [repository README](../README.md) for a member-list example.

`[listeners]` also takes `mqtts`, `stomps` and `stream_tls`, which use the `[tls]` certificate. User and vhost limits, topic permissions, runtime parameters (`/api/parameters`) and global parameters (`/api/global-parameters`) are stored in the metadata store, replicated to the other members, and included in the definitions export and import. With tracing on for a vhost, publishes and deliveries are copied to `amq.rabbitmq.trace`.

`fsync_policy = "every_n_ms"` completes a durable confirm when the group-commit fsync covers that append. The timer runs on `fsync_interval_ms` (100 in the example config). `always` and `every_n_messages` also wait for the fsync before the confirm.

For local development and PR workflow, see [`CONTRIBUTING.md`](CONTRIBUTING.md). For backup, restore, sessions, and production hardening, see [`docs/OPERATIONS.md`](docs/OPERATIONS.md). Historical architecture notes live in [`DESIGN-queueforge.md`](DESIGN-queueforge.md).

## Quick start

```bash
# Build
cargo build --release -p queueforge-broker

# Run with example config (Ctrl-C / SIGTERM to exit)
./target/release/queueforge --config configs/queueforge.example.toml --dev-bootstrap
```

Open the management UI at **http://127.0.0.1:15672/** (bootstrap user `admin` /
`devpassword12` with `--dev-bootstrap`).

### Management HTTP

Default bind: **`0.0.0.0:15672`** (`[listeners].management`). With TLS enabled,
use HTTPS (conventional port **15671**).

| Path | Meaning |
|------|---------|
| `GET /` | Embedded React SPA (login + overview) |
| `POST /api/login` | Session cookie (`HttpOnly; SameSite=Lax`; `Secure` only with TLS) |
| `POST /api/logout` | Invalidate session |
| `GET /api/whoami` | Current user |
| `GET /api/overview` | Broker snapshot |
| `GET /api/vhosts` | Paginated vhosts |
| `GET /api/queues/%2F` | Paginated queues (vhost `/` URL-encoded) |
| `GET /api/exchanges/%2F` | Paginated exchanges |
| `GET /api/connections` | Live AMQP connections |
| `DELETE /api/connections/{id}` | Force-close a live AMQP connection |
| `PUT/DELETE /api/queues/%2F/{name}` | Declare / delete queue (optional `arguments` x-args) |
| `PUT/DELETE /api/exchanges/%2F/{name}` | Declare / delete exchange |
| `POST /api/bindings/%2F` | Create binding |
| `POST .../publish` / `.../get` | Test publish / get |
| `GET/POST /api/definitions` | Topology export / import |
| `GET/PUT/DELETE /api/users` | User admin (administrator) |
| `GET /healthz` / `GET /readyz` | Liveness / readiness, unauthenticated |

List endpoints accept `?page_size=&cursor=&name_prefix=` (max `page_size` 500).
`basic.qos` prefetch 0 is unlimited, matching RabbitMQ. Backup is a
stopped copy of the data directory; see [`docs/OPERATIONS.md`](docs/OPERATIONS.md).

### UI development (Vite proxy)

The SPA source lives in [`ui/`](ui/). Production assets under `ui/dist` are
embedded into `queueforge-mgmt` with **rust-embed** at compile time.

```bash
# Terminal 1 — broker (management API on :15672)
cargo run -p queueforge-broker -- --config configs/queueforge.example.toml --dev-bootstrap

# Terminal 2 — hot-reload UI with API proxy
cd ui
npm install
npm run dev
# open http://127.0.0.1:5173
```

Vite proxies `/api`, `/healthz`, and `/readyz` to `http://127.0.0.1:15672`
(see `ui/vite.config.ts`). Session cookies stay same-origin on the Vite host.

After changing the frontend, rebuild assets before a release cargo build:

```bash
cd ui && npm ci && npm run build
# then: cargo build -p queueforge-broker
```

`ui/dist` is committed so CI and environments without Node can still compile.

### TLS (AMQPS + HTTPS)

Optional; **off by default** for local development.

```toml
[tls]
enabled = true
cert_path = "/etc/queueforge/tls/server.crt"
key_path = "/etc/queueforge/tls/server.key"
```

When enabled, both the AMQP listener (AMQPS) and management API (HTTPS) terminate
TLS with **rustls**. Metrics stay plain HTTP on loopback. Full production
checklist: **[docs/PRODUCTION_TLS.md](docs/PRODUCTION_TLS.md)**.

Env overrides: `QUEUEFORGE_TLS_ENABLED`, `QUEUEFORGE_TLS_CERT`, `QUEUEFORGE_TLS_KEY`.

### Metrics and health

Default bind: **`127.0.0.1:15692`** (`[listeners].metrics`).

| Path | Meaning |
|------|---------|
| `GET /metrics` | Prometheus text scrape (`queueforge_*` / `queueforge_process_*`) |
| `GET /healthz` | Liveness — **200** while the process is up, no session cookie |
| `GET /readyz` | Readiness — **503** until recovery marks ready; **200** when ready |

Health routes are also available on the management listener. Metrics default to
**localhost**; rebinding exposes unauthenticated `/metrics` — protect with
network policy.

### CLI

| Flag / env | Description |
|------------|-------------|
| `--config` / `QUEUEFORGE_CONFIG` | Path to TOML config |
| `--dev-bootstrap` | Local admin (`admin` / `devpassword12`) when user table empty |
| `--version` | Print version |

Other env overrides: `QUEUEFORGE_DATA_DIR`, `QUEUEFORGE_LOG`, `QUEUEFORGE_AMQP_ADDR`,
`QUEUEFORGE_MGMT_ADDR`, `QUEUEFORGE_TLS_ENABLED`, `QUEUEFORGE_TLS_CERT`,
`QUEUEFORGE_TLS_KEY`.

## Workspace

| Crate | Role |
|-------|------|
| `queueforge-broker` | Binary (`queueforge`) |
| `queueforge-core` | Shared config / domain types / queue runtime |
| `queueforge-amqp` | AMQP framing and methods |
| `queueforge-auth` | Password-hash users and permissions |
| `queueforge-store` | redb metadata |
| `queueforge-metrics` | Prometheus registry + health HTTP |
| `queueforge-mgmt` | Management HTTP API (sessions, CRUD, definitions) + embedded SPA (`ui/dist`) |
| `queueforge-bench` | Client load harness (shapes A–F) |

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

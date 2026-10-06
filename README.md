# QueueForge

Three AMQP 0-9-1 brokers live in this folder: Rust, Bun, and PHP. All three can be members of one cluster.

| Tree | What it is |
|------|------------|
| [`rust/`](rust/) | The Rust broker (Tokio, redb metadata, write-ahead log, management HTTP, SPA, bench). The repository root is not a Cargo workspace. |
| [`bun/`](bun/) | The Bun broker. Messages live in SQLite. Management HTTP is Elysia. AMQP is a Bun TCP listener. |
| [`php/`](php/) | The PHP broker. One process, one `stream_select()` loop, no Composer and no dependencies beyond `ext-sockets`. Messages live in an append-only log. Classic and quorum queues, policies, management HTTP, Prometheus, TLS, MQTT, STOMP, streams, and an AMQP 1.0 shim. A cluster member. Client-visible behaviour matches Rust and Bun; see [`php/README.md`](php/README.md) for where it deliberately does not. |

Build and test the Rust broker from its subdirectory:

```bash
cd rust
cargo test --workspace
cargo build --release -p queueforge-broker
./target/release/queueforge --config configs/queueforge.example.toml --dev-bootstrap
```

Run the Bun broker from its subdirectory:

```bash
cd bun
bun install
bun run start -- --config config.example.toml --dev-bootstrap
```

Run the PHP broker from its subdirectory. It has no install step:

```bash
cd php
php bin/queueforge --config config.example.toml --dev-bootstrap
```

Rust and Bun listen on AMQP `127.0.0.1:5672` and management `127.0.0.1:15672` unless the config says otherwise; the PHP example config uses `127.0.0.1:5675` so it can run alongside them. Bootstrap user with `--dev-bootstrap` is `admin` / `devpassword12`. Give every process its own AMQP, management, metrics, and cluster ports, and its own data directory.

`GET /healthz` returns `ok`. `GET /readyz` returns `ready` once the process can serve traffic. Management login sets the `queueforge_session` cookie. See [`rust/README.md`](rust/README.md), [`bun/README.md`](bun/README.md), and [`php/README.md`](php/README.md).

Tests: `cargo test --workspace` in `rust/`, `bun test` in `bun/`, and for PHP a container run with the test directory mounted, so the host needs no PHP:

```bash
docker compose -f docker-compose.bench.yml build php
docker compose -f docker-compose.bench.yml run --rm --no-deps \
  --entrypoint php -v ./php/test:/opt/queueforge/test \
  php test/run.php
```

## Cluster

Leave `[cluster].members` empty for a single node. For several processes, put the same member list on every node, including itself. Membership is that static list.

Classic queues have one home node, chosen by a hash of the vhost and queue name. Peers forward operations there, and the messages stay in that node's local engine. Rust hashes bytes with a 64-bit FNV; Bun and PHP use a 32-bit FNV-1a, so a mixed list does not agree on classic homes. Quorum queues are unaffected, being homed where they are declared.

A durable quorum queue (`x-queue-type` = `quorum`, durable, non-exclusive) confirms a persistent publish after a majority of the members have the body in their own durable store and that copy has been fsynced. The client can publish to whichever member is up. Each member writes the body into its own engine: the Rust write-ahead log, Bun SQLite, or the PHP append-only log. A node keeps its own data directory. All three encode and decode cluster protocol version 1, so a member list can mix the binaries. Once a majority is reachable, the live leader is the lowest member id among those peers.

With `fsync_policy = "every_n_ms"` a classic durable confirm returns when the interval fsync covers that append (`fsync_interval_ms` in the example configs is 100). `always` and `every_n_messages` also wait for the fsync. A confirmed durable publish is on disk.

`POST /api/nodes` with `{ "id", "addr" }` adds a member. `DELETE /api/nodes/{id}` removes one that is not the stored home of a classic queue. The list is written to `members.json` in the data directory and broadcast to peers. Existing classic queues keep the home stored at declare time.

```toml
[cluster]
node_id = "a" # "b" and "c" on the other processes
listen = "127.0.0.1:25672"
members = [
  { id = "a", addr = "127.0.0.1:25672" },
  { id = "b", addr = "127.0.0.1:25673" },
  { id = "c", addr = "127.0.0.1:25674" },
]
```

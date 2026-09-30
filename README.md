# QueueForge

Two AMQP 0-9-1 brokers with the same client-visible behavior live in this folder. A Rust process and a Bun process can also be members of one cluster.

| Tree | What it is |
|------|------------|
| [`rust/`](rust/) | The Rust broker (Tokio, redb metadata, write-ahead log, management HTTP, SPA, bench). The repository root is not a Cargo workspace. |
| [`bun/`](bun/) | The Bun broker. Messages live in SQLite. Management HTTP is Elysia. AMQP is a Bun TCP listener. |

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
bun test
bun run start -- --config config.example.toml --dev-bootstrap
```

Both listen on AMQP `127.0.0.1:5672` and management `127.0.0.1:15672` unless the config says otherwise. Bootstrap user with `--dev-bootstrap` is `admin` / `devpassword12`. Give every process its own AMQP, management, metrics, and cluster ports, and its own data directory.

`GET /healthz` returns `ok`. `GET /readyz` returns `ready` once the process can serve traffic. Management login sets the `queueforge_session` cookie. See [`rust/README.md`](rust/README.md) and [`bun/README.md`](bun/README.md).

## Cluster

Leave `[cluster].members` empty for a single node. For several processes, put the same member list on every node, including itself. Membership is that static list.

Classic queues have one home node, chosen by a hash of the vhost and queue name. Peers forward operations there, and the messages stay in that node's local engine.

A durable quorum queue (`x-queue-type` = `quorum`, durable, non-exclusive) confirms a persistent publish after a majority of the members hold the body in memory. The client can publish to whichever member is up. Each member writes the body into its own engine: the Rust write-ahead log, or Bun SQLite. A node keeps its own data directory. Rust and Bun encode and decode cluster protocol version 1, so a member list can mix both binaries. Once a majority is reachable, the live leader is the lowest member id among those peers.

With `fsync_policy = "every_n_ms"` the confirm returns after the buffered write. The interval fsync runs on a timer (`fsync_interval_ms` in the example configs is 100). `fsync_policy = "always"` waits for that fsync before the confirm.

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

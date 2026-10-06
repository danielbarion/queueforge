# QueueForge

Two AMQP 0-9-1 brokers, and a PHP process for the classic durable path, live in this folder. A Rust process and a Bun process can also be members of one cluster. The PHP process cannot.

| Tree | What it is |
|------|------------|
| [`rust/`](rust/) | The Rust broker (Tokio, redb metadata, write-ahead log, management HTTP, SPA, bench). The repository root is not a Cargo workspace. |
| [`bun/`](bun/) | The Bun broker. Messages live in SQLite. Management HTTP is Elysia. AMQP is a Bun TCP listener. |
| [`php/`](php/) | PHP broker for the single-node classic path: default, direct, fanout, and topic exchanges, bindings, confirms after `fsync`, manual ack, and `basic.nack` requeue. Not a cluster member. No quorum, management UI, or TLS. |

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

A durable quorum queue (`x-queue-type` = `quorum`, durable, non-exclusive) confirms a persistent publish after a majority of the members have the body in their own durable store and that copy has been fsynced. The client can publish to whichever member is up. Each member writes the body into its own engine: the Rust write-ahead log, or Bun SQLite. A node keeps its own data directory. Rust and Bun encode and decode cluster protocol version 1, so a member list can mix both binaries. Once a majority is reachable, the live leader is the lowest member id among those peers.

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

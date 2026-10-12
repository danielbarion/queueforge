# QueueForge

Three AMQP 0-9-1 brokers live in this folder: Rust, Bun, and PHP. All three can be members of one cluster.

| Tree | What it is |
|------|------------|
| [`rust/`](rust/) | The Rust broker (Tokio, redb metadata, write-ahead log, management HTTP, SPA, bench). The repository root is not a Cargo workspace. |
| [`bun/`](bun/) | The Bun broker. Messages live in SQLite. Management HTTP is Elysia. AMQP is a Bun TCP listener. |
| [`php/`](php/) | The PHP broker. One process, one event loop (epoll through the optional `ev` extension, which the Docker image installs, else `stream_select()`), no Composer, and nothing required beyond `ext-sockets`. Messages live in an append-only log. Classic and quorum queues, policies, management HTTP, Prometheus, TLS, MQTT, STOMP, streams, and an AMQP 1.0 shim. A cluster member. Client-visible behaviour matches Rust and Bun; see [`php/README.md`](php/README.md) for where it deliberately does not. |

Results against RabbitMQ are in [`BENCHMARK.md`](BENCHMARK.md), produced by the harness in [`bench/`](bench/). Open problems those runs found, with evidence and where to look, are in [`PENDING.md`](PENDING.md).

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

CI runs with Woodpecker; see [setup, checks, and GitHub CI rollback](docs/ci.md).

## Running the benchmarks

Every benchmark runs in Docker on this machine, one broker at a time, with
RabbitMQ 4.3 as the baseline and RabbitMQ's PerfTest as the client. One command
builds the images, runs everything and regenerates the tables:

```bash
python3 bench/run.py
```

Requirements: Docker Desktop running (the script resizes it to 8 CPUs / 24 GiB
for the big steps and puts it back to 2 CPUs / 8 GiB at the end, even after a
failure), Python 3, and a Rust toolchain for the paced step. A full run takes
several hours. Close other heavy work, since CPU contention skews the numbers.
On a Mac the script runs under `caffeinate` so the machine does not sleep.

Options:

```bash
python3 bench/run.py --kinds bun,php              # only these brokers; their rows replace older ones
python3 bench/run.py --steps build,scenarios,report
python3 bench/run.py --repeat 3                   # repeats per scenario cell (default 2)
python3 bench/run.py --out /tmp/qf-ab/try         # keep an experiment out of bench/results
```

Steps, in order: `build`, `scenarios`, `load`, `connections`, `paced`, `report`.
A failing step is recorded and the others still run.

What it writes:

- `bench/results/<UTC date>/`: raw cells, `bench.log` and `report.md`.
- `site/app/bench-data.ts`: the site's numbers, regenerated from those results.
- `bench/last-run.json`: progress and outcome, rewritten after every step.
  `status` is `running`, then `done`, `done-with-errors` or `interrupted`. It
  also lists each step's status and duration, the results directory, the image
  ids, and the commit the run measured.

`BENCHMARK.md` is written by hand around `report.md`; update it from the new
report. Only one benchmark may use the machine at a time: the script holds
`/tmp/queueforge-benchmark.lock` and refuses to start while it exists. Rules
for A/B comparisons are in [`PENDING.md`](PENDING.md) ("Measurement rules"),
and the harness details are in [`docs/benchmark-repeats.md`](docs/benchmark-repeats.md).

## Cluster

Leave `[cluster].members` empty for a single node. For several processes, put the same member list on every node, including itself. Membership is that static list.

Classic queues have one home node, chosen by a hash of the vhost and queue name that every implementation computes the same way ([docs/raft.md](docs/raft.md), section 9), or by `x-queue-leader-locator` (`client-local` or `balanced`) on Rust and Bun. Peers forward operations there, and the messages stay in that node's local engine. The home is stored with the queue. Quorum queues are homed where they are declared.

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

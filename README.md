# QueueForge

Two brokers with the same client-visible behavior live in this folder.

| Tree | What it is |
|------|------------|
| [`rust/`](rust/) | The Rust AMQP 0-9-1 broker (Tokio, redb, management HTTP, SPA, bench). This is no longer the Cargo root of the repository. |
| [`bun/`](bun/) | The same broker on Bun. Management HTTP is Elysia. AMQP is a Bun TCP listener. |

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

Both listen on AMQP `127.0.0.1:5672` and management `127.0.0.1:15672` unless the config says otherwise. Bootstrap user with `--dev-bootstrap` is `admin` / `devpassword12`. Give them different ports and data directories when both run at once.

`GET /healthz` is liveness. `GET /readyz` is readiness. See [`rust/README.md`](rust/README.md) for the management HTTP contract.

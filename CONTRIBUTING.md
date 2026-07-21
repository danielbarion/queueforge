# Contributing to QueueForge

Thanks for contributing. This document covers the local workflow expected for
pull requests.

## Prerequisites

- Rust stable (see `rust-version` in the workspace `Cargo.toml`; CI uses stable)
- `rustfmt` and `clippy` components: `rustup component add rustfmt clippy`
- Node.js 20+ and npm (only when changing the management SPA under `ui/`)

## Build

```bash
cargo build --workspace
# release broker binary:
cargo build --release -p queueforge-broker
```

## Format and lint

```bash
cargo fmt --all
cargo fmt --all -- --check   # CI mode
cargo clippy --workspace --all-targets -- -D warnings
```

## Tests

```bash
cargo test --workspace
```

Integration tests live under `crates/queueforge-broker/tests/` (connect, durable
restart, graceful shutdown, TLS smoke, feature matrix, etc.). They start an
in-process broker stack and do not require a separately running process.

## Management UI

Production assets under `ui/dist` are **committed** and embedded into
`queueforge-mgmt` via rust-embed. After any change under `ui/src` (or related
config):

```bash
cd ui
npm ci
npm run build
```

Commit the updated `ui/dist` together with your source changes. CI rebuilds the
UI and fails if `ui/dist` is out of date (`git diff --exit-code ui/dist`).

For hot-reload development:

```bash
# terminal 1
cargo run -p queueforge-broker -- --config configs/queueforge.example.toml --dev-bootstrap

# terminal 2
cd ui && npm ci && npm run dev
```

## Running the broker locally

```bash
cargo run -p queueforge-broker -- \
  --config configs/queueforge.example.toml \
  --dev-bootstrap
```

- Management UI / API: `http://127.0.0.1:15672/`
- AMQP: `amqp://admin:devpassword12@127.0.0.1:5672/%2f`
- `--dev-bootstrap` seeds `admin` / `devpassword12` only when the user table is
  empty. **Do not use in production** — set `QUEUEFORGE_ADMIN_USER` and
  `QUEUEFORGE_ADMIN_PASSWORD` instead (see `docs/OPERATIONS.md`).

## Optional checks

```bash
# Dependency advisories (also run in CI)
cargo install cargo-audit --locked
cargo audit

# Python client smoke (requires a running broker on :5672)
pip install pika
python3 scripts/pika_smoke.py
# or: scripts/run_pika_smoke.sh

# Lightweight frame-decode fuzz loop (no nightly / libFuzzer required)
scripts/fuzz-frame-decode.sh 10
```

Full `cargo fuzz` targets under `crates/queueforge-amqp/fuzz` need a nightly
toolchain and `cargo-fuzz`; see comments in that directory.

## Pull request expectations

Before opening a PR:

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test --workspace`
4. If you touched `ui/`, rebuild and commit `ui/dist` as above

Keep changes focused. Do not commit secrets, local data directories, or editor
junk (`.DS_Store`, etc.).

# QueueForge release notes

## Integrated milestone (queueforge-integrated)

This tree merges the completed feature PRs into a single building, tested broker.

### Included features

| Area | Highlights |
|------|------------|
| Core broker | AMQP 0-9-1 connect, channels, publish/consume, ack/nack, QoS prefetch |
| Routing | Direct / fanout / topic exchanges, bindings, default exchange |
| Persistence | Segmented WAL, group commit, recovery, durable queues |
| Confirms | `confirm.select` → publisher confirms mapped to durable enqueue |
| Policies | TTL, DLX, max-length / max-length-bytes, overflow, **priority queues** |
| Resources | Memory watermarks, disk free budget, max connections / message size |
| Shutdown | SIGTERM drain: readyz 503 → connection.close → queue Shutdown/fsync |
| Management | Session auth, resource CRUD, definitions import/export, publish/get |
| UI | Embedded React SPA: overview, queues (with x-args), exchanges, bindings, users, connections (force-close), definitions import/export, publish/get |
| TLS | rustls AMQPS + HTTPS management (`docs/PRODUCTION_TLS.md`) |
| Bench | `queueforge-bench` shapes A–E + `docs/PERFORMANCE.md` |
| Ops | Dockerfile, docker-compose, SECURITY.md |

### Behavior updates

- Durable fsync and metadata transactions run on Tokio's blocking pool. `fsync_policy = "always"` still waits for that queue's own fsync; it no longer pins a runtime worker.
- Client AMQP application headers round-trip, including across a durable restart. Broker `x-death` keys replace any client header of the same name.
- `channel.flow active=false` holds further publishes on that channel until `active=true`. Heartbeats, `channel.flow`, and close methods still run.
- `basic.qos` `prefetch_count = 0` is unlimited. `global=false` applies per consumer. `global=true` is shared by the consumers on that channel.
- Auto-ack messages stay unacked until the deliver frames are written. A crash before that write requeues them.
- Message TTL deadlines are unix timestamps in the WAL. Recovery keeps the remaining time. Records written before this field still get a fresh TTL.
- Management sessions reload user tags on each request. A deleted or demoted user loses the old privilege immediately.
- `[management] trusted_proxy_cidrs` opts in to using the left-most `X-Forwarded-For` hop for login rate limits. Leave it empty unless the TCP peer is a proxy you run.

### Breaking / operational notes

- Default listeners remain AMQP `5672`, management `15672`, metrics `15692`
  (see `configs/queueforge.example.toml`).
- TLS is **off** by default; enable `[tls]` for production.
- Graceful shutdown may exit non-zero if connection drain times out or a durable
  queue fsync fails — treat as unclean shutdown and inspect logs.
- Management SPA assets are embedded from `ui/dist`; rebuild the UI after UI
  source changes (`cd ui && npm ci && npm run build`).

### Ops / quality

- **CI** — `cargo fmt`, `clippy -D warnings`, `cargo test`, `cargo audit`, UI
  rebuild + `git diff --exit-code ui/dist`, optional pika AMQP smoke.
- **Ops docs** — [`OPERATIONS.md`](OPERATIONS.md) (backup/restore, in-memory
  management sessions, production checklist, graceful shutdown).
- **Security** — report via GitHub Security Advisories ([`SECURITY.md`](../SECURITY.md)).
- **Performance** — bench numbers in [`PERFORMANCE.md`](PERFORMANCE.md) remain
  unvalidated hypotheses; record runs under `docs/bench-results/` when available.
- **Connections** — `DELETE /api/connections/{id}` force-closes a live AMQP
  connection (`CONNECTION_FORCED`); process drain still sends `connection.close`
  then fsyncs durable queues. Queue declare via management accepts closed-set
  `arguments` (TTL, max-length, DLX, priority, …).
- **Containers** — production image `CMD` does not use `--dev-bootstrap`; compose
  supplies admin via env and does not publish unauthenticated metrics port 15692.

### Verification

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

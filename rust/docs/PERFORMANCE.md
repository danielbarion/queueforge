# QueueForge performance targets

**Status:** **Unvalidated hypotheses only.** Numbers in this document are design
targets to explore with `queueforge-bench`. They are **not** guarantees, product
promises, or contractual SLOs until measured on the reference hardware below,
reviewed, and this document is explicitly revised.

When you have real runs, record them under **`docs/bench-results/`** (one file
or subdirectory per host/commit is fine) and link or summarize them in the
revision log below. Do not treat unmeasured table rows as shipping criteria.

## Reference hardware

| Item | Spec |
|------|------|
| CPU | 8 vCPU |
| RAM | 32 GB |
| Storage | NVMe |
| OS | Linux x86_64 |
| Network | 1 Gbps+ (co-located client preferred for latency SLOs) |
| Fsync | group-commit ~100 ms (`fsync_policy = "every_n_ms"`, `fsync_interval_ms = 100`) |
| Payload | 1 KiB body (bench default) |
| TLS | Off unless noted |

Record the actual host model, kernel, and `queueforge` commit SHA next to every
result set.

## Load shapes

| Shape | Description |
|-------|-------------|
| **A** | 1 producer, 1 consumer, 1 queue (baseline transient) |
| **B** | 32 producers, 32 consumers, 1 queue (fan-in/out actor stress) |
| **C** | 1 producer, fanout exchange 1→10 queues, 1 consumer each |
| **D** | Durable queue + `delivery_mode=2` + `confirm.select`, topology of A |
| **E** | Management `GET /api/queues/{vhost}` list/poll concurrent with shape B |
| **F** *(optional)* | Priority mix 20% `priority=9` / 80% `priority=0` on `x-max-priority=9` queue |

## Hypothesis targets (not SLOs)

These rows are **aspirational** until a measured result set exists under
`docs/bench-results/`. [`2026-09-28-local-smoke.md`](bench-results/2026-09-28-local-smoke.md)
is a 5-second smoke on a developer Mac where every shape published and
consumed. It is not this table.

| Metric | Kind | Hypothesis | Shape |
|--------|------|------------|-------|
| Transient throughput | Client E2E ingress | ≥ **200k** msg/s *(stretch)* | A |
| Transient throughput multi-conn | Client E2E ingress | ≥ **100k** msg/s | B |
| Persistent + confirms | Client E2E ingress | ≥ **50k** msg/s | D |
| Fanout ingress | Client E2E publish rate | ≥ **50k** msg/s | C |
| Broker-internal route+enqueue | Internal histogram | p99 ≤ **200 µs** transient | A |
| Client E2E publish→consume latency | Client E2E | p99 ≤ **1 ms** co-located transient | A |
| Durable confirm latency | Client E2E | p99 ≤ **fsync_interval + 2 ms** | D |
| Idle connections | Scale | ≥ **10k** | — |
| Declared queues | Scale | ≥ **10k** | — |
| Management list 1k queues (paginated) | Client E2E | p99 ≤ **50 ms** | E |

Notes:

- **Client E2E** metrics are measured by `queueforge-bench` (lapin client).
- **Broker-internal** route+enqueue p99 is from Prometheus histograms on the
  broker (`queueforge_*`); not emitted by the client harness.
- Idle connection / declared-queue scale targets are soak scenarios outside the
  A–F messaging shapes; track separately.
- Shape **F** is observational until priority lands; no contractual rate target
  yet—compare overhead vs shape A on the same host.

## How to run

Build a release broker and the harness:

```bash
cargo build --release -p queueforge-broker -p queueforge-bench
./target/release/queueforge --config configs/queueforge.example.toml
```

In another shell:

```bash
# Single shape (default A), 30s measure + 3s warmup
./target/release/queueforge-bench --shape A --duration-secs 30

# Multi-conn stress
./target/release/queueforge-bench --shape B --duration-secs 30

# Durable + confirms (match broker fsync_interval_ms)
./target/release/queueforge-bench --shape D --fsync-interval-ms 100

# Management concurrent with B (broker must serve management HTTP)
./target/release/queueforge-bench \
  --shape E \
  --mgmt-url http://127.0.0.1:15672

# All core shapes A–E
./target/release/queueforge-bench --shape all --duration-secs 30

# Optional priority shape (PR 11b+)
./target/release/queueforge-bench --shape F
# or: --shape all --include-f

# Optional local check against hypothesis table (expect misses until tuned;
# do not treat --check-slo as a release gate until targets are validated)
./target/release/queueforge-bench --shape A --check-slo
```

### Useful flags

| Flag / env | Default | Meaning |
|------------|---------|---------|
| `--uri` / `QUEUEFORGE_BENCH_URI` | `amqp://admin:devpassword12@127.0.0.1:5672/%2f` | AMQP URI |
| `--shape` | `A` | `A`–`F` or `all` |
| `--duration-secs` | `30` | Measurement window |
| `--warmup-secs` | `3` | Discard window before counters |
| `--body-size` | `1024` | Payload bytes (≥ 8; first 8 = timestamp) |
| `--prefetch` | `100` | `basic.qos` |
| `--mgmt-url` | `http://127.0.0.1:15672` | Shape E base URL |
| `--fsync-interval-ms` | `100` | Shape D confirm latency budget base |
| `--check-slo` | off | Non-zero exit if any hypothesis misses |

Dev bootstrap credentials match `queueforge-auth` (`admin` / `devpassword12`) when
the broker is started with `--dev-bootstrap` / empty user table.

## Interpreting results

Each shape prints:

- **ingress msg/s** — successful publishes (and confirms for D) during the measure window
- **consume msg/s** — acked deliveries (fanout C counts one consume per queue copy)
- **publish/confirm latency** — client time for `basic.publish` (+ confirm wait when enabled)
- **E2E publish→consume** — timestamp embedded in the first 8 body bytes
- **mgmt list latency** — shape E only

Hypothesis checks are labeled `PASS` / `MISS`. Early builds are expected to miss
stretch targets. Use results to revise this table and to archive raw output under
`docs/bench-results/` — **not** to block merges or imply a contractual SLO.

### Revision log

| Date | Commit | Host | Notes |
|------|--------|------|-------|
| *(fill in after first bench PR run)* | | | Hypotheses as designed; **not yet measured** — see `docs/bench-results/` when available |

## Related design

See `DESIGN-queueforge.md` § Performance Targets for the source of these
hypotheses and the broader architecture that they constrain.

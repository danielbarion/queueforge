# QueueForge (PHP)

One process, one thread, one `stream_select()` loop. No Composer, no autoloader,
no build step: eleven `require` lines in `bin/queueforge` and the only extension
beyond the bundled ones is `sockets`, used to set `TCP_NODELAY` when it is
available and skipped when it is not.

Same as Rust and Bun:

- AMQP 0-9-1 login over PLAIN, channels, and the default exchange
- direct, fanout, topic, and headers exchanges, queue bindings, and
  exchange-to-exchange bindings
- the built-in `amq.direct`, `amq.fanout`, `amq.topic`, and `amq.headers`
  exchanges
- durable classic queues, publisher confirms released only after the covering
  fsync, and manual acks
- `basic.nack`, `basic.reject`, and `basic.recover`, each with requeue
- `basic.get`, `basic.cancel`, `queue.purge`, `queue.delete`, `queue.unbind`,
  `exchange.delete`, `exchange.bind`, `exchange.unbind`, and `channel.flow`
- `basic.return` when a mandatory publish matches nothing
- TTL per message and per queue, dead-lettering, `x-max-length` and
  `x-max-length-bytes` with `drop-head`, `reject-publish`, and
  `reject-publish-dlx`, and `x-max-priority` ordering
- quorum queues: a persistent publish confirms once a majority of members hold
  the body in their own durable store, and a replication that misses the
  majority is rolled back and nacked
- cluster protocol version 1, so a PHP process can sit in a Rust or Bun member
  list. Only the lower node id dials, so a pair gets one connection
- the management HTTP API and the SPA, Prometheus text on `/metrics`, and
  `/healthz` and `/readyz`
- RabbitMQ's `password_hash` format, verified against the same fixture the Bun
  suite pins
- TLS on the AMQP and management listeners when `[tls]` names a certificate
- MQTT 3.1.1, STOMP 1.2, and the RabbitMQ stream command set

Not the same:

- **Concurrency.** Rust is Tokio and Bun has worker threads; this is a single
  thread. Throughput is roughly an order of magnitude lower, which is the point
  of measuring it. See [`../BENCHMARK.md`](../BENCHMARK.md).
- **Quorum confirm latency.** Bun runs its local fsync and its peer appends at
  the same time. One thread cannot, so the appends go out non-blocking and the
  confirm gate is resolved on a later pass of the loop. Correctness is the same
  and latency is worse.
- **Classic queue homes agree with Bun, not Rust.** The home is picked by a
  32-bit FNV-1a hash. Bun's source notes that its hash already disagrees with
  Rust's 64-bit byte FNV, so matching both is impossible; this matches Bun.
- **On-disk format.** Rust has a write-ahead log and Bun has SQLite plus a
  preallocated fsync log. This is a single append-only log, rewritten when it is
  mostly dead records. Nothing crosses processes on disk, so only the wire
  formats have to agree.
- **No transactions.** `tx.select` and `tx.commit` are accepted, but publishes
  apply as they arrive rather than being buffered, so `tx.rollback` cannot undo
  one and is refused with a 540 rather than acknowledged.
- **Exchange-to-exchange bindings are in memory only** and do not survive a
  restart. Bun holds them the same way.
- **AMQP 1.0 is absent.** Bun's is a byte-scanning shim rather than a parser, so
  there is nothing worth reproducing.
- **Policies, limits, and topic permissions are in memory only.** Users, their
  tags, and their permissions persist; the rest is lost on restart.
- **One thread means one connection at a time gets served.** There is no flow
  control beyond prefetch, and `channel.flow` only echoes the requested state.

## Running

```bash
php bin/queueforge --config config.example.toml --dev-bootstrap
```

AMQP listens on `127.0.0.1:5675` in the example config. `--dev-bootstrap`
creates `admin` / `devpassword12` when the data directory has no users.

## Checks

Everything runs inside the bench container, so the host needs no PHP. The test
directory is mounted at run time, which keeps `queueforge-php:bench` free of
test code and identical to the image the benchmark measures:

```bash
docker compose -f ../docker-compose.bench.yml build php
docker compose -f ../docker-compose.bench.yml run --rm --no-deps \
  --entrypoint php -v ./test:/opt/queueforge/test \
  php test/run.php
```

A single file, by substring:

```bash
docker compose -f ../docker-compose.bench.yml run --rm --no-deps \
  --entrypoint php -v ./test:/opt/queueforge/test \
  php test/run.php quorum
```

Ten files and 328 checks: `lint`, `methods`, `routing`, `protocols`, `cluster`,
`cluster-live`, `quorum`, `mgmt`, plus the two original scripts, `roundtrip.php`
and `parity.php`. `roundtrip.php` is the strongest of them: it `SIGKILL`s the
broker and asserts that a confirmed but unacked durable message comes back.

Each file is its own process and reports through its exit code, so there is no
framework to install. `test/lib/Amqp.php` is a small AMQP client written for the
suite, which is why none of this needs a client library.

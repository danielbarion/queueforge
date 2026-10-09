# QueueForge (PHP)

One process, one thread, one `stream_select()` loop. No Composer, no autoloader,
no build step: explicit `require` lines in `bin/queueforge` and the only extension
beyond the bundled ones is `sockets`, used to set `TCP_NODELAY` when it is
available and skipped when it is not.

The target is client-visible parity with Rust and Bun: anything a client, a
peer, or the management UI can observe should behave the same. It cannot mean
identical internals, because Rust and Bun are not identical to each other
either — Rust has a write-ahead log, Bun has SQLite.

Same as Rust and Bun:

- AMQP 0-9-1 login over PLAIN, channels, and the default exchange, with the
  `connection.start` capabilities table advertising `publisher_confirms`,
  `consumer_cancel_notify`, and `basic.nack`
- direct, fanout, topic, and headers exchanges, queue bindings, and
  exchange-to-exchange bindings
- the built-in `amq.direct`, `amq.fanout`, `amq.topic`, and `amq.headers`
  exchanges, with the `amq.` namespace reserved against client declares
- alternate exchanges, in the exchange-row then operator-policy then
  user-policy precedence Bun uses, and internal exchanges refused with 403
- durable classic queues, publisher confirms released only after the covering
  fsync, and manual acks
- `basic.nack`, `basic.reject`, and `basic.recover`, each with requeue
- `basic.get`, `basic.cancel`, `queue.purge`, `queue.delete`, `queue.unbind`,
  `exchange.delete`, `exchange.bind`, `exchange.unbind`, and `channel.flow`
- `basic.return` when a mandatory publish matches nothing, and a 404 when the
  default exchange names a queue that does not exist
- passive declares, the 406 on a redeclare that disagrees about durability, the
  406 on an unknown `x-queue-type`, and the 541 `transient_nonexcl_queues`
  deprecation
- TTL per message and per queue with a sweep, so an expired message stops
  counting toward depth and max-length; dead-lettering with `x-death`,
  `x-first-death-reason` and `x-first-death-queue`; `x-dead-letter-strategy`
  including `at-least-once`; `x-max-length` and `x-max-length-bytes` with
  `drop-head`, `reject-publish`, and `reject-publish-dlx`; `x-max-priority`
  ordering; `x-expires`; and `x-delivery-limit`
- CC and BCC header routing, with BCC stripped before delivery
- consumer priority via `x-priority`, `x-single-active-consumer`, and exclusive
  consumers refused with 403
- policies and operator policies resolved into queue arguments, never
  overriding a declared value, and reapplied to existing queues on edit
- the permission refusals: vhost access and the connection limit on
  `connection.open`, the channel limit on `channel.open`, the queue limit on
  `queue.declare`, and topic-write on publish
- quorum queues: a persistent publish confirms once a majority of members hold
  the body in their own durable store, a replication that misses the majority
  is rolled back and nacked, and a body is not delivered before that majority
- classic queue homes: a queue lives on one node by hash, and a publish, get,
  subscribe, purge, or delete arriving elsewhere is forwarded to the home
- the consumed set, so a peer that replays its log does not hand out a quorum
  body a second time, and node-slotted session ids that cannot collide
- versioned cluster protocol with persistent Raft elections, replicated metadata,
  quorum commits, snapshots, and legacy protocol fallback. Only the lower node
  id dials, so a pair gets one connection
- the management HTTP API and the SPA with a history fallback, the full
  Prometheus series including the per-queue labelled gauges, and `/healthz`
  and `/readyz`
- RabbitMQ's `password_hash` format, verified against the same fixture the Bun
  suite pins
- TLS on the AMQP and management listeners when `[tls]` names a certificate
- MQTT 3.1.1 authentication, QoS, retained messages and persistent sessions;
  MQTT 5 properties and reason codes; STOMP 1.2 authentication, ACK/NACK,
  transactions, binary bodies and receipts; MQTT/STOMP WebSocket transports
- durable RabbitMQ streams with publisher deduplication, stored offsets, credit,
  single active consumers and super streams; AMQP 1.0 framing, SASL, settlement
  and binary message sections
- direct reply-to, delayed and consistent-hash exchanges, durable topology,
  definition imports/exports, live connections/channels and tracing/events
- verified JWKS authentication, LDAP binds and administrator-group lookup;
  TLS client-certificate authentication with a configured trusted CA
- AMQP 0-9-1 shovels and federation with remote authentication and confirmations

Not the same:

- **Concurrency.** Rust is Tokio and Bun has worker threads; this is a single
  thread. Throughput is roughly an order of magnitude lower, which is the point
  of measuring it. See [`../BENCHMARK.md`](../BENCHMARK.md).
- **Quorum confirm latency.** Bun runs its local fsync and its peer appends at
  the same time. One thread cannot, so the appends go out non-blocking and the
  confirm gate is resolved on a later pass of the loop. Confirm latency depends on the event-loop tick and cluster round trips.
- **A forwarded `basic.get` answers from a callback.** One thread cannot block
  on a peer, so the reply is written when it arrives; a peer that does not
  answer within the request timeout yields `basic.get-empty`.
- **Classic queue homes agree with Bun, not Rust.** The home is picked by a
  32-bit FNV-1a hash. Bun's source notes that its hash already disagrees with
  Rust's 64-bit byte FNV, so matching both is impossible; this matches Bun.
- **On-disk format.** Rust has a write-ahead log and Bun has SQLite plus a
  preallocated fsync log. This is a single append-only log, rewritten when it is
  mostly dead records. Nothing crosses processes on disk, so only the wire
  formats have to agree.
- **TLS with a configured client CA requires a client certificate.** PHP's
  standard verified handshake does not provide RabbitMQ's optional-certificate
  behavior.
- **Integration transports.** Shovels and federation currently bridge AMQP
  0-9-1; AMQP 1.0 and native-stream bridge transports are unsupported.
- **Remaining cluster work.** Mixed-stack rolling upgrades need additional
  interoperability validation.
- **Producer coordination.** Concurrent stream publishing with the same producer
  reference through different ingress nodes still needs leader coordination.
  Retries and concurrent publishes through one ingress are deduplicated.
- **Exchange auto-delete is stored and reported but not enforced.** Queue
  auto-delete runs when the last consumer leaves a queue that had a consumer.
- **One thread means one connection at a time gets served.** There is no flow
  control beyond prefetch, and `channel.flow` only echoes the requested state.
- **Every message is held in memory**, and log compaction transiently needs a
  second copy, so an unbounded queue with no consumer exhausts PHP's 128 MiB
  `memory_limit` at roughly 100,000 queued 256-byte messages. Bound a queue
  with `x-max-length` if nothing is consuming it.

## Running

```bash
php bin/queueforge --config config.example.toml --dev-bootstrap
```

AMQP listens on `127.0.0.1:5675` in the example config. `--dev-bootstrap`
creates `admin` / `devpassword12` when the data directory has no users.

DNS peers can be resolved at startup with `[cluster] discovery = "dns"`,
`dns_name` and optional `dns_port` (defaults to the cluster listener port).
Address IDs follow the Rust/Bun format, including bracketed IPv6. Set `node_id`
to the advertised address when the listener binds a wildcard address.
`QUEUEFORGE_MEMBERS` also accepts the shared JSON array of `{id, addr}` rows.

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

The suite includes protocol, management, authentication, Raft, snapshot,
retention and real three-node metadata checks. `roundtrip.php` also kills the
broker and checks that a confirmed but unacked durable message comes back.
Run locally with `php test/run.php` when PHP is available.

Each file is its own process and reports through its exit code, so there is no
framework to install. `test/lib/Amqp.php` is a small AMQP client written for the
suite, which is why none of this needs a client library.

`test/probe.php` is not part of the suite. It is a confirm-rate probe used to
check one build against another; its numbers are not comparable to the paced
ladder in [`../BENCHMARK.md`](../BENCHMARK.md).

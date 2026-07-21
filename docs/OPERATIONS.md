# QueueForge operations

Operator notes for single-node QueueForge deployments.

## Ports (defaults)

| Listener | Default | Notes |
|----------|---------|--------|
| AMQP | `0.0.0.0:5672` | AMQPS when `[tls]` enabled |
| Management HTTP / SPA | `0.0.0.0:15672` | HTTPS when TLS enabled; hosts `/healthz`, `/readyz` |
| Metrics | `127.0.0.1:15692` | Unauthenticated Prometheus scrape — **do not expose publicly** |

Config: `configs/queueforge.example.toml` (host) or `configs/queueforge.docker.toml` (containers).

## Backup

1. **Stop the broker** cleanly (SIGTERM) so WAL fsync and metadata close complete.
2. **Snapshot `data_dir`** (config `[data].dir`, e.g. `/var/lib/queueforge` or `./data`):
   copy the entire directory (redb metadata + durable queue WAL segments).
3. **Optional topology export** while the broker is still running and healthy:

   ```bash
   # After session login (cookie), or from an authenticated management client:
   curl -fsS -b cookies.txt 'http://127.0.0.1:15672/api/definitions' -o definitions.json
   ```

   Definitions capture users, vhosts, exchanges, queues, bindings, and
   permissions — not in-flight messages. Message durability lives in `data_dir`.

Store snapshots offline with the same care as a database volume.

## Restore

1. Stop any running broker that would use the same `data_dir`.
2. Replace `data_dir` with the snapshot (same absolute path layout preferred).
3. Start the broker with the same config (`QUEUEFORGE_CONFIG` / `--config`).
4. Wait until `GET /readyz` on the management listener returns **200** (recovery
   finished).
5. Optionally re-apply topology from a definitions file:

   ```bash
   curl -fsS -b cookies.txt -H 'Content-Type: application/json' \
     --data-binary @definitions.json \
     'http://127.0.0.1:15672/api/definitions'
   ```

   Import requires an **administrator** session. Prefer either a full data_dir
   restore **or** definitions-only on a fresh data dir — mixing may create
   conflicts for durable queue state.

## Management sessions

Sessions are **in-memory only**:

- Lost on process restart (users must log in again).
- **Single active session per user** — a new login invalidates prior sessions
  for that username.
- **Idle TTL:** 8 hours (refreshed on authenticated requests).
- **Absolute TTL:** 24 hours from session creation.

Cookie: `queueforge_session` (`HttpOnly; SameSite=Lax`; `Secure` when TLS is on).

## Production checklist

1. **Never** pass `--dev-bootstrap` outside local development.
2. Set **`QUEUEFORGE_ADMIN_USER`** and **`QUEUEFORGE_ADMIN_PASSWORD`** before first
   start (bootstrap when the user table is empty). Rotate after any leak.
3. Enable **`[tls]`** (or `QUEUEFORGE_TLS_*`) for AMQPS and HTTPS management.
   See [`PRODUCTION_TLS.md`](PRODUCTION_TLS.md).
4. Keep **metrics** on loopback or an internal scrape network; do not publish
   host port `15692` without an auth proxy.
5. Bind AMQP/management to private interfaces or put them behind a reverse proxy.
6. Use least-privilege AMQP users and vhost permissions; reserve `administrator`
   for operators.
7. Protect `data_dir` filesystem permissions.
8. Review [`../SECURITY.md`](../SECURITY.md).

Docker image: the production `CMD` does **not** include `--dev-bootstrap`. Local
`docker-compose.yml` supplies admin credentials via environment variables.

## Graceful shutdown

On SIGINT / SIGTERM the broker performs an ordered drain:

1. **`/readyz` → 503** so load balancers stop sending new traffic.
2. **Stop accepting** new AMQP connections.
3. **`connection.close`** to live clients; wait up to the connection drain
   timeout (~10s) for tasks to exit.
4. **Queue Shutdown / WAL fsync** for durable queues; close redb metadata.

If connection drain times out or a durable queue fsync fails, the process may
exit non-zero — treat as unclean shutdown, inspect logs, and verify data before
relying on the volume.

## Health checks

- **Liveness:** `GET /healthz` → 200 while the process is up (management and
  metrics listeners).
- **Readiness:** `GET /readyz` → 200 after recovery; **503** during startup
  recovery and during graceful drain.

Prefer probing the **management** listener for container healthchecks when
metrics are not published to the host.

# Security Policy

## Supported versions

QueueForge is pre-1.0. Security fixes land on the default development branch;
there are no long-term support releases yet. Upgrade by pulling the latest
tagged release when available.

## Reporting a vulnerability

**Please do not open public GitHub issues for security problems.**

Report vulnerabilities via **GitHub Security Advisories** (private report):

- **New advisory:** https://github.com/queueforge/queueforge/security/advisories/new

Include:

- Affected version / commit SHA
- Description of the issue and impact
- Steps to reproduce or a minimal proof-of-concept
- Whether you plan to disclose publicly and on what timeline

We aim to acknowledge reports within 5 business days and to provide a status
update within 14 days. Coordinated disclosure is preferred.

If you cannot use GitHub advisories, contact the maintainers through the
repository’s private security contact once published.

## Hardening checklist (production)

1. **TLS** — enable `[tls]` with a valid certificate chain and private key so
   AMQP (AMQPS) and management (HTTPS) are encrypted. See
   [`docs/PRODUCTION_TLS.md`](docs/PRODUCTION_TLS.md).
2. **Admin credentials** — set `QUEUEFORGE_ADMIN_USER` and
   `QUEUEFORGE_ADMIN_PASSWORD` (or create administrators via the management API).
   Never run production with `--dev-bootstrap`.
3. **Network exposure** — bind AMQP/management to private interfaces or put them
   behind a reverse proxy / load balancer with mTLS or IP allowlists as needed.
4. **Authorization** — use least-privilege users and vhost permissions; reserve
   the `administrator` tag for operators.
5. **Data directory** — protect the on-disk metadata/WAL path with filesystem
   permissions; treat it as sensitive as a database volume.
6. **Cookies** — when TLS is enabled, management session cookies are marked
   `Secure`. Prefer SameSite=Lax/Strict at any reverse proxy that rewrites
   cookies.
7. **Metrics** — `/metrics` is unauthenticated; keep the metrics listener on
   loopback or an internal scrape network (do not publish host port 15692
   without protection).
8. **Dependencies** — rebuild images regularly; CI runs `cargo audit`.

## Scope notes

- QueueForge speaks AMQP 0-9-1 and is not a drop-in RabbitMQ security model clone
  in every edge case; review permissions carefully when migrating.
- Denial-of-service via large publishes is mitigated by `max_message_bytes`,
  connection limits, and memory/disk watermarks — configure them for your host.

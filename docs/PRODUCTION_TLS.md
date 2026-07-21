# Production TLS checklist

QueueForge terminates TLS with **rustls** for:

| Listener | Protocol | Default bind | TLS-on conventional port |
|----------|----------|--------------|--------------------------|
| AMQP | AMQPS (TLS under AMQP 0-9-1) | `0.0.0.0:5672` | **5671** |
| Management | HTTPS | `0.0.0.0:15672` | **15671** |

Metrics (`127.0.0.1:15692` by default) stay **plain HTTP** and should remain on loopback (or behind network policy). Do not expose `/metrics` on a public interface.

TLS is **optional and off by default** so local development is unchanged. For any non-loopback / production deployment, enable TLS (or terminate TLS in a mesh/proxy in front of the broker).

## Minimal config

```toml
[listeners]
amqp = "0.0.0.0:5671"
management = "0.0.0.0:15671"
metrics = "127.0.0.1:15692"

[tls]
enabled = true
cert_path = "/etc/queueforge/tls/server.crt"
key_path = "/etc/queueforge/tls/server.key"
```

Environment overrides:

| Variable | Effect |
|----------|--------|
| `QUEUEFORGE_TLS_ENABLED` | `true` / `false` (also `1`/`0`, `on`/`off`) |
| `QUEUEFORGE_TLS_CERT` | PEM certificate path |
| `QUEUEFORGE_TLS_KEY` | PEM private key path |

## Certificate requirements

- **PEM** encoding.
- Certificate file: leaf certificate, optionally followed by intermediate chain (leaf first).
- Private key: PKCS#8 (`BEGIN PRIVATE KEY`) or RSA PKCS#1 (`BEGIN RSA PRIVATE KEY`).
- Key and cert must match; rustls rejects mismatched pairs at startup.
- Prefer certificates from a trusted CA (public or private PKI). Self-signed certs work for lab only; clients must trust them explicitly.
- Cover every hostname / IP clients use (`SAN` DNS and/or IP entries).
- Track expiry; renew before clients start failing handshakes.

### Example: generate a lab self-signed pair

```bash
openssl req -x509 -newkey rsa:2048 \
  -keyout server.key -out server.crt \
  -days 365 -nodes \
  -subj "/CN=queueforge.example.com" \
  -addext "subjectAltName=DNS:queueforge.example.com,DNS:localhost,IP:127.0.0.1"
```

## Production checklist

Use this before promoting a node beyond localhost:

- [ ] **`tls.enabled = true`** (or terminate TLS at a trusted proxy/mesh for *both* AMQP and management)
- [ ] **`cert_path` / `key_path`** point at readable PEM files on the host (or secret mount)
- [ ] Certificate **SAN** matches client connection hostnames
- [ ] Private key permissions restricted (e.g. `0600`, owned by the broker user)
- [ ] Listeners bound on intended interfaces; firewall only exposes AMQPS + HTTPS
- [ ] Conventional ports documented for operators: **5671** (AMQPS), **15671** (HTTPS management)
- [ ] Clients use `amqps://` (or TLS-capable AMQP URL) and HTTPS for management
- [ ] Management session cookies are **Secure** automatically when broker TLS is on
- [ ] Metrics remain on **127.0.0.1** (or network-policy protected); scrape via sidecar/localhost
- [ ] Bootstrap admin password rotated off any `--dev-bootstrap` / default lab credentials
- [ ] Disk free limits and backups tested (see design rollout checklist)
- [ ] Certificate renewal / rotation procedure runbooked (restart applies new cert; no hot-reload in v1)

## Client notes

### AMQP (AMQPS)

- URI scheme: `amqps://user:pass@host:5671/%2f`
- Clients (lapin, pika, Java, etc.) must enable TLS and trust the server CA
- Server does **not** request client certificates (server-only TLS in v1)

### Management UI / API

- Use `https://host:15671/...`
- Login cookie is `HttpOnly; SameSite=Lax; Secure` when TLS is enabled
- Health: `GET /healthz`, `GET /readyz` on the management bind (also on metrics bind)

## Operational notes

- **No config hot-reload** in v1: changing cert/key or `tls.enabled` requires a process restart.
- Enabling TLS does **not** keep a plain listener on the same port; plain and TLS are not dual-stacked on one socket.
- If you terminate TLS externally (ingress / service mesh), leave `tls.enabled = false` on the broker and ensure the mesh provides equivalent confidentiality; set reverse-proxy cookie/`X-Forwarded-Proto` handling carefully (QueueForge does not trust client-controlled forwarding headers for auth rate limits).
- Failure to load cert/key aborts startup with a clear config/TLS error (fail closed).

## Related

- Example config: [`configs/queueforge.example.toml`](../configs/queueforge.example.toml)
- Design: Security & Privacy, Rollout production checklist in `DESIGN-queueforge.md`

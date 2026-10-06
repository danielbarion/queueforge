# syntax=docker/dockerfile:1.6

FROM rust:1.88-bookworm AS builder
WORKDIR /src

# Build deps for native crates (ring, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Cache dependency builds.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY configs ./configs
COPY ui/dist ./ui/dist

RUN cargo build --release -p queueforge-broker \
    && strip target/release/queueforge

FROM debian:bookworm-slim
LABEL org.opencontainers.image.base.name="debian:bookworm-slim" \
      queueforge.build="rust:1.88-bookworm"
# curl: container healthchecks (management /readyz). ca-certificates + tini: TLS roots / PID 1.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tini curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/queueforge --create-home --shell /usr/sbin/nologin queueforge

COPY --from=builder /src/target/release/queueforge /usr/local/bin/queueforge
COPY configs/queueforge.docker.toml /etc/queueforge/queueforge.toml

ENV QUEUEFORGE_CONFIG=/etc/queueforge/queueforge.toml \
    RUST_LOG=info

VOLUME ["/var/lib/queueforge"]
# AMQP + management. Metrics (15692) stay internal — do not publish without auth.
EXPOSE 5672 15672

USER queueforge
WORKDIR /var/lib/queueforge

ENTRYPOINT ["/usr/bin/tini", "--"]
# Production: require QUEUEFORGE_ADMIN_USER + QUEUEFORGE_ADMIN_PASSWORD when the
# user table is empty. Local docker-compose sets those via environment (no
# --dev-bootstrap in the image CMD).
CMD ["queueforge", "--config", "/etc/queueforge/queueforge.toml"]

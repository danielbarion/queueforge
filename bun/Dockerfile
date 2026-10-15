# syntax=docker/dockerfile:1.6

FROM oven/bun:1.4.3-alpine
RUN apk add --no-cache curl \
    && mkdir -p /var/lib/queueforge \
    && chown bun:bun /var/lib/queueforge
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production
COPY src ./src
COPY config.docker.toml /etc/queueforge/queueforge.toml
LABEL org.opencontainers.image.base.name="oven/bun:1.4.3-alpine"
# One GC marking thread per process: JavaScriptCore sizes its helper pool from
# the host's cores, not the container's, and the helpers of every worker
# competed for the same CPUs (docs/bun-scaling.md, B14). Workers get it from
# the supervisor too; this covers the supervisor and the one-process broker.
ENV BUN_JSC_numberOfGCMarkers=1
USER bun
EXPOSE 5672 15672
CMD ["bun", "src/main.ts", "--config", "/etc/queueforge/queueforge.toml", "--dev-bootstrap"]

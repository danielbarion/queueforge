# syntax=docker/dockerfile:1.6

FROM oven/bun:1.4.2-alpine
RUN apk add --no-cache curl \
    && mkdir -p /var/lib/queueforge \
    && chown bun:bun /var/lib/queueforge
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production
COPY src ./src
COPY config.docker.toml /etc/queueforge/queueforge.toml
LABEL org.opencontainers.image.base.name="oven/bun:1.4.2-alpine"
USER bun
EXPOSE 5672 15672
CMD ["bun", "src/main.ts", "--config", "/etc/queueforge/queueforge.toml", "--dev-bootstrap"]

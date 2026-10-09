# The connection-hold client for bench/connections.py.
# Build: docker build -t qf-connscale:linux bench/connscale
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
COPY connscale-linux.c /src/
RUN gcc -O2 -Wall -Wextra -o /src/connscale-linux /src/connscale-linux.c

FROM debian:bookworm-slim
COPY --from=build /src/connscale-linux /usr/local/bin/connscale-linux
ENTRYPOINT ["sleep", "infinity"]

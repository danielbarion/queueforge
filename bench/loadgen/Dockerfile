# The load client for bench/load.py. Build: docker build -t qf-loadgen:linux bench/loadgen
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
COPY loadgen-linux.c /src/
RUN gcc -O2 -Wall -Wextra -o /src/loadgen /src/loadgen-linux.c

FROM debian:bookworm-slim
COPY --from=build /src/loadgen /usr/local/bin/loadgen
ENTRYPOINT ["sleep", "infinity"]

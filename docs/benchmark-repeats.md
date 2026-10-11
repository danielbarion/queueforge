# Reproducible scenario captures

The repeat harness keeps each run in an exclusive UUID directory. It
pins the broker and PerfTest image digests at invocation start, saves image
labels independently from the current checkout, and hashes the harness,
scenario and resource settings. Row/provenance JSON files cannot be replaced.
Failed, interrupted and missing planned repeats remain unscored. Reports use
one latest coherent cohort for each broker/size/scenario; different images,
profiling settings or invocations are never combined into one median.

Run only while holding the host lease described in `PENDING.md` ("Measurement
rules"). The ordinary single-cell command still works. With Docker Desktop
already configured for 8 CPUs and 24 GiB:

```sh
QF_BENCH_OUT=/tmp/qf-ab/current python3 bench/scenarios.py one large-1m php --repeat 2 --cpus 1
QF_BENCH_OUT=/tmp/qf-ab/current python3 bench/scenarios.py one work-queue php --repeat 5 --cpus 4
python3 bench/scenarios.py matrix --repeat 2      # the published matrix
```

The default tags are `queueforge-{bun,php,rust}:bench`; build/label the intended
image and record its digest before measuring. To pin an exact image without
changing the shared tags, set `C.IMAGES["<kind>"] = "sha256:..."` from a small
Python entry point that imports `bench/common.py` and `bench/scenarios.py`.
`python3 -m unittest discover -s bench -p 'test_*.py'` runs the harness
regressions.

Resource samples use the host Docker Engine stats API through the active
Docker context's Unix socket, with no process started inside the measured
broker. Total memory includes file cache; anonymous memory is not language
heap. Peaks are sampled once per second, so short spikes can be missed. Remote
TCP/SSH Docker contexts are explicitly unsupported by this sampler.

Diagnostic opt-ins are separate cohorts: `QF_BENCH_PROFILE=1` adds Bun process,
heap and storage counters; `QF_BENCH_CPU_PROFILE=1` adds CPU profiles and saves
collection errors per run. Neither diagnostic rate is a replacement for a
non-profiled result. Check CPU profile collection status and expected process
coverage before using a profile to attribute a multicore bottleneck.

Captures under `QF_BENCH_OUT` do not replace published numbers. Publishing means
a full run into `bench/results/<date>/`, then `python3 bench/report.py` to
regenerate `report.md` and `site/app/bench-data.ts`, and an update to
`BENCHMARK.md`.

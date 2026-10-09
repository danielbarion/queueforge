#!/usr/bin/env python3
"""Every Docker benchmark, in one run.

1. Docker Desktop to 8 CPUs: the load sweep (bench/load.py), the PerfTest
   scenarios (bench/scenarios.py) and the connection-hold sweep
   (bench/connections.py), one broker at a time.
2. Back to 2 CPUs: the paced ladder (bench/paced.py), each broker at 1 CPU /
   512 MiB with the client on the Mac.

Build first: `docker compose -f docker-compose.bench.yml build`,
`docker build -t qf-loadgen:linux bench/loadgen`,
`docker build -t qf-connscale:linux bench/connscale`, and
`cargo build --release -p queueforge-compare` in rust/.

Usage:
  bench/run-all.py [--skip load,scenarios,connections,paced] [--kinds bun,php]
With --kinds, only those brokers run, and their load and scenario rows replace
the ones already in the results directory (for a rebuilt image).
Results land in bench/results/<UTC date>/ (QF_BENCH_OUT overrides).
"""
import json
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402


def arg(name):
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1].split(",")
    return []


def step(name, fn):
    C.log(f"STEP {name} start")
    started = time.time()
    try:
        fn()
        C.log(f"STEP {name} done in {time.time() - started:.0f}s")
    except SystemExit as e:
        C.log(f"STEP {name} exit {e.code}")
    except Exception:
        C.log(f"STEP {name} FAILED\n" + traceback.format_exc())


def merge_load(load, kinds):
    """Replace these brokers' rows in load.tsv with the ones from load-rerun.tsv."""
    main = os.path.join(C.OUT_DIR, "load.tsv")
    fresh = load.OUT
    keep = [ln for ln in open(main)] if os.path.exists(main) else []
    head = keep[0] if keep else open(fresh).readline()
    old = [ln for ln in keep[1:] if ln.split("\t", 1)[0] not in kinds]
    new = [ln for ln in open(fresh)][1:]
    with open(main, "w") as f:
        f.writelines([head] + old + new)


def main():
    skip = set(arg("--skip"))
    kinds = arg("--kinds")
    if not C.docker_up():
        raise SystemExit("docker is down")
    meta_path = os.path.join(C.OUT_DIR, "run-meta.json")
    meta = json.load(open(meta_path)) if os.path.exists(meta_path) else {}
    meta.setdefault("runs", []).append(
        {"started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "kinds": kinds or "all", "images": C.image_ids()}
    )
    json.dump(meta, open(meta_path, "w"), indent=1)
    C.log("RUN kinds=" + (",".join(kinds) or "all") + " images " + json.dumps(C.image_ids()))

    import load
    import scenarios
    import connections

    big = [s for s in ("load", "scenarios", "connections") if s not in skip]
    if big:
        C.ensure_big()
        load.docker_up()
        try:
            if "load" not in skip:
                if kinds:
                    load.KINDS = kinds
                    load.OUT = os.path.join(C.OUT_DIR, "load-rerun.tsv")
                step("load", load.run_matrix)
                load.cleanup_load()
                if kinds:
                    merge_load(load, kinds)
            if "scenarios" not in skip:
                step("scenarios", lambda: scenarios.run_matrix(kinds or scenarios.KINDS))
                C.run(["docker", "rm", "-f", scenarios.BROKER, scenarios.CLIENT], check=False)
            if "connections" not in skip:
                sys.argv = ["connections.py"] + kinds
                step("connections", connections.main)
                connections.stop()
        finally:
            C.restore_small()
    if "paced" not in skip:
        import paced

        if kinds:
            paced.KINDS = [k for k in paced.KINDS if k[0] in kinds]
        step("paced", paced.main)
    C.log("RUN done")


if __name__ == "__main__":
    main()

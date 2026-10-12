#!/usr/bin/env python3
"""Every Docker benchmark, end to end, with a status file to poll.

Builds the images, runs the PerfTest scenarios (with repeats), the load
sweep, the connection-hold sweep and the paced ladder, then regenerates
`<results>/report.md` and `site/app/bench-data.ts` with bench/report.py.

Usage:
  python3 bench/run.py                       everything, 2 repeats per scenario cell
  python3 bench/run.py --kinds bun,php       only these brokers (their rows replace older ones)
  python3 bench/run.py --steps scenarios,report --repeat 3
  python3 bench/run.py --out /tmp/qf-ab/x    results somewhere other than bench/results/<UTC date>
  python3 bench/run.py --kinds bun --steps build,scenarios,report --scenarios quorum --sizes 4
  python3 bench/run.py --kinds rust,bun,php --baseline bench/results/2026-10-12
  python3 bench/run.py --changed             re-measure only brokers whose image changed
  python3 bench/run.py --quick               daily check: see below

`--changed` builds every image, then compares each broker's image ID with the
newest full results directory under bench/results (or `--baseline DIR`).
Brokers whose image is the same keep their rows from there; only the others
are measured. RabbitMQ's image does not change, so it is normally skipped.

`--quick` is `--changed --repeat 1 --steps build,scenarios,report`, written
to bench/results-quick/<UTC time> without touching site/app/bench-data.ts.
It ends with `compare.md`: each re-measured cell against the baseline, with
changes past 10% marked. One changed broker takes about 15 minutes; the full
run about 3 hours. Use it to check a change, then a full run to publish.

`--scenarios` and `--sizes` pick scenario cells (ids from bench/scenarios.py;
sizes 1 and 4 CPUs, where 4 CPU runs only its heavy subset). `--baseline DIR`
starts the new results directory as a copy of DIR, so brokers that are not
re-run (usually RabbitMQ) keep their rows there: scenario cells, load rows,
paced files and connection rows of the brokers being re-run are replaced.

Steps, in order: build, scenarios, load, connections, paced, report.
Kinds: rabbit, rust, bun, php.

Progress goes to the terminal and `<results>/bench.log`. `bench/last-run.json`
is rewritten after every step: `status` is "running", then "done" (every step
passed) or "done-with-errors". Docker Desktop is resized to 8 CPUs for the
big steps and put back to 2 CPUs / 8 GiB afterwards, even after a failure.
"""
import argparse
import csv
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
STATUS = os.path.join(HERE, "last-run.json")
LOCK = "/tmp/queueforge-benchmark.lock"
STEPS = ["build", "scenarios", "load", "connections", "paced", "report"]
KINDS = ["rabbit", "rust", "bun", "php"]


def parse():
    p = argparse.ArgumentParser(description="Run the Docker benchmarks and report.")
    p.add_argument("--kinds", default=",".join(KINDS), help="comma list of rabbit,rust,bun,php")
    p.add_argument("--steps", default=None, help="comma list of " + ",".join(STEPS))
    p.add_argument("--repeat", type=int, default=None, help="repeats per scenario cell (default 2, 1 with --quick)")
    p.add_argument("--changed", action="store_true", help="measure only brokers whose image differs from the baseline")
    p.add_argument("--quick", action="store_true", help="--changed, 1 repeat, scenarios only, compare.md, no site update")
    p.add_argument("--out", default="", help="results directory (default bench/results/<UTC date>)")
    p.add_argument("--scenarios", default="", help="comma list of scenario ids (default all)")
    p.add_argument("--sizes", default="", help="comma list of CPU counts, 1 and/or 4 (default both)")
    p.add_argument("--baseline", default="", help="copy this results directory first and keep the rows of brokers not re-run")
    opts = p.parse_args()
    if opts.quick:
        opts.changed = True
        if opts.steps is None:
            opts.steps = "build,scenarios,report"
        if opts.repeat is None:
            opts.repeat = 1
        if not opts.out:
            opts.out = os.path.join(HERE, "results-quick", time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime()))
    if opts.steps is None:
        opts.steps = ",".join(STEPS)
    if opts.repeat is None:
        opts.repeat = 2
    opts.kinds = [k for k in opts.kinds.split(",") if k]
    opts.steps = [s for s in opts.steps.split(",") if s]
    opts.scenarios = [s for s in opts.scenarios.split(",") if s]
    try:
        opts.sizes = [int(s) for s in opts.sizes.split(",") if s]
    except ValueError:
        p.error("--sizes takes 1 and/or 4")
    bad = [k for k in opts.kinds if k not in KINDS] + [s for s in opts.steps if s not in STEPS]
    bad += [f"size {s}" for s in opts.sizes if s not in (1, 4)]
    if bad or not opts.kinds or not opts.steps or opts.repeat < 1:
        p.error(f"unknown or empty: {bad or 'kinds/steps/repeat'}")
    if opts.baseline and not os.path.isdir(opts.baseline):
        p.error(f"--baseline {opts.baseline} is not a directory")
    if opts.changed and "build" not in opts.steps:
        p.error("--changed compares freshly built images, so it needs the build step")
    if opts.changed and not opts.baseline:
        opts.baseline = latest_results(os.path.join(HERE, "results")) or ""
        if not opts.baseline:
            p.error("--changed found no results under bench/results; run the full benchmark once")
    if opts.changed and not opts.out:
        # Today's directory is often the baseline itself; a merged run gets its own.
        day = os.path.join(HERE, "results", time.strftime("%Y-%m-%d", time.gmtime()))
        opts.out = day if not os.path.exists(day) else day + time.strftime("-%H%M", time.gmtime())
    if opts.baseline and opts.out and os.path.abspath(opts.baseline) == os.path.abspath(opts.out):
        p.error("--out is the baseline directory itself")
    opts.steps = [s for s in STEPS if s in opts.steps]
    return opts


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Status:
    def __init__(self, opts, out_dir):
        self.data = {
            "status": "running",
            "started": now(),
            "updated": now(),
            "finished": None,
            "out_dir": out_dir,
            "log": os.path.join(out_dir, "bench.log"),
            "report": os.path.join(out_dir, "report.md"),
            "site_data": os.path.join(ROOT, "site", "app", "bench-data.ts"),
            "kinds": opts.kinds,
            "repeat": opts.repeat,
            "scenarios": opts.scenarios or "all",
            "sizes": opts.sizes or "all",
            "baseline": os.path.abspath(opts.baseline) if opts.baseline else None,
            "commit": git("rev-parse", "HEAD"),
            "dirty": git("status", "--porcelain") != "",
            "images": {},
            "steps": {s: {"status": "pending"} for s in opts.steps},
        }
        self.write()

    def write(self):
        self.data["updated"] = now()
        tmp = STATUS + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=1)
            f.write("\n")
        os.replace(tmp, STATUS)

    def step(self, name, **fields):
        self.data["steps"][name].update(fields)
        self.write()


def git(*args):
    p = subprocess.run(["git", "-C", ROOT, *args], capture_output=True, text=True)
    return p.stdout.strip()


def sh(args, cwd=ROOT):
    """Run a command, streaming its output; raise on failure."""
    print("$ " + " ".join(args), flush=True)
    subprocess.run(args, cwd=cwd, check=True)


def take_lease(owner="bench/run.py", purpose="benchmark run"):
    try:
        os.mkdir(LOCK)
    except FileExistsError:
        owner = ""
        try:
            owner = open(os.path.join(LOCK, "owner")).read().strip()
        except OSError:
            pass
        raise SystemExit(f"{LOCK} exists: another benchmark is running ({owner or 'no owner file'}). "
                         "Remove it only if nothing else is measuring.")
    with open(os.path.join(LOCK, "owner"), "w") as f:
        f.write(f"owner: {owner} pid {os.getpid()}\npurpose: {purpose}\nstart: {now()}\n")


def release_lease():
    shutil.rmtree(LOCK, ignore_errors=True)


def main():
    opts = parse()
    # Keep a Mac awake for the whole run; the display may still sleep.
    if platform.system() == "Darwin" and os.environ.get("QF_CAFFEINATED") != "1" and shutil.which("caffeinate"):
        os.environ["QF_CAFFEINATED"] = "1"
        os.execvp("caffeinate", ["caffeinate", "-ims", sys.executable, *sys.argv])
    if opts.out:
        os.environ["QF_BENCH_OUT"] = os.path.abspath(opts.out)
    sys.path.insert(0, HERE)
    import common as C  # noqa: E402  (reads QF_BENCH_OUT at import)

    if not C.docker_up():
        raise SystemExit("Docker is not running. Start Docker Desktop first.")
    if opts.scenarios:
        import scenarios

        unknown = [s for s in opts.scenarios if s not in {sc["id"] for sc in scenarios.SCENARIOS}]
        if unknown:
            raise SystemExit(f"unknown scenario ids: {unknown}")
        if opts.sizes == [4] and not set(opts.scenarios) & set(scenarios.HEAVY):
            raise SystemExit(f"4 CPU runs only {scenarios.HEAVY}")
    if opts.baseline and not opts.changed:
        copy_baseline(C, os.path.abspath(opts.baseline), opts)
    take_lease()
    status = Status(opts, C.OUT_DIR)
    failed = False

    def run_step(name, fn):
        nonlocal failed
        if name not in opts.steps:
            return
        C.log(f"STEP {name} start")
        status.step(name, status="running", started=now())
        t0 = time.time()
        try:
            fn()
            status.step(name, status="ok", seconds=round(time.time() - t0))
            C.log(f"STEP {name} done in {time.time() - t0:.0f}s")
        except BaseException as e:  # noqa: BLE001 - a step must not stop the others
            if isinstance(e, KeyboardInterrupt):
                status.step(name, status="interrupted", seconds=round(time.time() - t0))
                raise
            failed = True
            text = f"exit {e.code}" if isinstance(e, SystemExit) else traceback.format_exc()
            status.step(name, status="failed", seconds=round(time.time() - t0), error=text[-2000:])
            C.log(f"STEP {name} FAILED\n{text}")

    try:
        queueforge = [k for k in opts.kinds if k != "rabbit"]

        def build():
            if queueforge:
                sh(["docker", "compose", "-f", "docker-compose.bench.yml", "build", *queueforge])
            if "load" in opts.steps:
                sh(["docker", "build", "-t", "qf-loadgen:linux", "bench/loadgen"])
            if "connections" in opts.steps:
                sh(["docker", "build", "-t", "qf-connscale:linux", "bench/connscale"])
            if "paced" in opts.steps:
                sh(["cargo", "build", "--release", "-p", "queueforge-compare"], cwd=os.path.join(ROOT, "rust"))

        run_step("build", build)
        status.data["images"] = C.image_ids()
        if opts.changed:
            opts.kinds = changed_kinds(status.data["images"], baseline_images(opts.baseline), opts.kinds)
            C.log(f"CHANGED {','.join(opts.kinds) or 'none'} against {opts.baseline}")
            status.data["kinds"] = opts.kinds
            copy_baseline(C, os.path.abspath(opts.baseline), opts)
            if not opts.kinds:
                for name in opts.steps:
                    if name not in ("build", "report"):
                        status.step(name, status="skipped")
                opts.steps = [s for s in opts.steps if s in ("build", "report")]
        status.write()

        big = [s for s in ("scenarios", "load", "connections") if s in opts.steps]
        if big:
            import connections
            import load
            import scenarios

            C.ensure_big()
            try:
                load.docker_up()

                def run_scenarios():
                    try:
                        scenarios.run_matrix(opts.kinds, opts.repeat, only=opts.scenarios or None, cpus_only=opts.sizes or None)
                    finally:
                        C.run(["docker", "rm", "-f", scenarios.BROKER, scenarios.CLIENT], check=False)

                def run_load():
                    all_kinds = opts.kinds == KINDS
                    if not all_kinds:
                        load.KINDS = opts.kinds
                        load.OUT = os.path.join(C.OUT_DIR, "load-rerun.tsv")
                    try:
                        load.run_matrix()
                    finally:
                        load.cleanup_load()
                    if not all_kinds:
                        merge_load(C, load.OUT, opts.kinds)

                def run_connections():
                    sys.argv = ["connections.py"] + ([] if opts.kinds == KINDS else opts.kinds)
                    try:
                        connections.main()
                    finally:
                        connections.stop()

                run_step("scenarios", run_scenarios)
                run_step("load", run_load)
                run_step("connections", run_connections)
            finally:
                C.restore_small()

        def run_paced():
            import paced

            if opts.kinds != KINDS:
                paced.KINDS = [k for k in paced.KINDS if k[0] in opts.kinds]
            paced.main()

        run_step("paced", run_paced)
        run_step("report", lambda: sh([sys.executable, os.path.join(HERE, "report.py"), C.OUT_DIR, *(["--no-site"] if opts.quick else [])]))
        if opts.changed and "scenarios" in opts.steps:
            path = os.path.join(C.OUT_DIR, "compare.md")
            text = compare_md(read_tsv(os.path.join(opts.baseline, "scenarios.tsv")),
                              read_tsv(os.path.join(C.OUT_DIR, "scenarios.tsv")), opts.kinds, opts.baseline)
            with open(path, "w") as f:
                f.write(text)
            status.data["compare"] = path
            print(text, flush=True)
    finally:
        release_lease()
        status.data["status"] = "done-with-errors" if failed else "done"
        status.data["finished"] = now()
        if any(s.get("status") in ("pending", "running", "interrupted") for s in status.data["steps"].values()):
            status.data["status"] = "interrupted"
        status.write()
        C.log(f"RUN {status.data['status']}: {STATUS}")


def copy_baseline(C, src, opts):
    """Start the results directory as a copy of `src`, minus logs and report.

    Scenario cells of re-run brokers are superseded by their newer cohort, load
    rows and paced files are replaced, and connection rows (the report keeps
    the best held count) of re-run brokers are dropped here.
    """
    dst = C.OUT_DIR
    if os.path.abspath(src) == os.path.abspath(dst):
        raise SystemExit("--baseline is the results directory itself")
    # Importing common creates the directory and an empty `paced/`; nothing else may be there yet.
    leftover = [
        name for name in os.listdir(dst)
        if name != "bench.log" and not (name == "paced" and not os.listdir(os.path.join(dst, name)))
    ]
    if leftover:
        raise SystemExit(f"{dst} already has results ({', '.join(sorted(leftover)[:5])}); use --out for a fresh directory")
    skip = shutil.ignore_patterns("*.log", "report.md", "c[0-9]*.txt")
    for name in os.listdir(src):
        path = os.path.join(src, name)
        if os.path.isdir(path):
            shutil.copytree(path, os.path.join(dst, name), ignore=skip, dirs_exist_ok=True)
        elif not skip(src, [name]):
            shutil.copy2(path, os.path.join(dst, name))
    conns = os.path.join(dst, "connections.tsv")
    if "connections" in opts.steps and os.path.exists(conns):
        lines = open(conns).readlines()
        keep = lines[:1] + [ln for ln in lines[1:] if ln.split("\t", 1)[0] not in opts.kinds]
        with open(conns, "w") as f:
            f.writelines(keep)
    C.log(f"BASELINE copied from {src}")


def latest_results(root):
    """The newest results directory under `root` that has scenario rows."""
    if not os.path.isdir(root):
        return None
    dirs = sorted(d for d in os.listdir(root) if os.path.isfile(os.path.join(root, d, "scenarios.tsv")))
    return os.path.join(root, dirs[-1]) if dirs else None


def read_tsv(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def baseline_images(directory):
    """Each broker's image ID in its newest scenario row in `directory`."""
    images = {}
    for row in sorted(read_tsv(os.path.join(directory, "scenarios.tsv")), key=lambda r: r.get("started_utc", "")):
        if row.get("broker_image"):
            images[row["broker"]] = row["broker_image"]
    return images


def changed_kinds(current, baseline, kinds):
    """Brokers to measure: a different or unknown image, or none in the baseline."""
    return [k for k in kinds if not current.get(k) or current.get(k) != baseline.get(k)]


def _cells(rows, kinds, skip_ids=()):
    """(broker, size, scenario) -> lists of score and p99, over ok rows."""
    out = {}
    for row in rows:
        if row.get("broker") not in kinds or row.get("run_id") in skip_ids or row.get("ok") != "1":
            continue
        score = row.get("mb_s") or row.get("received_s")
        try:
            score = float(score)
        except (TypeError, ValueError):
            continue
        try:
            p99 = float(row.get("lat_p99_ms") or "")
        except ValueError:
            p99 = None
        cell = out.setdefault((row["broker"], row["size"], row["scenario"]), {"score": [], "p99": [], "mb": bool(row.get("mb_s"))})
        cell["score"].append(score)
        if p99 is not None:
            cell["p99"].append(p99)
    return out


def _median(values):
    values = sorted(values)
    if not values:
        return None
    mid = len(values) // 2
    return values[mid] if len(values) % 2 else (values[mid - 1] + values[mid]) / 2


def _delta(new, old):
    if new is None or old is None or old == 0:
        return ""
    return f"{(new - old) / old * 100:+.0f}%"


def compare_md(base_rows, out_rows, kinds, baseline_dir, threshold=10.0):
    """Re-measured cells against the baseline's cells for the same brokers.

    The results directory starts as a copy of the baseline, so its new rows
    are the ones whose run_id the baseline does not have.
    """
    base_ids = {r.get("run_id") for r in base_rows}
    old = _cells(base_rows, kinds)
    new = _cells(out_rows, kinds, skip_ids=base_ids)
    lines = [
        f"# Compared with {baseline_dir}",
        "",
        f"Score is MB/s for the large-message cells, received msg/s for the rest; medians over repeats. "
        f"`!` marks a score change past {threshold:.0f}%. One repeat is noisier than two: confirm a",
        "change with a rerun of that cell (`--scenarios ID --sizes N`) before trusting it.",
        "",
        "| Broker | Size | Scenario | Baseline | Now | Change | p99 ms baseline | p99 ms now |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    flagged = 0
    for key in sorted(new):
        n, o = new[key], old.get(key, {"score": [], "p99": []})
        ns, os_ = _median(n["score"]), _median(o["score"])
        change = _delta(ns, os_)
        mark = ""
        if ns is not None and os_:
            if abs((ns - os_) / os_ * 100) > threshold:
                mark = " !"
                flagged += 1
        fmt = (lambda v: "" if v is None else f"{v:,.1f}") if n["mb"] else (lambda v: "" if v is None else f"{v:,.0f}")
        p99 = lambda v: "" if v is None else f"{v:,.2f}"
        lines.append(f"| {key[0]} | {key[1]} | {key[2]} | {fmt(os_)} | {fmt(ns)} | {change}{mark} | {p99(_median(o['p99']))} | {p99(_median(n['p99']))} |")
    if not new:
        lines.append("| | | nothing re-measured | | | | | |")
    lines += ["", f"{len(new)} cells, {flagged} past {threshold:.0f}%.", ""]
    return "\n".join(lines)


def merge_load(C, fresh, kinds):
    """Replace these brokers' rows in load.tsv with the ones in `fresh`, as bench/run-all.py does."""
    main = os.path.join(C.OUT_DIR, "load.tsv")
    keep = open(main).readlines() if os.path.exists(main) else []
    rows = open(fresh).readlines()
    head = keep[0] if keep else rows[0]
    old = [ln for ln in keep[1:] if ln.split("\t", 1)[0] not in kinds]
    with open(main, "w") as f:
        f.writelines([head] + old + rows[1:])


if __name__ == "__main__":
    main()

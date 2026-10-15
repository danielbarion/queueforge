"""Scaling curves: the same scenario on 1 to 6 broker CPUs.

    python3 bench/scaling.py run [--kinds bun,rust] [--scenarios work-queue,routing-64]
                                 [--sizes 1,2,3,4,5,6] [--repeat 3] [--out DIR]
    python3 bench/scaling.py report DIR

Docker Desktop runs at 15 CPUs: the broker gets CPUs 0..N-1, the client the
same eight CPUs (6-13) at every size, so a bigger broker never takes the
client's headroom, and the sidecars get CPU 14. Every cell records the
client's CPU; a point whose client used most of its CPUs, or whose loadgen
threads were each close to a full core, measures the client, not the broker,
and is flagged. The host has 12 performance and 4 efficiency cores, so the
6 CPU point (14 busy CPUs) may run partly on efficiency cores.

The report gives, per broker and scenario, the median and range at each size,
the efficiency X(N) / (N * X(1)), and a least-squares fit of the Universal
Scalability Law (Gunther), X(N) = X(1) * N / (1 + s(N - 1) + k N (N - 1)):
s is contention (work that serializes), k is coherency (work that grows with
every pair of cores). The fit's peak, sqrt((1 - s) / k), and its values at
larger N are a model's extrapolation, not measurements: a bigger machine adds
costs (NUMA, the network card, more processes) that eight cores cannot show.

The PerfTest scenarios keep their load fixed (four publishers with 100
unconfirmed messages each in work-queue), so past a point a bigger broker only
waits on the same window and the curve measures fsync latency. `weak` grows the
load with the broker: per broker CPU 4 classic durable queues, 4 publishers
(512 unconfirmed each, prefetch 1024) and 4 consumers, 256-byte persistent messages, run as
two bench/loadgen processes of 2 queues each (one single-threaded loadgen
tops out near 300,000 msg/s). The rate is the lower of the summed confirm and
consume rates. That is the curve that says what a bigger server does with
more tenants. `weak-t` is the same with transient messages: no fsync in the
confirm loop, so it shows CPU scaling alone.
"""
import argparse
import csv
import json
import math
import os
import random
import statistics
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

VM_CPUS = 15
VM_MEM_MIB = 49152
CLIENT_SET, CLIENT_CPUS = "6-13", 8
SIDECAR_SET = "14"
# A client above this share of its CPUs is the limit, not the broker.
CLIENT_BOUND = 0.85
# A broker below this share of its CPUs waited on latency (the confirm window,
# fsync), so the point says nothing about capacity.
BROKER_IDLE = 0.85
# A point whose repeats spread wider than this around the median is unstable.
SPREAD_MAX = 0.15
PROJECT = [16, 32, 64, 128]
MEM = {1: "1g", 2: "2g", 3: "3g", 4: "4g", 5: "5g", 6: "6g", 8: "8g"}
# loadgen processes per broker CPU in `weak`, and a thread near a full core is the limit.
WEAK_PROCS = 2
PROC_BOUND = 0.90
WEAK = {"QUEUES": "2", "PUBS": "2", "CONS": "2", "WINDOW": "512", "PREFETCH": "1024", "BODY": "256",
        "WARMUP_MS": "5000", "MEASURE_MS": "20000", "BLOCK": "1"}
LOADGEN = "qf-loadgen:linux"


def run_weak(C, S, kind, n, image, out, cohort, ordinal, persistent=True):
    """One `weak` (or transient `weak-t`) cell. Returns a row like scenarios.run_cell's."""
    row = {"ok": "0", "err": "", "output_dir": os.path.join("weak", f"{kind}-{n}-{ordinal}-{uuid.uuid4().hex[:8]}")}
    cell_dir = os.path.join(out, row["output_dir"])
    os.makedirs(cell_dir)
    sampler = None
    try:
        if not S.start_broker(kind, n, MEM[n], image):
            row["err"] = "boot"
            return row
        ip = C.run(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", S.BROKER]).stdout.strip()
        C.run(["docker", "rm", "-f", S.CLIENT], check=False, timeout=40)
        C.run(["docker", "run", "-d", "--name", S.CLIENT, "--network", S.NET, "--cpuset-cpus", CLIENT_SET,
               "--cpus", str(CLIENT_CPUS), "--memory", "2g", "--ulimit", "nofile=1048576:1048576", LOADGEN])
        sampler = S.Sampler(int(WEAK["WARMUP_MS"]) // 1000, kind)
        sampler.start()
        env = " ".join(f"{k}={v}" for k, v in {**WEAK, "PERSISTENT": "1" if persistent else "0"}.items())
        procs = WEAK_PROCS * n
        row["client_procs"] = procs
        script = (f"for i in $(seq 0 {procs - 1}); do {env} QPREFIX=p$i- loadgen {ip} 5672 > /tmp/o$i 2>&1 & done; wait; "
                  f"for i in $(seq 0 {procs - 1}); do echo \"== $i\"; cat /tmp/o$i; done")
        limit = (int(WEAK["WARMUP_MS"]) + int(WEAK["MEASURE_MS"])) / 1000 + 60
        proc = C.run(["docker", "exec", S.CLIENT, "sh", "-c", script], check=False, timeout=limit)
        sampler.stop_flag.set()
        sampler.join(timeout=15)
        text = proc.stdout or ""
        open(os.path.join(cell_dir, "loadgen.out"), "w").write(text + (proc.stderr or ""))
        results = [dict(kv.split("=", 1) for kv in line.split()[1:] if "=" in kv)
                   for line in text.splitlines() if line.startswith("RESULT ")]
        bcpu, ccpu = sampler.cpu()
        row.update(broker_cpu=bcpu, client_cpu=ccpu,
                   anon_mib=f"{sampler.peak_anon / 1048576:.0f}" if sampler.peak_anon else "")
        oom, running, status = C.container_state(S.BROKER)
        if oom == "true":
            row["err"] = "oom"
        elif not running:
            row["err"] = f"died-{status}"
        elif len(results) != procs or any(r.get("ok") != "1" for r in results):
            row["err"] = f"loadgen {len(results)}/{procs} ok"
        if results:
            confirm = sum(float(r.get("confirm_s", 0)) for r in results)
            consume = sum(float(r.get("consume_s", 0)) for r in results)
            row["received_s"] = f"{min(confirm, consume):.1f}"
            p99 = sorted(float(r.get("p99_us", 0)) for r in results)
            row["lat_p99_ms"] = f"{p99[len(p99) // 2] / 1000:.2f}"
        row["ok"] = "1" if not row["err"] else "0"
        C.log(f"CELL weak{'' if persistent else '-t'} {kind} {n} ok={row['ok']} err={row['err']} rate={row.get('received_s')} "
              f"cpu={bcpu} client={ccpu} p99={row.get('lat_p99_ms')}")
        return row
    except Exception as exc:
        row["err"] = "harness-" + type(exc).__name__
        C.log(f"FAILED weak {kind} {n}: {exc}")
        return row
    finally:
        if sampler is not None:
            sampler.stop_flag.set()
            sampler.join(timeout=15)
        C.run(["docker", "rm", "-fv", S.BROKER, S.CLIENT], check=False, timeout=40)


def cmd_run(args):
    os.environ.setdefault("QF_BENCH_OUT", args.out)
    import common as C
    import scenarios as S

    os.makedirs(args.out, exist_ok=True)
    C.OUT_DIR = args.out
    sizes = [int(x) for x in args.sizes.split(",")]
    if max(sizes) > CLIENT_SET_FIRST():
        raise SystemExit(f"broker sizes above {CLIENT_SET_FIRST()} overlap the client CPUs")
    C.broker_cpuset = lambda cpus: "0" if cpus == 1 else f"0-{cpus - 1}"
    if args.env:
        base_env = C.broker_env
        C.broker_env = lambda kind: base_env(kind) + [x for e in args.env for x in ("-e", e)]
    C.client_cpus = lambda: (CLIENT_SET, str(CLIENT_CPUS))
    if not C.docker_up():
        raise SystemExit("docker is down")
    if C.vm_ncpu != VM_CPUS or C.vm_mem < (VM_MEM_MIB - 4096) * 1024 ** 2:
        open(os.path.join(args.out, "was-running.txt"), "w").write("\n".join(C._running_names()) + "\n")
        if not C._quit_docker():
            raise SystemExit("docker did not quit")
        C._set_vm(VM_CPUS, VM_MEM_MIB)
        if not C._start_docker() or not C.docker_up() or C.vm_ncpu != VM_CPUS:
            raise SystemExit(f"vm not resized: ncpu={C.vm_ncpu}")
    C._sidecars(SIDECAR_SET)
    C.log(f"PROGRESS scaling vm ncpu={C.vm_ncpu} mem={C.vm_mem}")
    client = S.image_digest(S.PERF_TEST)
    images = {kind: S.image_digest(args.image.get(kind) or C.IMAGES[kind]) for kind in args.kinds}
    scens = [{"id": sid} if sid in ("weak", "weak-t") else next(s for s in S.SCENARIOS if s["id"] == sid) for sid in args.scenarios]
    cohort = uuid.uuid4().hex
    # Every repeat runs every cell once, in a fresh random order: drift in the
    # host (thermal state, background work) spreads over all points alike.
    rng = random.Random(args.seed)
    for r in range(args.repeat):
        cells = [(k, n, sc) for k in args.kinds for n in sizes for sc in scens]
        rng.shuffle(cells)
        for i, (kind, n, sc) in enumerate(cells):
            C.log(f"PROGRESS scaling repeat {r + 1}/{args.repeat} cell {i + 1}/{len(cells)} {kind} {n} {sc['id']}")
            if sc["id"] in ("weak", "weak-t"):
                row = run_weak(C, S, kind, n, images[kind], args.out, cohort, r + 1, persistent=sc["id"] == "weak")
            else:
                row = S.run_cell(kind, n, MEM[n], f"{n} CPU", sc, cohort_id=cohort, broker_image=images[kind],
                                 client_image=client, expected_repeats=args.repeat, repeat_ordinal=r + 1) or {}
            line = {"kind": kind, "cpus": n, "scenario": sc["id"], "repeat": r + 1,
                    "ok": row.get("ok"), "err": row.get("err"), "received_s": row.get("received_s"),
                    "lat_p99_ms": row.get("lat_p99_ms"), "broker_cpu": row.get("broker_cpu"),
                    "client_cpu": row.get("client_cpu"), "anon_mib": row.get("anon_mib"),
                    "output_dir": row.get("output_dir"), "broker_image": images[kind],
                    "client_procs": row.get("client_procs")}
            with open(os.path.join(args.out, "scaling.jsonl"), "a") as f:
                f.write(json.dumps(line) + "\n")
    if not args.keep_vm:
        C.restore_small()
    print(report(args.out))


def CLIENT_SET_FIRST():
    return int(CLIENT_SET.split("-")[0])


def usl_fit(points):
    """Least squares for N / C(N) - 1 = s (N - 1) + k N (N - 1), C(N) = X(N) / X(1).

    Returns (s, k), both clamped at zero, or None with fewer than three sizes.
    """
    base = points.get(1)
    if not base or len(points) < 3:
        return None
    rows = [(n - 1, n * (n - 1), n / (x / base) - 1) for n, x in points.items() if n > 1]
    a11 = sum(u * u for u, _v, _y in rows)
    a12 = sum(u * v for u, v, _y in rows)
    a22 = sum(v * v for _u, v, _y in rows)
    b1 = sum(u * y for u, _v, y in rows)
    b2 = sum(v * y for _u, v, y in rows)
    det = a11 * a22 - a12 * a12
    if abs(det) < 1e-12:
        return None
    s = (b1 * a22 - b2 * a12) / det
    k = (a11 * b2 - a12 * b1) / det
    if k < 0:
        # No measurable coherency cost: refit contention alone.
        k = 0.0
        s = b1 / a11 if a11 else 0.0
    if s < 0:
        s = 0.0
        k = b2 / a22 if a22 else 0.0
        k = max(k, 0.0)
    return s, k


def usl(x1, s, k, n):
    return x1 * n / (1 + s * (n - 1) + k * n * (n - 1))


def report(out):
    rows = [json.loads(line) for line in open(os.path.join(out, "scaling.jsonl"))]
    lines = ["# Scaling", "",
             f"Broker on CPUs 0..N-1; client on {CLIENT_CPUS} fixed CPUs. Median of repeats with its range.",
             f"Efficiency is X(N) / (N X(1)). `client-bound`: the client used over {CLIENT_BOUND:.0%} of its CPUs;"
             f" `unstable`: the range is wider than {SPREAD_MAX:.0%} of the median; `broker-idle`: the broker used under"
             f" {BROKER_IDLE:.0%} of its CPUs, so latency, not capacity, set the rate. All three are excluded from the fit.",
             "USL: X(N) = X(1) N / (1 + s(N-1) + k N(N-1)). Peak and projections are the model's extrapolation.", ""]
    groups = {}
    for r in rows:
        groups.setdefault((r["scenario"], r["kind"]), []).append(r)
    summary = []
    for (scenario, kind) in sorted(groups):
        cells = groups[(scenario, kind)]
        by_n = {}
        for r in cells:
            by_n.setdefault(r["cpus"], []).append(r)
        lines += [f"## {scenario}, {kind}", "",
                  "| CPUs | msg/s | range | n | per CPU | efficiency | broker CPU % | client CPU % | p99 ms | flags |",
                  "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
        med = {}
        fit_points = {}
        for n in sorted(by_n):
            good = [r for r in by_n[n] if r.get("ok") == "1" and r.get("received_s")]
            if not good:
                lines.append(f"| {n} | – | | 0 | | | | | | failed: {', '.join(sorted({r.get('err') or '?' for r in by_n[n]}))} |")
                continue
            xs = [float(r["received_s"]) for r in good]
            m = statistics.median(xs)
            med[n] = m
            cpu_b = statistics.median([float(r["broker_cpu"]) for r in good if r.get("broker_cpu")] or [0])
            cpu_c_list = [float(r["client_cpu"]) for r in good if r.get("client_cpu")]
            cpu_c = statistics.median(cpu_c_list) if cpu_c_list else None
            p99 = statistics.median([float(r["lat_p99_ms"]) for r in good if r.get("lat_p99_ms")] or [0])
            flags = []
            procs = max((int(r.get("client_procs") or 0) for r in good), default=0)
            if cpu_c is not None and (cpu_c >= CLIENT_BOUND * CLIENT_CPUS * 100 or (procs and cpu_c >= PROC_BOUND * 100 * procs)):
                flags.append("client-bound")
            if cpu_c is None:
                flags.append("no-client-cpu")
            if cpu_b and cpu_b < BROKER_IDLE * 100 * n:
                flags.append("broker-idle")
            if len(xs) > 1 and (max(xs) - min(xs)) / m > SPREAD_MAX:
                flags.append("unstable")
            if len(good) < len(by_n[n]):
                flags.append(f"{len(by_n[n]) - len(good)} failed")
            if not ({"client-bound", "unstable", "broker-idle"} & set(flags)):
                fit_points[n] = m
            eff = m / (n * med[1]) if 1 in med else None
            lines.append(
                f"| {n} | {m:,.0f} | {min(xs):,.0f}–{max(xs):,.0f} | {len(xs)} | {m / n:,.0f} | "
                f"{'' if eff is None else f'{eff:.2f}'} | {cpu_b:.0f} | {'' if cpu_c is None else f'{cpu_c:.0f}'} | "
                f"{p99:.2f} | {', '.join(flags)} |")
        fit = usl_fit(fit_points)
        lines.append("")
        if fit:
            s, k = fit
            x1 = fit_points[1]
            peak = math.sqrt((1 - s) / k) if k > 0 and s < 1 else math.inf
            proj = ", ".join(f"{n}: {usl(x1, s, k, n):,.0f}" for n in PROJECT)
            resid = max(abs(usl(x1, s, k, n) - x) / x for n, x in fit_points.items())
            lines.append(f"USL fit over {sorted(fit_points)}: s = {s:.4f}, k = {k:.5f}, "
                         f"worst point off by {resid:.1%}; peak at {'never' if peak == math.inf else f'N = {peak:.1f}'}; "
                         f"projected {proj}.")
            summary.append((scenario, kind, s, k, peak, {n: usl(x1, s, k, n) for n in PROJECT}, med))
        else:
            lines.append("USL fit: not enough clean sizes (needs 1 CPU and two more).")
        lines.append("")
    if summary:
        lines += ["## Summary", "", "| scenario | broker | X(1) | X(max N) measured | s | k | peak N | X(32) model | X(128) model |",
                  "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
        for scenario, kind, s, k, peak, proj, med in summary:
            top = max(med)
            lines.append(f"| {scenario} | {kind} | {med.get(1, 0):,.0f} | {med[top]:,.0f} ({top}) | {s:.4f} | {k:.5f} | "
                         f"{'∞' if peak == math.inf else f'{peak:.1f}'} | {proj[32]:,.0f} | {proj[128]:,.0f} |")
    text = "\n".join(lines) + "\n"
    open(os.path.join(out, "scaling.md"), "w").write(text)
    return text


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--kinds", default="bun,rust", type=lambda v: v.split(","))
    r.add_argument("--scenarios", default="work-queue,routing-64,fanout-20,heavy,quorum", type=lambda v: v.split(","))
    r.add_argument("--sizes", default="1,2,3,4,5,6")
    r.add_argument("--repeat", type=int, default=3)
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--out", default=os.path.join(HERE, "results", "scaling-" + time.strftime("%Y-%m-%dT%H%M")))
    r.add_argument("--image", action="append", default=[], help="kind=image, to test a build other than the default")
    r.add_argument("--keep-vm", action="store_true", help="leave Docker Desktop at 15 CPUs afterwards")
    r.add_argument("--env", action="append", default=[], help="KEY=VALUE for the broker container, to A/B a setting")
    p = sub.add_parser("report")
    p.add_argument("out")
    args = ap.parse_args()
    if args.cmd == "run":
        args.out = os.path.abspath(args.out)
        args.image = dict(item.split("=", 1) for item in args.image)
        # One benchmark on the machine at a time, as bench/run.py.
        import run as R
        R.take_lease("bench/scaling.py", "scaling curves")
        try:
            cmd_run(args)
        finally:
            R.release_lease()
    else:
        print(report(args.out))


if __name__ == "__main__":
    main()

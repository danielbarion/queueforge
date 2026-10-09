#!/usr/bin/env python3
"""Application scenarios with RabbitMQ PerfTest, one broker at a time.

PerfTest (pivotalrabbitmq/perf-test) is RabbitMQ's own load tool. Each cell
starts a fresh broker container, runs one scenario from a client container on
other CPUs, and records PerfTest's per-second samples plus the broker's cgroup
memory and CPU. The first `warm` seconds of every run are dropped.

Usage:
  bench/scenarios.py all              1 CPU / 1 GiB matrix and the 4 CPU / 4 GiB heavy subset
  bench/scenarios.py one <id> [kind]  one scenario at 1 CPU / 1 GiB, for a quick check
Needs Docker Desktop at 8 CPUs for `all` (see common.ensure_big).
"""
import json
import os
import re
import socket
import statistics
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

PERF_TEST = "pivotalrabbitmq/perf-test:2.25.0"
NET = "qf-sc"
BROKER = "qf-sc-broker"
CLIENT = "qf-sc-client"
HOST_PORT = 35700
KINDS = ["rabbit", "rust", "bun", "php"]

# id, title, what it stands for, PerfTest arguments, seconds, warm seconds.
# `rate` is the offered total when producers are paced; `bytes` scores MB/s.
SCENARIOS = [
    dict(id="work-queue", title="Work queue",
         real="Order processing: 4 services publish durable 1 KiB jobs with confirms, 4 workers ack each one.",
         args="-x 4 -y 4 -u jobs -f persistent -s 1024 -c 100 -q 100"),
    dict(id="telemetry", title="Fire-and-forget telemetry",
         real="Metrics and logs: 4 producers, no confirms, transient 256 B messages, auto-ack consumers.",
         args="-x 4 -y 4 -u telemetry -s 256 -a"),
    dict(id="fanout-20", title="Broadcast to 20 services",
         real="Domain events on a fanout exchange, each copied to 20 durable subscriber queues.",
         args="-t fanout -e events -qp events-%d -qpf 1 -qpt 20 -x 1 -y 20 -f persistent -s 512 -c 100 -q 100"),
    dict(id="routing-64", title="64 queues, own publisher each",
         real="Per-tenant queues: 64 publishers and 64 consumers on a direct exchange, durable 512 B.",
         args="-qp tenant-%d -qpf 1 -qpt 64 -x 64 -y 64 -f persistent -s 512 -c 20 -q 50"),
    dict(id="large-64k", title="64 KiB messages",
         real="Documents and images: 2 producers of durable 64 KiB bodies, scored in MB/s.",
         args="-x 2 -y 2 -u blobs -f persistent -s 65536 -c 20 -q 20", bytes=65536),
    dict(id="large-1m", title="1 MiB messages",
         real="Large payloads: 1 producer of durable 1 MiB bodies, 4 confirms in flight, scored in MB/s.",
         args="-x 1 -y 1 -u big -f persistent -s 1048576 -c 4 -q 4", bytes=1048576),
    dict(id="latency-1k", title="Latency at 1,000 msg/s",
         real="A steady API workload: 1,000 durable 1 KiB msg/s with confirms. Scored on end-to-end latency.",
         args="-x 1 -y 1 -u steady -f persistent -s 1024 -c 100 -q 100 -r 1000", rate=1000),
    dict(id="latency-10k", title="Latency at 10,000 msg/s",
         real="A busy API workload: 2 producers at 5,000 durable 1 KiB msg/s each.",
         args="-x 2 -y 2 -u busy -f persistent -s 1024 -c 100 -q 100 -r 5000", rate=10000),
    dict(id="many-queues", title="500 queues, 1,000 connections",
         real="Many small services: 500 queues, each with its own producer at 10 msg/s and its own consumer.",
         args="-qp svc-%d -qpf 1 -qpt 500 -x 500 -y 500 -f persistent -s 512 -c 10 -q 10 -r 10",
         rate=5000, seconds=50, warm=20),
    dict(id="slow-consumers", title="Slow workers, prefetch 1",
         real="20 workers that each spend 2 ms per job with prefetch 1, fed 5,000 msg/s.",
         args="-x 1 -y 20 -u tasks -f persistent -s 512 -c 100 -q 1 -L 2000 -r 5000", rate=5000),
    dict(id="quorum", title="Quorum queue",
         real="The replicated queue type, here on one node: 4 producers, 4 consumers, durable 1 KiB.",
         args="-x 4 -y 4 -u orders-qq -qq -f persistent -s 1024 -c 100 -q 100"),
    dict(id="stream", title="Stream queue",
         real="An append-only log read by 2 consumers from the start, over AMQP 0-9-1.",
         args="-x 2 -y 2 -u audit-log -sq -f persistent -s 1024 -c 100 -q 100"),
    dict(id="priority", title="Priority queue",
         real="x-max-priority=10, every message at priority 5, 2 producers and 2 consumers.",
         args="-x 2 -y 2 -u prio -qa x-max-priority=10 -mp priority=5 -f persistent -s 1024 -c 100 -q 100"),
    dict(id="transactions", title="Transactions",
         real="tx.select publishers committing every 10 messages, transactional consumers acking every 10.",
         args="-x 2 -y 2 -u txq -f persistent -s 1024 -m 10 -n 10 -q 100"),
    dict(id="heavy", title="Heavy mixed load",
         real="64 producers and 64 consumers across 16 durable queues, 200 confirms in flight each.",
         args="-qp heavy-%d -qpf 1 -qpt 16 -x 64 -y 64 -f persistent -s 1024 -c 200 -q 200"),
    dict(id="backlog", title="Backlog fill and drain",
         real="A consumer outage: 300,000 durable 1 KiB messages pile up, then 4 consumers drain them.",
         phases=[
             ("-x 4 -y 0 -u backlog -f persistent -s 1024 -c 200 -C 75000", None),
             # -D is per consumer, and a consumer that stops early keeps its
             # prefetch. A time limit drains the queue; the rate is over the
             # seconds that moved messages.
             ("-x 0 -y 4 -u backlog -q 200", 90),
         ], seconds=240),
]
HEAVY = ["work-queue", "telemetry", "fanout-20", "routing-64", "heavy", "quorum", "many-queues"]
SIZES = [(1, "1g", "1 CPU / 1 GiB"), (4, "4g", "4 CPU / 4 GiB")]

COLS = [
    "broker", "size", "scenario", "ok", "err", "sent_s", "confirmed_s", "received_s", "mb_s",
    "lat_p50_ms", "lat_p99_ms", "confirm_p50_ms", "confirm_p99_ms", "peak_mib", "anon_mib", "broker_cpu",
    "client_cpu", "kept", "seconds",
]

SAMPLE = re.compile(
    r"time (?P<t>[\d.]+) s(?:, sent: (?P<sent>\d+) msg/s)?"
    r"(?:, returned: \d+ msg/s)?"
    r"(?:, confirmed: (?P<conf>\d+) msg/s)?"
    r"(?:, nacked: \d+ msg/s)?"
    r"(?:, received: (?P<recv>\d+) msg/s)?"
    r"(?:, min/median/75th/95th/99th/max consumer latency: (?P<lat>[\d/]+) µs)?"
    r"(?:, (?:min/median/75th/95th/99th/max )?confirm latency: (?P<clat>[\d/]+) µs)?"
)


def tsv_path():
    return os.path.join(C.OUT_DIR, "scenarios.tsv")


def write_row(row):
    path = tsv_path()
    fresh = not os.path.exists(path)
    with open(path, "a") as f:
        if fresh:
            f.write("\t".join(COLS) + "\n")
        f.write("\t".join(str(row.get(c, "")) for c in COLS) + "\n")


def amqp_ready(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _oom, running, _status = C.container_state(BROKER)
        if not running:
            return False
        try:
            with socket.create_connection(("127.0.0.1", HOST_PORT), timeout=2) as s:
                s.sendall(b"AMQP\x00\x00\x09\x01")
                s.settimeout(3)
                head = s.recv(7)
                if len(head) == 7 and head[0] == 1:
                    return True
        except OSError:
            pass
        time.sleep(0.5)
    return False


def start_broker(kind, cpus, mem):
    C.run(["docker", "rm", "-f", BROKER], check=False, timeout=40)
    C.run(["docker", "network", "create", NET], check=False)
    cmd = [
        "docker", "run", "-d", "--name", BROKER, "--network", NET,
        "--cpuset-cpus", C.broker_cpuset(cpus), "--cpus", str(cpus),
        "--memory", mem, "--memory-swap", mem,
        "--ulimit", "nofile=1048576:1048576", "--sysctl", "net.core.somaxconn=4096",
        "-p", f"127.0.0.1:{HOST_PORT}:5672",
    ] + C.broker_env(kind) + [C.IMAGES[kind]]
    p = C.run(cmd, check=False, timeout=60)
    if p.returncode != 0:
        C.log("start failed " + (p.stderr or "")[-300:])
        return False
    if not amqp_ready():
        return False
    # RabbitMQ accepts AMQP before its boot steps finish. Give every broker the same settle.
    time.sleep(3)
    return True


class Sampler(threading.Thread):
    """Peak broker memory once a second, and CPU over the measured window."""

    def __init__(self, warm):
        super().__init__(daemon=True)
        self.warm = warm
        self.peak = 0
        self.peak_anon = 0
        self.stop_flag = threading.Event()
        self.cpu_marks = {}

    def run(self):
        started = time.monotonic()
        marked = False
        while not self.stop_flag.is_set():
            mem = C.mem_bytes(BROKER)
            if mem:
                self.peak = max(self.peak, mem)
            anon = C.anon_bytes(BROKER)
            if anon:
                self.peak_anon = max(self.peak_anon, anon)
            if not marked and time.monotonic() - started >= self.warm:
                self.cpu_marks["a"] = (time.monotonic(), C.cpu_usec(BROKER), C.cpu_usec(CLIENT))
                marked = True
            self.stop_flag.wait(1.0)
        self.cpu_marks["b"] = (time.monotonic(), C.cpu_usec(BROKER), C.cpu_usec(CLIENT))

    def cpu(self):
        a, b = self.cpu_marks.get("a"), self.cpu_marks.get("b")
        if not a or not b or b[0] <= a[0]:
            return "", ""
        dt = b[0] - a[0]

        def pct(x, y):
            return f"{(y - x) / (dt * 1e6) * 100:.0f}" if x is not None and y is not None else ""

        return pct(a[1], b[1]), pct(a[2], b[2])


def perf_test(args, seconds, out_path, limited):
    C.run(["docker", "rm", "-f", CLIENT], check=False, timeout=40)
    cset, ccpus = C.client_cpus()
    cmd = [
        "docker", "run", "--rm", "--name", CLIENT, "--network", NET,
        "--cpuset-cpus", cset, "--cpus", ccpus, "--memory", "4g",
        "--ulimit", "nofile=1048576:1048576",
        "-e", "JAVA_TOOL_OPTIONS=-Xmx3g",
        PERF_TEST, "-h", f"amqp://admin:devpassword12@{BROKER}:5672", "-i", "1", "-ad", "false",
    ] + args.split()
    if not limited:
        cmd += ["-z", str(seconds)]
    with open(out_path, "w") as fh:
        p = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT)
        try:
            p.wait(timeout=seconds + 120)
        except subprocess.TimeoutExpired:
            C.run(["docker", "rm", "-f", CLIENT], check=False)
            p.kill()
            return 124
    return p.returncode


def parse(path):
    """Per-second samples and the error blocks that are not the client's own teardown.

    A block starts at an ERROR or "caught exception" line. It is the client
    tearing down when its cause is PerfTest closing its own connection, or when
    it comes after the last sample of a run that reached its time limit.
    """
    rows = []
    blocks = []  # [line index, first line, teardown]
    last_sample = -1
    timed = False
    for i, line in enumerate(open(path, errors="replace")):
        m = SAMPLE.search(line)
        if m:
            rows.append(m.groupdict())
            last_sample = i
            continue
        if "Reached time limit" in line:
            timed = True
        if " ERROR " in line or "caught exception" in line:
            blocks.append([i, line.strip()[:160], False])
        elif blocks and "Caused by" in line and ("Closed by PerfTest" in line or "clean connection shutdown" in line):
            blocks[-1][2] = True
    errors = [b[1] for b in blocks if not b[2] and not (timed and b[0] > last_sample)]
    return rows, errors


def pct(field, idx, rows):
    vals = [int(r[field].split("/")[idx]) for r in rows if r.get(field)]
    return statistics.median(vals) / 1000 if vals else None


def summarize(samples, warm):
    steady = [r for r in samples if float(r["t"]) > warm]
    if len(steady) > 2:
        steady = steady[:-1]  # the last sample is often a partial second
    if not steady:
        return {}

    def avg(key):
        vals = [int(r[key]) for r in steady if r.get(key) is not None]
        return sum(vals) / len(vals) if vals else None

    return {
        "sent_s": avg("sent"), "confirmed_s": avg("conf"), "received_s": avg("recv"),
        "lat_p50_ms": pct("lat", 1, steady), "lat_p99_ms": pct("lat", 4, steady),
        "confirm_p50_ms": pct("clat", 1, steady), "confirm_p99_ms": pct("clat", 4, steady),
        "seconds": len(steady),
    }


def active_rate(samples, key):
    """Messages per second over the seconds that moved any: a fill or a drain."""
    vals = [int(r[key]) for r in samples if r.get(key) is not None]
    moving = [v for v in vals if v > 0]
    return (sum(vals) / len(moving), len(moving)) if moving else (0.0, 0)


def fmt(v, digits=1):
    if v is None or v == "":
        return ""
    return f"{v:.{digits}f}" if isinstance(v, float) else str(v)


def run_cell(kind, cpus, mem, size, sc):
    row = {"broker": kind, "size": size, "scenario": sc["id"], "ok": "0", "err": ""}
    if not start_broker(kind, cpus, mem):
        oom, _r, status = C.container_state(BROKER)
        row["err"] = f"boot-{status}" + ("-oom" if oom == "true" else "")
        write_row(row)
        C.log(f"FAILED {kind} {size} {sc['id']} {row['err']}")
        return row
    warm = sc.get("warm", 5)
    seconds = sc.get("seconds", 25)
    sampler = Sampler(warm)
    sampler.start()
    tag = f"{kind}-{cpus}-{sc['id']}"
    phases = sc.get("phases")
    errs = []
    codes = []
    if phases:
        paths = []
        for i, (args, limit) in enumerate(phases):
            path = os.path.join(C.OUT_DIR, "cells", f"{tag}-{i}.out")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            codes.append(perf_test(args, limit or seconds, path, limited=limit is None))
            paths.append(path)
        fill, _ = parse(paths[0])
        drain, _ = parse(paths[1])
        errs = parse(paths[0])[1] + parse(paths[1])[1]
        sent, _n1 = active_rate(fill, "sent")
        recv, _n2 = active_rate(drain, "recv")
        total_sent = sum(int(r["sent"] or 0) for r in fill)
        total_recv = sum(int(r["recv"] or 0) for r in drain)
        stats = {"sent_s": sent, "received_s": recv, "seconds": len(fill) + len(drain),
                 "lat_p50_ms": None, "lat_p99_ms": None}
        stats["confirm_p50_ms"] = pct("clat", 1, fill)
        stats["confirm_p99_ms"] = pct("clat", 4, fill)
        # The last partial second is not printed, so a full run sums to a little under 300,000.
        row["kept"] = "yes" if total_recv >= 297000 else f"sent {total_sent} got {total_recv}"
    else:
        path = os.path.join(C.OUT_DIR, "cells", f"{tag}.out")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        codes.append(perf_test(sc["args"], seconds, path, limited=False))
        samples, errs = parse(path)
        stats = summarize(samples, warm)
        if sc.get("rate") and stats.get("received_s") is not None:
            row["kept"] = "yes" if stats["received_s"] >= 0.95 * sc["rate"] else "no"
        elif stats.get("sent_s"):
            fan = 20 if sc["id"] == "fanout-20" else 1
            row["kept"] = "yes" if stats["received_s"] >= 0.95 * stats["sent_s"] * fan else "no"
    sampler.stop_flag.set()
    sampler.join(timeout=15)
    oom, running, status = C.container_state(BROKER)
    bcpu, ccpu = sampler.cpu()
    row.update({k: fmt(v, 2 if k.endswith("_ms") else 1) for k, v in stats.items()})
    if sc.get("bytes") and stats.get("received_s"):
        row["mb_s"] = fmt(stats["received_s"] * sc["bytes"] / 1e6, 1)
    row["peak_mib"] = f"{sampler.peak / 1048576:.0f}" if sampler.peak else ""
    row["anon_mib"] = f"{sampler.peak_anon / 1048576:.0f}" if sampler.peak_anon else ""
    row["broker_cpu"], row["client_cpu"] = bcpu, ccpu
    if oom == "true":
        row["err"] = "oom"
    elif not running:
        row["err"] = f"died-{status}"
    elif any(c != 0 for c in codes):
        row["err"] = "client-exit-" + ",".join(str(c) for c in codes)
    elif errs:
        row["err"] = "exception"
    elif not stats.get("received_s"):
        row["err"] = "no-delivery"
    row["ok"] = "1" if not row["err"] else "0"
    if errs:
        C.log(f"EXCEPTION {kind} {sc['id']}: {errs[0]}")
    if row["err"]:
        logs = C.run(["docker", "logs", "--tail", "15", BROKER], check=False)
        C.log("broker-log " + ((logs.stdout or "") + (logs.stderr or ""))[-600:].replace("\n", " | "))
    C.run(["docker", "rm", "-f", BROKER], check=False, timeout=40)
    write_row(row)
    C.log(
        f"CELL {kind} {size} {sc['id']} ok={row['ok']} err={row['err']} sent={row.get('sent_s')} "
        f"recv={row.get('received_s')} mb={row.get('mb_s', '')} p50={row.get('lat_p50_ms')} "
        f"p99={row.get('lat_p99_ms')} mib={row['peak_mib']} anon={row['anon_mib']} cpu={bcpu} client={ccpu} kept={row.get('kept')}"
    )
    return row


def run_matrix(kinds=KINDS):
    C.run(["docker", "pull", "-q", PERF_TEST], check=False, timeout=600)
    meta = {"perf_test": PERF_TEST, "images": C.image_ids(), "vm_ncpu": C.vm_ncpu, "vm_mem": C.vm_mem,
            "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "scenarios": SCENARIOS}
    json.dump(meta, open(os.path.join(C.OUT_DIR, "scenarios-meta.json"), "w"), indent=1)
    for cpus, mem, size in SIZES:
        for sc in SCENARIOS:
            if cpus > 1 and sc["id"] not in HEAVY:
                continue
            for kind in kinds:
                run_cell(kind, cpus, mem, size, sc)
    C.log("DONE scenarios")


def rescore():
    """Clear `exception` on cells whose only errors came after PerfTest stopped."""
    path = tsv_path()
    with open(path) as f:
        lines = [ln.rstrip("\n").split("\t") for ln in f]
    head, body = lines[0], lines[1:]
    ix = {c: i for i, c in enumerate(head)}
    for row in body:
        if row[ix["err"]] != "exception":
            continue
        cpus = 1 if row[ix["size"]].startswith("1 ") else 4
        cell = os.path.join(C.OUT_DIR, "cells", f"{row[ix['broker']]}-{cpus}-{row[ix['scenario']]}.out")
        if os.path.exists(cell) and not parse(cell)[1]:
            row[ix["err"]] = ""
            row[ix["ok"]] = "1"
            C.log(f"RESCORE {row[ix['broker']]} {row[ix['size']]} {row[ix['scenario']]} ok")
    with open(path, "w") as f:
        for row in [head] + body:
            f.write("\t".join(row) + "\n")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "rescore":
        rescore()
        return
    if not C.docker_up():
        raise SystemExit("docker is down")
    if cmd == "one":
        sc = next(s for s in SCENARIOS if s["id"] == sys.argv[2])
        for kind in (sys.argv[3:] or KINDS):
            run_cell(kind, 1, "1g", "1 CPU / 1 GiB", sc)
        return
    if cmd == "matrix":  # assumes the VM is already large
        run_matrix(sys.argv[2:] or KINDS)
        return
    if cmd == "all":
        C.ensure_big()
        try:
            run_matrix()
        finally:
            C.run(["docker", "rm", "-f", BROKER, CLIENT], check=False)
            C.restore_small()
        return
    raise SystemExit(__doc__)


if __name__ == "__main__":
    main()

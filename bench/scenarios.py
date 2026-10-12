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
import argparse
import csv
import hashlib
import json
import os
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

PERF_TEST = "pivotalrabbitmq/perf-test:2.25.0"
NET = "qf-sc"
BROKER = "qf-sc-broker"
CLIENT = "qf-sc-client"
HOST_PORT = 35700
KINDS = ["rabbit", "rust", "bun", "php"]
CPU_PROFILE_DIR = "/var/lib/queueforge/profiles"
CPU_PROFILE_FLAGS = "--cpu-prof --cpu-prof-dir=/var/lib/queueforge/profiles --preload=/var/lib/queueforge/profiles/exit.ts"
# A diagnostic-only PID 1 gives workers time to finish profiles after the
# supervisor signals them and exits. Bun's default filenames include PID/time.
CPU_PROFILE_CMD = """mkdir -p /var/lib/queueforge/profiles || exit 1
cat > /var/lib/queueforge/profiles/exit.ts <<'PROFILE_EXIT'
// Diagnostic-only: Bun's default signal exit does not finish CPU profiles.
process.once("SIGTERM", () => process.exit(0));
process.once("SIGINT", () => process.exit(0));
PROFILE_EXIT
bun src/main.ts --config /etc/queueforge/queueforge.toml --dev-bootstrap &
broker_pid=$!
stop_profile() {
    trap '' TERM INT
    bun_pids=$(ps -o pid,comm | awk '$2 == "bun" {print $1}')
    printf '%s\n' "$bun_pids" > /var/lib/queueforge/profiles/expected-pids.txt
    kill -TERM "$broker_pid" 2>/dev/null || true
    # The preload can exit the supervisor before its normal child signal hook.
    # Signal the complete snapshot explicitly while shell PID 1 stays alive.
    for pid in $bun_pids; do
        [ "$pid" = "$broker_pid" ] || kill -TERM "$pid" 2>/dev/null || true
    done
    wait "$broker_pid" 2>/dev/null || true
    attempt=0
    while [ "$attempt" -lt 100 ]; do
        alive=0
        for pid in $bun_pids; do
            if kill -0 "$pid" 2>/dev/null; then
                state=$(awk '/^State:/ {print $2}' "/proc/$pid/status" 2>/dev/null)
                [ "$state" = Z ] || alive=1
            fi
        done
        [ "$alive" -eq 0 ] && break
        sleep 0.1
        attempt=$((attempt + 1))
    done
    if [ "$alive" -eq 0 ]; then
        echo complete > /var/lib/queueforge/profiles/shutdown-status.txt
    else
        echo timed-out > /var/lib/queueforge/profiles/shutdown-status.txt
        echo 'cpu-profile: worker exit wait expired' >&2
    fi
    exit 0
}
trap stop_profile TERM INT
wait "$broker_pid"
"""

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
COLS += ["run_id", "cohort_id", "broker_image", "client_image", "harness_sha256", "scenario_sha256", "resources_sha256", "started_utc", "output_dir", "expected_repeats", "repeat_ordinal"]


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def image_digest(image):
    digest = C.run(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise RuntimeError(f"Invalid immutable image ID for {image}")
    return digest


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def run_provenance(kind, cpus, mem, sc, run_id, cohort_id, broker_image, client_image):
    harness = hashlib.sha256()
    for name in ("scenarios.py", "common.py", "report.py", "rabbitmq.conf"):
        harness.update(name.encode())
        with open(os.path.join(C.BENCH, name), "rb") as f:
            harness.update(f.read())
    commit = C.run(["git", "-C", C.ROOT, "rev-parse", "HEAD"], check=False).stdout.strip()
    dirty = C.run(["git", "-C", C.ROOT, "status", "--porcelain"], check=False).stdout.strip()
    source_hash = hashlib.sha256()
    paths = ["bun/src", "php/src", "rust", "bun/Dockerfile", "php/Dockerfile", "rust/Dockerfile", "docker-compose.bench.yml"]
    source_files = C.run(["git", "-C", C.ROOT, "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", *paths], check=False).stdout.split("\0")
    for name in sorted(set(source_files) - {""}):
        source_hash.update(name.encode())
        if os.path.isfile(os.path.join(C.ROOT, name)):
            with open(os.path.join(C.ROOT, name), "rb") as f:
                source_hash.update(f.read())
        else:
            source_hash.update(b"<deleted>")
    cset, ccpus = C.client_cpus()
    labels_result = C.run(["docker", "image", "inspect", "--format", "{{json .Config.Labels}}", broker_image], check=False)
    try:
        labels = json.loads(labels_result.stdout or "null") or {}
    except (ValueError, TypeError):
        labels = {}
    resources = {"broker_cpus": cpus, "broker_cpuset": C.broker_cpuset(cpus), "broker_memory": mem,
                 "client_cpus": ccpus, "client_cpuset": cset, "client_memory": "4g", "client_java_heap": "3g",
                 "profile": kind == "bun" and os.environ.get("QF_BENCH_PROFILE") == "1",
                 "cpu_profile": kind == "bun" and os.environ.get("QF_BENCH_CPU_PROFILE") == "1",
                 "vm_cpus": C.vm_ncpu, "vm_memory": C.vm_mem}
    return {"run_id": run_id, "cohort_id": cohort_id, "broker": kind, "broker_image": broker_image,
            "client_image": client_image, "invocation_source_commit": commit, "invocation_source_dirty": bool(dirty),
            "invocation_source_dirty_paths": dirty.splitlines(), "invocation_source_content_sha256": source_hash.hexdigest(),
            "image_source_revision": labels.get("org.queueforge.revision") or labels.get("org.opencontainers.image.revision"),
            "image_source_sha256": labels.get("org.queueforge.source-sha256"), "harness_sha256": harness.hexdigest(),
            "scenario": sc, "scenario_sha256": json_hash(sc), "resources": resources,
            "resources_sha256": json_hash(resources), "started_utc": utc_now()}


def immutable_json(path, value):
    # Publish only complete bytes. Hard-link creation is atomic and refuses to
    # replace an existing result, unlike rename/replace.
    fd, temporary = tempfile.mkstemp(prefix=".json-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(value, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.link(temporary, path)
        directory = os.open(os.path.dirname(path), os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        os.unlink(temporary)

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
    if not fresh:
        with open(path) as f:
            reader = csv.DictReader(f, delimiter="\t")
            if reader.fieldnames != COLS:
                old = list(reader)
            else:
                old = None
        if old is not None:
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                writer = csv.DictWriter(f, fieldnames=COLS, delimiter="\t", lineterminator="\n")
                writer.writeheader()
                writer.writerows({key: r.get(key, "") for key in COLS} for r in old)
            os.replace(tmp, path)
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


def start_broker(kind, cpus, mem, image=None):
    C.run(["docker", "rm", "-f", BROKER], check=False, timeout=40)
    C.run(["docker", "network", "create", NET], check=False)
    cmd = [
        "docker", "run", "-d", "--name", BROKER, "--network", NET,
        "--cpuset-cpus", C.broker_cpuset(cpus), "--cpus", str(cpus),
        "--memory", mem, "--memory-swap", mem,
        "--ulimit", "nofile=1048576:1048576", "--sysctl", "net.core.somaxconn=4096",
        "-p", f"127.0.0.1:{HOST_PORT}:5672",
    ] + C.broker_env(kind) + (["-e", "QUEUEFORGE_PROFILE=1"] if kind == "bun" and os.environ.get("QF_BENCH_PROFILE") == "1" else []) + [image or image_digest(C.IMAGES[kind])]
    if kind == "bun" and os.environ.get("QF_BENCH_CPU_PROFILE") == "1":
        # Environment flags also reach supervised workers, whose CLI drops flags.
        cmd[-1:-1] = ["-e", "BUN_OPTIONS=" + CPU_PROFILE_FLAGS]
        cmd.extend(["sh", "-c", CPU_PROFILE_CMD])
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
        self.samples = []
        self.error = None

    def run(self):
        started = time.monotonic()
        marked = False
        while not self.stop_flag.is_set():
            try:
                usage = C.container_usage(BROKER)
            except Exception as exc:
                self.error = str(exc)
                return
            self.samples.append({"elapsed_s": time.monotonic() - started, **usage})
            mem = usage["memory"]
            if mem:
                self.peak = max(self.peak, mem)
            anon = usage["anonymous"]
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


def perf_test(args, seconds, out_path, limited, image=None):
    C.run(["docker", "rm", "-f", CLIENT], check=False, timeout=40)
    cset, ccpus = C.client_cpus()
    cmd = [
        "docker", "run", "--rm", "--name", CLIENT, "--network", NET,
        "--cpuset-cpus", cset, "--cpus", ccpus, "--memory", "4g",
        "--ulimit", "nofile=1048576:1048576",
        "-e", "JAVA_TOOL_OPTIONS=-Xmx3g",
        image or image_digest(PERF_TEST), "-h", f"amqp://admin:devpassword12@{BROKER}:5672", "-i", "1", "-ad", "false",
    ] + args.split()
    if not limited:
        cmd += ["-z", str(seconds)]
    with open(out_path, "x") as fh:
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


def run_cell(kind, cpus, mem, size, sc, run_id=None, cohort_id=None, broker_image=None, client_image=None, expected_repeats=1, repeat_ordinal=1):
    if not isinstance(expected_repeats, int) or not isinstance(repeat_ordinal, int) or expected_repeats < 1 or not 1 <= repeat_ordinal <= expected_repeats:
        raise ValueError("Invalid repeat count or ordinal")
    run_id = run_id or uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run ID")
    cohort_id = cohort_id or run_id
    broker_image = broker_image or C.IMAGES[kind]
    client_image = client_image or PERF_TEST
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", broker_image):
        broker_image = image_digest(broker_image)
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", client_image):
        client_image = image_digest(client_image)
    output_dir = os.path.join("runs", run_id)
    absolute_dir = os.path.join(C.OUT_DIR, output_dir)
    os.makedirs(absolute_dir, exist_ok=False)
    provenance = run_provenance(kind, cpus, mem, sc, run_id, cohort_id, broker_image, client_image)
    provenance.update(expected_repeats=expected_repeats, repeat_ordinal=repeat_ordinal)
    immutable_json(os.path.join(absolute_dir, "provenance.json"), provenance)
    row = {"broker": kind, "size": size, "scenario": sc["id"], "ok": "0", "err": ""}
    row.update({key: provenance[key] for key in ("run_id", "cohort_id", "broker_image", "client_image", "harness_sha256", "scenario_sha256", "resources_sha256", "started_utc")})
    row["output_dir"] = output_dir
    row.update(expected_repeats=expected_repeats, repeat_ordinal=repeat_ordinal)
    def finish():
        immutable_json(os.path.join(absolute_dir, "row.json"), {**row, "finished_utc": utc_now()})
        write_row(row)
    sampler = None
    try:
        if not start_broker(kind, cpus, mem, broker_image):
            oom, _r, status = C.container_state(BROKER)
            row["err"] = f"boot-{status}" + ("-oom" if oom == "true" else "")
            finish()
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
                path = os.path.join(absolute_dir, f"{tag}-{i}.out")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                codes.append(perf_test(args, limit or seconds, path, limited=limit is None, image=client_image))
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
            path = os.path.join(absolute_dir, f"{tag}.out")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            codes.append(perf_test(sc["args"], seconds, path, limited=False, image=client_image))
            samples, errs = parse(path)
            stats = summarize(samples, warm)
            if sc.get("rate") and stats.get("received_s") is not None:
                row["kept"] = "yes" if stats["received_s"] >= 0.95 * sc["rate"] else "no"
            elif stats.get("sent_s"):
                fan = 20 if sc["id"] == "fanout-20" else 1
                row["kept"] = "yes" if stats["received_s"] >= 0.95 * stats["sent_s"] * fan else "no"
        sampler.stop_flag.set()
        sampler.join(timeout=15)
        if sampler.error:
            raise RuntimeError("Resource sampler failed: " + sampler.error)
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
        finish()
        C.log(
            f"CELL {kind} {size} {sc['id']} ok={row['ok']} err={row['err']} sent={row.get('sent_s')} "
            f"recv={row.get('received_s')} mb={row.get('mb_s', '')} p50={row.get('lat_p50_ms')} "
            f"p99={row.get('lat_p99_ms')} mib={row['peak_mib']} anon={row['anon_mib']} cpu={bcpu} client={ccpu} kept={row.get('kept')}"
        )
        return row
    except Exception as exc:
        if sampler is not None:
            sampler.stop_flag.set()
            sampler.join(timeout=15)
            row["peak_mib"] = f"{sampler.peak / 1048576:.0f}" if sampler.peak else ""
            row["anon_mib"] = f"{sampler.peak_anon / 1048576:.0f}" if sampler.peak_anon else ""
            row["broker_cpu"], row["client_cpu"] = sampler.cpu()
        if os.path.exists(os.path.join(absolute_dir, "row.json")):
            raise  # An already persisted result is never rewritten.
        row.update(ok="0", err="harness-" + type(exc).__name__)
        finish()
        C.log(f"FAILED {kind} {size} {sc['id']} {row['err']}: {exc}")
        return row
    finally:
        try:
            if sampler is not None:
                sampler.stop_flag.set()
                sampler.join(timeout=15)
                immutable_json(os.path.join(absolute_dir, "resource-samples.json"),
                               {"method": "Docker Engine stats API; total includes cache", "interval_s": 1,
                                "error": sampler.error, "samples": sampler.samples})
            if provenance["resources"]["cpu_profile"]:
                collection = {"source": CPU_PROFILE_DIR, "flags": CPU_PROFILE_FLAGS,
                              "scope": "supervisor and workers via inherited BUN_OPTIONS",
                              "signal_exit": "diagnostic preload calls process.exit(0) on SIGTERM/SIGINT",
                              "worker_exit_wait_s": 10, "files": [], "errors": []}
                try:
                    stopped = C.run(["docker", "stop", "--time", "20", BROKER], check=False, timeout=40)
                    collection.update(stop_exit_code=stopped.returncode, stop_stderr=stopped.stderr or "")
                    if stopped.returncode != 0:
                        collection["errors"].append("graceful-stop-failed")
                    profile_dir = os.path.join(absolute_dir, "cpu-profiles")
                    os.makedirs(profile_dir, exist_ok=False)
                    copied = C.run(["docker", "cp", f"{BROKER}:{CPU_PROFILE_DIR}/.", profile_dir], check=False, timeout=40)
                    collection.update(copy_exit_code=copied.returncode, copy_stderr=copied.stderr or "")
                    if copied.returncode != 0:
                        collection["errors"].append("profile-copy-failed")
                    for directory, _dirs, files in os.walk(profile_dir):
                        for name in sorted(files):
                            path = os.path.join(directory, name)
                            collection["files"].append({"path": os.path.relpath(path, absolute_dir), "bytes": os.path.getsize(path)})
                    if not any(item["path"].endswith(".cpuprofile") and item["bytes"] > 0 for item in collection["files"]):
                        collection["errors"].append("missing-cpu-profiles")
                    pids_path = os.path.join(profile_dir, "expected-pids.txt")
                    expected_pids = []
                    if os.path.isfile(pids_path):
                        with open(pids_path) as f:
                            expected_pids = f.read().split()
                    collection["expected_pids"] = expected_pids
                    if not expected_pids:
                        collection["errors"].append("missing-expected-processes")
                    profiled_pids = set()
                    for item in collection["files"]:
                        # Bun's default CPU.<date>.<time>.<pid>.<tid>.<seq> name.
                        match = re.fullmatch(r"CPU\.\d+\.\d+\.(\d+)\.\d+\.\d+\.cpuprofile", os.path.basename(item["path"]))
                        if match and item["bytes"] > 0:
                            profiled_pids.add(match.group(1))
                    collection["missing_pids"] = sorted(set(expected_pids) - profiled_pids)
                    if collection["missing_pids"]:
                        collection["errors"].append("missing-process-profiles")
                    status_path = os.path.join(profile_dir, "shutdown-status.txt")
                    collection["shutdown_status"] = None
                    if os.path.isfile(status_path):
                        with open(status_path) as f:
                            collection["shutdown_status"] = f.read().strip()
                    if collection["shutdown_status"] != "complete":
                        collection["errors"].append("worker-shutdown-incomplete")
                except Exception as exc:
                    collection["errors"].append(type(exc).__name__ + ": " + str(exc))
                immutable_json(os.path.join(absolute_dir, "cpu-profile-collection.json"), collection)
                if collection["errors"]:
                    C.log("cpu-profile collection: " + ", ".join(collection["errors"]))
            if provenance["resources"]["profile"] or provenance["resources"]["cpu_profile"]:
                logs = C.run(["docker", "logs", BROKER], check=False)
                with open(os.path.join(absolute_dir, "broker.log"), "x") as f:
                    f.write((logs.stdout or "") + (logs.stderr or ""))
        finally:
            C.run(["docker", "rm", "-f", BROKER, CLIENT], check=False, timeout=40)



def run_matrix(kinds=KINDS, repeat=1, only=None, cpus_only=None):
    """Every cell, or only the scenario ids in `only` at the CPU counts in `cpus_only`."""
    if repeat < 1:
        raise ValueError("repeat must be positive")
    C.run(["docker", "pull", "-q", PERF_TEST], check=False, timeout=600)
    images = {kind: image_digest(C.IMAGES[kind]) for kind in kinds}
    client_image = image_digest(PERF_TEST)
    cohort_id = uuid.uuid4().hex
    for cpus, mem, size in SIZES:
        if cpus_only and cpus not in cpus_only:
            continue
        for sc in SCENARIOS:
            if cpus > 1 and sc["id"] not in HEAVY:
                continue
            if only and sc["id"] not in only:
                continue
            for kind in kinds:
                for ordinal in range(1, repeat + 1):
                    run_cell(kind, cpus, mem, size, sc, cohort_id=cohort_id, broker_image=images[kind], client_image=client_image, expected_repeats=repeat, repeat_ordinal=ordinal)
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
        if "run_id" in ix and row[ix["run_id"]]:
            continue  # Immutable run records retain their original result.
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cmd", choices=["one", "matrix", "all", "rescore"])
    parser.add_argument("items", nargs="*")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--cpus", type=int, choices=[1, 4], default=1)
    opts = parser.parse_args()
    if opts.repeat < 1:
        parser.error("--repeat must be positive")
    cmd = opts.cmd
    if cmd == "rescore":
        rescore()
        return
    if not C.docker_up():
        raise SystemExit("docker is down")
    if cmd == "one":
        if not opts.items or opts.items[0] not in {s["id"] for s in SCENARIOS}:
            parser.error("one requires a known scenario ID")
        sc = next(s for s in SCENARIOS if s["id"] == opts.items[0])
        kinds = opts.items[1:] or KINDS
        if any(kind not in KINDS for kind in kinds):
            parser.error("unknown broker")
        C.run(["docker", "pull", "-q", PERF_TEST], check=False, timeout=600)
        images = {kind: image_digest(C.IMAGES[kind]) for kind in kinds}
        client_image = image_digest(PERF_TEST)
        cohort_id = uuid.uuid4().hex
        cpus, mem, size = next(size for size in SIZES if size[0] == opts.cpus)
        for kind in kinds:
            for ordinal in range(1, opts.repeat + 1):
                run_cell(kind, cpus, mem, size, sc, cohort_id=cohort_id, broker_image=images[kind], client_image=client_image, expected_repeats=opts.repeat, repeat_ordinal=ordinal)
        return
    if cmd == "matrix":  # assumes the VM is already large
        run_matrix(opts.items or KINDS, opts.repeat)
        return
    if cmd == "all":
        C.ensure_big()
        try:
            run_matrix(opts.items or KINDS, opts.repeat)
        finally:
            C.run(["docker", "rm", "-f", BROKER, CLIENT], check=False)
            C.restore_small()
        return
    raise SystemExit(__doc__)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Load sweep: unpaced messages per second for every broker at five container sizes.

Usage: bench/load.py all     (resizes Docker Desktop to 8 CPUs and back)
Client: bench/loadgen (docker build -t qf-loadgen:linux bench/loadgen).

One broker at a time. Classic durable queues, 256-byte persistent messages,
publisher confirms, and acking consumers. The client runs in a container on
the broker network.
"""
import os
import re
import signal
import subprocess
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

ROOT = C.ROOT
HERE = C.OUT_DIR
LOG = os.path.join(HERE, "load.log")
OUT = os.path.join(HERE, "load.tsv")
SETTINGS = C.SETTINGS
CONF = C.RABBIT_CONF

STEPS = [(1, "512m", 512), (1, "1g", 1024), (2, "2g", 2048), (4, "4g", 4096), (4, "8g", 8192)]
SHAPES = [
    ("single", {"QUEUES": "1", "PUBS": "1", "CONS": "1", "WINDOW": "128", "PREFETCH": "256"}),
    ("shared", {"QUEUES": "1", "PUBS": "16", "CONS": "16", "WINDOW": "512", "PREFETCH": "1024"}),
    ("spread", {"QUEUES": "16", "PUBS": "16", "CONS": "16", "WINDOW": "512", "PREFETCH": "1024"}),
]
COLS = [
    "kind", "cpus", "mem_mb", "shape", "ok", "confirm_s", "consume_s", "sent_s", "p50_us", "p99_us",
    "confirmed", "consumed", "sent", "samples", "inflight", "blocked", "nacks", "missed", "returns",
    "elapsed_s", "broker_cpu", "client_cpu", "broker_mib", "oom", "early_confirm", "boot_s", "err",
]
KINDS = ["rabbit", "rust", "bun", "php"]
need_restore = False
vm_ncpu = 2
vm_mem = 0


def log(msg):
    line = time.strftime("%H:%M:%S") + " " + msg
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def as_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def run(args, timeout=60, check=True):
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        stdout = as_text(e.stdout)
        stderr = as_text(e.stderr) or "timeout"
        if check:
            raise RuntimeError(f"timeout {args} {stderr[-300:]}")
        return subprocess.CompletedProcess(args, 124, stdout, stderr)
    if check and p.returncode != 0:
        raise RuntimeError(f"{args} -> {p.returncode} {(p.stderr or '')[-800:]}")
    return p


def docker_up():
    p = run(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"], timeout=15, check=False)
    if p.returncode != 0:
        return False
    parts = (p.stdout or "").split()
    if len(parts) != 2:
        return False
    global vm_ncpu, vm_mem
    vm_ncpu = int(parts[0])
    vm_mem = int(parts[1])
    return True


def ensure_big():
    C.ensure_big()
    docker_up()


def restore_small():
    cleanup_load()
    ok = C.restore_small()
    docker_up()
    return ok


def cleanup_load():
    if not docker_up():
        return
    run(["docker", "rm", "-f", "qf-load-broker", "qf-load-client"], check=False, timeout=40)
    run(["docker", "network", "rm", "qf-load"], check=False, timeout=20)


def broker_cpuset(cpus):
    if vm_ncpu >= 8:
        return {1: "0", 2: "0,1", 4: "0-3"}[cpus]
    if cpus == 1:
        return "0"
    raise RuntimeError(f"vm has {vm_ncpu} cpus, cannot place {cpus}")


def client_cpus():
    if vm_ncpu >= 8:
        return "4-6", "3"
    return "1", "1"


def ensure_client():
    run(["docker", "network", "create", "qf-load"], check=False)
    p = run(["docker", "inspect", "-f", "{{.State.Running}}", "qf-load-client"], check=False)
    if (p.stdout or "").strip() == "true":
        return
    run(["docker", "rm", "-f", "qf-load-client"], check=False)
    cpuset, cpus = client_cpus()
    run(
        [
            "docker", "run", "-d", "--name", "qf-load-client",
            "--network", "qf-load",
            "--cpuset-cpus", cpuset, "--cpus", cpus,
            "--memory", "1g", "--memory-swap", "1g",
            "--ulimit", "nofile=1048576:1048576",
            "qf-loadgen:linux",
        ]
    )


def start_broker(kind, cpus, mem):
    run(["docker", "rm", "-f", "qf-load-broker"], check=False, timeout=40)
    common = [
        "docker", "run", "-d", "--name", "qf-load-broker",
        "--network", "qf-load",
        "--cpuset-cpus", broker_cpuset(cpus), "--cpus", str(cpus),
        "--memory", mem, "--memory-swap", mem,
        "--ulimit", "nofile=1048576:1048576",
        "--sysctl", "net.core.somaxconn=4096",
    ]
    if kind == "rabbit":
        cmd = common + [
            "-e", "RABBITMQ_DEFAULT_USER=admin",
            "-e", "RABBITMQ_DEFAULT_PASS=devpassword12",
            "-e", "RABBITMQ_LOG=warning",
            "-v", f"{CONF}:/etc/rabbitmq/rabbitmq.conf:ro",
            "rabbitmq:4.3-management",
        ]
    elif kind == "rust":
        cmd = common + [
            "-e", "QUEUEFORGE_ADMIN_USER=admin",
            "-e", "QUEUEFORGE_ADMIN_PASSWORD=devpassword12",
            "-e", "RUST_LOG=warn",
            "queueforge-rust:bench",
        ]
    elif kind == "php":
        cmd = common + ["queueforge-php:bench"]
    else:
        cmd = common + ["queueforge-bun:bench"]
    p = run(cmd, check=False, timeout=60)
    if p.returncode != 0:
        log("start failed " + (p.stderr or "")[-500:])
        return ""
    for _ in range(30):
        ip = run(
            ["docker", "inspect", "-f", '{{(index .NetworkSettings.Networks "qf-load").IPAddress}}', "qf-load-broker"],
            check=False,
        ).stdout.strip()
        if ip:
            return ip
        time.sleep(0.2)
    return ""


def broker_dead():
    p = run(
        ["docker", "inspect", "-f", "{{.State.OOMKilled}} {{.State.Running}} {{.State.Status}}", "qf-load-broker"],
        check=False,
    )
    parts = (p.stdout or "false false missing").split()
    oom = parts[0] if parts else "false"
    running = len(parts) > 1 and parts[1] == "true"
    status = parts[2] if len(parts) > 2 else "missing"
    return oom, running, status


def wait_ready(ip):
    deadline = time.time() + 90
    while time.time() < deadline:
        oom, running, status = broker_dead()
        if not running:
            log(f"broker down during boot oom={oom} status={status}")
            return 0
        p = run(
            ["docker", "exec", "-e", "PING=1", "qf-load-client", "loadgen", ip, "5672"],
            check=False,
            timeout=8,
        )
        if "connected=1" in (p.stdout or ""):
            return time.time()
        time.sleep(0.4)
    logs = run(["docker", "logs", "--tail", "20", "qf-load-broker"], check=False)
    log("boot timeout " + ((logs.stdout or "") + (logs.stderr or ""))[-400:].replace("\n", " | "))
    return 0


def cgroup_text(name, path):
    p = run(["docker", "exec", name, "cat", path], check=False, timeout=10)
    if p.returncode != 0:
        return ""
    return p.stdout or ""


def cpu_usec(name):
    text = cgroup_text(name, "/sys/fs/cgroup/cpu.stat")
    for line in text.splitlines():
        if line.startswith("usage_usec "):
            return int(line.split()[1])
    return None


def mem_bytes(name):
    text = cgroup_text(name, "/sys/fs/cgroup/memory.current").strip()
    if text.isdigit():
        return int(text)
    return None


def sample_cpu(span):
    t0 = time.monotonic()
    a_b = cpu_usec("qf-load-broker")
    a_c = cpu_usec("qf-load-client")
    time.sleep(span)
    b_b = cpu_usec("qf-load-broker")
    b_c = cpu_usec("qf-load-client")
    dt = time.monotonic() - t0
    mem = mem_bytes("qf-load-broker")

    def pct(a, b):
        if a is None or b is None or dt <= 0:
            return ""
        return f"{(b - a) / (dt * 1e6) * 100:.0f}"

    mib = f"{mem / 1048576:.0f}" if mem is not None else ""
    return pct(a_b, b_b), pct(a_c, b_c), mib


def early_confirm():
    p = run(
        ["docker", "exec", "qf-load-broker", "curl", "-sf", "http://127.0.0.1:15692/metrics"],
        check=False,
        timeout=8,
    )
    if p.returncode != 0:
        if not getattr(early_confirm, "logged", False):
            early_confirm.logged = True
            log("metrics curl failed " + ((p.stderr or "") + (p.stdout or ""))[-240:].replace("\n", " | "))
        return ""
    for line in (p.stdout or "").splitlines():
        if line.startswith("queueforge_confirm_before_fsync_total "):
            return line.split()[1]
    if not getattr(early_confirm, "missing", False):
        early_confirm.missing = True
        log("metrics scrape had no confirm_before_fsync line")
    return ""


def parse_result(text):
    line = ""
    for ln in text.splitlines():
        if ln.startswith("RESULT "):
            line = ln
    if not line:
        return None
    fields = {}
    for part in line.split()[1:]:
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        fields[k] = v
    return fields


def write_row(tsv, row):
    tsv.write("\t".join(str(row.get(c, "")) for c in COLS) + "\n")
    tsv.flush()


def dump_logs():
    p = run(["docker", "logs", "--tail", "30", "qf-load-broker"], check=False, timeout=15)
    text = ((p.stdout or "") + (p.stderr or ""))[-800:].replace("\n", " | ")
    if text:
        log("broker-log " + text)


def run_cell(tsv, kind, cpus, mem, mem_mb, shape, env, warmup_ms, measure_ms, check_queues):
    row = {c: "" for c in COLS}
    row.update({"kind": kind, "cpus": cpus, "mem_mb": mem_mb, "shape": shape, "ok": "0", "err": "start"})
    ensure_client()
    t_boot = time.time()
    ip = start_broker(kind, cpus, mem)
    if not ip:
        row["err"] = "start-failed"
        write_row(tsv, row)
        log(f"FAILED kind={kind} cpus={cpus} mem={mem_mb} shape={shape} err=start-failed")
        return row
    ready_at = wait_ready(ip)
    if not ready_at:
        oom, _running, status = broker_dead()
        row["oom"] = oom
        row["err"] = "boot-failed-" + status
        row["boot_s"] = f"{time.time() - t_boot:.1f}"
        write_row(tsv, row)
        log(f"FAILED kind={kind} cpus={cpus} mem={mem_mb} shape={shape} err={row['err']} oom={oom}")
        dump_logs()
        run(["docker", "rm", "-f", "qf-load-broker"], check=False)
        return row
    boot_s = ready_at - t_boot
    out_path = os.path.join(HERE, "cells", f"{kind}-{cpus}-{mem_mb}-{shape}.out")
    os.makedirs(os.path.join(HERE, "cells"), exist_ok=True)
    cmd = ["docker", "exec"]
    merged = dict(env)
    merged["WARMUP_MS"] = str(warmup_ms)
    merged["MEASURE_MS"] = str(measure_ms)
    merged["BODY"] = "256"
    for key, value in merged.items():
        cmd.extend(["-e", f"{key}={value}"])
    cmd.extend(["qf-load-client", "loadgen", ip, "5672"])
    with open(out_path, "w") as fh:
        proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT)
        deadline = time.time() + (warmup_ms + measure_ms) / 1000 + 45
        seen = False
        mark_at = 0
        sampled = False
        broker_cpu = client_cpu = broker_mib = ""
        settle, span = (0.5, 2.0) if measure_ms >= 8000 else (0.2, 0.8)
        while time.time() < deadline:
            oom, running, status = broker_dead()
            if not running:
                row["oom"] = oom
                row["err"] = "died-" + status
                log(f"broker died oom={oom} status={status}")
                break
            text = open(out_path).read()
            if not seen and "MARK" in text:
                seen = True
                mark_at = time.time()
            if seen and not sampled and time.time() - mark_at >= settle:
                broker_cpu, client_cpu, broker_mib = sample_cpu(span)
                sampled = True
            if "RESULT " in text:
                break
            time.sleep(0.2)
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        text = open(out_path).read()
    fields = parse_result(text)
    oom, _running, status = broker_dead()
    if kind in ("bun", "rust"):
        row["early_confirm"] = early_confirm()
    if kind == "rabbit" and check_queues[0]:
        q = run(
            ["docker", "exec", "qf-load-broker", "rabbitmqctl", "list_queues", "name", "durable", "type", "arguments"],
            check=False,
            timeout=30,
        )
        log("RABBIT_QUEUES " + ((q.stdout or "") + (q.stderr or ""))[-500:].replace("\n", " | "))
        if q.returncode == 0:
            check_queues[0] = False
    run(["docker", "rm", "-f", "qf-load-broker"], check=False, timeout=40)
    if fields:
        for key in (
            "ok", "confirm_s", "consume_s", "sent_s", "p50_us", "p99_us", "confirmed", "consumed", "sent",
            "samples", "inflight", "blocked", "nacks", "missed", "returns", "elapsed_s", "err",
        ):
            if key in fields:
                row[key] = fields[key]
    else:
        row["err"] = row["err"] if row["err"] not in ("", "start") else "no-result"
        log("no result tail " + text[-300:].replace("\n", " | "))
        dump_logs()
    row["broker_cpu"] = broker_cpu
    row["client_cpu"] = client_cpu
    row["broker_mib"] = broker_mib
    row["oom"] = oom
    row["boot_s"] = f"{boot_s:.1f}"
    write_row(tsv, row)
    level = "FAILED" if not fields or float(fields.get("confirmed", "0") or 0) <= 0 else "PROGRESS"
    log(
        f"{level} kind={kind} cpus={cpus} mem={mem_mb} shape={shape} ok={row['ok']} "
        f"confirm_s={row['confirm_s']} consume_s={row['consume_s']} p50_us={row['p50_us']} "
        f"p99_us={row['p99_us']} broker_cpu={broker_cpu} client_cpu={client_cpu} "
        f"broker_mib={broker_mib} oom={oom} early={row['early_confirm']} boot_s={row['boot_s']} err={row['err']}"
    )
    return row


def summarize(rows):
    shapes = []
    for row in rows:
        if row["shape"] not in shapes:
            shapes.append(row["shape"])
    for shape in shapes:
        log(f"TABLE {shape} confirm/s p50ms")
        log("size\t" + "\t".join(KINDS))
        for cpus, _mem, mem_mb in STEPS:
            cells = []
            for kind in KINDS:
                match = [r for r in rows if r["kind"] == kind and r["shape"] == shape and str(r["mem_mb"]) == str(mem_mb) and str(r["cpus"]) == str(cpus)]
                if not match:
                    cells.append("-")
                    continue
                r = match[-1]
                try:
                    rate = float(r["confirm_s"] or 0)
                    p50 = float(r["p50_us"] or -1) / 1000
                    cells.append(f"{rate:.0f} ({p50:.2f})" if r["confirm_s"] else r["err"])
                except ValueError:
                    cells.append(r["err"] or "-")
            log(f"{cpus}cpu/{mem_mb}m\t" + "\t".join(cells))


def open_tsv():
    os.makedirs(HERE, exist_ok=True)
    f = open(OUT, "w")
    f.write("\t".join(COLS) + "\n")
    f.flush()
    return f


def run_smoke():
    if not docker_up():
        raise RuntimeError("docker is down")
    if vm_ncpu < 2:
        raise RuntimeError("need at least 2 vm cpus for smoke")
    tsv = open_tsv()
    rows = []
    try:
        for kind, shape in (("bun", "single"), ("bun", "shared"), ("rust", "shared"), ("rabbit", "shared")):
            env = dict(SHAPES[[s[0] for s in SHAPES].index(shape)][1])
            row = run_cell(tsv, kind, 1, "512m", 512, shape, env, 1000, 3000, [True])
            rows.append(row)
            if float(row.get("confirmed") or 0) <= 0:
                raise RuntimeError(f"smoke failed {kind} {shape} {row.get('err')}")
    finally:
        tsv.close()
    summarize(rows)


def run_matrix():
    tsv = open_tsv()
    rows = []
    check_queues = [True]
    try:
        for kind in KINDS:
            for cpus, mem, mem_mb in STEPS:
                for shape, env in SHAPES:
                    row = run_cell(tsv, kind, cpus, mem, mem_mb, shape, env, 2000, 8000, check_queues)
                    rows.append(row)
    finally:
        tsv.close()
    summarize(rows)
    return rows


def main():
    global need_restore
    signal.signal(signal.SIGTERM, lambda *_a: sys.exit(143))
    os.makedirs(HERE, exist_ok=True)
    open(LOG, "w").close()
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    log("WORKLOAD classic durable queue, x-queue-type=classic, 256-byte delivery-mode 2, confirms, acking consumers")
    log("SHAPES single=1x1 window 128; shared=16 publishers 16 consumers 1 queue window 512; spread=16 queues window 512")
    if cmd == "smoke":
        try:
            run_smoke()
            log("DONE smoke")
        except Exception:
            log("FAILED smoke\n" + traceback.format_exc())
            raise
        finally:
            cleanup_load()
        return
    if cmd == "all":
        need_restore = True
        try:
            ensure_big()
            run_matrix()
        except Exception:
            log("FAILED matrix\n" + traceback.format_exc())
            raise
        finally:
            cleanup_load()
            ok = restore_small()
            if ok:
                log(f"DONE restore ncpu={vm_ncpu} mem={vm_mem}")
            else:
                log("FAILED DONE restore incomplete")
        return
    raise SystemExit("usage: run_load.py smoke|all")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Find how many AMQP connections each broker can hold.

One broker at a time. Container sizes: 1x512MiB, 2x2GiB, 4x4GiB, 4x8GiB.
The client containers sit on other VM CPUs. A run passes when every connection
stays up and 40 confirms succeed.
"""
import math
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

ROOT = C.ROOT
HERE = C.OUT_DIR
LOG = os.path.join(HERE, "connections.log")
OUT = os.path.join(HERE, "connections.tsv")
PER_CLIENT = 50000
STEPS = [(1, "512m", 512), (2, "2g", 2048), (4, "4g", 4096), (4, "8g", 8192)]
SWAP_OK = True


def log(msg):
    line = time.strftime("%H:%M:%S") + " " + msg
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def run(args, timeout=90, check=True):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"{args} -> {p.returncode} {p.stderr[-800:]}")
    return p


def broker_cpuset(cpus):
    return {1: "0", 2: "0,1", 4: "0-3"}[cpus]


def mem_flags(mem):
    if SWAP_OK:
        return ["--memory", mem, "--memory-swap", mem]
    return ["--memory", mem]


def stop():
    p = run(["docker", "ps", "-aq", "--filter", "name=qf-lim-"], check=False)
    ids = p.stdout.split()
    if ids:
        run(["docker", "rm", "-f", *ids], timeout=120, check=False)


def broker_status():
    p = run(
        ["docker", "inspect", "-f", "{{.State.OOMKilled}} {{.State.Status}}", "qf-lim-broker"],
        check=False,
    )
    bits = (p.stdout or "false missing").split()
    oom = bits[0] if bits else "false"
    status = bits[1] if len(bits) > 1 else "missing"
    return oom, status


def parse_fields(line):
    fields = {}
    for part in line.split()[1:]:
        if "=" in part:
            k, v = part.split("=", 1)
            fields[k] = v
    return fields


def parse_mib(text):
    m = re.search(r"([0-9.]+)\s*([KMG]iB)", text or "")
    if not m:
        return None
    v = float(m.group(1))
    unit = m.group(2)
    if unit.startswith("G"):
        return v * 1024
    if unit.startswith("K"):
        return v / 1024
    return v


def split_n(n, k):
    base, rem = divmod(n, k)
    return [base + (1 if i < rem else 0) for i in range(k)]


def start_clients(k):
    for i in range(k):
        run(
            [
                "docker", "run", "-d", "--name", f"qf-lim-c{i}",
                "--network", "qf-limit",
                "--cpuset-cpus", "4-6", "--cpus", "3",
                "--ulimit", "nofile=1048576:1048576",
                "--sysctl", "net.ipv4.ip_local_port_range=1024 65535",
                "qf-connscale:linux",
            ]
        )


def start_broker(kind, cpus, mem):
    name = "qf-lim-broker"
    common = [
        "docker", "run", "-d", "--name", name,
        "--network", "qf-limit", "--network-alias", "broker",
        "--cpuset-cpus", broker_cpuset(cpus), "--cpus", str(cpus),
        *mem_flags(mem),
        "--ulimit", "nofile=1048576:1048576",
        "--sysctl", "net.core.somaxconn=4096",
    ]
    if kind == "rabbit":
        cmd = common + [
            "-e", "RABBITMQ_DEFAULT_USER=admin",
            "-e", "RABBITMQ_DEFAULT_PASS=devpassword12",
            "-e", "RABBITMQ_LOG=warning",
            "-v", f"{ROOT}/bench/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro",
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
    run(cmd)
    ip = run(
        ["docker", "inspect", "-f", '{{(index .NetworkSettings.Networks "qf-limit").IPAddress}}', name]
    ).stdout.strip()
    return ip


def wait_ready(ip):
    for i in range(1, 91):
        p = run(
            ["docker", "exec", "qf-lim-c0", "/usr/local/bin/connscale-linux", "5672", "1", "200", "0", ip],
            timeout=30,
            check=False,
        )
        if "connected=1" in (p.stdout or ""):
            return i
        time.sleep(1)
    logs = run(["docker", "logs", "--tail", "30", "qf-lim-broker"], check=False)
    log("FAILED boot\n" + (logs.stdout or "")[-500] + (logs.stderr or "")[-500])
    return 0


def file_has(path, needle):
    try:
        with open(path) as f:
            return needle in f.read()
    except FileNotFoundError:
        return False


def run_once(kind, cpus, mem, mem_mb, n):
    stop()
    k = max(1, math.ceil(n / PER_CLIENT))
    parts = split_n(n, k)
    log(f"CELL {kind} cpus={cpus} mem={mem} n={n} clients={k} parts={parts}")
    start_clients(k)
    ip = start_broker(kind, cpus, mem)
    ready_s = wait_ready(ip)
    if not ready_s:
        stop()
        return fail_row(kind, cpus, mem, n, "boot-failed")
    log(f"ready {kind} after {ready_s}s ip={ip}")
    files = []
    procs = []
    t0 = time.time()
    try:
        for i, count in enumerate(parts):
            path = os.path.join(HERE, f"c{i}.txt")
            fh = open(path, "w")
            files.append((path, fh))
            procs.append(
                subprocess.Popen(
                    [
                        "docker", "exec", "-e", "CONN_DEADLINE_MS=180000", "-e", "CONN_INFLIGHT=32",
                        f"qf-lim-c{i}", "/usr/local/bin/connscale-linux",
                        "5672", str(count), "600000", "0", ip,
                    ],
                    stdout=fh,
                    stderr=subprocess.STDOUT,
                )
            )
        deadline = time.time() + 200
        oom, status = "false", "running"
        while time.time() < deadline:
            oom, status = broker_status()
            if oom == "true" or status != "running":
                log(f"broker_down {kind} {oom} {status}")
                break
            if all(file_has(path, "HOLD ") for path, _fh in files):
                break
            if all(p.poll() is not None for p in procs):
                break
            time.sleep(0.5)
        connect_ms = int((time.time() - t0) * 1000)
        held_hint = 0
        addr = timeout = pending = refused = reset = other = 0
        for path, _fh in files:
            try:
                text = open(path).read()
            except FileNotFoundError:
                text = ""
            hold = re.search(r"HOLD connected=(\d+)", text)
            if hold:
                held_hint += int(hold.group(1))
        if oom == "true" or status != "running" or not all(file_has(path, "HOLD ") for path, _fh in files):
            row = tally(kind, cpus, mem, n, held_hint, connect_ms, oom, status, 0, -1, -1, "?", addr)
            stop()
            return row
        time.sleep(5)
        oom, status = broker_status()
        stats = run(
            ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}} {{.CPUPerc}}", "qf-lim-broker"],
            check=False,
        ).stdout.strip() or "? ?"
        probe_ok, p50, p99 = 0, -1.0, -1.0
        if oom != "true" and status == "running":
            probe = run(
                ["docker", "exec", "qf-lim-c0", "/usr/local/bin/connscale-linux", "5672", "1", "0", "40", ip],
                timeout=60,
                check=False,
            )
            m = re.search(r"RESULT .*", (probe.stdout or "") + (probe.stderr or ""))
            if m:
                fields = parse_fields(m.group(0))
                probe_ok = int(float(fields.get("probe_ok", "0")))
                p50 = float(fields.get("probe_p50_ms", "-1"))
                p99 = float(fields.get("probe_p99_ms", "-1"))
        for i in range(k):
            run(["docker", "exec", f"qf-lim-c{i}", "sh", "-c", "touch /tmp/qf-release"], timeout=20, check=False)
        for p, (path, fh) in zip(procs, files):
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
            fh.close()
        oom2, status2 = broker_status()
        if oom2 == "true":
            oom, status = oom2, status2
        held = addr = timeout = pending = refused = reset = other = 0
        client_connect = 0
        for path, _fh in files:
            text = open(path).read()
            result = re.search(r"RESULT .*", text)
            fields = parse_fields(result.group(0) if result else "RESULT")
            if result:
                held += int(fields.get("held", "0"))
                client_connect = max(client_connect, int(fields.get("connect_ms", "0")))
            else:
                hold = re.search(r"HOLD connected=(\d+)", text)
                held += int(hold.group(1)) if hold else 0
            addr += int(fields.get("addr", "0"))
            timeout += int(fields.get("timeout", "0"))
            pending += int(fields.get("pending", "0"))
            refused += int(fields.get("refused", "0"))
            reset += int(fields.get("reset", "0"))
            other += int(fields.get("other", "0"))
        if client_connect:
            connect_ms = client_connect
        row = tally(
            kind, cpus, mem, n, held, connect_ms, oom, status, probe_ok, p50, p99, stats, addr,
            timeout, pending, refused, reset, other,
        )
        return row
    finally:
        for _path, fh in files:
            fh.close()
        stop()


def fail_row(kind, cpus, mem, n, why):
    return {
        "kind": kind, "cpus": cpus, "mem": mem, "n": n, "held": 0, "connect_ms": 0,
        "oom": "false", "probe_ok": 0, "p50": -1.0, "p99": -1.0, "reason": why,
        "stats": "?", "used_mib": None, "ok": False, "addr": 0,
    }


def tally(kind, cpus, mem, n, held, connect_ms, oom, status, probe_ok, p50, p99, stats, addr=0,
          timeout=0, pending=0, refused=0, reset=0, other=0):
    used = parse_mib(stats.split("/")[0] if isinstance(stats, str) else "")
    mem_mb = {"512m": 512, "2g": 2048, "4g": 4096, "8g": 8192}[mem]
    short = n - held
    if held >= n and oom != "true" and status == "running" and probe_ok >= 40:
        why = ""
    elif oom == "true":
        why = "oom-killed"
    elif status != "running":
        why = f"broker-exited:{status}"
    elif held >= n and probe_ok < 40:
        why = "confirms-failed"
    elif addr > 0 and addr * 2 >= max(short, 1):
        why = "ephemeral-ports"
    elif (pending > 0 and connect_ms >= 170000) or (timeout > 0 and timeout * 2 >= max(short, 1)):
        why = "login-window"
    else:
        why = f"short held={held} refused={refused} reset={reset} timeout={timeout} other={other} addr={addr} pending={pending}"
    ok = why == ""
    row = {
        "kind": kind, "cpus": cpus, "mem": mem, "n": n, "held": held, "connect_ms": connect_ms,
        "oom": oom, "probe_ok": probe_ok, "p50": p50, "p99": p99, "reason": why,
        "stats": stats, "used_mib": used, "ok": ok, "addr": addr, "mem_mb": mem_mb,
    }
    with open(OUT, "a") as f:
        f.write("\t".join([
            kind, str(cpus), mem, str(n), str(held), str(connect_ms), oom, str(probe_ok),
            f"{p50:.3f}", f"{p99:.3f}", why, stats.replace("\t", " "),
        ]) + "\n")
    log(
        f"PROGRESS {kind} cpus={cpus} mem={mem} n={n} held={held} connect_ms={connect_ms} "
        f"oom={oom} probe_ok={probe_ok} p50={p50:.3f} p99={p99:.3f} reason={why or 'held'} mem={stats}"
    )
    return row


def first_n(kind, mem_mb):
    if kind == "bun":
        guess = int((mem_mb - 40) / 0.0092)
        if mem_mb <= 512:
            return 54000
        return min(guess, 80000)
    elif kind == "rust":
        guess = min(100000, int((mem_mb - 40) / 0.050))
        if mem_mb <= 512:
            return 12000
        return min(guess, 40000)
    else:
        guess = max(1000, int((mem_mb - 160) / 0.070))
        if mem_mb <= 512:
            return 7000
        return min(guess, 20000)
    return guess


def find_max(kind, cpus, mem, mem_mb):
    cap = 100000 if kind == "rust" else PER_CLIENT * 8
    lo = 0
    hi = None
    best = None
    n = first_n(kind, mem_mb)
    seen = set()
    for _attempt in range(6):
        n = max(1000, min(cap, int(n)))
        if n in seen:
            break
        seen.add(n)
        row = run_once(kind, cpus, mem, mem_mb, n)
        if row["ok"]:
            lo = row["held"]
            best = row
            if n >= cap:
                break
            used = row["used_mib"] or mem_mb
            if used < mem_mb * 0.72:
                jump = int(n * (mem_mb * 0.85) / max(used, 1))
                nxt = min(cap, max(n + 2000, jump))
            else:
                nxt = min(cap, int(n * 1.12) + 1000)
            if hi is not None:
                nxt = min(nxt, (lo + hi) // 2)
            nxt = min(nxt, n * 2)
            if nxt <= n or (hi is not None and hi - lo <= max(1500, int(lo * 0.08))):
                break
            n = nxt
        else:
            hi = n
            if row["reason"] == "login-window" and lo > 0 and (row["used_mib"] or 0) < mem_mb * 0.75:
                log(f"login-window {kind} mem={mem} held={lo} used={row['used_mib']}")
                break
            if lo == 0:
                n = max(1000, n // 2)
            else:
                if hi - lo <= max(1500, int(lo * 0.08)):
                    break
                n = (lo + hi) // 2
    if best:
        log(
            f"MAX {kind} cpus={cpus} mem={mem} n={best['held']} connect_ms={best['connect_ms']} "
            f"p50={best['p50']:.3f} p99={best['p99']:.3f} usage={best['stats']}"
        )
    else:
        log(f"MAX {kind} cpus={cpus} mem={mem} n=0")
    return best


def smoke():
    row = run_once("bun", 1, "512m", 512, 200)
    if not row["ok"]:
        log("FAILED smoke")
        sys.exit(1)
    log("SMOKE ok")


def main():
    global SWAP_OK
    os.makedirs(HERE, exist_ok=True)
    if not os.path.exists(OUT):
        with open(OUT, "w") as f:
            f.write("broker\tcpus\tmem\tasked\theld\tconnect_ms\toom\tprobe_ok\tp50\tp99\treason\tusage\n")
    ncpu = int(run(["docker", "info", "--format", "{{.NCPU}}"]).stdout.strip())
    mem = int(run(["docker", "info", "--format", "{{.MemTotal}}"]).stdout.strip())
    log(f"START vm ncpu={ncpu} mem={mem} mode={sys.argv[1] if len(sys.argv) > 1 else 'full'}")
    if ncpu < 8 or mem < 20 * 1024**3:
        log(f"FAILED vm too small ncpu={ncpu} mem={mem}")
        sys.exit(1)
    probe = run(
        ["docker", "run", "--rm", "--memory", "64m", "--memory-swap", "64m", "debian:bookworm-slim", "true"],
        check=False,
    )
    SWAP_OK = probe.returncode == 0
    log(f"memory_swap={'on' if SWAP_OK else 'off'}")
    run(["docker", "network", "inspect", "qf-limit"], check=False)
    if run(["docker", "network", "inspect", "qf-limit"], check=False).returncode != 0:
        run(["docker", "network", "create", "qf-limit"])
    if len(sys.argv) > 1 and sys.argv[1] == "smoke":
        smoke()
        return
    kinds = sys.argv[1:] or ["rabbit", "rust", "bun", "php"]
    for kind in kinds:
        for cpus, mem_flag, mem_mb in STEPS:
            find_max(kind, cpus, mem_flag, mem_mb)
    log("DONE")


if __name__ == "__main__":
    try:
        main()
    finally:
        stop()
        pass

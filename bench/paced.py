#!/usr/bin/env python3
"""Paced queueforge-compare, one broker at a time, 1 CPU / 512 MiB."""
import os
import subprocess
import time

import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

ROOT = C.ROOT
COMPOSE = ["docker", "compose", "-f", "docker-compose.bench.yml"]
COMPARE = os.path.join(ROOT, "rust/target/release/queueforge-compare")
HERE = os.path.join(C.OUT_DIR, "paced")
os.makedirs(HERE, exist_ok=True)
LOG = os.path.join(HERE, "paced.log")
URL = "amqp://admin:devpassword12@127.0.0.1:{port}/%2f"
KINDS = [("rabbit", "35672"), ("rust", "35673"), ("bun", "35674"), ("php", "35675")]


def log(msg):
    line = time.strftime("%H:%M:%S") + " " + msg
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def run(args, env=None, timeout=900):
    log("RUN " + " ".join(args[:6]))
    p = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout)
    return p


def wait_ready(port):
    env = os.environ.copy()
    env.pop("QUEUEFORGE_COMPARE_RATES", None)
    env["AMQP_URL"] = URL.format(port=port)
    for i in range(90):
        p = subprocess.run([COMPARE, "--check"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=20)
        if p.returncode == 0:
            return i
        time.sleep(2)
    return None


def main():
    info = subprocess.run(["docker", "info", "--format", "ncpu={{.NCPU}} mem={{.MemTotal}}"], capture_output=True, text=True)
    log("START " + (info.stdout or "").strip())
    for kind, port in KINDS:
        subprocess.run(COMPOSE + ["rm", "-sf", kind], cwd=ROOT, check=False)
        up = run(COMPOSE + ["up", "-d", "--no-deps", kind], timeout=180)
        if up.returncode != 0:
            log(f"FAILED up {kind} {(up.stderr or '')[-400:]}")
            continue
        ready = wait_ready(port)
        if ready is None:
            log(f"FAILED ready {kind}")
            subprocess.run(COMPOSE + ["logs", "--tail", "40", kind], cwd=ROOT)
            subprocess.run(COMPOSE + ["rm", "-sf", kind], cwd=ROOT, check=False)
            continue
        log(f"ready {kind} after {ready * 2}s")
        env = os.environ.copy()
        env.pop("QUEUEFORGE_COMPARE_RATES", None)
        env["AMQP_URL"] = URL.format(port=port)
        for inflight in (1, 128):
            out = os.path.join(HERE, f"{kind}-{inflight}.txt")
            log(f"CELL {kind} inflight={inflight}")
            p = subprocess.run(
                [COMPARE, f"--inflight={inflight}"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=900,
            )
            open(out, "w").write((p.stdout or "") + "\n" + (p.stderr or ""))
            log(f"DONE {kind} inflight={inflight} exit={p.returncode}")
            if p.returncode != 0:
                log((p.stderr or p.stdout or "")[-500:])
        subprocess.run(COMPOSE + ["rm", "-sf", kind], cwd=ROOT, check=False)
    info = subprocess.run(["docker", "info", "--format", "ncpu={{.NCPU}} mem={{.MemTotal}}"], capture_output=True, text=True)
    log("END " + (info.stdout or "").strip())


if __name__ == "__main__":
    main()

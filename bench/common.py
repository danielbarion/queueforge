"""Shared settings for the Docker benchmarks in this directory.

Every broker runs alone in a container on one Docker network, with the client
in its own container on other VM CPUs. The large runs need Docker Desktop at 8
CPUs and at least 20 GiB. `ensure_big()` resizes it and `restore_small()` puts
it back at 2 CPUs and 8 GiB.

Environment:
  QF_BENCH_OUT       results directory (default bench/results/<UTC date>)
  QF_BENCH_SIDECARS  comma-separated containers to keep running and pin to
                     CPU 7 during the large runs (for example a local database)
  DOCKER_SETTINGS    Docker Desktop settings-store.json path
"""
import os
import re
import subprocess
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH = os.path.join(ROOT, "bench")
OUT_DIR = os.environ.get("QF_BENCH_OUT") or os.path.join(BENCH, "results", time.strftime("%Y-%m-%d", time.gmtime()))
os.makedirs(OUT_DIR, exist_ok=True)
RABBIT_CONF = os.path.join(BENCH, "rabbitmq.conf")
SETTINGS = os.environ.get(
    "DOCKER_SETTINGS",
    os.path.expanduser("~/Library/Group Containers/group.com.docker/settings-store.json"),
)
SIDECARS = [s for s in os.environ.get("QF_BENCH_SIDECARS", "").split(",") if s]

IMAGES = {
    "rabbit": "rabbitmq:4.3-management",
    "rust": "queueforge-rust:bench",
    "bun": "queueforge-bun:bench",
    "php": "queueforge-php:bench",
}
NAMES = {"rabbit": "RabbitMQ", "rust": "Rust", "bun": "Bun", "php": "PHP"}
LOG = os.path.join(OUT_DIR, "bench.log")

vm_ncpu = 0
vm_mem = 0


def log(msg):
    line = time.strftime("%H:%M:%S") + " " + msg
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def run(args, timeout=60, check=True):
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        if check:
            raise RuntimeError(f"timeout {args}")
        return subprocess.CompletedProcess(args, 124, out, "timeout")
    if check and p.returncode != 0:
        raise RuntimeError(f"{args} -> {p.returncode} {(p.stderr or '')[-800:]}")
    return p


def broker_env(kind):
    """Arguments that give each broker the admin / devpassword12 login."""
    if kind == "rabbit":
        return [
            "-e", "RABBITMQ_DEFAULT_USER=admin",
            "-e", "RABBITMQ_DEFAULT_PASS=devpassword12",
            "-e", "RABBITMQ_LOG=warning",
            "-v", f"{RABBIT_CONF}:/etc/rabbitmq/rabbitmq.conf:ro",
        ]
    if kind == "rust":
        return [
            "-e", "QUEUEFORGE_ADMIN_USER=admin",
            "-e", "QUEUEFORGE_ADMIN_PASSWORD=devpassword12",
            "-e", "RUST_LOG=warn",
        ]
    # Bun and PHP bench images start with --dev-bootstrap (admin / devpassword12).
    return []


def docker_up():
    global vm_ncpu, vm_mem
    p = run(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"], timeout=15, check=False)
    parts = (p.stdout or "").split()
    if p.returncode != 0 or len(parts) != 2:
        return False
    vm_ncpu, vm_mem = int(parts[0]), int(parts[1])
    return True


def image_ids():
    out = {}
    for kind, image in IMAGES.items():
        p = run(["docker", "image", "inspect", "-f", "{{.Id}}", image], check=False)
        out[kind] = (p.stdout or "").strip()
    return out


def _backend_gone():
    return subprocess.run(["pgrep", "-x", "com.docker.backend"], capture_output=True).returncode != 0


def _running_names():
    p = run(["docker", "ps", "--format", "{{.Names}}"], check=False)
    return [n for n in (p.stdout or "").split() if n and not n.startswith("qf-")]


def _set_vm(cpus, mem_mib):
    for _ in range(6):
        if not _backend_gone():
            time.sleep(1)
            continue
        text = open(SETTINGS).read()
        text, n1 = re.subn(r'("Cpus"\s*:\s*)\d+', rf"\g<1>{cpus}", text, count=1)
        text, n2 = re.subn(r'("MemoryMiB"\s*:\s*)\d+', rf"\g<1>{mem_mib}", text, count=1)
        if n1 != 1 or n2 != 1:
            raise RuntimeError("Docker settings edit failed")
        open(SETTINGS, "w").write(text)
        time.sleep(1)
        back = open(SETTINGS).read()
        if f'"Cpus": {cpus}' in back and f'"MemoryMiB": {mem_mib}' in back:
            return
    raise RuntimeError("Docker settings edit did not stick")


def _quit_docker():
    log("PROGRESS docker quit")
    subprocess.run(["osascript", "-e", 'tell application "Docker Desktop" to quit'], check=False)
    for _ in range(120):
        if _backend_gone():
            time.sleep(2)
            if _backend_gone():
                return True
        time.sleep(1)
    return False


def _start_docker():
    log("PROGRESS docker start")
    subprocess.run(["open", "-a", "Docker"], check=False)
    for _ in range(90):
        if docker_up():
            return True
        time.sleep(2)
    return False


def _sidecars(cpuset):
    for name in SIDECARS:
        run(["docker", "start", name], check=False, timeout=40)
        run(["docker", "update", "--cpuset-cpus", cpuset, name], check=False, timeout=30)
        log(f"sidecar {name} cpuset={cpuset}")


def ensure_big():
    """Docker Desktop at 8 CPUs and 24 GiB. CPUs 0-3 broker, 4-6 client, 7 sidecars."""
    if not docker_up():
        raise RuntimeError("docker is down")
    log(f"PROGRESS vm before ncpu={vm_ncpu} mem={vm_mem}")
    if vm_ncpu >= 8 and vm_mem >= 20 * 1024 ** 3:
        _sidecars("7")
        return
    saved = _running_names()
    open(os.path.join(OUT_DIR, "was-running.txt"), "w").write("\n".join(saved) + "\n")
    if not _quit_docker():
        raise RuntimeError("docker did not quit")
    _set_vm(8, 24576)
    if not _start_docker():
        raise RuntimeError("docker did not start large")
    for name in saved:
        run(["docker", "start", name], check=False, timeout=40)
    if not docker_up() or vm_ncpu < 8 or vm_mem < 20 * 1024 ** 3:
        raise RuntimeError(f"vm still small ncpu={vm_ncpu} mem={vm_mem}")
    log(f"PROGRESS vm large ncpu={vm_ncpu} mem={vm_mem}")
    _sidecars("7")


def restore_small():
    """Back to 2 CPUs and 8 GiB, restarting the containers that were running."""
    log("PROGRESS restore docker to 2 cpu 8192 MiB")
    saved = []
    path = os.path.join(OUT_DIR, "was-running.txt")
    if docker_up():
        saved = _running_names()
    elif os.path.exists(path):
        saved = [ln.strip() for ln in open(path) if ln.strip()]
    for name in SIDECARS:
        if name not in saved:
            saved.append(name)
    if not _quit_docker():
        log("FAILED restore quit")
        return False
    _set_vm(2, 8192)
    if not _start_docker():
        log("FAILED restore start")
        return False
    for name in saved:
        run(["docker", "start", name], check=False, timeout=40)
    _sidecars("0-1")
    ok = docker_up() and vm_ncpu == 2
    log(f"PROGRESS restore ncpu={vm_ncpu} mem={vm_mem} ok={ok}")
    return ok


def broker_cpuset(cpus):
    if vm_ncpu >= 8:
        return {1: "0", 2: "0,1", 4: "0-3"}[cpus]
    if cpus == 1:
        return "0"
    raise RuntimeError(f"vm has {vm_ncpu} cpus, cannot place {cpus}")


def client_cpus():
    """cpuset and --cpus for the client container."""
    if vm_ncpu >= 8:
        return "4-6", "3"
    return "1", "1"


def cgroup_text(name, path):
    p = run(["docker", "exec", name, "cat", path], check=False, timeout=10)
    return p.stdout or "" if p.returncode == 0 else ""


def cpu_usec(name):
    for line in cgroup_text(name, "/sys/fs/cgroup/cpu.stat").splitlines():
        if line.startswith("usage_usec "):
            return int(line.split()[1])
    return None


def mem_bytes(name):
    text = cgroup_text(name, "/sys/fs/cgroup/memory.current").strip()
    return int(text) if text.isdigit() else None


def anon_bytes(name):
    """Anonymous memory (heap and stacks, no page cache) from memory.stat."""
    for line in cgroup_text(name, "/sys/fs/cgroup/memory.stat").splitlines():
        if line.startswith("anon "):
            return int(line.split()[1])
    return None


def container_state(name):
    """(oom_killed, running, status) for a container."""
    p = run(["docker", "inspect", "-f", "{{.State.OOMKilled}} {{.State.Running}} {{.State.Status}}", name], check=False)
    parts = (p.stdout or "false false missing").split()
    return parts[0], len(parts) > 1 and parts[1] == "true", parts[2] if len(parts) > 2 else "missing"

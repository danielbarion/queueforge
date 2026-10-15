"""Host ceiling for bench/scaling.py: K independent 1-CPU brokers, each on its
own CPU with its own two loadgen processes, `weak-t` load.

    python3 bench/ceiling.py IMAGE KIND K[,K...] REPEATS    (Docker Desktop already at 15 CPUs)

Nothing is shared between the brokers but the host, so if K brokers' sum stays
near K x one broker the host scales, and any loss is the host's, not a
broker's multi-process mode. Compare a broker's scaling curve against this.
"""
import os, subprocess, sys, time, statistics, threading
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("QF_BENCH_OUT", "/tmp/qf-ceiling")
os.makedirs(os.environ["QF_BENCH_OUT"], exist_ok=True)
import common as C
import scaling as SC
import run as R

image, kind = sys.argv[1], sys.argv[2]
ks = [int(x) for x in sys.argv[3].split(",")]
repeats = int(sys.argv[4])
NET = "qf-ceil"
env = " ".join(f"{k}={v}" for k, v in {**SC.WEAK, "PERSISTENT": "0"}.items())


def sh(args, timeout=120):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def cpu_usec(name):
    return C.cpu_usec(name)


def one(k):
    sh(["docker", "network", "create", NET])
    names = [f"qf-ceil-b{i}" for i in range(k)]
    sh(["docker", "rm", "-fv", *names, "qf-ceil-client"])
    for i, n in enumerate(names):
        p = sh(["docker", "run", "-d", "--name", n, "--network", NET, "--cpuset-cpus", str(i), "--cpus", "1",
                "--memory", "1g", "--ulimit", "nofile=1048576:1048576"] + C.broker_env(kind) + [image])
        if p.returncode:
            raise SystemExit(p.stderr)
    sh(["docker", "run", "-d", "--name", "qf-ceil-client", "--network", NET, "--cpuset-cpus", SC.CLIENT_SET,
        "--cpus", str(SC.CLIENT_CPUS), "--memory", "2g", "--ulimit", "nofile=1048576:1048576", SC.LOADGEN])
    ips = [sh(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", n]).stdout.strip() for n in names]
    # Wait for every broker's AMQP port.
    for ip in ips:
        for _ in range(100):
            if sh(["docker", "exec", "qf-ceil-client", "sh", "-c", f"timeout 1 bash -c 'echo > /dev/tcp/{ip}/5672'"]).returncode == 0:
                break
            time.sleep(0.2)
    time.sleep(3)
    jobs = []
    for b, ip in enumerate(ips):
        for j in range(SC.WEAK_PROCS):
            jobs.append(f"{env} QPREFIX=b{b}p{j}- loadgen {ip} 5672 > /tmp/o{b}_{j} 2>&1 &")
    script = " ".join(jobs) + " wait; cat /tmp/o*"
    marks = {}

    def sample():
        time.sleep(7)
        marks["a"] = (time.monotonic(), [cpu_usec(n) for n in names])
        time.sleep(15)
        marks["b"] = (time.monotonic(), [cpu_usec(n) for n in names])

    t = threading.Thread(target=sample)
    t.start()
    out = sh(["docker", "exec", "qf-ceil-client", "bash", "-c", script], timeout=120).stdout
    t.join()
    res = [dict(kv.split("=", 1) for kv in line.split()[1:] if "=" in kv) for line in out.splitlines() if line.startswith("RESULT ")]
    sh(["docker", "rm", "-fv", *names, "qf-ceil-client"])
    if len(res) != k * SC.WEAK_PROCS or any(r.get("ok") != "1" for r in res):
        return None, None
    rate = min(sum(float(r["confirm_s"]) for r in res), sum(float(r["consume_s"]) for r in res))
    dt = marks["b"][0] - marks["a"][0]
    cpus = [(b - a) / (dt * 1e6) * 100 for a, b in zip(marks["a"][1], marks["b"][1])]
    return rate, sum(cpus)


C.docker_up()
if C.vm_ncpu != SC.VM_CPUS:
    raise SystemExit(f"Docker Desktop has {C.vm_ncpu} CPUs; run bench/scaling.py with --keep-vm first ({SC.VM_CPUS} CPUs)")
R.take_lease("bench/ceiling.py", "host ceiling")
print(f"vm ncpu={C.vm_ncpu}")
base = None
try:
    for k in ks:
        rates, cpus = [], []
        for r in range(repeats):
            rate, cpu = one(k)
            if rate:
                rates.append(rate)
                cpus.append(cpu)
        if not rates:
            print(f"K={k} brokers: every repeat failed", flush=True)
            continue
        m = statistics.median(rates)
        if base is None:
            base = m / k
        print(f"K={k} brokers: {m:,.0f} msg/s [{min(rates):,.0f}-{max(rates):,.0f}] cpu={statistics.median(cpus):.0f}% "
              f"per broker {m / k:,.0f} eff={m / (k * base):.2f} us/msg={statistics.median(cpus) / 100 / m * 1e6:.2f}", flush=True)
finally:
    R.release_lease()

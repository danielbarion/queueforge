#!/usr/bin/env python3
"""The disk's own write ceiling, where the brokers write.

A durable confirm waits on a write and an fdatasync. How fast that can go
depends on how much one sync covers, so each block size here is written with
`dd oflag=dsync` (one sync per block), plus one bulk write with a single
`fdatasync` at the end. The container has the 1 CPU / 1 GiB scenario shape and
writes to an anonymous volume, as the broker containers do. Results go to
`<results>/disk.tsv`: one row per block size, the median MB/s of the repeats
and their range.

Usage:
  bench/disk.py [--repeat 3]
"""
import argparse
import csv
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402

IMAGE = "debian:bookworm-slim"
NAME = "qf-disk"
MIB = 1024 * 1024
# (label, dd block size, total bytes, one sync per block). Each case runs a
# second or more; shorter runs swung 2x on Docker Desktop, whose disk is a file
# on the host. 4 KiB writes less: at one sync each it runs about 17 MB/s here.
CASES = [
    ("4 KiB", "4K", 64 * MIB, True),
    ("64 KiB", "64K", 512 * MIB, True),
    ("256 KiB", "256K", 1024 * MIB, True),
    ("1 MiB", "1M", 1024 * MIB, True),
    ("4 MiB", "4M", 2048 * MIB, True),
    ("2 GiB, one sync", "1M", 2048 * MIB, False),
]
COLS = ["case", "block_bytes", "synced_each", "mb_s", "min_mb_s", "max_mb_s", "repeats"]
COPIED = re.compile(r"^(\d+) bytes .* copied, ([\d.]+) s")


def block_bytes(size):
    return int(size[:-1]) * {"K": 1024, "M": MIB}[size[-1]]


def script(repeat):
    """One shell run: every case `repeat` times, each line `index bytes seconds`."""
    lines = []
    for _ in range(repeat):
        for i, (_label, bs, total, each) in enumerate(CASES):
            count = total // block_bytes(bs)
            flag = "oflag=dsync" if each else "conv=fdatasync"
            lines.append(
                f"r=$(dd if=/dev/zero of=/data/f bs={bs} count={count} {flag} 2>&1 | tail -1); "
                f'echo "{i} $r"; rm -f /data/f'
            )
    return "\n".join(lines)


def parse(text, repeat):
    """MB/s samples per case index from the shell output."""
    samples = {i: [] for i in range(len(CASES))}
    for line in text.splitlines():
        head, _, rest = line.partition(" ")
        if not head.isdigit():
            continue
        match = COPIED.match(rest)
        if match and float(match.group(2)) > 0:
            samples[int(head)].append(int(match.group(1)) / float(match.group(2)) / 1e6)
    missing = [CASES[i][0] for i, values in samples.items() if len(values) != repeat]
    if missing:
        raise RuntimeError(f"disk probe missed {missing}")
    return samples


def rows(samples, repeat):
    out = []
    for i, (label, bs, _total, each) in enumerate(CASES):
        values = samples[i]
        out.append({
            "case": label, "block_bytes": block_bytes(bs), "synced_each": "1" if each else "0",
            "mb_s": f"{statistics.median(values):.1f}", "min_mb_s": f"{min(values):.1f}",
            "max_mb_s": f"{max(values):.1f}", "repeats": repeat,
        })
    return out


def measure(repeat=3):
    C.run(["docker", "rm", "-fv", NAME], check=False, timeout=40)
    try:
        p = C.run([
            "docker", "run", "--rm", "--name", NAME,
            "--cpuset-cpus", C.broker_cpuset(1), "--cpus", "1", "--memory", "1g", "--memory-swap", "1g",
            "-v", "/data", IMAGE, "sh", "-c", script(repeat),
        ], timeout=900)
    finally:
        C.run(["docker", "rm", "-fv", NAME], check=False, timeout=40)
    result = rows(parse(p.stdout or "", repeat), repeat)
    path = os.path.join(C.OUT_DIR, "disk.tsv")
    with open(path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=COLS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(result)
    for r in result:
        C.log(f"DISK {r['case']}: {r['mb_s']} MB/s ({r['min_mb_s']}-{r['max_mb_s']})")
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repeat", type=int, default=3)
    opts = p.parse_args(argv)
    if opts.repeat < 1:
        p.error("--repeat must be positive")
    measure(opts.repeat)


if __name__ == "__main__":
    main()

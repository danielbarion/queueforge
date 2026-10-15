#!/usr/bin/env python3
"""A/B one broker's images on the same scenario cells, interleaved.

Each repeat runs every image once per scenario before the next repeat, so a
machine that slows down during the run slows every image alike. Results go to
their own directory with a summary in ab.md.

Usage:
  python3 bench/ab.py bun before=qf-coco:bun-88ef after=queueforge-bun:bench \\
      --scenarios large-1m,telemetry --repeat 3 [--cpus 1] [--out DIR]

`name=image` labels each image in the summary; the first is the reference.
"""
import argparse
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def parse(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("kind", choices=["rabbit", "rust", "bun", "php"])
    p.add_argument("images", nargs="+", help="name=image, two or more; the first is the reference")
    p.add_argument("--scenarios", required=True, help="comma list of scenario ids")
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--cpus", type=int, choices=[1, 4], default=1)
    p.add_argument("--out", default="")
    opts = p.parse_args(argv)
    pairs = []
    for item in opts.images:
        name, sep, image = item.partition("=")
        if not sep or not name or not image:
            p.error(f"{item!r} is not name=image")
        pairs.append((name, image))
    if len(pairs) < 2 or len({n for n, _ in pairs}) != len(pairs):
        p.error("give two or more images with distinct names")
    if opts.repeat < 1:
        p.error("--repeat must be positive")
    opts.images = pairs
    opts.scenarios = [s for s in opts.scenarios.split(",") if s]
    if not opts.out:
        opts.out = os.path.join(HERE, "results-quick", "ab-" + time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime()))
    return opts


def score(row):
    """MB/s for the large-message cells, received msg/s for the rest; None when the cell failed."""
    if str(row.get("ok")) != "1":
        return None
    value = row.get("mb_s") or row.get("received_s")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def summary_md(kind, size, images, scenarios, rows):
    """`rows` maps (image name, scenario) to the list of rows run for it."""
    ref = images[0][0]
    lines = [
        f"# A/B: {kind}, {size}", "",
        "Median score over repeats (MB/s for large-message cells, received msg/s otherwise), "
        f"with the spread and the change against `{ref}`.", "",
        "| Scenario | " + " | ".join(name for name, _ in images) + " |",
        "| --- | " + " | ".join("---:" for _ in images) + " |",
    ]
    for sc in scenarios:
        cells = []
        base = None
        for name, _ in images:
            values = [v for v in (score(r) for r in rows.get((name, sc), [])) if v is not None]
            if not values:
                cells.append("failed")
                continue
            med = statistics.median(values)
            text = f"{med:,.1f} ({min(values):,.1f}–{max(values):,.1f})"
            if name == ref:
                base = med
            elif base:
                text += f" {(med - base) / base * 100:+.0f}%"
            cells.append(text)
        lines.append(f"| {sc} | " + " | ".join(cells) + " |")
    lines += ["", "Images: " + ", ".join(f"`{n}` = `{i}`" for n, i in images), ""]
    return "\n".join(lines)


def main():
    opts = parse()
    os.environ["QF_BENCH_OUT"] = os.path.abspath(opts.out)
    sys.path.insert(0, HERE)
    import common as C  # noqa: E402  (reads QF_BENCH_OUT at import)
    import run  # noqa: E402
    import scenarios as S  # noqa: E402

    known = {s["id"]: s for s in S.SCENARIOS}
    unknown = [s for s in opts.scenarios if s not in known]
    if unknown:
        raise SystemExit(f"unknown scenario ids: {unknown}")
    if not C.docker_up():
        raise SystemExit("Docker is not running.")
    digests = {name: S.image_digest(image) for name, image in opts.images}
    cpus, mem, size = next(s for s in S.SIZES if s[0] == opts.cpus)
    run.take_lease("bench/ab.py", f"A/B of {opts.kind} images")
    rows = {}
    try:
        C.ensure_big()
        C.run(["docker", "pull", "-q", S.PERF_TEST], check=False, timeout=600)
        client = S.image_digest(S.PERF_TEST)
        cohorts = {name: os.urandom(16).hex() for name, _ in opts.images}
        for ordinal in range(1, opts.repeat + 1):
            for sc_id in opts.scenarios:
                for name, _ in opts.images:
                    C.log(f"AB {name} {sc_id} repeat {ordinal}/{opts.repeat}")
                    row = S.run_cell(opts.kind, cpus, mem, size, known[sc_id], cohort_id=cohorts[name],
                                     broker_image=digests[name], client_image=client,
                                     expected_repeats=opts.repeat, repeat_ordinal=ordinal)
                    rows.setdefault((name, sc_id), []).append(row or {})
    finally:
        C.run(["docker", "rm", "-fv", S.BROKER, S.CLIENT], check=False)
        C.restore_small()
        run.release_lease()
    text = summary_md(opts.kind, size, [(n, digests[n]) for n, _ in opts.images], opts.scenarios, rows)
    with open(os.path.join(C.OUT_DIR, "ab.md"), "w") as f:
        f.write(text)
    print(text)
    print(os.path.join(C.OUT_DIR, "ab.md"))


if __name__ == "__main__":
    main()

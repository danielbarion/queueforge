#!/usr/bin/env python3
"""Turn one results directory into BENCHMARK.md tables and the site's data file.

Usage: bench/report.py [results dir]   (default: the newest bench/results/*)
Writes <dir>/report.md and site/app/bench-data.ts. BENCHMARK.md and the site
text are edited by hand around those tables.
"""
import csv
import glob
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C  # noqa: E402
import scenarios as S  # noqa: E402

ORDER = ["rabbit", "rust", "bun", "php"]
TONE = {"rabbit": "mq", "rust": "rust", "bun": "bun", "php": "php"}
LATENCY = {"latency-1k", "latency-10k", "many-queues", "slow-consumers"}


def tsv(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f, delimiter="\t"))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fmt(v, digits=0):
    if v is None:
        return "–"
    return f"{v:,.{digits}f}"


# --- paced ---------------------------------------------------------------

def paced(root):
    out = {}
    for path in glob.glob(os.path.join(root, "paced", "*-*.txt")):
        kind, inflight = os.path.basename(path)[:-4].rsplit("-", 1)
        cur = None
        for line in open(path, errors="replace"):
            line = line.strip()
            if line.startswith("scenario="):
                cur = {"scenario": line.split("=", 1)[1]}
                out.setdefault((kind, int(inflight)), []).append(cur)
            elif cur is not None and "=" in line:
                for part in line.split():
                    if "=" in part:
                        k, v = part.split("=", 1)
                        cur.setdefault(k, v)
    return out


def paced_md(data):
    lines = []
    for inflight in (128, 1):
        lines.append(f"#### {'128 confirms' if inflight == 128 else 'One confirm'} in flight\n")
        lines.append("| Broker | durable-256 msg/s | first miss | confirm p50 ms | fan-2x2 msg/s | first miss |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        for kind in ORDER:
            rows = {r["scenario"]: r for r in data.get((kind, inflight), [])}
            d, f = rows.get("durable-256", {}), rows.get("fan-2x2", {})

            def miss(r):
                load = r.get("saturation_load", "")
                return "kept" if r.get("kept_up") == "true" else load or "–"

            lines.append(
                f"| {C.NAMES[kind]} | {fmt(num(d.get('pace_messages_per_sec')), 2)} | {miss(d)} | "
                f"{d.get('confirm_p50_ms', '–')} | {fmt(num(f.get('pace_messages_per_sec')), 2)} | {miss(f)} |"
            )
        lines.append("")
    return "\n".join(lines)


# --- load ----------------------------------------------------------------

SIZE = {(1, 512): "1 CPU / 512 MiB", (1, 1024): "1 CPU / 1 GiB", (2, 2048): "2 CPU / 2 GiB",
        (4, 4096): "4 CPU / 4 GiB", (4, 8192): "4 CPU / 8 GiB"}


def load(root):
    cells = {}
    for r in tsv(os.path.join(root, "load.tsv")):
        key = (r["kind"], int(r["cpus"]), int(r["mem_mb"]), r["shape"])
        cells[key] = r
    return cells


def load_rate(r, shape):
    """The rate the site shows: the lower of confirm/s and consume/s, so a growing queue does not score."""
    if not r or r.get("ok") != "1":
        return None
    a, b = num(r.get("confirm_s")), num(r.get("consume_s"))
    if a is None or b is None:
        return None
    return min(a, b)


def load_md(cells):
    lines = ["| Container | Broker | single confirm/s | single consume/s | shared confirm/s | shared consume/s | spread confirm/s | spread consume/s | spread MiB |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for (cpus, mem), size in SIZE.items():
        for kind in ORDER:
            cols = []
            for shape in ("single", "shared", "spread"):
                r = cells.get((kind, cpus, mem, shape))
                if not r or r.get("ok") != "1":
                    err = (r or {}).get("err") or "–"
                    cols += [err, ""]
                else:
                    cols += [fmt(num(r["confirm_s"]), 1), fmt(num(r["consume_s"]), 1)]
            spread = cells.get((kind, cpus, mem, "spread")) or {}
            lines.append(f"| {size} | {C.NAMES[kind]} | " + " | ".join(cols) + f" | {spread.get('broker_mib', '')} |")
    return "\n".join(lines)


# --- connections ---------------------------------------------------------

def connections(root):
    best = {}
    for r in tsv(os.path.join(root, "connections.tsv")):
        if r.get("reason"):
            continue
        key = (r["broker"], int(r["cpus"]), r["mem"])
        held = int(r["held"])
        if held > best.get(key, (0, ""))[0]:
            best[key] = (held, r.get("usage", "").split("/")[0].strip())
    return best


# --- scenarios -----------------------------------------------------------

def scenario_cells(root):
    cells = {}
    for r in tsv(os.path.join(root, "scenarios.tsv")):
        cells[(r["broker"], r["size"], r["scenario"])] = r
    return cells


def primary(sc, r):
    """(value, unit, higher_is_better) for one cell, or None when it did not run cleanly."""
    if not r or r.get("ok") != "1":
        return None
    if sc["id"] == "backlog":
        if r.get("kept") != "yes":
            return None
        return (num(r["sent_s"]), num(r["received_s"]))
    if sc.get("bytes"):
        return num(r["mb_s"])
    if sc["id"] in LATENCY:
        return num(r["lat_p99_ms"])
    return num(r["received_s"])


def cell_text(sc, r):
    if not r:
        return "–"
    if r.get("ok") != "1":
        return f"failed ({r.get('err') or 'no result'})"
    if sc["id"] == "backlog" and r.get("kept") != "yes":
        return (f"{fmt(num(r.get('sent_s')))} / {fmt(num(r.get('received_s')))} "
                f"(incomplete: {r.get('kept') or 'completion not verified'})")
    v = primary(sc, r)
    if sc["id"] == "backlog":
        return f"{fmt(v[0])} / {fmt(v[1])}"
    if sc.get("bytes"):
        return f"{fmt(v, 1)} MB/s"
    if sc["id"] in LATENCY:
        kept = "" if r.get("kept") == "yes" else " (missed rate)"
        return f"{fmt(v, 2)} ms{kept}"
    return fmt(v)


def scenarios_md(cells, size):
    lines = [f"| Scenario | What it stands for | Score | " + " | ".join(C.NAMES[k] for k in ORDER) + " |",
             "| --- | --- | --- | " + " | ".join("---:" for _ in ORDER) + " |"]
    for sc in S.SCENARIOS:
        row = [cells.get((k, size, sc["id"])) for k in ORDER]
        if not any(row):
            continue
        if sc["id"] == "backlog":
            score = "fill / drain msg/s"
        elif sc.get("bytes"):
            score = "delivered MB/s"
        elif sc["id"] in LATENCY:
            score = "p99 latency at the offered rate"
        elif sc["id"] == "fanout-20":
            score = "deliveries/s (20 per publish)"
        else:
            score = "delivered msg/s"
        lines.append(f"| {sc['title']} | {sc['real']} | {score} | " + " | ".join(cell_text(sc, r) for r in row) + " |")
    return "\n".join(lines)


def scenarios_mem_md(cells, size):
    lines = ["| Scenario | " + " | ".join(C.NAMES[k] for k in ORDER) + " |",
             "| --- | " + " | ".join("---:" for _ in ORDER) + " |"]
    for sc in S.SCENARIOS:
        row = [cells.get((k, size, sc["id"])) for k in ORDER]
        if not any(row):
            continue

        def m(r):
            if not r:
                return "–"
            return f"{r.get('anon_mib') or '?'} / {r.get('peak_mib') or '?'}"

        lines.append(f"| {sc['title']} | " + " | ".join(m(r) for r in row) + " |")
    return "\n".join(lines)


# --- site data -----------------------------------------------------------

def site_ts(paced_data, cells, conns, sc_cells):
    def paced_rate(kind):
        rows = {r["scenario"]: r for r in paced_data.get((kind, 128), [])}
        return num(rows.get("durable-256", {}).get("pace_messages_per_sec")) or 0

    out = ["// Generated by bench/report.py from bench/results. Do not edit by hand.", "",
           'import type { Bar, LoadRow, ScenarioRow } from "./bench";', ""]
    out.append("export const paced: Bar[] = [")
    for kind in ORDER:
        out.append(f'  {{ name: "{C.NAMES[kind]}", rate: {paced_rate(kind):.2f}, tone: "{TONE[kind]}" }},')
    out.append("];\n")
    out.append("export const scale: Bar[] = [")
    for kind in ORDER:
        r = cells.get((kind, 4, 4096, "spread"))
        out.append(f'  {{ name: "{C.NAMES[kind]}", rate: {load_rate(r, "spread") or 0:.1f}, tone: "{TONE[kind]}" }},')
    out.append("];\n")
    out.append("export const loadRows: LoadRow[] = [")
    mem_name = {512: "512m", 1024: "1g", 2048: "2g", 4096: "4g", 8192: "8g"}
    for (cpus, mem), size in SIZE.items():
        for kind in ORDER:
            one = load_rate(cells.get((kind, cpus, mem, "single")), "single")
            shared = load_rate(cells.get((kind, cpus, mem, "shared")), "shared")
            spread = load_rate(cells.get((kind, cpus, mem, "spread")), "spread")
            caveats = {k: "failed" for k, v in (("one", one), ("shared", shared), ("spread", spread)) if v is None}
            conn = conns.get((kind, cpus, mem_name[mem]))
            mib = (cells.get((kind, cpus, mem, "spread")) or {}).get("broker_mib", "")
            out.append(
                f'  {{ app: "{C.NAMES[kind]}", size: "{size}", one: {one if one is not None else "null"}, '
                f'shared: {shared or 0}, spread: {spread or 0}, '
                + (f"caveats: {json.dumps(caveats)}, " if caveats else "")
                + f'connections: "{f"{conn[0]:,}" if conn else "n/a"}", mib: "{mib}", login: "{conn[1] if conn else "n/a"}" }},'
            )
    out.append("];\n")
    out.append("export const scenarioRows: ScenarioRow[] = [")
    for size in [s[2] for s in S.SIZES]:
        for sc in S.SCENARIOS:
            row = {k: sc_cells.get((k, size, sc["id"])) for k in ORDER}
            if not any(row.values()):
                continue
            vals = {}
            for k, r in row.items():
                v = primary(sc, r)
                if isinstance(v, tuple):
                    v = v[1]
                vals[TONE[k]] = None if v is None else round(v, 2)
            kind = "latency" if sc["id"] in LATENCY else ("bytes" if sc.get("bytes") else "rate")
            texts = {TONE[k]: cell_text(sc, r) for k, r in row.items()}
            out.append(
                f'  {{ size: "{size}", id: "{sc["id"]}", title: {json.dumps(sc["title"])}, real: {json.dumps(sc["real"])}, '
                f'kind: "{kind}", values: {json.dumps(vals)}, text: {json.dumps(texts)} }},'
            )
    out.append("];")
    return "\n".join(out) + "\n"


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else sorted(glob.glob(os.path.join(C.BENCH, "results", "*")))[-1]
    p = paced(root)
    cells = load(root)
    conns = connections(root)
    sc = scenario_cells(root)
    parts = ["### Paced\n", paced_md(p), "### Load\n", load_md(cells), ""]
    for size in [s[2] for s in S.SIZES]:
        parts += [f"### Scenarios, {size}\n", scenarios_md(sc, size), "",
                  f"Peak memory, {size} (anonymous / cgroup total MiB):\n", scenarios_mem_md(sc, size), ""]
    parts += ["### Connections\n", "| Broker | Size | Held | Memory at hold |", "| --- | --- | ---: | ---: |"]
    for (kind, cpus, mem), (held, used) in sorted(conns.items(), key=lambda kv: (ORDER.index(kv[0][0]), kv[0][1], kv[0][2])):
        parts.append(f"| {C.NAMES[kind]} | {cpus} CPU / {mem} | {held:,} | {used} |")
    open(os.path.join(root, "report.md"), "w").write("\n".join(parts) + "\n")
    open(os.path.join(C.ROOT, "site", "app", "bench-data.ts"), "w").write(site_ts(p, cells, conns, sc))
    print(os.path.join(root, "report.md"))


if __name__ == "__main__":
    main()

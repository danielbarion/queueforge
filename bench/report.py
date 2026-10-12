#!/usr/bin/env python3
"""Turn one results directory into BENCHMARK.md tables and the site's data file.

Usage: bench/report.py [results dir]   (default: the newest bench/results/*)
Writes <dir>/report.md and site/app/bench-data.ts. BENCHMARK.md and the site
text are edited by hand around those tables.
"""
import csv
import glob
import json
import math
import os
import re
import statistics
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
        value = float(v)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def fmt(v, digits=0):
    if v is None:
        return "–"
    return f"{v:,.{digits}f}"


def spaced_units(text):
    """`454.7MiB` as docker stats prints it, read as `454.7 MiB`."""
    return re.sub(r"(\d)([KMGT]i?B|B)\b", r"\1 \2", text)


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
            best[key] = (held, spaced_units(r.get("usage", "").split("/")[0].strip()))
    return best


# --- scenarios -----------------------------------------------------------

def scenario_cells(root):
    return {key: value["row"] for key, value in scenario_repeat_stats(root).items()}


SCENARIO_METRICS = ["sent_s", "confirmed_s", "received_s", "mb_s", "lat_p50_ms", "lat_p99_ms",
                    "confirm_p50_ms", "confirm_p99_ms", "peak_mib", "anon_mib", "broker_cpu", "client_cpu"]


def scenario_repeat_stats(root):
    """Latest invocation/coherent provenance cohort per cell, with all outcomes retained.

    Legacy rows without run IDs keep last-row selection. New run JSON is the
    authoritative record; unfinished provenance records count as incomplete.
    Metrics include every numeric observation, including failed-run memory.
    """
    rows = tsv(os.path.join(root, "scenarios.tsv"))
    by_id = {r["run_id"]: i for i, r in enumerate(rows) if r.get("run_id")}
    def read_object(path):
        try:
            with open(path) as f:
                value = json.load(f)
            return value if isinstance(value, dict) else None
        except (OSError, ValueError):
            return None
    for path in sorted(glob.glob(os.path.join(root, "runs", "*", "provenance.json"))):
        meta = read_object(path)
        metadata_keys = ("run_id", "cohort_id", "broker_image", "client_image", "harness_sha256", "scenario_sha256", "resources_sha256", "started_utc")
        if meta is not None and (not all(key in meta for key in metadata_keys) or not isinstance(meta.get("broker"), str)
                                 or not isinstance(meta.get("scenario"), dict) or not isinstance(meta["scenario"].get("id"), str)
                                 or not isinstance(meta.get("resources"), dict) or "broker_cpus" not in meta["resources"]):
            meta = None
        result = os.path.join(os.path.dirname(path), "row.json")
        row = read_object(result)
        if row is not None and not all(isinstance(row.get(key), str) for key in ("broker", "size", "scenario", "ok")):
            row = None
        directory_id = os.path.basename(os.path.dirname(path))
        if meta is None:
            # Preserve attribution from the redundant TSV/result if possible;
            # otherwise retain an explicitly unassigned, unscored artifact.
            row = row or (dict(rows[by_id[directory_id]]) if directory_id in by_id else {})
            row.update(ok="0", err="incomplete", artifact_error=path)
            row.setdefault("run_id", directory_id)
            row.setdefault("broker", "unknown")
            row.setdefault("size", "unknown")
            row.setdefault("scenario", "unreadable:" + directory_id)
        else:
            if row is None:
                resource = meta["resources"]
                row = {"broker": meta["broker"], "scenario": meta["scenario"]["id"],
                       "size": next((size for cpus, _mem, size in S.SIZES if cpus == resource["broker_cpus"]), str(resource)),
                       "ok": "0", "err": "incomplete"}
                if os.path.exists(result):
                    row["artifact_error"] = result
            row.update({k: meta[k] for k in ("run_id", "cohort_id", "broker_image", "client_image", "harness_sha256", "scenario_sha256", "resources_sha256", "started_utc")})
            row.update({k: meta[k] for k in ("expected_repeats", "repeat_ordinal") if k in meta})
        if row["run_id"] in by_id:
            rows[by_id[row["run_id"]]] = row
        else:
            rows.append(row)
    groups = {}
    latest = {}
    for index, row in enumerate(rows):
        cell = (row["broker"], row["size"], row["scenario"])
        cohort = tuple(row.get(k, "") for k in ("cohort_id", "broker_image", "client_image", "harness_sha256", "scenario_sha256", "resources_sha256")) if row.get("run_id") else ("legacy", index)
        groups.setdefault((cell, cohort), []).append(row)
        # Timestamps are UTC ISO strings. A legacy row's append position is
        # the only available time; retain its existing last-row semantics.
        order = (row.get("started_utc", ""), index)
        if cell not in latest or order > latest[cell][0]:
            latest[cell] = (order, cohort)
    out = {}
    for cell, (_order, cohort) in latest.items():
        repeats = groups[(cell, cohort)]
        sc = next((s for s in S.SCENARIOS if s["id"] == cell[2]), {"id": cell[2]})
        failed = sum(r.get("ok") != "1" and r.get("err") != "incomplete" for r in repeats)
        incomplete = sum(r.get("err") == "incomplete" or (sc["id"] == "backlog" and r.get("ok") == "1" and r.get("kept") != "yes") for r in repeats)
        missed = sum(r.get("ok") == "1" and sc["id"] != "backlog" and (r.get("kept") != "yes" if sc["id"] in LATENCY else r.get("kept") not in (None, "", "yes")) for r in repeats)
        success = len(repeats) - failed - incomplete - missed
        planned = [int(value) for r in repeats if (value := num(r.get("expected_repeats"))) is not None and value >= 1 and value.is_integer()]
        expected = max([len(repeats), *planned])
        missing = expected - len(repeats)
        incomplete += missing
        metrics = {}
        for metric in SCENARIO_METRICS:
            values = [v for r in repeats if (v := num(r.get(metric))) is not None]
            metrics[metric] = {"n": len(values), "median": statistics.median(values) if values else None,
                               "min": min(values) if values else None, "max": max(values) if values else None}
        row = dict(repeats[-1])
        for metric, summary in metrics.items():
            row[metric] = summary["median"]
        summary = {"cohort": cohort, "n": len(repeats), "expected": expected, "missing": missing, "success": success, "failed": failed,
                   "incomplete": incomplete, "missed": missed, "metrics": metrics, "run_ids": [r.get("run_id") for r in repeats],
                   "unreadable_artifacts": [r["artifact_error"] for r in repeats if r.get("artifact_error")]}
        row["_repeat_stats"] = summary
        out[cell] = {**summary, "row": row}
    return out


def primary(sc, r):
    """(value, unit, higher_is_better) for one cell, or None when it did not run cleanly."""
    if not r or r.get("ok") != "1":
        return None
    repeats = r.get("_repeat_stats", {})
    if repeats.get("failed", 0) or repeats.get("incomplete", 0) or repeats.get("missed", 0):
        return None
    if sc["id"] == "backlog" and r.get("kept") != "yes":
        return None
    if sc["id"] in LATENCY and r.get("kept") != "yes":
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
    """A cell's score for BENCHMARK.md, with the range across repeats."""
    if not r:
        return "–"
    repeat = r.get("_repeat_stats", {})
    if (repeat.get("n", 0) > 1 or repeat.get("missing", 0)) and (repeat["failed"] or repeat["incomplete"] or repeat["missed"]):
        return f"unscored ({repeat['success']}/{repeat.get('expected', repeat['n'])} complete; {repeat['failed']} failed, {repeat['incomplete']} incomplete, {repeat['missed']} missed rate)"
    if r.get("ok") != "1":
        return f"failed ({r.get('err') or 'no result'})"
    if sc["id"] == "backlog" and r.get("kept") != "yes":
        return (f"{fmt(num(r.get('sent_s')))} / {fmt(num(r.get('received_s')))} "
                f"(incomplete: {r.get('kept') or 'completion not verified'})")
    v = primary(sc, r)
    def repeat_range(metric, digits=0):
        if repeat.get("n", 0) < 2:
            return ""
        stats = repeat["metrics"][metric]
        return f" [{fmt(stats['min'], digits)}–{fmt(stats['max'], digits)}; n={repeat['n']}]"
    if sc["id"] == "backlog":
        if v is None:
            return f"incomplete ({r.get('kept') or 'drain unverified'})"
        return f"{fmt(v[0])} / {fmt(v[1])}" + repeat_range("received_s")
    if sc.get("bytes"):
        note = " (missed delivery rate)" if v is None and r.get("kept") == "no" else ""
        return f"{fmt(num(r.get('mb_s')), 1)} MB/s{note}" + repeat_range("mb_s", 1)
    if sc["id"] in LATENCY:
        kept = "" if r.get("kept") == "yes" else " (missed rate)"
        return f"{fmt(num(r.get('lat_p99_ms')), 2)} ms{kept}" + repeat_range("lat_p99_ms", 2)
    note = " (missed delivery rate)" if v is None and r.get("kept") == "no" else ""
    return fmt(num(r.get("received_s"))) + note + repeat_range("received_s")


def site_text(sc, r):
    """A cell's score for the site: the number alone, since the row names the
    unit, or a short reason it has none."""
    if not r:
        return "–"
    repeat = r.get("_repeat_stats", {})
    if (repeat.get("n", 0) > 1 or repeat.get("missing", 0)) and (repeat["failed"] or repeat["incomplete"] or repeat["missed"]):
        return "missed rate" if repeat["missed"] else "failed" if repeat["failed"] else "incomplete"
    if r.get("ok") != "1":
        return "failed"
    v = primary(sc, r)
    if sc["id"] == "backlog":
        return "incomplete" if v is None else f"{fmt(v[0])} / {fmt(v[1])}"
    if sc.get("bytes"):
        return fmt(num(r.get("mb_s")), 1) + (" (missed rate)" if v is None and r.get("kept") == "no" else "")
    if sc["id"] in LATENCY:
        return fmt(num(r.get("lat_p99_ms")), 2) + ("" if r.get("kept") == "yes" else " (missed rate)")
    return fmt(num(r.get("received_s"))) + (" (missed rate)" if v is None and r.get("kept") == "no" else "")


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
            def metric(name):
                value = num(r.get(name))
                return "?" if value is None else fmt(value)
            return f"{metric('anon_mib')} / {metric('peak_mib')}"

        lines.append(f"| {sc['title']} | " + " | ".join(m(r) for r in row) + " |")
    return "\n".join(lines)


# --- site data -----------------------------------------------------------

def site_ts(paced_data, cells, conns, sc_cells, measured=""):
    def paced_rate(kind):
        rows = {r["scenario"]: r for r in paced_data.get((kind, 128), [])}
        return num(rows.get("durable-256", {}).get("pace_messages_per_sec")) or 0

    out = ["// Generated by bench/report.py from bench/results. Do not edit by hand.", "",
           'import type { Bar, LoadRow, ScenarioRow } from "./bench";', ""]
    # The date the pages quote for this run, so it is not typed by hand.
    out.append(f'export const MEASURED = "{measured}";\n')
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
            texts = {TONE[k]: site_text(sc, r) for k, r in row.items()}
            memory = {TONE[k]: {"anonymous": num((r or {}).get("anon_mib")),
                                "cgroup": num((r or {}).get("peak_mib"))} for k, r in row.items()}
            out.append(
                f'  {{ size: "{size}", id: "{sc["id"]}", title: {json.dumps(sc["title"])}, real: {json.dumps(sc["real"])}, '
                f'kind: "{kind}", values: {json.dumps(vals)}, text: {json.dumps(texts)}, memory: {json.dumps(memory)} }},'
            )
    out.append("];")
    return "\n".join(out) + "\n"


def main():
    # `--no-site` leaves site/app/bench-data.ts alone, for quick runs.
    args = [a for a in sys.argv[1:] if a != "--no-site"]
    write_site = "--no-site" not in sys.argv[1:]
    root = args[0] if args else sorted(glob.glob(os.path.join(C.BENCH, "results", "*")))[-1]
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
    if write_site:
        measured = re.match(r"\d{4}-\d{2}-\d{2}", os.path.basename(os.path.normpath(root)))
        open(os.path.join(C.ROOT, "site", "app", "bench-data.ts"), "w").write(site_ts(p, cells, conns, sc, measured.group(0) if measured else ""))
    print(os.path.join(root, "report.md"))


if __name__ == "__main__":
    main()

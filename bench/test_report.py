"""Reporting regressions; run with python3 -B -m unittest discover -s bench."""
import unittest
import csv
import json
import tempfile
from pathlib import Path

import report
import scenarios


class RepeatReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def row(self, run, rate, **extra):
        return {"broker": "bun", "size": "1 CPU / 1 GiB", "scenario": "work-queue",
                "ok": "1", "kept": "yes", "err": "", "received_s": str(rate),
                "run_id": run, "cohort_id": "baseline", "broker_image": "image-a",
                "client_image": "client-a", "harness_sha256": "harness-a",
                "scenario_sha256": "scenario-a", "resources_sha256": "resources-a",
                "started_utc": f"2026-10-09T12:00:0{run[-1]}Z", **extra}

    def write(self, rows):
        fields = sorted({key for row in rows for key in row})
        with (self.root / "scenarios.tsv").open("w") as f:
            writer = csv.DictWriter(f, fields, delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)

    def selected(self):
        return report.scenario_repeat_stats(str(self.root))[("bun", "1 CPU / 1 GiB", "work-queue")]

    def test_repeat_median_and_range_retain_each_observation(self):
        self.write([self.row("run1", 100, anon_mib="10"), self.row("run2", 300, anon_mib="30")])
        stats = self.selected()
        self.assertEqual((stats["n"], stats["success"]), (2, 2))
        self.assertEqual(stats["metrics"]["received_s"], {"n": 2, "median": 200, "min": 100, "max": 300})
        self.assertIn("100–300; n=2", report.cell_text({"id": "work-queue"}, stats["row"]))

    def test_different_images_and_invocations_are_not_merged(self):
        self.write([self.row("run1", 100), self.row("run2", 300),
                    self.row("run3", 900, broker_image="image-b", cohort_id="candidate")])
        stats = self.selected()
        self.assertEqual(stats["run_ids"], ["run3"])
        self.assertEqual(stats["metrics"]["received_s"]["median"], 900)

    def test_failed_repeat_is_unscored_and_its_memory_is_retained(self):
        self.write([self.row("run1", 100, anon_mib="10"),
                    self.row("run2", "", ok="0", err="oom", anon_mib="999")])
        stats = self.selected()
        self.assertEqual((stats["success"], stats["failed"]), (1, 1))
        self.assertIsNone(report.primary({"id": "work-queue"}, stats["row"]))
        self.assertEqual(stats["metrics"]["anon_mib"]["max"], 999)
        self.assertIn("1 failed", report.cell_text({"id": "work-queue"}, stats["row"]))

    def test_interrupted_run_is_counted_without_a_result_row(self):
        row = self.row("run1", 100)
        self.write([row])
        directory = self.root / "runs" / "run2"
        directory.mkdir(parents=True)
        meta = {**self.row("run2", 0), "scenario": {"id": "work-queue"}, "resources": {"broker_cpus": 1}}
        (directory / "provenance.json").write_text(json.dumps(meta))
        stats = self.selected()
        self.assertEqual((stats["n"], stats["incomplete"]), (2, 1))
        self.assertIsNone(report.primary({"id": "work-queue"}, stats["row"]))

    def test_latency_miss_cannot_win_a_site_score(self):
        sc = {"id": "latency-1k"}
        row = {"ok": "1", "kept": "no", "lat_p99_ms": "0.1"}
        self.assertIsNone(report.primary(sc, row))
        self.assertIn("0.10 ms (missed rate)", report.cell_text(sc, row))

    def test_interrupted_repeat_plan_does_not_score_the_first_success(self):
        self.write([self.row("run1", 100, expected_repeats="5", repeat_ordinal="1")])
        stats = self.selected()
        self.assertEqual((stats["expected"], stats["missing"], stats["incomplete"]), (5, 4, 4))
        self.assertIsNone(report.primary({"id": "work-queue"}, stats["row"]))

    def test_truncated_result_is_reported_as_incomplete(self):
        directory = self.root / "runs" / "run1"
        directory.mkdir(parents=True)
        meta = {**self.row("run1", 100), "scenario": {"id": "work-queue"}, "resources": {"broker_cpus": 1}}
        (directory / "provenance.json").write_text(json.dumps(meta))
        (directory / "row.json").write_text('{"ok":')
        stats = self.selected()
        self.assertEqual(stats["incomplete"], 1)
        self.assertIsNone(report.primary({"id": "work-queue"}, stats["row"]))
        self.assertEqual(stats["unreadable_artifacts"], [str(directory / "row.json")])

    def test_immutable_json_cannot_overwrite_an_existing_result(self):
        path = self.root / "row.json"
        scenarios.immutable_json(str(path), {"value": 1})
        with self.assertRaises(FileExistsError):
            scenarios.immutable_json(str(path), {"value": 2})
        self.assertEqual(json.loads(path.read_text()), {"value": 1})

    def test_site_text_is_the_number_alone(self):
        self.write([self.row("run1", 100), self.row("run2", 300)])
        stats = self.selected()
        sc = {"id": "work-queue"}
        self.assertEqual(report.site_text(sc, stats["row"]), "200")
        self.assertIn("100–300; n=2", report.cell_text(sc, stats["row"]))
        self.assertEqual(report.site_text({"id": "latency-1k"}, {"ok": "1", "kept": "yes", "lat_p99_ms": "1.5"}), "1.50")
        self.assertEqual(report.site_text(sc, {}), "–")

    def test_site_text_names_why_a_cell_is_unscored(self):
        self.write([self.row("run1", 100), self.row("run2", "", ok="0", err="oom")])
        self.assertEqual(report.site_text({"id": "work-queue"}, self.selected()["row"]), "failed")

    def test_measured_spans_the_days_of_the_published_cells(self):
        one = {"a": {"started_utc": "2026-10-12T10:00:00Z"}, "b": {"started_utc": "2026-10-12T12:00:00Z"}}
        self.assertEqual(report.measured_dates(one, "/x/2026-10-13"), "2026-10-12")
        two = {**one, "c": {"started_utc": "2026-10-13T01:00:00Z"}}
        self.assertEqual(report.measured_dates(two, "/x/2026-10-13"), "2026-10-12 to 2026-10-13")
        self.assertEqual(report.measured_dates({"a": {}}, "/x/2026-10-09"), "2026-10-09")

    def test_units_are_spaced(self):
        self.assertEqual(report.spaced_units("454.7MiB"), "454.7 MiB")
        self.assertEqual(report.spaced_units("1.93GiB"), "1.93 GiB")
        self.assertEqual(report.spaced_units("298.6 MiB"), "298.6 MiB")

    def test_site_keeps_memory_when_a_cell_is_unscored(self):
        row = self.row("run1", "", ok="0", err="oom", anon_mib="999", peak_mib="1024")
        generated = report.site_ts({}, {}, {}, {("bun", row["size"], "work-queue"): row})
        self.assertIn('"bun": {"anonymous": 999.0, "cgroup": 1024.0}', generated)

    def test_legacy_cells_preserve_last_row_selection(self):
        self.write([{k: v for k, v in self.row("run1", 100).items() if k not in scenarios.COLS[19:]},
                    {k: v for k, v in self.row("run2", 300).items() if k not in scenarios.COLS[19:]}])
        self.assertEqual(self.selected()["metrics"]["received_s"]["median"], 300)


class BacklogReportTests(unittest.TestCase):
    def setUp(self):
        self.scenario = next(s for s in report.S.SCENARIOS if s["id"] == "backlog")
        self.row = {"ok": "1", "sent_s": "60000", "received_s": "810.6",
                    "kept": "sent 300000 got 76200"}

    def test_incomplete_drain_retains_evidence_but_has_no_score(self):
        self.assertIsNone(report.primary(self.scenario, self.row))
        text = report.cell_text(self.scenario, self.row)
        self.assertIn("60,000 / 811", text)
        self.assertIn("incomplete: sent 300000 got 76200", text)
        cells = {("php", report.S.SIZES[0][2], "backlog"): self.row}
        generated = report.site_ts({}, {}, {}, cells)
        self.assertIn('"php": null', generated)
        self.assertIn('"php": "incomplete"', generated)

    def test_completed_drain_keeps_fill_and_drain_score(self):
        self.row["kept"] = "yes"
        self.assertEqual(report.primary(self.scenario, self.row), (60000.0, 810.6))
        self.assertEqual(report.cell_text(self.scenario, self.row), "60,000 / 811")


if __name__ == "__main__":
    unittest.main()

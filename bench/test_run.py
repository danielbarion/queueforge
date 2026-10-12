import os
import sys
import tempfile
import unittest
from unittest.mock import patch

import run

HEAD = "broker\tsize\tscenario\tok\treceived_s\tmb_s\tlat_p99_ms\trun_id\tbroker_image\tstarted_utc\n"


def tsv(directory, *rows):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "scenarios.tsv"), "w") as f:
        f.write(HEAD)
        for row in rows:
            f.write("\t".join(row) + "\n")


class ChangedTests(unittest.TestCase):
    def test_only_brokers_with_a_different_or_unknown_image_are_measured(self):
        current = {"rabbit": "sha256:r", "rust": "sha256:a", "bun": "sha256:new", "php": ""}
        baseline = {"rabbit": "sha256:r", "rust": "sha256:a", "bun": "sha256:old", "php": "sha256:p"}
        self.assertEqual(run.changed_kinds(current, baseline, ["rabbit", "rust", "bun", "php"]), ["bun", "php"])

    def test_a_broker_missing_from_the_baseline_is_measured(self):
        self.assertEqual(run.changed_kinds({"rust": "sha256:a"}, {}, ["rust"]), ["rust"])

    def test_the_newest_row_gives_the_baseline_image(self):
        with tempfile.TemporaryDirectory() as d:
            tsv(d, ["bun", "1 CPU / 1 GiB", "telemetry", "1", "10", "", "1", "a", "sha256:new", "2026-10-12T10:00:00Z"],
                ["bun", "1 CPU / 1 GiB", "telemetry", "1", "10", "", "1", "b", "sha256:old", "2026-10-11T10:00:00Z"])
            self.assertEqual(run.baseline_images(d), {"bun": "sha256:new"})

    def test_the_newest_directory_with_scenarios_is_the_baseline(self):
        with tempfile.TemporaryDirectory() as root:
            tsv(os.path.join(root, "2026-10-11"))
            tsv(os.path.join(root, "2026-10-12"))
            os.makedirs(os.path.join(root, "2026-10-13"))
            self.assertEqual(run.latest_results(root), os.path.join(root, "2026-10-12"))
            self.assertIsNone(run.latest_results(os.path.join(root, "missing")))


class CompareTests(unittest.TestCase):
    def test_new_rows_are_compared_with_baseline_medians(self):
        base = [
            {"broker": "bun", "size": "1 CPU / 1 GiB", "scenario": "telemetry", "ok": "1", "received_s": "100", "mb_s": "", "lat_p99_ms": "2", "run_id": "a"},
            {"broker": "bun", "size": "1 CPU / 1 GiB", "scenario": "telemetry", "ok": "1", "received_s": "120", "mb_s": "", "lat_p99_ms": "4", "run_id": "b"},
            {"broker": "rust", "size": "1 CPU / 1 GiB", "scenario": "telemetry", "ok": "1", "received_s": "50", "mb_s": "", "lat_p99_ms": "1", "run_id": "c"},
        ]
        new = base + [
            {"broker": "bun", "size": "1 CPU / 1 GiB", "scenario": "telemetry", "ok": "1", "received_s": "132", "mb_s": "", "lat_p99_ms": "3", "run_id": "d"},
            {"broker": "bun", "size": "1 CPU / 1 GiB", "scenario": "large-1m", "ok": "1", "received_s": "600", "mb_s": "630.5", "lat_p99_ms": "", "run_id": "e"},
            {"broker": "bun", "size": "1 CPU / 1 GiB", "scenario": "heavy", "ok": "0", "received_s": "9", "mb_s": "", "lat_p99_ms": "", "run_id": "f"},
        ]
        text = run.compare_md(base, new, ["bun"], "bench/results/x")
        self.assertIn("| bun | 1 CPU / 1 GiB | telemetry | 110 | 132 | +20% ! | 3.00 | 3.00 |", text)
        self.assertIn("| bun | 1 CPU / 1 GiB | large-1m |  | 630.5 |  |  |  |", text)
        self.assertNotIn("heavy", text)
        self.assertNotIn("| rust |", text)
        self.assertIn("2 cells, 1 past 10%.", text)

    def test_nothing_measured_says_so(self):
        self.assertIn("nothing re-measured", run.compare_md([], [], [], "x"))


class OptionTests(unittest.TestCase):
    def parse(self, *argv):
        with patch.object(sys, "argv", ["run.py", *argv]), patch.object(run, "latest_results", return_value="/tmp/base"), \
                patch("os.path.isdir", return_value=True):
            return run.parse()

    def test_quick_is_changed_one_repeat_scenarios_only(self):
        opts = self.parse("--quick")
        self.assertTrue(opts.changed)
        self.assertEqual((opts.repeat, opts.steps, opts.baseline), (1, ["build", "scenarios", "report"], "/tmp/base"))
        self.assertIn("results-quick", opts.out)

    def test_quick_keeps_explicit_choices(self):
        opts = self.parse("--quick", "--repeat", "2", "--steps", "build,scenarios,connections,report", "--out", "/tmp/o")
        self.assertEqual((opts.repeat, opts.steps, opts.out), (2, ["build", "scenarios", "connections", "report"], "/tmp/o"))

    def test_defaults_are_unchanged(self):
        opts = self.parse()
        self.assertEqual((opts.changed, opts.repeat, opts.steps), (False, 2, run.STEPS))

    def test_changed_needs_the_build_step(self):
        with self.assertRaises(SystemExit):
            self.parse("--changed", "--steps", "scenarios")

    def test_changed_never_writes_into_its_baseline(self):
        with patch("os.path.exists", return_value=True):
            opts = self.parse("--changed")
        self.assertRegex(os.path.basename(opts.out), r"^\d{4}-\d{2}-\d{2}-\d{4}$")
        with self.assertRaises(SystemExit):
            self.parse("--baseline", "/tmp/base", "--out", "/tmp/base")


if __name__ == "__main__":
    unittest.main()

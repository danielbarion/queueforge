"""Reporting regressions; run with python3 -B -m unittest discover -s bench."""
import unittest

import report


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
        self.assertIn("incomplete: sent 300000 got 76200", generated)

    def test_completed_drain_keeps_fill_and_drain_score(self):
        self.row["kept"] = "yes"
        self.assertEqual(report.primary(self.scenario, self.row), (60000.0, 810.6))
        self.assertEqual(report.cell_text(self.scenario, self.row), "60,000 / 811")


if __name__ == "__main__":
    unittest.main()

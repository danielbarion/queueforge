import json
import os
import tempfile
import unittest

import scaling as S


class UslFit(unittest.TestCase):
    def test_linear_scaling_has_no_cost(self):
        self.assertEqual(S.usl_fit({1: 100, 2: 200, 4: 400}), (0.0, 0.0))

    def test_recovers_contention_and_coherency(self):
        s, k = 0.05, 0.002
        points = {n: S.usl(1000, s, k, n) for n in (1, 2, 3, 4, 6)}
        fs, fk = S.usl_fit(points)
        self.assertAlmostEqual(fs, s, places=6)
        self.assertAlmostEqual(fk, k, places=6)

    def test_needs_one_cpu_and_two_more(self):
        self.assertIsNone(S.usl_fit({1: 100, 2: 180}))
        self.assertIsNone(S.usl_fit({2: 180, 3: 250, 4: 300}))


class Report(unittest.TestCase):
    def rows(self, cells):
        out = tempfile.mkdtemp()
        with open(os.path.join(out, "scaling.jsonl"), "w") as f:
            for c in cells:
                f.write(json.dumps({"kind": "bun", "scenario": "weak-t", "ok": "1", "err": "", "lat_p99_ms": "5",
                                    "client_procs": 2 * c["cpus"], **c}) + "\n")
        return S.report(out)

    def test_flags_client_bound_idle_and_unstable(self):
        text = self.rows([
            {"cpus": 1, "repeat": 1, "received_s": "100", "broker_cpu": "99", "client_cpu": "20"},
            {"cpus": 2, "repeat": 1, "received_s": "190", "broker_cpu": "120", "client_cpu": "30"},
            {"cpus": 3, "repeat": 1, "received_s": "270", "broker_cpu": "297", "client_cpu": "590"},
            {"cpus": 4, "repeat": 1, "received_s": "300", "broker_cpu": "396", "client_cpu": "40"},
            {"cpus": 4, "repeat": 2, "received_s": "400", "broker_cpu": "396", "client_cpu": "40"},
        ])
        lines = {line.split("|")[1].strip(): line for line in text.splitlines() if line.startswith("| ") and line.split("|")[1].strip().isdigit()}
        self.assertIn("broker-idle", lines["2"])
        self.assertIn("client-bound", lines["3"])
        self.assertIn("unstable", lines["4"])
        self.assertNotIn("broker-idle", lines["1"])
        self.assertIn("not enough clean sizes", text)


if __name__ == "__main__":
    unittest.main()

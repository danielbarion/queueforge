import unittest

import ab


class AbTests(unittest.TestCase):
    def test_images_are_named_and_the_first_is_the_reference(self):
        opts = ab.parse(["bun", "before=img:a", "after=img:b", "--scenarios", "large-1m,telemetry"])
        self.assertEqual(opts.images, [("before", "img:a"), ("after", "img:b")])
        self.assertEqual(opts.scenarios, ["large-1m", "telemetry"])
        self.assertIn("results-quick", opts.out)

    def test_bad_image_arguments_are_refused(self):
        for argv in (["bun", "only=img:a", "--scenarios", "x"], ["bun", "a=x", "a=y", "--scenarios", "x"], ["bun", "noequals", "b=y", "--scenarios", "x"]):
            with self.assertRaises(SystemExit):
                ab.parse(argv)

    def test_summary_takes_medians_spread_and_change(self):
        rows = {
            ("before", "large-1m"): [{"ok": "1", "mb_s": "440"}, {"ok": "1", "mb_s": "420"}, {"ok": "1", "mb_s": "430"}],
            ("after", "large-1m"): [{"ok": "1", "mb_s": "473"}, {"ok": "0", "mb_s": "1"}, {"ok": "1", "mb_s": "473"}],
            ("before", "telemetry"): [{"ok": "1", "received_s": "100"}],
            ("after", "telemetry"): [{"ok": "0", "received_s": "5"}],
        }
        text = ab.summary_md("bun", "1 CPU / 1 GiB", [("before", "a"), ("after", "b")], ["large-1m", "telemetry"], rows)
        self.assertIn("| large-1m | 430.0 (420.0–440.0) | 473.0 (473.0–473.0) +10% |", text)
        self.assertIn("| telemetry | 100.0 (100.0–100.0) | failed |", text)


if __name__ == "__main__":
    unittest.main()

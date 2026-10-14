"""Disk and fsync measurement; run with python3 -B -m unittest discover -s bench."""
import unittest

import disk
import scenarios


class FsyncTotalsTests(unittest.TestCase):
    def test_queueforge_histogram_sums_every_series(self):
        text = "\n".join([
            "# TYPE queueforge_wal_fsync_seconds summary",
            'queueforge_wal_fsync_seconds{quantile="0.5"} 0.001',
            'queueforge_wal_fsync_seconds_count{queue="a"} 10',
            'queueforge_wal_fsync_seconds_count{queue="b"} 5',
            "queueforge_wal_fsync_seconds_sum 0.03",
        ])
        self.assertEqual(scenarios.fsync_totals("rust", text), (15.0, 0.03, 15.0))

    def test_rabbitmq_adds_quorum_wal_batches_without_a_time(self):
        text = "\n".join([
            "rabbitmq_io_sync_ops_total 40",
            "rabbitmq_io_sync_time_seconds_total 0.2",
            'rabbitmq_detailed_raft_batches{module="ra_log_wal",ra_system="quorum_queues"} 100',
            'rabbitmq_detailed_raft_batches{module="ra_log_wal",ra_system="coordination"} 2',
            'rabbitmq_detailed_raft_batches{module="ra_log_segment_writer",ra_system="quorum_queues"} 9',
        ])
        self.assertEqual(scenarios.fsync_totals("rabbit", text), (142.0, 0.2, 40.0))

    def test_a_missing_counter_is_no_measurement(self):
        self.assertIsNone(scenarios.fsync_totals("bun", "queueforge_wal_fsync_seconds_count 3\n"))
        self.assertIsNone(scenarios.fsync_totals("rabbit", "rabbitmq_io_sync_ops_total 3\n"))


class IoRatesTests(unittest.TestCase):
    def test_rates_over_the_window(self):
        rates = scenarios.io_rates((10.0, 1_000_000, (100.0, 0.5, 100.0)), (12.0, 201_000_000, (300.0, 0.9, 300.0)))
        self.assertAlmostEqual(rates["disk_mb_s"], 100.0)
        self.assertAlmostEqual(rates["fsyncs_s"], 100.0)
        self.assertAlmostEqual(rates["fsync_ms"], 2.0)

    def test_untimed_syncs_have_no_mean(self):
        rates = scenarios.io_rates((0.0, 0, (0.0, 0.0, 0.0)), (1.0, 0, (50.0, 0.0, 0.0)))
        self.assertEqual((rates["fsyncs_s"], rates["fsync_ms"]), (50.0, None))

    def test_no_syncs_has_no_mean_and_no_counters_leave_blanks(self):
        rates = scenarios.io_rates((0.0, 0, (5.0, 1.0, 5.0)), (1.0, 0, (5.0, 1.0, 5.0)))
        self.assertEqual((rates["fsyncs_s"], rates["fsync_ms"]), (0.0, None))
        rates = scenarios.io_rates((0.0, None, None), (1.0, None, None))
        self.assertEqual(rates, {"disk_mb_s": None, "fsyncs_s": None, "fsync_ms": None})


class DiskProbeTests(unittest.TestCase):
    def test_parse_reads_dd_lines_per_case(self):
        lines = [f"{i} 1000000 bytes (1.0 MB, 977 KiB) copied, 0.5 s, 2.0 MB/s" for i in range(len(disk.CASES))]
        samples = disk.parse("\n".join(lines), 1)
        self.assertEqual(samples[0], [2.0])
        rows = disk.rows(samples, 1)
        self.assertEqual((rows[0]["case"], rows[0]["mb_s"], rows[-1]["synced_each"]), ("4 KiB", "2.0", "0"))

    def test_a_missing_case_fails_the_probe(self):
        with self.assertRaisesRegex(RuntimeError, "missed"):
            disk.parse("0 1000 bytes (1 kB) copied, 0.1 s, 10 kB/s", 1)


if __name__ == "__main__":
    unittest.main()

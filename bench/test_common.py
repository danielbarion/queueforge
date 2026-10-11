import io
import json
import unittest
from unittest.mock import patch
import common


class EngineStatsTests(unittest.TestCase):
    def usage(self, data, status=200):
        response = io.BytesIO(json.dumps(data).encode())
        response.status = status
        class Connection:
            def request(self, method, path):
                assert method == "GET" and "stream=false&one-shot=true" in path
            def getresponse(self): return response
            def close(self): pass
        with patch.object(common, "_stats_endpoint", "unix:///tmp/docker.sock"), patch.object(common, "_UnixHTTP", return_value=Connection()):
            return common.container_usage("broker")

    def test_total_includes_cache_and_anonymous_remains_separate(self):
        self.assertEqual(self.usage({"memory_stats": {"usage": 1000, "stats": {"anon": 200, "inactive_file": 500}},
                                     "cpu_stats": {"cpu_usage": {"total_usage": 9000}}}),
                         {"memory": 1000, "anonymous": 200, "cpu_usec": 9})

    def test_cgroup_v1_rss_is_supported(self):
        self.assertEqual(self.usage({"memory_stats": {"usage": 1000, "stats": {"total_rss": 300}}})["anonymous"], 300)

    def test_removed_container_has_missing_counters(self):
        self.assertEqual(self.usage({}, 404), {"memory": None, "anonymous": None, "cpu_usec": None})

    def test_failed_stats_request_does_not_become_zero_memory(self):
        with self.assertRaisesRegex(RuntimeError, "HTTP 500"):
            self.usage({}, 500)


if __name__ == "__main__": unittest.main()

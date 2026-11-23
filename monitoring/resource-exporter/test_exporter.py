import copy
import unittest
from exporter import normalize, cpuset_size, exposition


class ResourceTests(unittest.TestCase):
    def test_quota_cpuset_and_fractional_capacity(self):
        stats = {"cpu_stats": {"cpu_usage": {"total_usage": 2_000_000_000}}}
        previous = {"cpu_stats": {"cpu_usage": {"total_usage": 1_000_000_000}}}
        config = {"HostConfig": {"NanoCpus": 500_000_000, "CpusetCpus": "0-3"}}
        result = normalize(stats, config, 16, 10000, previous, 4)
        self.assertEqual(.5, result["cpu_capacity_cores"])
        self.assertEqual(.25, result["cpu_used_cores"])
        self.assertEqual(.5, result["cpu_utilization_ratio"])
        self.assertEqual(1, result["cpu_has_explicit_limit"])

    def test_unlimited_is_labeled_host_shared_and_memory_excludes_inactive_file(self):
        result = normalize({"memory_stats": {"usage": 600, "limit": 1000, "stats": {"inactive_file": 100}}}, {}, 8, 2000)
        self.assertEqual(0, result["cpu_has_explicit_limit"])
        self.assertEqual(0, result["memory_has_explicit_limit"])
        self.assertEqual(.5, result["memory_utilization_ratio"])
        self.assertNotIn("cpu_utilization_ratio", result)

    def test_missing_data_and_counter_resets_do_not_become_zero_utilization(self):
        result = normalize({}, {}, None, None)
        self.assertNotIn("cpu_seconds_total", result)
        self.assertNotIn("memory_utilization_ratio", result)
        result = normalize({"cpu_stats": {"cpu_usage": {"total_usage": 1}}}, {}, 4, 100,
                           {"cpu_stats": {"cpu_usage": {"total_usage": 2}}}, 1)
        self.assertNotIn("cpu_utilization_ratio", result)

    def test_throttling_and_disk_network_units(self):
        stats = {"cpu_stats": {"throttling_data": {"periods": 20, "throttled_periods": 6, "throttled_time": 1e9}},
                 "networks": {"eth0": {"rx_bytes": 100, "tx_bytes": 200}},
                 "blkio_stats": {"io_service_bytes_recursive": [{"op": "Read", "value": 123}, {"op": "Write", "value": 456}]}}
        previous = {"cpu_stats": {"throttling_data": {"periods": 10, "throttled_periods": 1}}}
        result = normalize(stats, {}, 2, 1000, previous, 1)
        self.assertEqual(.5, result["cpu_throttled_periods_ratio"])
        self.assertEqual(1, result["cpu_throttled_seconds_total"])
        self.assertEqual(123, result["block_read_bytes_total"])
        self.assertEqual(200, result["network_tx_bytes_total"])

    def test_cpuset_and_exposition_escaping(self):
        self.assertEqual(5, cpuset_size("0-3,2,7"))
        text = exposition([({"service": 'a"b'}, {"cpu_seconds_total": 1, "sample_success": 1})], 0)
        self.assertIn('service="a\\"b"', text)
        self.assertIn("# TYPE pipeline_container_cpu_seconds_total counter", text)


if __name__ == "__main__":
    unittest.main()

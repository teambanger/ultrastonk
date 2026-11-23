import unittest

from benchmarks import growth


def profile(count=300):
    return {"symbol_ranks": [{"symbol": f"S{rank}.FR", "rows": 1000 - rank, "share": (1000 - rank) / 1000}
                             for rank in range(1, count + 1)]}


class CoverageTest(unittest.TestCase):
    def test_coverage_is_cumulative_share_of_the_selected_prefix(self):
        shares = [0.5, 0.25, 0.125, 0.0625, 0.0625]
        result = growth.coverage(shares)
        self.assertAlmostEqual(result["top1"], 0.5)
        self.assertAlmostEqual(result["top10"], 1.0)
        self.assertEqual(result["symbols"], 5)

    def test_coverage_rejects_an_empty_population(self):
        with self.assertRaises(ValueError):
            growth.coverage([])


class RankWeightsTest(unittest.TestCase):
    def test_prefix_is_normalized_and_ordered(self):
        result = growth.rank_weights(profile(), 100)
        self.assertEqual(len(result["weights"]), 100)
        self.assertAlmostEqual(sum(result["weights"]), 1.0)
        self.assertEqual(result["symbols"][0], "S1.FR")
        self.assertGreater(result["weights"][0], result["weights"][99])

    def test_extending_the_prefix_keeps_hot_symbols_and_grows_the_tail(self):
        small = growth.rank_weights(profile(), 100)
        large = growth.rank_weights(profile(), 200)
        self.assertEqual(large["symbols"][:100], small["symbols"])
        self.assertLess(large["coverage"]["top1"], small["coverage"]["top1"])
        self.assertLess(large["coverage"]["top100"], small["coverage"]["top100"])

    def test_requesting_more_symbols_than_the_profile_has_fails_closed(self):
        with self.assertRaises(ValueError):
            growth.rank_weights(profile(50), 51)


class ImbalanceTest(unittest.TestCase):
    def test_uneven_partitions_report_spread(self):
        result = growth.imbalance({"0": 500, "1": 250, "2": 250})
        self.assertEqual(result["partitions"], 3)
        self.assertEqual(result["records"], 1000)
        self.assertAlmostEqual(result["max_share"], 0.5)
        self.assertAlmostEqual(result["max_over_mean"], 1.5)
        self.assertGreater(result["coefficient_of_variation"], 0)

    def test_even_partitions_have_zero_variation(self):
        result = growth.imbalance({"0": 300, "1": 300, "2": 300})
        self.assertAlmostEqual(result["coefficient_of_variation"], 0.0)
        self.assertAlmostEqual(result["max_over_mean"], 1.0)

    def test_missing_partition_counts_are_absent(self):
        self.assertIsNone(growth.imbalance({}))
        self.assertIsNone(growth.imbalance({"0": 0, "1": 0}))


class MetricsTest(unittest.TestCase):
    def driver_result(self, clients=None):
        measurement = {
            "achieved_originals_per_second": 3999.5, "committed_prices_per_second": 3930.0,
            "observed_prices": 78600, "expected_prices": 78600, "missing_prices": 0,
            "unexpected_prices": 0, "drain_seconds": 1.2,
            "visible_roundtrip_ms": {"count": 100, "p50": 3.0, "p95": 5.0, "p99": 7.0, "max": 9.0},
            "input_partition_records": {"0": 400, "1": 600},
        }
        result = {"run_id": "s-000", "arguments": {"symbols": 200, "skew": 0.0,
                                                   "clients": len(clients or [])},
                  "measurement": measurement, "price_correctness_passed": True, "telemetry": []}
        if clients is not None:
            measurement.update({
                "websocket_clients": len(clients), "clients": clients,
                "fanout_frames_per_second": 12000.0 * len(clients),
                "websocket_delivered_prices_per_second": 3930.0,
                "websocket_frames": 235800 * len(clients),
            })
            result["gateway_correctness_passed"] = all(not client["missing_prices"] for client in clients)
            result["telemetry"] = [{"gateway": {"totals": {"in": 78600,
                                                           "forwarded": 235800 * len(clients),
                                                           "dropped": 0, "invalid": 0},
                                                "clients": len(clients), "memoryMb": 120}}]
        return result

    def client(self, missing=0):
        return {"index": 0, "prices": 78600, "missing_prices": missing, "unexpected_prices": 0,
                "missing_emas": 0, "unexpected_emas": 0, "missing_advisories": 0,
                "latency_ms": {"p50": 2.0, "p99": 8.0}, "errors": []}

    def test_run_metrics_carries_partition_and_latency_evidence(self):
        metrics = growth.run_metrics(self.driver_result())
        self.assertEqual(metrics["symbols"], 200)
        self.assertAlmostEqual(metrics["latency_ms"]["p99"], 7.0)
        self.assertAlmostEqual(metrics["input_partition_imbalance"]["max_over_mean"], 1.2)
        self.assertNotIn("gateway", metrics)

    def test_run_metrics_verifies_every_client(self):
        metrics = growth.run_metrics(self.driver_result([self.client(), self.client(5)]))
        gateway = metrics["gateway"]
        self.assertEqual(gateway["clients"], 2)
        self.assertFalse(gateway["delivered_exact_all_clients"])
        self.assertEqual(gateway["client_missing_prices"], [0, 5])
        self.assertEqual(gateway["health"]["clients"], 2)
        self.assertAlmostEqual(gateway["fanout_frames_per_second"], 24000.0)

    def test_compare_uses_dotted_paths_and_ratios(self):
        lower = {"latency_ms": {"p99": 10.0}, "gateway": {"fanout_frames_per_second": 100.0}}
        higher = {"latency_ms": {"p99": 20.0}, "gateway": {"fanout_frames_per_second": 200.0}}
        result = growth.compare(lower, higher, ("latency_ms.p99", "gateway.fanout_frames_per_second",
                                                "gateway.missing"))
        self.assertEqual(result["latency_ms.p99"]["ratio"], 2.0)
        self.assertEqual(result["gateway.fanout_frames_per_second"]["ratio"], 2.0)
        self.assertNotIn("gateway.missing", result)


class PrometheusServicesTest(unittest.TestCase):
    def document(self):
        return {"data": {"result": [
            {"metric": {"__name__": "pipeline_container_cpu_utilization_ratio", "service": "gateway"},
             "values": [[1, "0.2"], [2, "0.4"]]},
            {"metric": {"__name__": "pipeline_container_cpu_utilization_ratio", "service": "gateway"},
             "values": [[1, "0.3"]]},
            {"metric": {"__name__": "pipeline_container_cpu_throttled_periods_ratio", "service": "benchmark"},
             "values": [[1, "0.0"]]},
            {"metric": {"__name__": "unrelated", "service": "gateway"}, "values": [[1, "9.0"]]},
        ]}}

    def test_service_maxima_ignore_unrelated_series(self):
        services = growth.prometheus_services(self.document())
        self.assertAlmostEqual(services["gateway"]["pipeline_container_cpu_utilization_ratio"], 0.4)
        self.assertAlmostEqual(services["benchmark"]["pipeline_container_cpu_throttled_periods_ratio"], 0.0)
        self.assertNotIn("unrelated", str(services))

    def test_empty_document_is_empty(self):
        self.assertEqual(growth.prometheus_services(None), {})


class ContainerStatsTest(unittest.TestCase):
    def test_parses_docker_stats_rows(self):
        rows = [{"Name": "p-1-gateway-1", "CPUPerc": "87.50%", "MemUsage": "91.2MiB / 512MiB"}]
        result = growth.container_stats(rows, "-gateway-")
        self.assertEqual(result["cpu_percent"], 87.5)
        self.assertAlmostEqual(result["memory_bytes"], 91.2 * 2 ** 20)

    def test_missing_container_is_none(self):
        self.assertIsNone(growth.container_stats([], "-gateway-"))


if __name__ == "__main__":
    unittest.main()
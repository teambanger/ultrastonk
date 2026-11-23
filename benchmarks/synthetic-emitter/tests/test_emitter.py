import copy
import json
import math
import unittest
from dummy_emitter import DEFAULTS, Generator, Telemetry, ComponentProfiler, merge, rate_at, validate


class EmitterTests(unittest.TestCase):
    def config(self, overrides=None):
        return validate(merge(DEFAULTS, overrides or {}))

    def test_fixed_timeline_and_seed_reproduce_values(self):
        config = self.config({"event_time": {"mode": "accelerated", "start_epoch_ms": 1000, "speed": 10}})
        a, b = Generator(config, 99), Generator(config, 88)
        for i in range(100):
            event = a.event(i / 10, 9000)
            self.assertEqual(event, b.event(i / 10, 3000))
            self.assertEqual(1000 + i * 1000, event["timestamp_emit"])
            self.assertEqual({"timestamp_emit", "ticker", "sec_type", "price"}, set(event))
            self.assertTrue(math.isfinite(event["price"]))

    def test_duplicate_payload_and_same_timestamp_correction(self):
        config = self.config({"faults": {"duplicate_probability": 1, "duplicate_copies": 2,
            "duplicate_delay_ms": 20, "correction_probability": 1, "delay_probability": 0}})
        gen = Generator(config, 1000)
        event = gen.event(0, 1000)
        values = list(gen.records(event))
        self.assertEqual(4, len(values))
        self.assertEqual([0, 20, 40, 0], [r[0] for r in values])
        self.assertEqual(values[0][2], values[1][2])
        self.assertEqual(values[0][2], values[2][2])
        corrected = json.loads(values[3][2])
        self.assertEqual(event["timestamp_emit"], corrected["timestamp_emit"])
        self.assertNotEqual(event["price"], corrected["price"])

    def test_delay_and_backdating_are_independent(self):
        config = self.config({"faults": {"delay_probability": 1, "delay_min_ms": 2000, "delay_max_ms": 2000,
            "backdate_probability": 1, "backdate_ms": 3000, "duplicate_probability": 0}})
        gen = Generator(config, 0)
        event = gen.event(0, 10000)
        self.assertEqual(7000, event["timestamp_emit"])
        self.assertEqual(2000, next(gen.records(event))[0])

    def test_fault_modes_include_kafka_tombstone(self):
        for mode in ["malformed_json", "missing_price", "bad_price", "bad_ticker", "negative_timestamp", "tombstone"]:
            config = self.config({"faults": {"invalid_probability": 1, "invalid_modes": [mode], "duplicate_probability": 0}})
            gen = Generator(config, 0)
            record = next(gen.records(gen.event(0, 1000)))
            self.assertEqual(mode, record[3])
            if mode == "tombstone":
                self.assertIsNone(record[2])

    def test_rate_stages_bursts_and_pauses(self):
        config = self.config({"rate": {"schedule": [{"duration_seconds": 10, "events_per_second": 100},
            {"duration_seconds": 10, "events_per_second": 200}], "burst_every_seconds": 5,
            "burst_duration_seconds": 1, "burst_multiplier": 2, "pause_every_seconds": 10, "pause_duration_seconds": .5}})
        self.assertEqual(0, rate_at(config, 0))
        self.assertEqual(200, rate_at(config, .75))
        self.assertEqual(100, rate_at(config, 2))
        self.assertEqual(400, rate_at(config, 10.75))
        self.assertEqual(200, rate_at(config, 12))

    def test_bad_configs_fail_early(self):
        for overrides in [{"oops": 1}, {"faults": {"duplicate_probability": 2}}, {"symbols": {"tickers": ["BAD"]}},
                          {"kafka": {"idempotence": True, "acks": "0"}}, {"runtime": {"max_pending_events": 1}},
                          {"price": {"sine_period_seconds": 0}}, {"rate": {"max_events": 1.2}}]:
            with self.assertRaises(ValueError, msg=str(overrides)):
                self.config(overrides)

    def test_histogram_and_delivery_errors(self):
        metrics = Telemetry()
        metrics.delivery(None, 5, 100)
        metrics.delivery(None, 2000, 200)
        metrics.delivery("failed", 10, 0)
        self.assertEqual(2, metrics.snapshot()["acknowledged"])
        self.assertEqual(1, metrics.snapshot()["delivery_errors"])
        exposition = metrics.prometheus()
        self.assertIn('dummy_emitter_delivery_ms_bucket{le="5"} 1', exposition)
        self.assertIn('dummy_emitter_delivery_ms_bucket{le="+Inf"} 2', exposition)

    def test_component_profile_sampling_and_disable(self):
        metrics = Telemetry()
        profiler = ComponentProfiler(metrics, "generation", every=3)
        for _ in range(7):
            started = profiler.begin()
            sum(i * i for i in range(100))
            profiler.end(started)
        values = metrics.snapshot()["component_profiles"]["generation"]
        self.assertEqual(7, values["calls"])
        self.assertEqual(3, values["samples"])
        self.assertGreater(values["wall_ns"], 0)
        self.assertGreater(values["cpu_ns"], 0)
        self.assertIn('dummy_emitter_profile_samples_total{component="generation"} 3.0', metrics.prometheus())
        self.assertIsNone(ComponentProfiler(metrics, "disabled", enabled=False).begin())


if __name__ == "__main__":
    unittest.main()

import copy
import unittest

from benchmarks import capture_pilot

BASE = (1_600_000_000_000 // 300_000) * 300_000
WINDOW = 300_000
DISORDER = 5_000
# Wall clocks in the fixture are relative offsets from a real epoch so event age is realistic.
EPOCH = 1_700_000_000_000


def shift(records):
    for record in records:
        for field in ("wall_ms", "emit_ms"):
            if record.get(field) is not None:
                record[field] += EPOCH
    return records


def price(ticker, event_ms, value, wall_ms):
    return {"ticker": ticker, "type": "price", "event_ms": event_ms, "value": value, "wall_ms": wall_ms}


def ema(ticker, event_ms, kind, value, wall_ms):
    return {"ticker": ticker, "type": kind, "event_ms": event_ms, "value": value, "wall_ms": wall_ms}


def passing_run():
    """A hand-built artifact set for one measured window on one subscribed ticker."""
    input_records = [
        {"ticker": "A.FR", "event_ms": BASE + 1_000, "value": 10.0, "wall_ms": 1_000, "emit_ms": 900},
        {"ticker": "A.FR", "event_ms": BASE + 2_000, "value": 11.0, "wall_ms": 1_100, "emit_ms": 1_000},
        {"ticker": "A.FR", "event_ms": BASE + 305_000, "value": 12.0, "wall_ms": 1_200, "emit_ms": 1_050},
    ]
    closing = BASE + 2_000
    committed = [
        price("A.FR", BASE + 1_000, 10.0, 1_150),
        price("A.FR", BASE + 2_000, 11.0, 1_200),
        price("A.FR", BASE + 305_000, 12.0, 1_250),
        ema("A.FR", closing, "EMA38", 10.5, 1_500),
        ema("A.FR", closing, "EMA100", 10.2, 1_500),
    ]
    websocket = copy.deepcopy(committed)
    shift(input_records)
    shift(committed)
    shift(websocket)
    watermark_samples = shift([{"wall_ms": 1_100, "watermark": BASE + 2_000},
                               {"wall_ms": 1_260, "watermark": BASE + 305_000}])
    return {
        "settings": {"window_ms": WINDOW, "max_out_of_order_ms": DISORDER, "checkpoint_interval_ms": 1_000,
                     "flink_parallelism": 2, "dedup": "NONE", "late": "DROP", "speed_multiplier": 20.0},
        "clock": {"basis": "same_host_wall", "max_clock_error_ms": 250},
        "limits": {"flink_parallelism": 2, "kafka_partitions": 6, "checkpoint_interval_ms": 1_000,
                   "taskmanager_cpus": 1.0, "taskmanager_memory_bytes": 1610612736},
        "job_running": True,
        "pacing": {
            "pacing_report_version": 1,
            "mode": "capture",
            "speed_multiplier": 20.0,
            "missing_capture_time_policy": "event",
            "preroll_ms": 0,
            "measure_ms": 600_000,
            "missing_capture_rows": 0,
            "submitted": 3,
            "failed": 0,
            "segments": {
                "PRE_ROLL": {"rows": 1, "scheduled_ms": 0, "actual_ms": 0, "achieved_ratio": None,
                             "max_lag_ms": 0, "missing_capture_rows": 0},
                "MEASURED": {"rows": 2, "scheduled_ms": 10_000, "actual_ms": 10_050, "achieved_ratio": 0.995,
                             "max_lag_ms": 60, "missing_capture_rows": 0},
                "TRAILING": {"rows": 1, "scheduled_ms": 0, "actual_ms": 0, "achieved_ratio": None,
                             "max_lag_ms": 0, "missing_capture_rows": 0},
            },
            "clock_offset": {"samples": 3, "min_ms": 1_599_000_000_000, "p50_ms": 1_599_000_000_500,
                             "max_ms": 1_599_000_001_000},
            "samples": [
                {"source_ordinal": 0, "ticker": "A.FR", "segment": "MEASURED", "enqueued_epoch_ms": 1_700_000_000_000,
                 "ack_epoch_ms": 1_700_000_000_004},
                {"source_ordinal": 1, "ticker": "A.FR", "segment": "MEASURED", "enqueued_epoch_ms": 1_700_000_000_500,
                 "ack_epoch_ms": 1_700_000_000_506},
            ],
        },
        "watermark_samples": watermark_samples,
        "input": input_records,
        "committed": committed,
        "websocket": websocket,
        "ws_errors": [],
        "progress": [{"wall_ms": 1_000, "input": 1, "output": 1, "websocket": 1},
                     {"wall_ms": 1_300, "input": 3, "output": 5, "websocket": 5}],
    }


class CapturePilotEvaluationTests(unittest.TestCase):
    def test_passing_pilot_separates_event_age_watermark_wait_and_transport(self):
        result = capture_pilot.evaluate(passing_run())
        self.assertTrue(result["overall_passed"], result)
        for check in result["checks"].values():
            self.assertTrue(check["passed"], check)
        measurements = result["measurements"]
        self.assertGreater(measurements["historical_event_age_ms"]["p50"], 1_000_000)
        self.assertGreaterEqual(measurements["transport_ms"]["p50"], 0)
        self.assertGreaterEqual(measurements["watermark_wait_ms"]["p50"], 0)
        self.assertGreater(measurements["window_finalization_ms"]["count"], 0)

    def test_missing_pacing_report_is_missing_telemetry_not_a_pass(self):
        run = passing_run()
        run["pacing"] = None
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertFalse(result["checks"]["telemetry"]["passed"])
        self.assertIn("pacing report", result["checks"]["telemetry"]["missing"])

    def test_missing_watermark_samples_are_detected(self):
        run = passing_run()
        run["watermark_samples"] = []
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertIn("watermark samples", result["checks"]["telemetry"]["missing"])

    def test_missing_limits_are_detected_when_required(self):
        run = passing_run()
        run["limits"] = None
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertIn("effective limits", result["checks"]["telemetry"]["missing"])

    def test_insufficient_pacing_fails(self):
        run = passing_run()
        run["pacing"]["segments"]["MEASURED"]["achieved_ratio"] = 0.5
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertFalse(result["checks"]["pacing"]["passed"])
        self.assertIn("insufficient pacing", result["checks"]["pacing"]["reason"])

    def test_non_capture_pacing_report_cannot_prove_capture_pacing(self):
        run = passing_run()
        run["pacing"]["mode"] = "event"
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertFalse(result["checks"]["pacing"]["passed"])

    def test_missing_committed_delivery_fails(self):
        run = passing_run()
        run["committed"] = [record for record in run["committed"] if record["event_ms"] != BASE + 1_000]
        run["websocket"] = copy.deepcopy(run["committed"])
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertFalse(result["checks"]["delivery"]["passed"])
        self.assertEqual(result["checks"]["delivery"]["missing"][0]["count"], 1)

    def test_duplicate_websocket_delivery_fails(self):
        run = passing_run()
        run["websocket"].append(price("A.FR", BASE + 1_000, 10.0, 1_300))
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertFalse(result["checks"]["delivery"]["passed"])
        self.assertEqual(result["checks"]["delivery"]["extra"][0]["count"], 1)

    def test_tied_identities_are_counted_not_collapsed(self):
        run = passing_run()
        tied = {"ticker": "A.FR", "event_ms": BASE + 1_000, "value": 10.0,
                "wall_ms": EPOCH + 1_050, "emit_ms": EPOCH + 1_010}
        run["input"].append(dict(tied))
        run["pacing"]["submitted"] = 4
        run["committed"].append(price("A.FR", BASE + 1_000, 10.0, EPOCH + 1_160))
        run["websocket"] = copy.deepcopy(run["committed"])
        result = capture_pilot.evaluate(run)
        self.assertTrue(result["overall_passed"], result)
        self.assertEqual(result["measurements"]["tied_identities"], 0)
        self.assertEqual(result["measurements"]["duplicate_identities"], 1)

    def test_missing_one_of_two_tied_identities_is_a_loss_not_an_extra(self):
        run = passing_run()
        run["input"].append({"ticker": "A.FR", "event_ms": BASE + 1_000, "value": 10.0,
                             "wall_ms": EPOCH + 1_020, "emit_ms": EPOCH + 1_000})
        run["pacing"]["submitted"] = 4
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertEqual(result["checks"]["delivery"]["missing"][0]["count"], 1)
        self.assertEqual(result["checks"]["delivery"]["extra"], [])

    def test_clock_skew_beyond_documented_error_fails_correlation(self):
        run = passing_run()
        for record in run["committed"]:
            if record["type"] == "price":
                record["wall_ms"] = 100  # earlier than the enqueue attempt at 900+ ms
        run["websocket"] = copy.deepcopy(run["committed"])
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertFalse(result["checks"]["identity_clock"]["passed"])

    def test_subscriber_observing_before_the_pilot_consumer_is_not_a_clock_error(self):
        run = passing_run()
        for record in run["committed"]:
            if record["type"] == "price":
                record["wall_ms"] += 3_000  # the pilot polled committed output after the browser received it
        result = capture_pilot.evaluate(run)
        self.assertTrue(result["checks"]["identity_clock"]["passed"], result["checks"]["identity_clock"])
        self.assertGreater(result["checks"]["identity_clock"]["websocket_before_committed_observation"], 0)

    def test_window_receipt_slightly_before_the_polled_watermark_is_within_tolerance(self):
        run = passing_run()
        for record in run["committed"]:
            if record["type"].startswith("EMA"):
                record["wall_ms"] -= 300
        run["websocket"] = copy.deepcopy(run["committed"])
        result = capture_pilot.evaluate(run)
        self.assertTrue(result["checks"]["windows"]["passed"])
        self.assertEqual(result["checks"]["windows"]["pre_observation_count"], 1)

    def test_window_receipt_far_before_the_polled_watermark_fails(self):
        run = passing_run()
        for record in run["committed"]:
            if record["type"].startswith("EMA"):
                record["wall_ms"] -= 60_000
        run["websocket"] = copy.deepcopy(run["committed"])
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertFalse(result["checks"]["windows"]["passed"])

    def test_quote_rows_without_a_trade_price_do_not_break_identity_matching(self):
        run = passing_run()
        run["input"].append({"ticker": "A.FR", "event_ms": BASE + 1_100, "value": None,
                             "wall_ms": EPOCH + 1_030, "emit_ms": EPOCH + 1_020})
        run["pacing"]["submitted"] = 4
        result = capture_pilot.evaluate(run)
        self.assertTrue(result["overall_passed"], result)

    def test_missing_window_finalization_output_fails(self):
        run = passing_run()
        run["committed"] = [record for record in run["committed"] if record["type"] == "price"]
        run["websocket"] = copy.deepcopy(run["committed"])
        result = capture_pilot.evaluate(run)
        self.assertFalse(result["overall_passed"])
        self.assertFalse(result["checks"]["windows"]["passed"])

    def test_report_keeps_failures_and_actual_settings(self):
        run = passing_run()
        run["pacing"]["segments"]["MEASURED"]["achieved_ratio"] = 0.5
        result = capture_pilot.evaluate(run)
        self.assertEqual(result["settings"]["window_ms"], WINDOW)
        self.assertIn("checks", result)
        self.assertIn("failures", result)


if __name__ == "__main__":
    unittest.main()

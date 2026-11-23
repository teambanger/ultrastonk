import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks import capacity_model as cm

ROOT = Path(__file__).resolve().parents[2]


def week_run_fixture(**overrides):
    run = {
        "path": "/synthetic/week-run", "present": True, "workers": 2, "submitted": 289_049_752,
        "emitter_ev_per_second": 545_563, "emitter_wall_seconds": 530.0,
        "processing_seconds": 428.0, "processing_rate_per_second": 289_049_752 / 428.0,
        "dump_seconds": 521.854, "output_records": 62_380_615, "output_prices": 59_391_670,
        "output_bytes": 5_282_388_854, "broker_bytes": 16_507_840_934,
    }
    run.update(overrides)
    return run


def week_profile_fixture(**overrides):
    profile = {
        "path": "/synthetic/week-profile", "present": True, "rows_read": 289_116_808,
        "rows_emitted": 289_049_752, "peak_rows": 1_223_873, "peak_rate_per_second": 1_223_873 / 300,
        "peak_start": "2021-11-09T15:58:01", "trade_rows": 59_391_670,
        "trade_share": 59_391_670 / 289_049_752, "top_symbol_shares": {"1": 0.0239, "10": 0.1571, "100": 0.5853},
        "days": [],
    }
    profile.update(overrides)
    return profile


def ladder_points():
    return [
        {"workers": 1, "offered": 1000, "verdict": "PASS", "achieved_per_second": 1000.0,
         "latency_ms": {"p50": 5.0, "p95": 10.0, "p99": 20.0}, "drain_seconds": 1.0,
         "worker_cpu_utilization_ratio": 0.5, "worker_throttled_periods_ratio": 0.0,
         "worker_memory_working_set_bytes": 512 * 1024 ** 2},
        {"workers": 2, "offered": 2000, "verdict": "PASS", "achieved_per_second": 1800.0,
         "latency_ms": {"p50": 8.0, "p95": 16.0, "p99": 40.0}, "drain_seconds": 2.0,
         "worker_cpu_utilization_ratio": 0.7, "worker_throttled_periods_ratio": 0.01,
         "worker_memory_working_set_bytes": 768 * 1024 ** 2},
    ]


class PureMathTests(unittest.TestCase):
    def test_rows_per_second(self):
        self.assertEqual(cm.rows_per_second(1000, 10), 100)
        self.assertAlmostEqual(cm.rows_per_second(289_049_752, 428.0), 289_049_752 / 428.0)

    def test_rows_per_second_degenerate(self):
        self.assertIsNone(cm.rows_per_second(1000, 0))
        self.assertIsNone(cm.rows_per_second(1000, -1))
        self.assertIsNone(cm.rows_per_second(None, 10))
        self.assertIsNone(cm.rows_per_second(-5, 10))
        self.assertEqual(cm.rows_per_second(0, 10), 0)

    def test_seconds_for_volume(self):
        self.assertEqual(cm.seconds_for_volume(1000, 100), 10)
        self.assertEqual(cm.seconds_for_volume(0, 100), 0)
        self.assertIsNone(cm.seconds_for_volume(1000, 0))
        self.assertIsNone(cm.seconds_for_volume(None, 100))
        self.assertIsNone(cm.seconds_for_volume(-1, 100))

    def test_per_worker_rate(self):
        self.assertEqual(cm.per_worker_rate(1000, 2), 500)
        self.assertEqual(cm.per_worker_rate(1000, 1), 1000)
        self.assertIsNone(cm.per_worker_rate(1000, 0))
        self.assertIsNone(cm.per_worker_rate(None, 2))

    def test_safe_ratio(self):
        self.assertEqual(cm.safe_ratio(10, 2), 5)
        self.assertIsNone(cm.safe_ratio(10, 0))
        self.assertIsNone(cm.safe_ratio(None, 2))
        self.assertIsNone(cm.safe_ratio(10, None))


class ScalingEfficiencyTests(unittest.TestCase):
    def test_linear_scaling_is_one(self):
        self.assertAlmostEqual(cm.scaling_efficiency(2000, 1000, 2, 1), 1.0)

    def test_sublinear_scaling(self):
        self.assertAlmostEqual(cm.scaling_efficiency(1800, 1000, 2, 1), 0.9)

    def test_from_ladder_points(self):
        points = ladder_points()
        efficiency = cm.scaling_efficiency(points[1]["achieved_per_second"],
                                           points[0]["achieved_per_second"],
                                           points[1]["workers"], points[0]["workers"])
        self.assertAlmostEqual(efficiency, 0.9)

    def test_degenerate(self):
        self.assertIsNone(cm.scaling_efficiency(None, 1000, 2, 1))
        self.assertIsNone(cm.scaling_efficiency(2000, 0, 2, 1))
        self.assertIsNone(cm.scaling_efficiency(2000, 1000, 0, 1))
        self.assertIsNone(cm.scaling_efficiency(2000, 1000, 2, 0))


class MonotoneFitTests(unittest.TestCase):
    def test_identity_on_monotone_points(self):
        self.assertEqual(cm.monotone_fit([1.0, 2.0, 3.0, 4.0]), [1.0, 2.0, 3.0, 4.0])

    def test_non_decreasing_on_violating_points(self):
        fitted = cm.monotone_fit([3.0, 1.0, 2.0])
        self.assertEqual(len(fitted), 3)
        self.assertLessEqual(fitted[0], fitted[1])
        self.assertLessEqual(fitted[1], fitted[2])

    def test_pool_adjacent_violators(self):
        fitted = cm.monotone_fit([1.0, 3.0, 2.0, 4.0])
        self.assertAlmostEqual(fitted[1], 2.5)
        self.assertAlmostEqual(fitted[2], 2.5)

    def test_degenerate(self):
        self.assertEqual(cm.monotone_fit([]), [])
        self.assertEqual(cm.monotone_fit([7.0]), [7.0])


class InterpolationTests(unittest.TestCase):
    def test_interpolate_midpoint(self):
        self.assertAlmostEqual(cm.interpolate_linear(1500, [(1000, 1000.0), (2000, 2000.0)]), 1500.0)

    def test_interpolate_clamps_outside_range(self):
        points = [(1000, 10.0), (2000, 20.0)]
        self.assertEqual(cm.interpolate_linear(500, points), 10.0)
        self.assertEqual(cm.interpolate_linear(5000, points), 20.0)

    def test_interpolate_single_point(self):
        self.assertEqual(cm.interpolate_linear(9999, [(1000, 42.0)]), 42.0)

    def test_interpolate_degenerate(self):
        self.assertIsNone(cm.interpolate_linear(10, []))

    def test_throughput_at(self):
        points = ladder_points()
        self.assertAlmostEqual(cm.throughput_at(1500, points), 1400.0)

    def test_throughput_at_skips_incomplete_points(self):
        points = [{"offered": 1000, "achieved_per_second": 1000.0},
                  {"offered": 2000, "achieved_per_second": None}]
        self.assertEqual(cm.throughput_at(1000, points), 1000.0)

    def test_throughput_at_degenerate(self):
        self.assertIsNone(cm.throughput_at(1000, []))

    def test_latency_at_interpolates_p99(self):
        points = ladder_points()
        self.assertAlmostEqual(cm.latency_at(1500, points, key="p99"), 30.0)

    def test_latency_at_other_key(self):
        points = ladder_points()
        self.assertAlmostEqual(cm.latency_at(1500, points, key="p50"), 6.5)

    def test_latency_at_degenerate(self):
        self.assertIsNone(cm.latency_at(1000, [], key="p99"))
        self.assertIsNone(cm.latency_at(1000, [{"offered": 1000, "latency_ms": {}}]))


class ScalingBoundTests(unittest.TestCase):
    def test_measured_worker_count_collapses(self):
        bounds = cm.scaling_bounds(1_000_000, 100_000, 2, 2)
        self.assertAlmostEqual(bounds["low"], 10.0)
        self.assertAlmostEqual(bounds["high"], 10.0)

    def test_one_worker_bounds_are_two_x(self):
        bounds = cm.scaling_bounds(1_000_000, 100_000, 2, 1)
        self.assertAlmostEqual(bounds["low"], 10.0)
        self.assertAlmostEqual(bounds["high"], 20.0)

    def test_more_workers_halve_the_upper_bound(self):
        bounds = cm.scaling_bounds(1_000_000, 100_000, 2, 4)
        self.assertAlmostEqual(bounds["low"], 5.0)
        self.assertAlmostEqual(bounds["high"], 10.0)

    def test_degenerate(self):
        self.assertIsNone(cm.scaling_bounds(None, 100_000, 2, 1))
        self.assertIsNone(cm.scaling_bounds(1_000_000, None, 2, 1))
        self.assertIsNone(cm.scaling_bounds(1_000_000, 100_000, 0, 1))
        self.assertIsNone(cm.scaling_bounds(1_000_000, 100_000, 2, 0))

    def test_prediction_range_prefers_bounds(self):
        bounds = cm.scaling_bounds(1_000_000, 100_000, 2, 1)
        ranged = cm.prediction_range(20.0, bounds, measured=False)
        self.assertEqual(ranged["basis"], "monotone-bounds")
        self.assertEqual(ranged["low"], 10.0)
        self.assertEqual(ranged["high"], 20.0)

    def test_prediction_range_band_at_measured(self):
        bounds = cm.scaling_bounds(1_000_000, 100_000, 2, 2)
        ranged = cm.prediction_range(10.0, bounds, measured=True)
        self.assertEqual(ranged["basis"], "one-repetition-band")
        self.assertLess(ranged["low"], 10.0)
        self.assertGreater(ranged["high"], 10.0)


class ResourceEnvelopeTests(unittest.TestCase):
    def test_worker_quota_scales_with_workers(self):
        envelope = cm.resource_envelope(1_000_000, 2, per_worker_cpu_cores=1.0,
                                        per_worker_memory_bytes=2 * 1024 ** 3)
        self.assertEqual(envelope["total_worker_cpu_cores"], 2.0)
        self.assertEqual(envelope["total_worker_memory_bytes"], 4 * 1024 ** 3)

    def test_disk_per_million_rows_and_output(self):
        envelope = cm.resource_envelope(2_000_000, 2, per_worker_cpu_cores=1.0,
                                        per_worker_memory_bytes=1, disk_bytes_per_row=10.0,
                                        output_bytes_per_row=4.0, output_records_per_row=0.2)
        self.assertEqual(envelope["disk_bytes_per_million_rows"], 10_000_000.0)
        self.assertEqual(envelope["disk_bytes"], 20_000_000.0)
        self.assertEqual(envelope["output_bytes"], 8_000_000.0)
        self.assertEqual(envelope["output_records"], 400_000.0)

    def test_missing_rates_become_none(self):
        envelope = cm.resource_envelope(1_000_000, 2, per_worker_cpu_cores=1.0,
                                        per_worker_memory_bytes=1)
        self.assertIsNone(envelope["disk_bytes"])
        self.assertIsNone(envelope["output_bytes"])


class PeakHeadroomTests(unittest.TestCase):
    def test_headroom(self):
        self.assertAlmostEqual(cm.peak_headroom(4000.0, 675_000.0), 168.75)

    def test_degenerate(self):
        self.assertIsNone(cm.peak_headroom(0, 675_000.0))
        self.assertIsNone(cm.peak_headroom(4000.0, None))


class ModelAndPredictionTests(unittest.TestCase):
    def test_build_model_from_week_point(self):
        model = cm.build_model(week_profile_fixture(), week_run_fixture(), {"present": False, "points": []})
        self.assertEqual(model["weeks"][0]["workers"], 2)
        self.assertAlmostEqual(model["per_worker_rate_per_second"], 289_049_752 / 428.0 / 2)
        self.assertIsNone(model["scaling_efficiency"])
        self.assertFalse(model["ladder_present"])

    def test_throughput_extrapolation_for_a_volume(self):
        model = cm.build_model(week_profile_fixture(),
                               week_run_fixture(submitted=1_000_000, processing_seconds=100.0,
                                                processing_rate_per_second=10_000.0, workers=1),
                               {"present": False, "points": []})
        prediction = cm.predict(model, 1_000_000, 1)
        self.assertAlmostEqual(prediction["predicted_processing_seconds"], 100.0)

    def test_more_workers_reduce_predicted_time(self):
        model = cm.build_model(week_profile_fixture(), week_run_fixture(), {"present": False, "points": []})
        one = cm.predict(model, 289_049_752, 1)["predicted_processing_seconds"]
        two = cm.predict(model, 289_049_752, 2)["predicted_processing_seconds"]
        self.assertAlmostEqual(two, 428.0, places=1)
        self.assertGreater(one, two)

    def test_double_volume_doubles_time(self):
        model = cm.build_model(week_profile_fixture(), week_run_fixture(), {"present": False, "points": []})
        single = cm.predict(model, 289_049_752, 2)["predicted_processing_seconds"]
        triple = cm.predict(model, 3 * 289_049_752, 2)["predicted_processing_seconds"]
        self.assertAlmostEqual(triple, 3 * single, places=3)

    def test_scaling_efficiency_fitted_from_ladder(self):
        bracket = {"present": True, "points": ladder_points(), "source_sessions": ["sess"],
                   "bracket": {}, "worker_capacity_gain": []}
        model = cm.build_model(week_profile_fixture(), week_run_fixture(), bracket)
        self.assertTrue(model["ladder_present"])
        self.assertAlmostEqual(model["scaling_efficiency"], 0.9)

    def test_degenerate_no_points(self):
        model = cm.build_model({"present": False}, {"present": False}, {"present": False, "points": []})
        self.assertIsNone(model["per_worker_rate_per_second"])
        prediction = cm.predict(model, 1_000_000, 1)
        self.assertIsNone(prediction["predicted_processing_seconds"])
        self.assertIsNone(prediction["predicted_processing_range"]["low"])

    def test_predict_rejects_zero_workers(self):
        model = cm.build_model(week_profile_fixture(), week_run_fixture(), {"present": False, "points": []})
        self.assertIsNone(cm.predict(model, 1_000_000, 0)["predicted_processing_seconds"])


class FormatterTests(unittest.TestCase):
    def setUp(self):
        model = cm.build_model(week_profile_fixture(), week_run_fixture(), {"present": False, "points": []})
        self.predictions = []
        for workers in (1, 2):
            prediction = cm.predict(model, 289_049_752, workers)
            prediction["label"] = f"official week volume at {workers} worker(s)"
            self.predictions.append(prediction)
        self.model = model

    def test_human_report_contains_sections(self):
        text = cm.format_report(self.model, self.predictions)
        self.assertIn("recorded raw peak", text)
        self.assertIn("Whole-week measured point", text)
        self.assertIn("official week volume at 2 worker(s)", text)
        self.assertIn("PENDING", text)

    def test_human_report_with_peak_fit(self):
        peak_fit = {"peak_rate": 4079.6, "capacity_rate": 675_000.0, "headroom": 165.5, "verdict": "fits"}
        text = cm.format_report(self.model, self.predictions, peak_fit=peak_fit, notes=["a caveat"])
        self.assertIn("headroom", text)
        self.assertIn("a caveat", text)

    def test_json_report_is_serialisable(self):
        payload = cm.json_report(self.model, self.predictions, notes=["x"])
        round_tripped = json.loads(json.dumps(payload))
        self.assertEqual(round_tripped["predictions"][0]["workers"], 1)

    def test_human_bytes_and_seconds(self):
        self.assertEqual(cm.human_bytes(None), "PENDING")
        self.assertIn("GiB", cm.human_bytes(2 * 1024 ** 3))
        self.assertIn("s", cm.human_seconds(5))


class LoaderTests(unittest.TestCase):
    def test_load_week_profile_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            profile = cm.load_week_profile(Path(tmp) / "missing.json")
            self.assertFalse(profile["present"])

    def test_load_week_profile_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "week-profile.json"
            path.write_text(json.dumps({
                "files": [{"key": "d1.csv", "rows": 100, "size": 1000}, {"key": "d2.csv", "rows": 50, "size": 500}],
                "raw_peak": {"rows": 3000, "start": "2021-11-09T15:58:01"},
                "row_categories": {"trade": 40}, "replay_screening": {"emitted": 150},
                "top_symbol_shares": {"1": 0.02}}))
            profile = cm.load_week_profile(path)
            self.assertEqual(profile["rows_read"], 150)
            self.assertEqual(profile["rows_emitted"], 150)
            self.assertAlmostEqual(profile["peak_rate_per_second"], 10.0)
            self.assertAlmostEqual(profile["trade_share"], 40 / 150)

    def test_load_week_run_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = cm.load_week_run(Path(tmp) / "missing")
            self.assertFalse(run["present"])

    def test_load_week_run_parses_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "logs").mkdir()
            (root / "evidence").mkdir()
            (root / "logs/emitter.log").write_text(
                "Done: 289,049,752 submitted, 0 failed, 545,563 ev/s\n"
                "Total: read 289,116,808 -> emitted 289,049,752\n")
            (root / "logs/emitter_inspect.json").write_text(json.dumps(
                {"State": {"StartedAt": "2026-11-23T16:36:48.795480107Z",
                           "FinishedAt": "2026-11-23T16:45:39.181421110Z"}}))
            (root / "logs/jobmanager.log").write_text(
                '{"timestamp":"2026-11-23T16:45:44.953Z","message":"Job X switched from state CREATED to RUNNING."}\n'
                '{"timestamp":"2026-11-23T16:52:52.984Z","message":"Job X switched from state RUNNING to FINISHED."}\n')
            (root / "logs/dump-output.log").write_text(json.dumps({"seconds": 521.854}))
            (root / "logs/kafka_data_bytes.txt").write_text("16507840934\t/path\n")
            (root / "evidence/normalize.json").write_text(json.dumps({"counts": {"records": 62_380_615}}))
            (root / "evidence/output.jsonl").write_bytes(b"x" * 1024)
            run = cm.load_week_run(root)
            self.assertEqual(run["submitted"], 289_049_752)
            self.assertEqual(run["emitter_ev_per_second"], 545_563)
            self.assertAlmostEqual(run["emitter_wall_seconds"], 530.385, places=2)
            self.assertAlmostEqual(run["processing_seconds"], 428.031, places=2)
            self.assertEqual(run["output_records"], 62_380_615)
            self.assertEqual(run["broker_bytes"], 16_507_840_934)
            self.assertEqual(run["workers"], 2)

    def test_load_bracket_absent_is_pending(self):
        bracket = cm.load_bracket(Path("/nonexistent/bracket.json"))
        self.assertFalse(bracket["present"])
        self.assertEqual(bracket["points"], [])

    def test_load_bracket_falls_back_to_named_sessions(self):
        with tempfile.TemporaryDirectory() as tmp:
            results = Path(tmp)
            (results / "sessA-matrix.csv").write_text(
                "run_id,parallelism,target_rate,price_correctness_passed,committed_prices_per_second,"
                "achieved_originals_per_second,latency_p50_ms,latency_p95_ms,latency_p99_ms,drain_seconds\n"
                "sessA-000,1,1000,True,1000.0,1000.0,1.0,2.0,3.0,0.5\n"
                "sessA-001,2,2000,True,1900.0,2000.0,4.0,5.0,6.0,1.5\n")
            bracket = cm.load_bracket(Path("/nonexistent/bracket.json"), results_dir=results,
                                      sessions=["sessA"])
            self.assertTrue(bracket["present"])
            self.assertEqual(len(bracket["points"]), 2)
            self.assertEqual(bracket["points"][0]["achieved_per_second"], 1000.0)
            self.assertEqual(bracket["points"][1]["latency_ms"]["p99"], 6.0)
            # build_model then fits scaling efficiency from the two sessions.
            model = cm.build_model(week_profile_fixture(), week_run_fixture(), bracket)
            self.assertAlmostEqual(model["scaling_efficiency"], 0.95)

    def test_load_matrix_points_empty_without_sessions(self):
        self.assertEqual(cm.load_matrix_points("/nonexistent", []), [])

    def test_load_bracket_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bracket.json"
            path.write_text(json.dumps({
                "source_sessions": ["sess"],
                "bracket": {"1": {"highest_passing": 4000, "first_failing": 8000, "separated": True}},
                "points": [{
                    "workers": 1, "offered": 4000, "verdict": "PASS",
                    "steady_committed_per_second": {"median": 4000.0, "min": 3900.0, "max": 4100.0},
                    "visible_latency_ms": {"p50": 1.0, "p95": 2.0, "p99": 3.0},
                    "worker_cpu_utilization_ratio_max": {"median": 0.9},
                    "worker_throttled_periods_ratio_max": {"median": 0.02},
                }],
                "worker_capacity_gain": [],
            }))
            bracket = cm.load_bracket(path)
            self.assertTrue(bracket["present"])
            self.assertEqual(bracket["points"][0]["achieved_per_second"], 4000.0)
            self.assertEqual(bracket["points"][0]["latency_ms"]["p99"], 3.0)
            self.assertAlmostEqual(bracket["points"][0]["worker_cpu_utilization_ratio"], 0.9)

    def test_load_policy_penalties_absent(self):
        self.assertFalse(cm.load_policy_penalties(Path("/nonexistent/comparison.json"))["present"])

    def test_default_disk_rates(self):
        rates = cm.default_disk_rates(week_run_fixture())
        self.assertAlmostEqual(rates["output_bytes_per_row"], 5_282_388_854 / 289_049_752)
        self.assertAlmostEqual(rates["output_records_per_row"], 62_380_615 / 289_049_752)


class CliTests(unittest.TestCase):
    def test_main_runs_against_real_artifacts(self):
        # Reads the committed week profile and the out-of-Git week run; returns 0
        # and never starts a container. Falls back gracefully if a path is absent.
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "report.txt"
            with contextlib.redirect_stdout(io.StringIO()):
                code = cm.main(["--json", "--fraction", "0.5", "--multiple", "3",
                                "--workers", "1,2", "--out", str(out)])
            self.assertEqual(code, 0)
            payload = json.loads(out.read_text())
            self.assertEqual(len(payload["predictions"]), 4)

    def test_main_human_default(self):
        with contextlib.redirect_stdout(io.StringIO()) as stream:
            code = cm.main(["--volume", "1000000", "--workers", "1,2"])
        self.assertEqual(code, 0)
        self.assertIn("capacity model", stream.getvalue())


if __name__ == "__main__":
    unittest.main()

import csv
import tempfile
import unittest
from pathlib import Path

from benchmarks import live_peak


WINDOW = 300_000
DISORDER = 5_000
BASE = 1_700_000_100_000 // WINDOW * WINDOW


def slice_manifest():
    return {"emitter": {"PACING": "capture", "REPLAY_PREROLL_MS": 300_000, "REPLAY_MEASURE_MS": 300_000}}


def sample_run(**overrides):
    inputs = [
        {"ticker": "A.FR", "event_ms": BASE + 1_000, "value": 10.0, "emit_ms": 1_800_000_000_000,
         "wall_ms": 1_800_000_000_050},
        {"ticker": "A.FR", "event_ms": BASE + 2_000, "value": 11.0, "emit_ms": 1_800_000_000_100,
         "wall_ms": 1_800_000_000_160},
        {"ticker": "A.FR", "event_ms": BASE + 305_000, "value": 12.0, "emit_ms": 1_800_000_001_000,
         "wall_ms": 1_800_000_001_080},
    ]
    committed = [
        {"ticker": "A.FR", "type": "price", "event_ms": BASE + 1_000, "value": 10.0, "wall_ms": 1_800_000_000_200},
        {"ticker": "A.FR", "type": "price", "event_ms": BASE + 2_000, "value": 11.0, "wall_ms": 1_800_000_000_260},
        {"ticker": "A.FR", "type": "price", "event_ms": BASE + 305_000, "value": 12.0, "wall_ms": 1_800_000_001_180},
        {"ticker": "A.FR", "type": "EMA38", "event_ms": BASE + 2_000, "value": 10.5, "wall_ms": 1_800_000_001_400},
        {"ticker": "A.FR", "type": "EMA100", "event_ms": BASE + 2_000, "value": 10.2, "wall_ms": 1_800_000_001_400},
        {"ticker": "A.FR", "type": "BUY", "event_ms": BASE + 2_000, "value": 11.0, "wall_ms": 1_800_000_001_450},
    ]
    websocket = [dict(record) for record in committed]
    run = {
        "run_id": "peak-warm-1x-r1",
        "condition": "warm@1x",
        "state": "warm",
        "settings": {"window_ms": WINDOW, "max_out_of_order_ms": DISORDER, "allowed_lateness_ms": 0,
                     "checkpoint_interval_ms": live_peak.FROZEN_CHECKPOINT_INTERVAL_MS,
                     "flink_parallelism": 2, "dedup": "NONE", "late": "DROP", "speed_multiplier": 1.0},
        "clock": {"basis": "same_host_wall", "max_clock_error_ms": 250},
        "limits": {"containers": [{"name": "/taskmanager", "oom_killed": False}]},
        "job_running": True,
        "input": inputs,
        "committed": committed,
        "websocket": websocket,
        "ws_errors": [],
        "watermark_samples": [{"wall_ms": 1_800_000_000_300, "watermark": BASE + 305_000}],
        "progress": [
            {"wall_ms": 1_800_000_000_000, "input": 3, "output": 1, "websocket": 1},
            {"wall_ms": 1_800_000_002_000, "input": 3, "output": 3, "websocket": 3},
        ],
    }
    run.update(overrides)
    return run


class PlanTests(unittest.TestCase):
    def test_headline_plan_is_two_pacings_by_three_repeats(self):
        runs = live_peak.plan_runs()
        self.assertEqual(len(runs), 6)
        self.assertEqual([run["speed_multiplier"] for run in runs if run["state"] == "warm"],
                         [1.0, 1.0, 1.0, 1.25, 1.25, 1.25])
        self.assertEqual({run["condition"] for run in runs}, {"warm@1x", "warm@1.25x"})

    def test_cold_state_is_a_separate_labeled_condition(self):
        runs = live_peak.plan_runs(states=("warm", "cold"), repeats=2)
        self.assertEqual(len(runs), 8)
        self.assertIn("cold@1x", {run["condition"] for run in runs})
        self.assertIn("cold@1.25x", {run["condition"] for run in runs})

    def test_rollover_is_a_separate_run_not_a_headline_condition(self):
        runs = live_peak.plan_runs(include_rollover=True)
        rollover = [run for run in runs if run["kind"] == "rollover"]
        self.assertEqual(len(rollover), 1)
        self.assertNotIn("rollover", {run["condition"] for run in runs if run["kind"] == "headline"})

    def test_zero_repeats_is_rejected(self):
        with self.assertRaises(ValueError):
            live_peak.plan_runs(repeats=0)


class IsolationContractTests(unittest.TestCase):
    def env(self, project="ultrastonk-peak-abc123"):
        run = live_peak.plan_runs(repeats=1)[0]
        return live_peak.compose_env(run, slice_manifest(), project)

    def test_compose_env_satisfies_the_isolation_contract(self):
        env = self.env()
        self.assertEqual(live_peak.contract_violations(env), [])
        self.assertEqual(env["FLINK_UI_PORT"], "0")
        self.assertEqual(env["FLINK_BOUNDED_REPLAY"], "false")
        self.assertEqual(env["EXPECTED_TASKMANAGERS"], env["FLINK_PARALLELISM"])
        suffix = live_peak._topic_suffix(env["COMPOSE_PROJECT_NAME"])
        for key in ("KAFKA_INPUT_TOPIC", "KAFKA_OUTPUT_TOPIC", "KAFKA_REJECTED_TOPIC"):
            self.assertIn(suffix, env[key])

    def test_capture_offsets_come_from_the_slice_manifest(self):
        env = self.env()
        self.assertEqual(env["PACING"], "capture")
        self.assertEqual(env["REPLAY_PREROLL_MS"], "300000")
        self.assertEqual(env["REPLAY_MEASURE_MS"], "300000")

    def test_verify_namespace_and_running_project_are_rejected(self):
        self.assertTrue(live_peak.contract_violations(self.env("ultrastonk-verify-1")))
        self.assertTrue(live_peak.contract_violations(self.env("ultrastonk")))

    def test_bounded_replay_and_shared_port_are_rejected(self):
        env = self.env()
        env["FLINK_BOUNDED_REPLAY"] = "true"
        self.assertTrue(any("BOUNDED_REPLAY" in problem for problem in live_peak.contract_violations(env)))
        env = self.env()
        env["FLINK_UI_PORT"] = "8081"
        self.assertTrue(any("UI_PORT" in problem for problem in live_peak.contract_violations(env)))

    def test_taskmanager_mismatch_is_rejected(self):
        env = self.env()
        env["EXPECTED_TASKMANAGERS"] = "1"
        self.assertTrue(any("EXPECTED_TASKMANAGERS" in problem
                            for problem in live_peak.contract_violations(env)))


class DeliveryAndBacklogTests(unittest.TestCase):
    def test_delivered_by_subscription_counts_expected_committed_and_websocket(self):
        result = live_peak.delivered_by_subscription(sample_run())
        self.assertEqual(result["expected"], 3)
        self.assertEqual(result["committed"], 3)
        self.assertEqual(result["websocket"], 3)
        self.assertEqual(result["by_ticker"]["A.FR"], {"expected": 3, "committed": 3, "websocket": 3})

    def test_stage_backlog_reports_stage_gaps_and_growth(self):
        run = sample_run()
        run["progress"] = [
            {"wall_ms": 0, "input": 1, "output": 1, "websocket": 1},
            {"wall_ms": 1, "input": 10, "output": 5, "websocket": 4},
            {"wall_ms": 2, "input": 30, "output": 6, "websocket": 5},
        ]
        backend = live_peak.stage_backlog(run)
        self.assertEqual(backend["flink_backlog"]["final"], 24)
        self.assertTrue(backend["flink_backlog_growing"])
        self.assertIn("no broker consumer-group lag telemetry", backend["notes"])

    def test_stage_backlog_uses_broker_lag_when_present(self):
        run = sample_run()
        run["consumer_lag"] = {"flink_ingest": {"max": 100, "final": 0}, "gateway": {"max": 5, "final": 1}}
        backend = live_peak.stage_backlog(run)
        self.assertEqual(backend["kafka_lag"], {"max": 100, "final": 0})
        self.assertEqual(backend["gateway_lag"], {"max": 5, "final": 1})

    def test_restart_oom_summary_surfaces_oom_kills(self):
        run = sample_run()
        run["limits"] = {"containers": [{"name": "/taskmanager", "oom_killed": True},
                                        {"name": "/gateway", "oom_killed": False}]}
        run["restarts"] = 1
        summary = live_peak.restart_oom_summary(run)
        self.assertEqual(summary["oom_killed"], ["/taskmanager"])
        self.assertEqual(summary["restarts"], 1)

    def test_drain_summary_measures_from_last_input_to_last_delivery(self):
        run = sample_run()
        summary = live_peak.drain_summary(run)
        self.assertTrue(summary["available"])
        self.assertEqual(summary["seconds"], 2.0)

    def test_drain_summary_prefers_an_explicit_measurement(self):
        summary = live_peak.drain_summary(sample_run(drain_seconds=7.5))
        self.assertEqual(summary["seconds"], 7.5)


class LatencyDecisionTests(unittest.TestCase):
    def test_latency_verdict_is_inconclusive_below_min_samples(self):
        verdict = live_peak.latency_verdict([100, 200], min_samples=20)
        self.assertEqual(verdict["verdict"], "inconclusive")
        self.assertEqual(verdict["samples"], 2)

    def test_latency_verdict_passes_and_fails_at_the_target(self):
        passing = live_peak.latency_verdict([100.0] * 30, min_samples=30, target_ms=1000)
        self.assertEqual(passing["verdict"], "pass")
        failing = live_peak.latency_verdict([1500.0] * 30, min_samples=30, target_ms=1000)
        self.assertEqual(failing["verdict"], "fail")

    def test_trade_target_is_reported_independently_of_advisory_target(self):
        run = sample_run()
        decisions = live_peak.decisions(run)
        self.assertIn("trade_enqueue_to_websocket", decisions)
        self.assertIn("window_finalization_to_advisory_receipt", decisions)
        self.assertNotEqual(decisions["trade_enqueue_to_websocket"]["min_samples"],
                            decisions["window_finalization_to_advisory_receipt"]["min_samples"])

    def test_sparse_live_advisories_are_reported_inconclusive_not_fudged(self):
        decisions = live_peak.decisions(sample_run())
        self.assertEqual(decisions["window_finalization_to_advisory_receipt"]["verdict"], "inconclusive")
        self.assertEqual(decisions["live_advisory_p99_ms"], "inconclusive")

    def test_advisory_latency_uses_the_window_eligibility_bound(self):
        run = sample_run()
        samples = live_peak.advisory_window_latencies(run)
        self.assertEqual(samples, [1_800_000_001_450 - 1_800_000_001_000])

    def test_rollover_fixture_is_reported_separately(self):
        run = sample_run(rollover={"fixture": "rollover.csv", "p99": 12})
        decisions = live_peak.decisions(run)
        self.assertEqual(decisions["rollover_fixture"], {"fixture": "rollover.csv", "p99": 12})


class CheckpointCadenceTests(unittest.TestCase):
    def test_frozen_default_cadence_is_recorded(self):
        cadence = live_peak.checkpoint_cadence(sample_run())
        self.assertEqual(cadence["interval_ms"], live_peak.FROZEN_CHECKPOINT_INTERVAL_MS)
        self.assertTrue(cadence["frozen"])
        self.assertFalse(cadence["tuned"])

    def test_a_tuned_cadence_is_recorded_with_no_tuning_by_default(self):
        run = sample_run()
        run["settings"]["checkpoint_interval_ms"] = 500
        cadence = live_peak.checkpoint_cadence(run, tuned={"matched": True})
        self.assertFalse(cadence["frozen"])
        self.assertTrue(cadence["tuned"])
        self.assertEqual(cadence["tuning_comparison"], {"matched": True})

    def test_cadence_comparison_records_visibility_and_cost(self):
        baseline = {"checkpoint": {"interval_ms": 10_000}, "measurements": {"transport_ms": {"p99": 900}}}
        candidate = {"checkpoint": {"interval_ms": 500}, "measurements": {"transport_ms": {"p99": 700}},
                     "decision": "adopt 500 ms"}
        comparison = live_peak.cadence_tuning_comparison(baseline, candidate)
        self.assertEqual(comparison["visibility_p99_ms"], {"baseline": 900, "candidate": 700})
        self.assertEqual(comparison["decision"], "adopt 500 ms")


class OracleDeviationTests(unittest.TestCase):
    def report(self):
        return {"match": False,
                "missing": [{"symbol": "A.FR", "window": BASE, "type": "EMA"}],
                "extra": [], "incorrect": []}

    def test_missing_window_is_transport_when_committed_inputs_are_short(self):
        expected = {("A.FR", BASE): 3}
        committed = {("A.FR", BASE): 2}
        result = live_peak.classify_oracle_deviations(self.report(), expected, committed)
        self.assertEqual(len(result["transport"]), 1)
        self.assertEqual(len(result["policy"]), 0)

    def test_missing_window_is_policy_when_inputs_reached_the_query(self):
        expected = {("A.FR", BASE): 3}
        committed = {("A.FR", BASE): 3}
        result = live_peak.classify_oracle_deviations(self.report(), expected, committed)
        self.assertEqual(len(result["policy"]), 1)
        self.assertEqual(len(result["transport"]), 0)

    def test_duplicate_input_extra_is_policy_related(self):
        report = {"match": False, "missing": [], "incorrect": [],
                  "extra": [{"symbol": "A.FR", "window": BASE, "type": "EMA"}]}
        result = live_peak.classify_oracle_deviations(report, {("A.FR", BASE): 3}, {("A.FR", BASE): 4})
        self.assertEqual(result["counts"]["policy"], 1)

    def test_unexplained_extra_is_kept_visible(self):
        report = {"match": False, "missing": [], "incorrect": [],
                  "extra": [{"symbol": "Z.ZZ", "window": BASE, "type": "BUY"}]}
        result = live_peak.classify_oracle_deviations(report, {}, {})
        self.assertEqual(result["counts"]["unexplained"], 1)


class LedgerTests(unittest.TestCase):
    def test_ledger_keeps_failed_and_invalid_attempts(self):
        ledger = live_peak.new_ledger("scored")
        run = live_peak.plan_runs(repeats=1)[0]
        live_peak.append_entry(ledger, live_peak.ledger_entry(run, "failed", reasons=["oom"]))
        live_peak.append_entry(ledger, live_peak.ledger_entry(run, "invalid", reasons=["pacing 0.5"]))
        live_peak.append_entry(ledger, live_peak.ledger_entry(run, "valid"))
        self.assertEqual(live_peak.ledger_status_counts(ledger),
                         {"valid": 1, "invalid": 1, "failed": 1})
        self.assertEqual(ledger["entries"][0]["reasons"], ["oom"])

    def test_unknown_status_is_rejected(self):
        with self.assertRaises(ValueError):
            live_peak.ledger_entry(live_peak.plan_runs(repeats=1)[0], "ok")


class AggregateTests(unittest.TestCase):
    def summary(self, p99, passed, verdict="pass"):
        return {
            "condition": "warm@1x", "state": "warm", "pacing_level": 1.0, "overall_passed": passed,
            "decisions": {"trade_enqueue_to_websocket": {"verdict": verdict, "p99": p99},
                          "window_finalization_to_advisory_receipt": {"verdict": "inconclusive"},
                          "live_advisory_p99_ms": "inconclusive"},
            "failures": [] if passed else [{"check": "delivery", "reason": "missing"}],
        }

    def test_aggregate_reports_median_spread_and_keeps_failures(self):
        rows = live_peak.aggregate_repeats([self.summary(500, True), self.summary(700, True),
                                            self.summary(900, False)])
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["passes"], 2)
        self.assertEqual(row["valid"], 3)
        self.assertEqual(row["trade_p99_ms"], 600)
        self.assertEqual(row["trade_p99_spread"], 200)
        self.assertEqual(row["failures"][0]["reason"], "missing")


class HostCommandTests(unittest.TestCase):
    def test_steps_offer_the_slice_and_subscribe_before_publication(self):
        run = live_peak.plan_runs(repeats=1)[0]
        env = live_peak.compose_env(run, slice_manifest(), "ultrastonk-peak-xyz")
        steps = live_peak.host_commands(run, env, "/tmp/slice/peak-warm.csv", "ultrastonk-peak-xyz",
                                        "/tmp/override.yaml", "/tmp/results", ["A.FR", "B.NL"])
        joined = " ".join(" ".join(step["argv"]) for step in steps)
        self.assertIn("A.FR,B.NL", joined)
        emitter = next(step for step in steps if "--profile" in step["argv"] and "emitter" in step["argv"])
        self.assertEqual(emitter["env"]["CSV_PATH"], "/data/peak-warm.csv")
        self.assertEqual(emitter["env"]["REPLAY_DATA_DIR"], "/tmp/slice")
        client_index = next(i for i, step in enumerate(steps) if "client" in step["argv"])
        emitter_index = next(i for i, step in enumerate(steps) if "emitter" in step["argv"])
        self.assertLess(client_index, emitter_index)

    def test_gateway_override_raises_the_ticker_cap(self):
        override = live_peak.gateway_override("p", 1234)
        self.assertEqual(override["services"]["gateway"]["environment"]["MAX_TICKERS_PER_CLIENT"], "1234")


class SliceEligibilityTests(unittest.TestCase):
    def test_counts_only_eligible_oracle_trades_by_window(self):
        def cell(symbol, time, last):
            row = [""] * 40
            row[0], row[1], row[2], row[3], row[21] = symbol, "E", "09-11-2021", time, last
            return row

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "slice.csv"
            rows = [cell("A.FR", "10:00:10.000", "10.5"),
                    cell("A.FR", "10:00:11.000", "0"),
                    cell("B.NL", "10:00:12.000", "-1"),
                    cell("BAD", "10:00:13.000", "5.0"),
                    cell("C.ETR", "10:04:10.000", "7.0")]
            with path.open("w", newline="") as target:
                target.write("header\n" * 13)
                csv.writer(target).writerows(rows)
            counts = live_peak.count_slice_eligible(path, window_ms=1000)
            self.assertEqual(sum(counts.values()), 2)
            self.assertEqual(sorted(symbol for symbol, _ in counts), ["A.FR", "C.ETR"])


class KeepingUpTests(unittest.TestCase):
    def test_exact_delivery_without_growth_is_kept_up(self):
        verdict = live_peak.keeping_up_verdict(sample_run())
        self.assertEqual(verdict["verdict"], "kept_up")
        self.assertTrue(verdict["exact_delivery"])
        self.assertFalse(verdict["sustained_backlog_growth"])

    def test_sustained_backlog_growth_is_not_keeping_up(self):
        run = sample_run()
        run["progress"] = [
            {"wall_ms": 0, "input": 1, "output": 1, "websocket": 1},
            {"wall_ms": 1, "input": 10, "output": 4, "websocket": 4},
            {"wall_ms": 2, "input": 30, "output": 5, "websocket": 5},
        ]
        verdict = live_peak.keeping_up_verdict(run)
        self.assertEqual(verdict["verdict"], "did_not_keep_up")
        self.assertTrue(verdict["sustained_backlog_growth"])

    def test_missing_delivery_is_not_keeping_up(self):
        run = sample_run()
        run["websocket"] = [record for record in run["websocket"] if record["type"] != "price"]
        verdict = live_peak.keeping_up_verdict(run)
        self.assertEqual(verdict["verdict"], "did_not_keep_up")
        self.assertFalse(verdict["exact_delivery"])

    def test_oom_kill_is_not_keeping_up(self):
        run = sample_run()
        run["limits"] = {"containers": [{"name": "/taskmanager", "oom_killed": True}]}
        verdict = live_peak.keeping_up_verdict(run)
        self.assertEqual(verdict["verdict"], "did_not_keep_up")
        self.assertTrue(verdict["oom_killed"])

    def test_missing_evidence_is_surfaced(self):
        run = sample_run()
        run["pacing"] = None
        summary = live_peak.summarize_run(run)
        self.assertIn("pacing report", summary["missing_evidence"])


class SummarizeRunTests(unittest.TestCase):
    def test_summary_attaches_all_metric_groups(self):
        summary = live_peak.summarize_run(sample_run())
        for key in ("checks", "measurements", "subscriptions", "stage_backlog", "restarts",
                    "drain", "checkpoint", "decisions", "keeping_up", "missing_evidence", "failures"):
            self.assertIn(key, summary)
        self.assertTrue(summary["checkpoint"]["frozen"])


if __name__ == "__main__":
    unittest.main()

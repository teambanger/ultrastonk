import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmarks import capacity_bracket as cb

ROOT = Path(__file__).resolve().parents[2]


def flat_progress(count=24, step=100):
    return make_progress(lambda index, expected: (0, 0), count, step)


def make_progress(backlogs, count=24, step=100):
    rows = []
    for index in range(count):
        expected = (index + 1) * step
        committed, websocket = backlogs(index, expected)
        rows.append({"elapsed_seconds": index + 1, "originals": expected,
                     "expected_measured_prices": expected,
                     "committed_measured_prices": expected - committed,
                     "websocket_measured_prices": expected - committed - websocket})
    return rows


def passing_run(**overrides):
    run = {
        "run_id": "s-000", "workers": 1, "offered": 1000.0, "repetition": 0,
        "arguments": {"duration": 60, "rate": 1000, "flink_parallelism": 1},
        "settings": {"window_ms": 1000, "max_out_of_order_ms": 100, "allowed_lateness_ms": 2000,
                     "checkpoint_interval_ms": 1000, "flink_parallelism": 1},
        "measurement": {"achieved_originals_per_second": 1000.0, "committed_prices_per_second": 1000.0,
                        "websocket_prices_per_second": 1000.0, "drain_seconds": 2.0,
                        "missing_prices": 0, "unexpected_prices": 0,
                        "visible_roundtrip_ms": {"p50": 1, "p95": 2, "p99": 3, "max": 4},
                        "websocket_roundtrip_ms": {"p50": 5, "p95": 6, "p99": 7, "max": 8}},
        "counters": {"producer_queue_full": 0},
        "progress": flat_progress(),
        "telemetry": [{"timestamp": index} for index in range(10)],
        "errors": [],
        "limits": {"worker_count": 1, "aggregate_worker_cpu_quota_cores": 1.0,
                   "aggregate_worker_memory_bytes": 2 ** 31, "workers": [{"name": "/p-taskmanager-1"}]},
        "hardware": {"cpu_count": 12, "memory_bytes": 33219461120},
        "resources": None, "prometheus": None,
        "worker_cpu": {"source": "prometheus", "samples": 10, "used_cores_max": 0.8,
                       "utilization_ratio_max": 0.8, "throttled_periods_ratio_max": 0.0},
        "gateway_present": True, "price_correctness_passed": True, "gateway_correctness_passed": True,
        "resource_profile": "perworker",
    }
    run.update(overrides)
    return run


class RulesTests(unittest.TestCase):
    def test_pre_registered_rules_match_code(self):
        path = ROOT / "benchmarks/evidence/sustainable-limit/pre-registered-rules.json"
        self.assertEqual(json.loads(path.read_text()), cb.default_rules())


class AcceptanceTests(unittest.TestCase):
    def test_clean_run_passes(self):
        result = cb.evaluate_run(passing_run())
        self.assertEqual(result["verdict"], cb.PASS, result)
        self.assertEqual(result["reasons"], [])

    def test_generator_shortfall_is_inconclusive_not_a_pass(self):
        run = passing_run(measurement={"achieved_originals_per_second": 900.0, "missing_prices": 0,
                                       "unexpected_prices": 0})
        result = cb.evaluate_run(run)
        self.assertEqual(result["verdict"], cb.INCONCLUSIVE)
        self.assertIn("shortfall", result["reasons"][0])

    def test_shortfall_outranks_a_delivery_failure(self):
        run = passing_run(price_correctness_passed=False,
                          measurement={"achieved_originals_per_second": 500.0, "missing_prices": 10,
                                       "unexpected_prices": 0})
        self.assertEqual(cb.evaluate_run(run)["verdict"], cb.INCONCLUSIVE)

    def test_delivery_failure_is_a_fail(self):
        result = cb.evaluate_run(passing_run(price_correctness_passed=False))
        self.assertEqual(result["verdict"], cb.FAIL)
        self.assertIn("delivery", result["reasons"][0])

    def test_gateway_failure_is_a_fail(self):
        result = cb.evaluate_run(passing_run(gateway_correctness_passed=False))
        self.assertEqual(result["verdict"], cb.FAIL)

    def test_missing_progress_is_missing_telemetry(self):
        result = cb.evaluate_run(passing_run(progress=[]))
        self.assertEqual(result["verdict"], cb.INCONCLUSIVE)
        self.assertIn("progress samples", result["checks"]["telemetry"]["missing"])

    def test_missing_limits_is_missing_telemetry(self):
        result = cb.evaluate_run(passing_run(limits=None))
        self.assertEqual(result["verdict"], cb.INCONCLUSIVE)
        self.assertIn("effective limits", result["checks"]["telemetry"]["missing"])

    def test_missing_hardware_is_missing_telemetry(self):
        result = cb.evaluate_run(passing_run(hardware=None))
        self.assertIn("host hardware", result["checks"]["telemetry"]["missing"])

    def test_missing_worker_cpu_is_missing_telemetry(self):
        result = cb.evaluate_run(passing_run(worker_cpu=None))
        self.assertIn("aggregate worker CPU", result["checks"]["telemetry"]["missing"])

    def test_missing_gateway_correctness_is_missing_telemetry(self):
        result = cb.evaluate_run(passing_run(gateway_present=True, gateway_correctness_passed=None))
        self.assertIn("gateway correctness", result["checks"]["telemetry"]["missing"])

    def test_sustained_backlog_growth_fails(self):
        run = passing_run(progress=make_progress(lambda index, expected: (index * 10, 0)))
        result = cb.evaluate_run(run)
        self.assertEqual(result["verdict"], cb.FAIL)
        self.assertIn("input_to_committed", result["checks"]["backlog"]["growing_stages"])

    def test_checkpoint_batching_spikes_are_ignored(self):
        spiky = make_progress(lambda index, expected: (50 if index % 3 == 2 else 0, 0))
        run = passing_run(progress=spiky,
                          settings={"window_ms": 1000, "checkpoint_interval_ms": 3000, "flink_parallelism": 1})
        result = cb.evaluate_run(run)
        self.assertEqual(result["verdict"], cb.PASS, result)
        trend = result["checks"]["backlog"]["stages"]["input_to_committed"]
        self.assertGreater(trend["max"], 0)
        self.assertFalse(trend["growing"])

    def test_a_rising_trough_is_sustained_growth_even_with_spikes(self):
        rising = make_progress(lambda index, expected: (50 if index % 3 == 2 else min(index // 3, 20) * 5, 0))
        run = passing_run(progress=rising,
                          settings={"window_ms": 1000, "checkpoint_interval_ms": 3000, "flink_parallelism": 1})
        self.assertEqual(cb.evaluate_run(run)["verdict"], cb.FAIL)

    def test_websocket_stage_growth_fails(self):
        run = passing_run(progress=make_progress(lambda index, expected: (0, index * 5)))
        self.assertEqual(cb.evaluate_run(run)["verdict"], cb.FAIL)

    def test_backlog_trend_suppresses_spikes_by_checkpoint_bucket(self):
        points = [(index, 50 if index % 3 == 2 else 0) for index in range(12)]
        trend = cb.backlog_trend(points, checkpoint_interval_ms=3000)
        self.assertEqual(trend["bucket_width_samples"], 3)
        self.assertEqual(trend["spikes_suppressed"], 8)
        self.assertFalse(trend["growing"])

    def test_stage_backlog_excludes_drain_samples(self):
        progress = flat_progress(count=30) + [{"elapsed_seconds": 80, "expected_measured_prices": 9000,
                                               "committed_measured_prices": 9000, "websocket_measured_prices": 9000}]
        stages = cb.stage_backlogs(progress, load_window_s=60, slack_s=2)
        self.assertEqual(len(stages["input_to_committed"]), 30)


class BracketTests(unittest.TestCase):
    def record(self, workers, offered, verdict):
        return {"workers": workers, "offered": offered, "verdict": verdict, "repetition": 0}

    def test_point_verdict_precedence(self):
        self.assertEqual(cb.point_verdict([cb.PASS, cb.PASS]), cb.PASS)
        self.assertEqual(cb.point_verdict([cb.PASS, cb.FAIL]), cb.FAIL)
        self.assertEqual(cb.point_verdict([cb.FAIL, cb.INCONCLUSIVE]), cb.INCONCLUSIVE)

    def test_separated_bracket_reports_bounds(self):
        records = [self.record(1, 1000, cb.PASS), self.record(1, 2000, cb.PASS), self.record(1, 4000, cb.FAIL)]
        result = cb.bracket(records)[1]
        self.assertTrue(result["separated"])
        self.assertEqual(result["highest_passing"], 2000)
        self.assertEqual(result["first_failing"], 4000)

    def test_no_failing_load_is_unbracketed(self):
        result = cb.bracket([self.record(1, 1000, cb.PASS)])[1]
        self.assertFalse(result["separated"])
        self.assertIn("no failing", result["unbracketed_reason"])

    def test_no_passing_load_is_unbracketed(self):
        result = cb.bracket([self.record(2, 1000, cb.FAIL)])[2]
        self.assertFalse(result["separated"])
        self.assertIn("no passing", result["unbracketed_reason"])

    def test_inconclusive_point_blocks_a_clean_bracket(self):
        records = [self.record(1, 1000, cb.PASS), self.record(1, 2000, cb.INCONCLUSIVE)]
        result = cb.bracket(records)[1]
        self.assertEqual(result["highest_passing"], 1000)
        self.assertFalse(result["separated"])
        self.assertIn("no failing", result["unbracketed_reason"])

    def test_non_monotonic_result_is_flagged(self):
        records = [self.record(1, 1000, cb.FAIL), self.record(1, 2000, cb.PASS)]
        result = cb.bracket(records)[1]
        self.assertTrue(result["non_monotonic"])


class ScheduleTests(unittest.TestCase):
    def test_coarse_rates_are_geometric_and_inclusive(self):
        self.assertEqual(cb.coarse_rates(1000, 8000, 2.0), [1000, 2000, 4000, 8000])
        self.assertEqual(cb.coarse_rates(1000, 5000, 2.0), [1000, 2000, 4000, 5000])

    def test_boundary_rates_lie_between_bounds(self):
        rates = cb.boundary_rates(2000, 4000, 3)
        self.assertEqual(rates, [2500, 3000, 3500])

    def test_fine_plan_is_built_per_worker_from_the_coarse_bracket(self):
        coarse = {1: {"highest_passing": 2000, "first_failing": 4000},
                  2: {"highest_passing": 4000, "first_failing": 8000}}
        plans = cb.build_fine_plans(coarse, [1, 2], 2, 120, 10, 3, 0.25)
        self.assertEqual([plan["workers"] for plan in plans], [1, 2])
        self.assertEqual(plans[0]["duration"], 120)
        self.assertTrue(all(1500 <= rate <= 5000 for rate in plans[0]["rates"]))

    def test_fine_plan_skips_an_unseparated_worker(self):
        coarse = {1: {"highest_passing": None, "first_failing": 4000}}
        self.assertEqual(cb.build_fine_plans(coarse, [1], 2, 120, 10, 3, 0.25), [])

    def coarse_bracket(self):
        return {1: {"separated": True, "highest_passing": 2000, "first_failing": 4000},
                2: {"separated": True, "highest_passing": 4000, "first_failing": 8000}}

    def test_select_fine_workers_defaults_to_every_separated_worker(self):
        self.assertEqual(cb.select_fine_workers(self.coarse_bracket()), [1, 2])

    def test_select_fine_workers_single_picks_the_most_informative(self):
        self.assertEqual(cb.select_fine_workers(self.coarse_bracket(), single=True), [2])

    def test_select_fine_workers_explicit_restricts(self):
        self.assertEqual(cb.select_fine_workers(self.coarse_bracket(), explicit=[1]), [1])

    def test_select_fine_workers_ignores_unseparated(self):
        coarse = {1: {"separated": False}, 2: {"separated": True, "highest_passing": 1, "first_failing": 2}}
        self.assertEqual(cb.select_fine_workers(coarse, explicit=[1, 2]), [2])

    def test_extra_rate_only_when_every_worker_passed_its_top(self):
        passing = {1: {"points": {2000: cb.PASS, 4000: cb.PASS}},
                   2: {"points": {4000: cb.PASS, 8000: cb.PASS}}}
        failing = {1: {"points": {4000: cb.PASS}}, 2: {"points": {8000: cb.FAIL}}}
        self.assertTrue(cb.should_add_extra_rate(passing, 8000))
        self.assertFalse(cb.should_add_extra_rate(failing, 16000))
        self.assertFalse(cb.should_add_extra_rate(passing, None))

    def test_explicit_coarse_rates_override_the_geometric_ladder(self):
        completed = subprocess.run(
            [sys.executable, str(ROOT / "benchmarks/capacity_bracket.py"), "--dry-run",
             "--profile-spec", "taskmanager:1:2g", "--coarse-rates", "1000,4000,8000"],
            text=True, capture_output=True, check=True, cwd=ROOT)
        self.assertEqual(json.loads(completed.stdout)["coarse"]["rates"], [1000, 4000, 8000])

    def test_stage_fits_respects_the_wall_budget(self):
        self.assertTrue(cb.stage_fits(None, 120, 10, 3))
        self.assertTrue(cb.stage_fits(600, 60, 5, 1))
        self.assertFalse(cb.stage_fits(120, 120, 10, 3))

    def test_downscaled_dry_run_locks_the_scoped_command(self):
        completed = subprocess.run(
            [sys.executable, str(ROOT / "benchmarks/capacity_bracket.py"), "--dry-run",
             "--profile-spec", "taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g",
             "--coarse-start", "500", "--coarse-stop", "4000", "--coarse-factor", "2",
             "--duration-coarse", "30", "--warmup", "5", "--coarse-extra-rate", "8000",
             "--fine-single", "--fine-points", "1", "--duration-fine", "60", "--repetitions-fine", "1",
             "--wall-budget-seconds", "2700"],
            text=True, capture_output=True, check=True, cwd=ROOT)
        plan = json.loads(completed.stdout)
        self.assertEqual(plan["coarse"]["rates"], [500, 1000, 2000, 4000])
        self.assertEqual(plan["coarse_extra_rate"], 8000)
        self.assertEqual(plan["wall_budget_seconds"], 2700)
        joined = " ".join(plan["coarse_command"])
        self.assertIn("--rates 500,1000,2000,4000", joined)
        self.assertIn("--duration 30", joined)
        self.assertIn("--parallelism 1,2", joined)


class SummaryTests(unittest.TestCase):
    def point(self, workers, offered, steady, verdict, throttle=0.0, utilization=0.5):
        return {"workers": workers, "offered": offered, "verdict": verdict,
                "steady_committed_per_second": {"median": steady},
                "worker_cpu_utilization_ratio_max": {"median": utilization},
                "worker_throttled_periods_ratio_max": {"median": throttle},
                "producer_queue_full": {"median": 0}}

    def test_summarize_reports_median_and_repeat_spread(self):
        records = []
        for steady in (1000, 1100, 1200):
            record = cb.evaluate_run(passing_run())
            record["measurements"] = {"steady_committed_per_second": steady, "drain_seconds": 1.0}
            records.append(record)
        points = cb.summarize(records)
        self.assertEqual(len(points), 1)
        spread = points[0]["steady_committed_per_second"]
        self.assertEqual(spread["median"], 1100)
        self.assertEqual(spread["spread"], 200)
        self.assertEqual(spread["count"], 3)

    def test_worker_capacity_gain_reports_total_resources(self):
        points = [self.point(1, 1000, 1000, cb.PASS), self.point(2, 1000, 1800, cb.PASS)]
        gains = cb.worker_capacity_gain(points)
        self.assertEqual(len(gains), 1)
        self.assertAlmostEqual(gains[0]["gain"], 1.8)
        self.assertEqual(gains[0]["total_resource_multiplier"], 2)
        self.assertFalse(gains[0]["stretch_met"])

    def test_stretch_gain_only_counts_when_flink_is_the_limiter(self):
        points = [self.point(1, 1000, 1000, cb.PASS), self.point(2, 1000, 1600, cb.PASS)]
        self.assertFalse(cb.worker_capacity_gain(points, flink_limiter=False)[0]["stretch_met"])
        self.assertTrue(cb.worker_capacity_gain(points, flink_limiter=True)[0]["stretch_met"])

    def test_flink_limiter_evidence_reads_throttling(self):
        self.assertTrue(cb.flink_limiter_evidence(self.point(1, 1000, 1000, cb.PASS, throttle=0.2))["flink_bound"])
        self.assertTrue(cb.flink_limiter_evidence(self.point(1, 1000, 1000, cb.PASS, utilization=0.95))["flink_bound"])
        self.assertFalse(cb.flink_limiter_evidence(self.point(1, 1000, 1000, cb.PASS))["flink_bound"])


class BottleneckTests(unittest.TestCase):
    def limits(self, cpu, memory):
        return {"worker_count": 1, "aggregate_worker_cpu_quota_cores": cpu,
                "aggregate_worker_memory_bytes": memory, "workers": [{"name": "/p-taskmanager-1"}]}

    def test_one_budget_change_validates_a_single_moved_budget(self):
        base = passing_run(limits=self.limits(1.0, 2 ** 31))
        changed = passing_run(limits=self.limits(2.0, 2 ** 31))
        result = cb.one_budget_change(base, changed, "taskmanager_cpu")
        self.assertTrue(result["valid"])
        self.assertEqual(result["differing_budgets"], ["taskmanager_cpu"])

    def test_two_moved_budgets_are_not_a_one_budget_change(self):
        base = passing_run(limits=self.limits(1.0, 2 ** 31))
        changed = passing_run(limits=self.limits(2.0, 2 ** 32))
        self.assertFalse(cb.one_budget_change(base, changed, "taskmanager_cpu")["valid"])

    def test_a_workload_change_confounds_the_pair(self):
        base = passing_run(limits=self.limits(1.0, 2 ** 31))
        changed = passing_run(offered=2000.0, limits=self.limits(2.0, 2 ** 31))
        self.assertFalse(cb.one_budget_change(base, changed, "taskmanager_cpu")["valid"])

    def test_parallelism_change_is_a_valid_one_budget_change(self):
        base = passing_run(workers=1, limits=self.limits(1.0, 2 ** 31))
        changed = passing_run(workers=2, limits=self.limits(1.0, 2 ** 31))
        self.assertTrue(cb.one_budget_change(base, changed, "parallelism")["valid"])

    def test_stage_evidence_reports_backlog_and_cpu_pressure(self):
        evidence = cb.stage_evidence(passing_run())
        self.assertIn("input_to_committed_max", evidence)
        self.assertIn("worker_cpu_utilization_ratio_max", evidence)


class LedgerTests(unittest.TestCase):
    def test_ledger_retains_failed_attempts_and_deduplicates(self):
        with tempfile.TemporaryDirectory() as temp:
            ledger = cb.Ledger(Path(temp) / "run-ledger.jsonl")
            ledger.append([{"run_id": "a", "verdict": cb.FAIL}, {"run_id": "b", "verdict": cb.INCONCLUSIVE}])
            ledger.append([{"run_id": "a", "verdict": cb.PASS}, {"run_id": "c", "verdict": cb.PASS}])
            entries = ledger.records()
            self.assertEqual([entry["run_id"] for entry in entries], ["a", "b", "c"])
            self.assertEqual(entries[0]["verdict"], cb.FAIL)
            self.assertIn("recorded_at_epoch_seconds", entries[0])


class NormalizationTests(unittest.TestCase):
    def artifacts(self):
        row = {"run_id": "s-000", "parallelism": "2", "target_rate": "5000", "repetition": "0",
               "quality": "baseline", "dedup": "NONE", "late": "DROP", "resource_profile": "perworker",
               "exit_code": "0", "price_correctness_passed": "True", "gateway_correctness_passed": "True"}
        driver = {"run_id": "s-000", "arguments": {"duration": 15, "rate": 5000, "flink_parallelism": 2},
                  "settings": {"WINDOW_SIZE_MS": "1000", "MAX_OUT_OF_ORDER_MS": "100",
                               "ALLOWED_LATENESS_MS": "2000", "CHECKPOINT_INTERVAL_MS": "1000",
                               "FLINK_PARALLELISM": "2"},
                  "measurement": {"achieved_originals_per_second": 4999.0}, "counters": {},
                  "progress": flat_progress(), "price_correctness_passed": True, "gateway_correctness_passed": True}
        limits = {"profile": "perworker", "requested": {}, "containers": [
            {"name": "/p-taskmanager-1", "nano_cpus": 1000000000, "memory_bytes": 2147483648, "oom_killed": False},
            {"name": "/p-taskmanager-2", "nano_cpus": 1000000000, "memory_bytes": 2147483648, "oom_killed": False}]}
        manifest = {"host_cpu_count_and_memory_bytes": "12 33219461120",
                    "docker_server_version": "29.10.0", "git_commit": "abc"}
        resources = [{"Name": "/p-taskmanager-1", "CPUPerc": "75.0%", "MemUsage": "1.5GiB / 2GiB"},
                     {"Name": "/p-taskmanager-2", "CPUPerc": "80.0%", "MemUsage": "1.2GiB / 2GiB"},
                     {"Name": "/p-jobmanager-1", "CPUPerc": "5.0%", "MemUsage": "0.5GiB / 2GiB"}]
        prometheus = {"status": "success", "data": {"result": [
            {"metric": {"__name__": "pipeline_container_cpu_used_cores", "service": "taskmanager",
                        "container": "tm1"}, "values": [[1, "0.7"], [2, "0.8"]]},
            {"metric": {"__name__": "pipeline_container_cpu_used_cores", "service": "taskmanager",
                        "container": "tm2"}, "values": [[1, "0.6"], [2, "0.9"]]},
            {"metric": {"__name__": "pipeline_container_cpu_throttled_periods_ratio", "service": "taskmanager",
                        "container": "tm1"}, "values": [[1, "0.1"], [2, "0.2"]]},
            {"metric": {"__name__": "pipeline_container_memory_working_set_bytes", "service": "taskmanager",
                        "container": "tm1"}, "values": [[1, "100000000"]]}]}}
        return row, driver, limits, manifest, resources, prometheus

    def test_normalize_run_reads_workers_caps_hardware_and_worker_cpu(self):
        row, driver, limits, manifest, resources, prometheus = self.artifacts()
        record = cb.normalize_run(row, driver, limits, manifest, resources, prometheus)
        self.assertEqual(record["workers"], 2)
        self.assertEqual(record["offered"], 5000.0)
        self.assertEqual(record["settings"]["checkpoint_interval_ms"], 1000)
        self.assertEqual(record["limits"]["worker_count"], 2)
        self.assertEqual(record["limits"]["aggregate_worker_cpu_quota_cores"], 2.0)
        self.assertEqual(record["hardware"]["cpu_count"], 12)
        self.assertAlmostEqual(record["worker_cpu"]["used_cores_max"], 1.7)
        self.assertAlmostEqual(record["worker_cpu"]["throttled_periods_ratio_max"], 0.2)
        self.assertAlmostEqual(record["resources"]["cpu_percent_sum"], 155.0)

    def test_worker_cpu_prefers_prometheus_over_the_snapshot(self):
        row, driver, limits, manifest, resources, prometheus = self.artifacts()
        record = cb.normalize_run(row, driver, limits, manifest, resources, prometheus)
        self.assertEqual(record["worker_cpu"]["source"], "prometheus")

    def test_effective_limits_marks_oom(self):
        limits = {"profile": "p", "containers": [{"name": "/p-taskmanager-1", "nano_cpus": 0,
                                                  "memory_bytes": 0, "oom_killed": True}]}
        self.assertTrue(cb.effective_limits(limits)["oom_killed"])

    def test_parse_bytes_handles_binary_units(self):
        self.assertEqual(cb._parse_bytes("1.5GiB"), 1.5 * 2 ** 30)
        self.assertEqual(cb._parse_bytes("512MiB"), 512 * 2 ** 20)


class CommandTests(unittest.TestCase):
    def test_matrix_command_holds_fixed_parameters(self):
        command = cb.matrix_command("benchmarks/matrix.py", workers=[1, 2], rates=[1000, 2000], duration=60,
                                    warmup=10, repetitions=3, profile_name="perworker",
                                    profile_spec="taskmanager:1:2g", project="ultrastonk-capacity-coarse",
                                    symbols=1000, skew=1.2, window_ms=1000, grace_ms=2000)
        joined = " ".join(command)
        for fragment in ("--rates 1000,2000", "--parallelism 1,2", "--dedup NONE", "--late DROP",
                         "--qualities baseline", "--late-rate 0", "--duplicate-rate 0",
                         "--resource-profile perworker=taskmanager:1:2g", "--repetitions 3",
                         "--gateway", "--monitoring"):
            self.assertIn(fragment, joined)

    def test_parse_session_reads_the_matrix_banner(self):
        self.assertEqual(cb.parse_session("Benchmark session abc123: Compose project p"), "abc123")
        self.assertIsNone(cb.parse_session("no banner here"))

    def test_dry_run_prints_the_plan_without_running_anything(self):
        completed = subprocess.run(
            [sys.executable, str(ROOT / "benchmarks/capacity_bracket.py"), "--dry-run",
             "--profile-spec", "taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g"],
            text=True, capture_output=True, check=True, cwd=ROOT)
        plan = json.loads(completed.stdout)
        self.assertEqual(plan["rules"], cb.default_rules())
        self.assertEqual(plan["coarse"]["phase"], "coarse")
        self.assertIn("matrix.py", " ".join(plan["coarse_command"]))
        self.assertIn("fine", plan)
        self.assertIsNone(plan["coarse_extra_rate"])


if __name__ == "__main__":
    unittest.main()

import unittest

from benchmarks import drop_vs_grace as dg

WINDOW = 300_000
DISORDER = 5_000
GRACE = 30_000
SEED = 11


def fixture(seed=SEED, late_price=1000.0, **overrides):
    options = {"seed": seed, "window_ms": WINDOW, "max_out_of_order_ms": DISORDER,
               "grace_ms": GRACE, "late_price": late_price}
    options.update(overrides)
    return dg.scripted_disorder(**options)


def adv(symbol, window, kind):
    return {"symbol": symbol, "window": window, "type": kind}


def ema(symbol, window, fast, slow, timestamp_ms=0):
    return {"symbol": symbol, "window": window, "type": "EMA", "ema38": fast,
            "ema100": slow, "timestamp_ms": timestamp_ms}


def advisory_set(records):
    return {(record["window"], record["type"]) for record in records
            if record["type"] in ("BUY", "SELL")}


class ScriptedDisorderTests(unittest.TestCase):
    def test_same_seed_produces_an_identical_schedule(self):
        self.assertEqual(fixture(), fixture())

    def test_different_seed_changes_the_schedule(self):
        self.assertNotEqual(fixture(seed=SEED), fixture(seed=SEED + 1))

    def test_fixture_digest_is_stable_and_seed_sensitive(self):
        self.assertEqual(dg.fixture_digest(fixture()), dg.fixture_digest(fixture()))
        self.assertNotEqual(dg.fixture_digest(fixture()), dg.fixture_digest(fixture(seed=SEED + 1)))

    def test_late_trade_is_the_last_of_two_trades_in_one_window(self):
        schedule = fixture()
        late = schedule["late_trade"]
        window = late["window_index"]
        base = schedule["base_ms"]
        trades = [step for step in schedule["steps"] if step["kind"] == "trade"
                  and step["ticker"] == schedule["symbol"]
                  and (step["event_ms"] - base) // WINDOW == window]
        self.assertEqual(len(trades), 2, trades)
        self.assertEqual(max(trades, key=lambda step: step["event_ms"])["event_ms"], late["event_ms"])
        self.assertEqual(late["price"], 1000.0)

    def test_rejects_invalid_settings(self):
        for options in ({"window_ms": 0}, {"max_out_of_order_ms": -1}, {"grace_ms": -1}):
            with self.subTest(options=options):
                with self.assertRaises(ValueError):
                    dg.scripted_disorder(**options)

    def test_runner_sync_step_precedes_the_late_trade(self):
        schedule = fixture()
        kinds = [step["kind"] for step in schedule["steps"]]
        self.assertIn("sync", kinds)
        sync_index = kinds.index("sync")
        late_index = next(index for index, step in enumerate(schedule["steps"])
                          if step is schedule["late_trade"])
        self.assertLess(sync_index, late_index)
        # The pure expectation ignores the runner-only sync step.
        self.assertEqual(dg.policy_expectation(schedule, "GRACE"), dg.oracle_truth(schedule))


class PolicyExpectationTests(unittest.TestCase):
    def setUp(self):
        self.schedule = fixture()
        self.window = self.schedule["late_trade"]["window"]

    def test_grace_matches_complete_input_truth_and_drop_does_not(self):
        truth = dg.oracle_truth(self.schedule)
        self.assertEqual(dg.policy_expectation(self.schedule, "GRACE"), truth)
        self.assertNotEqual(dg.policy_expectation(self.schedule, "DROP"), truth)

    def test_late_trade_changes_the_closing_price_only_under_grace(self):
        drop = dg.policy_expectation(self.schedule, "DROP")
        grace = dg.policy_expectation(self.schedule, "GRACE")
        drop_ema = next(r for r in drop if r["type"] == "EMA" and r["window"] == self.window)
        grace_ema = next(r for r in grace if r["type"] == "EMA" and r["window"] == self.window)
        self.assertNotEqual(drop_ema["ema38"], grace_ema["ema38"])
        self.assertLess(drop_ema["ema38"], 1.0)
        self.assertGreater(grace_ema["ema38"], 10.0)

    def test_late_trade_creates_a_buy_crossover_only_under_grace(self):
        drop = advisory_set(dg.policy_expectation(self.schedule, "DROP"))
        grace = advisory_set(dg.policy_expectation(self.schedule, "GRACE"))
        truth = advisory_set(dg.oracle_truth(self.schedule))
        self.assertEqual(truth - drop, {(self.window, "BUY")})
        self.assertEqual(grace, truth)
        self.assertIn((self.schedule["base_ms"], "BUY"), grace)
        self.assertIn((self.schedule["base_ms"] + 30 * WINDOW, "SELL"), grace)

    def test_unknown_policy_is_rejected(self):
        with self.assertRaises(ValueError):
            dg.policy_expectation(self.schedule, "SIDE_OUTPUT")


class ClassifyTests(unittest.TestCase):
    def test_matching_records_have_no_differences(self):
        records = [adv("S", 0, "BUY"), ema("S", 0, 1.0, 100.0, 1000)]
        report = dg.classify(records, list(reversed(records)))
        self.assertTrue(report["match"])
        self.assertEqual(report["counts"], {"missing": 0, "extra": 0, "wrong": 0,
                                            "delayed": 0, "ema_incorrect": 0})

    def test_counts_missing_extra_wrong_and_delayed_advisories(self):
        expected = [adv("S", 0, "BUY"), adv("S", 10, "SELL"), adv("S", 20, "BUY")]
        actual = [adv("S", 0, "BUY"), adv("S", 10, "BUY"), adv("S", 30, "BUY"), adv("S", 40, "SELL")]
        report = dg.classify(expected, actual)
        self.assertFalse(report["match"])
        self.assertEqual(report["counts"]["missing"], 0)
        self.assertEqual(report["counts"]["wrong"], 1)
        self.assertEqual(report["counts"]["delayed"], 1)
        self.assertEqual(report["counts"]["extra"], 1)
        self.assertEqual(report["advisories"]["delayed"][0]["observed_window"], 30)
        self.assertEqual(report["advisories"]["wrong"][0]["expected"], "SELL")

    def test_missing_advisory_without_a_later_match_stays_missing(self):
        report = dg.classify([adv("S", 0, "SELL")], [adv("S", 0, "BUY")])
        self.assertEqual(report["counts"]["missing"], 0)
        self.assertEqual(report["counts"]["wrong"], 1)

    def test_ema_tolerance_and_incorrect_field(self):
        expected = [ema("S", 0, 1.0, 100.0)]
        self.assertTrue(dg.classify(expected, [ema("S", 0, 1.0000000005, 100.0)])["match"])
        report = dg.classify(expected, [ema("S", 0, 1.0000002, 100.0)])
        self.assertFalse(report["match"])
        self.assertEqual(report["counts"]["ema_incorrect"], 1)
        self.assertEqual(report["ema"]["incorrect"][0]["field"], "ema38")

    def test_rejects_invalid_records(self):
        with self.assertRaises(ValueError):
            dg.classify([{"symbol": "S", "window": 0, "type": "HOLD"}], [])


class NormalizeCommittedTests(unittest.TestCase):
    def test_merges_ema_pair_and_derives_window_from_close_timestamp(self):
        committed = [
            {"timestamp_emit": WINDOW - 1000, "ticker": "S", "type": "EMA100", "value": 0.77},
            {"timestamp_emit": WINDOW - 1000, "ticker": "S", "type": "EMA38", "value": 2.0},
            {"timestamp_emit": WINDOW - 1000, "ticker": "S", "type": "BUY", "value": 39.0},
            {"timestamp_emit": WINDOW - 1000, "ticker": "S", "type": "price", "value": 39.0},
        ]
        self.assertEqual(dg.normalize_committed(committed, WINDOW), [
            {"symbol": "S", "window": 0, "type": "EMA", "ema38": 2.0, "ema100": 0.77,
             "timestamp_ms": WINDOW - 1000},
            {"symbol": "S", "window": 0, "type": "BUY"},
        ])

    def test_rejects_an_unpaired_ema_record(self):
        with self.assertRaises(ValueError):
            dg.normalize_committed([{"timestamp_emit": 1000, "ticker": "S", "type": "EMA38", "value": 1.0}], WINDOW)


class EvaluateArmTests(unittest.TestCase):
    def setUp(self):
        self.schedule = fixture()
        self.window = self.schedule["late_trade"]["window"]

    def test_committed_matching_the_policy_has_no_defects(self):
        for policy in ("DROP", "GRACE"):
            with self.subTest(policy=policy):
                arm = dg.evaluate_arm(self.schedule, policy, dg.policy_expectation(self.schedule, policy))
                self.assertTrue(arm["passed"], arm)
                self.assertEqual(arm["defects"]["counts"]["missing"], 0)

    def test_expected_policy_difference_is_not_a_defect(self):
        arm = dg.evaluate_arm(self.schedule, "DROP", dg.policy_expectation(self.schedule, "DROP"))
        self.assertTrue(arm["passed"], arm)
        self.assertEqual(arm["vs_truth"]["counts"]["missing"], 1)
        self.assertEqual(arm["defects"]["counts"]["missing"], 0)
        differences = arm["expected_policy_differences"]
        self.assertEqual(len(differences), 1)
        self.assertEqual(differences[0]["type"], "BUY")
        self.assertTrue(differences[0]["in_truth"])
        self.assertFalse(differences[0]["in_expectation"])

    def test_a_processing_defect_is_reported(self):
        committed = [r for r in dg.policy_expectation(self.schedule, "GRACE")
                     if not (r["type"] == "BUY" and r["window"] == self.window)]
        arm = dg.evaluate_arm(self.schedule, "GRACE", committed)
        self.assertFalse(arm["passed"])
        self.assertEqual(arm["defects"]["counts"]["missing"], 1)

    def test_reports_policy_intrinsic_finalization_lag(self):
        self.assertEqual(dg.evaluate_arm(self.schedule, "DROP", [])["finalization_lag_ms"], 0)
        self.assertEqual(dg.evaluate_arm(self.schedule, "GRACE", [])["finalization_lag_ms"], GRACE)

    def test_reports_delay_state_and_sample_size(self):
        measurements = {
            "finalization_delay_ms": [1000, 2000, 3000],
            "publication_delay_ms": [10, 20, 30],
            "state": {"checkpoint_bytes": 123456, "checkpoint_ms": 45, "rocksdb_bytes": 999,
                      "heap_used_bytes": 777},
            "resources": {"cpu_pct": 12.5, "memory_bytes": 1000},
            "samples": 3,
        }
        committed = dg.policy_expectation(self.schedule, "GRACE")
        arm = dg.evaluate_arm(self.schedule, "GRACE", committed, measurements)
        self.assertEqual(arm["delay"]["finalization_ms"]["p50"], 2000)
        self.assertEqual(arm["delay"]["finalization_ms"]["count"], 3)
        self.assertEqual(arm["state"]["checkpoint_bytes"], 123456)
        self.assertEqual(arm["sample"]["committed"], len(committed))


class CompareArmsTests(unittest.TestCase):
    def arm(self, schedule, policy, delay=1000, checkpoint=100):
        return {"fixture": schedule, "policy": policy,
                "committed": dg.policy_expectation(schedule, policy),
                "measurements": {"finalization_delay_ms": [delay], "publication_delay_ms": [delay // 10],
                                 "state": {"checkpoint_bytes": checkpoint, "checkpoint_ms": 5,
                                           "rocksdb_bytes": checkpoint // 2, "heap_used_bytes": 10},
                                 "resources": {"cpu_pct": 5.0, "memory_bytes": 100},
                                 "samples": 1},
                "settings": {"dedup": "NONE", "parallelism": 2, "checkpoint_interval_ms": 1000}}

    def test_recommends_grace_when_it_recovers_a_late_trade(self):
        schedule = fixture()
        report = dg.compare_arms({"DROP": self.arm(schedule, "DROP", delay=1000),
                                  "GRACE": self.arm(schedule, "GRACE", delay=31000)})
        self.assertEqual(report["recommendation"]["default"], "GRACE")
        self.assertIn("late", report["recommendation"]["reason"].lower())
        self.assertEqual(report["matched"]["fixture_digest"], dg.fixture_digest(schedule))

    def test_recommendation_reason_cites_the_grace_cost(self):
        schedule = fixture()
        report = dg.compare_arms({"DROP": self.arm(schedule, "DROP"),
                                  "GRACE": self.arm(schedule, "GRACE")})
        self.assertIn("event-time finalization delay", report["recommendation"]["reason"])

    def test_recommends_drop_when_grace_only_adds_delay(self):
        schedule = fixture(late_price=0.01)
        report = dg.compare_arms({"DROP": self.arm(schedule, "DROP", delay=1000),
                                  "GRACE": self.arm(schedule, "GRACE", delay=31000)})
        self.assertEqual(report["recommendation"]["default"], "DROP")

    def test_rejects_arms_built_from_different_schedules(self):
        with self.assertRaises(ValueError):
            dg.compare_arms({"DROP": self.arm(fixture(seed=1), "DROP"),
                             "GRACE": self.arm(fixture(seed=2), "GRACE")})

    def test_rejects_arms_with_different_settings(self):
        schedule = fixture()
        drop = self.arm(schedule, "DROP")
        grace = self.arm(schedule, "GRACE")
        grace["settings"] = {"dedup": "EXACT", "parallelism": 2, "checkpoint_interval_ms": 1000}
        with self.assertRaises(ValueError):
            dg.compare_arms({"DROP": drop, "GRACE": grace})

    def test_requires_both_arms(self):
        schedule = fixture()
        with self.assertRaises(ValueError):
            dg.compare_arms({"DROP": self.arm(schedule, "DROP")})


if __name__ == "__main__":
    unittest.main()

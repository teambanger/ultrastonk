"""Tests for the full-week reconciliation tooling (ticket 06).

Pure functions are exercised against hand-built inputs; the whole-week run itself is an
evidence run performed by benchmarks/full_week.py, not part of the fast suite.
"""
import json
import tempfile
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

from benchmarks import full_week
from benchmarks import reconcile_replay

W1 = 1704067200000  # 2024-01-01T00:00:00Z -> 2024-01-01 01:00 Europe/Berlin
W2 = 1704153600000  # 2024-01-02T00:00:00Z -> 2024-01-02 01:00 Europe/Berlin


class ExchangeTests(unittest.TestCase):
    def test_exchange_is_the_uppercased_final_symbol_segment(self):
        self.assertEqual(full_week.exchange_of("RDSA.NL"), "NL")
        self.assertEqual(full_week.exchange_of("A2ASZ7.ETR"), "ETR")
        self.assertEqual(full_week.exchange_of("x.y.FR"), "FR")
        self.assertEqual(full_week.exchange_of("NOSUFFIX"), "UNKNOWN")
        self.assertEqual(full_week.exchange_of(""), "UNKNOWN")


class DayTests(unittest.TestCase):
    def test_window_day_uses_the_configured_event_time_zone(self):
        zone = ZoneInfo("Europe/Berlin")
        # 2023-12-31T23:00Z is already 2024-01-01 00:00 in Berlin.
        self.assertEqual(full_week.window_day(1704063600000, zone), "2024-01-01")


class CompareFullTests(unittest.TestCase):
    def write(self, path, records):
        Path(path).write_text("".join(json.dumps(record) + "\n" for record in records))

    def build(self):
        return [
            {"symbol": "A.NL", "window": W1, "type": "EMA", "ema38": 1.0, "ema100": 2.0},
            {"symbol": "B.FR", "window": W1, "type": "BUY"},
            {"symbol": "C.ETR", "window": W2, "type": "EMA", "ema38": 5.0, "ema100": 6.0},
            {"symbol": "D.NL", "window": W2, "type": "BUY"},
        ], [
            {"symbol": "A.NL", "window": W1, "type": "EMA", "ema38": 1.0, "ema100": 2.0},
            {"symbol": "A.NL", "window": W1, "type": "EMA", "ema38": 1.0, "ema100": 2.0},
            {"symbol": "B.FR", "window": W1, "type": "SELL"},
            {"symbol": "C.ETR", "window": W2, "type": "EMA", "ema38": 5.0000001, "ema100": 6.0},
            {"symbol": "D.NL", "window": W2, "type": "SELL"},
            {"symbol": "E.ETR", "window": W2, "type": "EMA", "ema38": 9.0, "ema100": 9.0},
        ]

    def test_counts_every_error_kind_and_groups_by_day_and_exchange(self):
        with tempfile.TemporaryDirectory() as tmp:
            expected, actual, db = (Path(tmp) / name for name in
                                    ("expected.jsonl", "actual.jsonl", "compare.sqlite"))
            exp, act = self.build()
            self.write(expected, exp)
            self.write(actual, act)
            report = full_week.compare_full(expected, actual, db, ZoneInfo("Europe/Berlin"))

        self.assertFalse(report["match"])
        # A wrong-direction pair is one missing expected advisory plus one extra actual
        # advisory (B.FR, D.NL); it is additionally counted once as wrong_direction.
        self.assertEqual(report["counts"], {"missing": 2, "extra": 3, "numeric": 1,
                                            "duplicate": 1, "wrong_direction": 2})
        self.assertEqual(report["expected_final_window"], W2)
        self.assertEqual(report["actual_final_window"], W2)
        self.assertEqual(report["per_day"]["2024-01-01"]["duplicate"], 1)
        self.assertEqual(report["per_day"]["2024-01-01"]["wrong_direction"], 1)
        self.assertEqual(report["per_day"]["2024-01-01"]["missing"], 1)
        self.assertEqual(report["per_day"]["2024-01-02"]["numeric"], 1)
        self.assertEqual(report["per_day"]["2024-01-02"]["extra"], 2)
        self.assertEqual(report["per_day"]["2024-01-02"]["wrong_direction"], 1)
        self.assertEqual(report["per_exchange"]["FR"]["wrong_direction"], 1)
        self.assertEqual(report["per_exchange"]["NL"]["duplicate"], 1)
        self.assertEqual(report["per_exchange"]["NL"]["wrong_direction"], 1)
        self.assertEqual(report["per_exchange"]["ETR"]["numeric"], 1)
        self.assertEqual(report["per_exchange"]["ETR"]["extra"], 1)

    def test_exact_reference_reports_a_clean_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            expected, actual, db = (Path(tmp) / name for name in
                                    ("expected.jsonl", "actual.jsonl", "compare.sqlite"))
            exp, _ = self.build()
            self.write(expected, exp)
            self.write(actual, exp)
            report = full_week.compare_full(expected, actual, db, ZoneInfo("Europe/Berlin"))
        self.assertTrue(report["match"])
        self.assertEqual(report["counts"], {"missing": 0, "extra": 0, "numeric": 0,
                                            "duplicate": 0, "wrong_direction": 0})


class MergeScansTests(unittest.TestCase):
    def scan(self, emitted, trades, exchanges):
        return {
            "csv": "x.csv",
            "rows_read": emitted + 10,
            "loader": {"emitted": emitted, "dropped_width": 0, "dropped_no_symbol": 0,
                       "dropped_no_time": 10, "dropped_timestamp": 0, "malformed_prices": 0,
                       "fallback_events": 0},
            "query": {"eligible_trades": trades, "eligible_symbols": 2,
                      "invalid_symbol_rows": 0, "nonpositive_or_missing_last_rows": emitted - trades},
            "reversals": {"events": 1, "eligible_trades": 1, "beyond_disorder_budget": 0,
                          "max_ms": 5, "eligible_max_ms": 5},
            "windows": {"count": 1, "final_nonempty": W1, "final_event": W1,
                        "counts": {str(W1): {"rows": emitted, "trades": trades}}},
            "exchanges": exchanges,
        }

    def test_merge_keeps_per_day_and_per_exchange_dispositions(self):
        day1 = self.scan(100, 40, {
            "NL": {"emitted": 60, "eligible_trades": 30, "invalid_symbol_rows": 0,
                   "nonpositive_or_missing_last_rows": 30,
                   "windows": {str(W1): {"rows": 60, "trades": 30}}},
            "ETR": {"emitted": 40, "eligible_trades": 10, "invalid_symbol_rows": 0,
                    "nonpositive_or_missing_last_rows": 30,
                    "windows": {str(W1): {"rows": 40, "trades": 10}}},
        })
        day2 = self.scan(50, 20, {
            "NL": {"emitted": 50, "eligible_trades": 20, "invalid_symbol_rows": 0,
                   "nonpositive_or_missing_last_rows": 30,
                   "windows": {str(W2): {"rows": 50, "trades": 20}}},
        })
        merged = full_week.merge_source_scans([
            {"day": "2021-11-08", "scan": day1},
            {"day": "2021-11-09", "scan": day2},
        ])
        self.assertEqual(merged["totals"]["rows_read"], 170)
        self.assertEqual(merged["totals"]["loader"]["emitted"], 150)
        self.assertEqual(merged["totals"]["query"]["eligible_trades"], 60)
        self.assertEqual(merged["per_day"]["2021-11-08"]["query"]["eligible_trades"], 40)
        self.assertEqual(merged["per_day"]["2021-11-09"]["query"]["eligible_trades"], 20)
        self.assertEqual(merged["per_exchange"]["NL"]["query"]["eligible_trades"], 50)
        self.assertEqual(merged["per_exchange"]["ETR"]["query"]["eligible_trades"], 10)
        self.assertEqual(merged["per_exchange"]["NL"]["windows"][str(W1)]["trades"], 30)


class ParseTests(unittest.TestCase):
    def test_day_from_filename_parses_the_provider_day_suffix(self):
        self.assertEqual(full_week.day_from_filename("debs2022-gc-trading-day-08-11-21.csv"),
                         "2021-11-08")
        self.assertEqual(full_week.day_from_filename("debs2022-gc-trading-day-14-11-21.csv"),
                         "2021-11-14")
        with self.assertRaises(ValueError):
            full_week.day_from_filename("whatever.csv")

    def test_emitter_log_parses_totals_dispositions_and_failures(self):
        log = "\n".join([
            "Replaying /data/debs2022-gc-trading-day-08-11-21.csv",
            "  debs2022-gc-trading-day-08-11-21.csv: read 53,994,554 -> emitted 53,990,001 "
            "(0.0084% dropped: 0 bad width, 12 no symbol, 4,541 no event time, 0 filtered out by "
            "none, 0 unparsable timestamp; 0 malformed prices nulled)",
            "Done: 53990001 submitted, 0 failed, 350000 ev/s, first paced time 1636329600000",
            "Total: read 289,116,808 -> emitted 289,049,752 (0.0232% dropped: 8 bad width, "
            "120 no symbol, 67,049 no event time, 0 filtered out by none, 7 unparsable timestamp; "
            "0 malformed prices nulled)",
        ])
        parsed = full_week.parse_emitter_log(log)
        self.assertEqual(parsed["total"], {"read": 289116808, "emitted": 289049752})
        self.assertEqual(parsed["done"], {"submitted": 53990001, "failed": 0})
        self.assertEqual(parsed["files"][0]["emitted"], 53990001)
        self.assertEqual(parsed["accounting"]["dropped_no_time"], 67049)
        self.assertEqual(parsed["accounting"]["dropped_width"], 8)
        self.assertEqual(parsed["accounting"]["dropped_timestamp"], 7)
        self.assertEqual(parsed["accounting"]["price_filter"], "none")

    def test_emitter_log_without_summary_is_an_error(self):
        with self.assertRaises(ValueError):
            full_week.parse_emitter_log("Replaying something\n")

    def test_classify_audit_separates_late_from_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "audit.jsonl"
            path.write_text("\n".join([
                json.dumps({"raw": "x", "reason": "window already finalized"}),
                json.dumps({"raw": "y", "reason": "invalid JSON"}),
                json.dumps({"raw": "z", "reason": "window already finalized"}),
            ]) + "\n")
            counts = full_week.classify_audit(path)
        self.assertEqual(counts, {"late": 2, "rejected": 1})


class GateTests(unittest.TestCase):
    def manifest(self):
        return {
            "mode": "capture-order",
            "source": {"rows_read": 100, "eligible_trades": 60, "final_window": W2},
            "emitter": {"emitted": 90, "acknowledged": 90, "failed": 0, "complete": True},
            "input": {"records": 90, "complete": True},
            "audit": {"late": 0, "rejected": 0, "complete": True},
            "output": {"prices": 60, "final_window": W2, "complete": True},
            "comparison": {"match": True, "counts": {"missing": 0, "extra": 0, "numeric": 0,
                                                     "duplicate": 0, "wrong_direction": 0}},
            "expected_final_window": W2,
            "errors": [],
        }

    def test_passes_when_accounting_coverage_and_zero_errors_hold(self):
        passed, checks = full_week.evaluate(self.manifest())
        self.assertTrue(passed, checks)
        self.assertTrue(all(checks.values()), checks)

    def test_fails_on_late_exclusion_missing_rows_direction_or_counts(self):
        cases = []
        m = self.manifest(); m["audit"]["late"] = 1; cases.append(m)
        m = self.manifest(); m["source"]["rows_read"] = 0; cases.append(m)
        m = self.manifest(); m["emitter"]["emitted"] = 80; cases.append(m)
        m = self.manifest(); m["input"]["records"] = 80; cases.append(m)
        m = self.manifest(); m["output"]["prices"] = 50; cases.append(m)
        m = self.manifest(); m["output"]["final_window"] = W1; cases.append(m)
        m = self.manifest(); m["comparison"]["counts"]["wrong_direction"] = 1
        m["comparison"]["match"] = False; cases.append(m)
        m = self.manifest(); m["comparison"]["counts"]["numeric"] = 1
        m["comparison"]["match"] = False; cases.append(m)
        m = self.manifest(); m["expected_final_window"] = None; cases.append(m)
        m = self.manifest(); m["mode"] = "sorted"; cases.append(m)
        m = self.manifest(); m["errors"] = ["boom"]; cases.append(m)
        for manifest in cases:
            with self.subTest(manifest=manifest):
                passed, checks = full_week.evaluate(manifest)
                self.assertFalse(passed, checks)


if __name__ == "__main__":
    unittest.main()

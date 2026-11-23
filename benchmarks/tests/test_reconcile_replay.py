import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

from benchmarks import reconcile_replay

HEADER = "".join(f"# header {i}\n" for i in range(13))


def row(symbol="A.X", sec="E", system_date="01-01-2024", system_time="00:00:01",
        last="39", trading_date="", trading_time="", width=40):
    cells = [""] * width
    if width > 0:
        cells[0] = symbol
    if width > 1:
        cells[1] = sec
    if width > 2:
        cells[2] = system_date
    if width > 3:
        cells[3] = system_time
    if width > 21:
        cells[21] = last
    if width > 23:
        cells[23] = trading_time
    if width > 26:
        cells[26] = trading_date
    return cells


def write_csv(path, rows, header=HEADER):
    with Path(path).open("w", newline="", encoding="utf-8") as stream:
        stream.write(header)
        csv.writer(stream).writerows(rows)


class ScanTests(unittest.TestCase):
    def test_scan_accounts_for_every_loader_and_query_disposition(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "slice.csv"
            write_csv(source, [
                row(system_time="00:00:01.123", last="39"),
                row(system_time="00:00:01.000", last="40"),
                row(system_time="00:00:00.500", last="41"),
                row(system_time="00:05:01", last=""),
                row(system_time="00:04:59", last="abc"),
                row(system_time="00:05:02", last="0"),
                row(symbol="B.X", system_time="00:00:01", last="50"),
                row(symbol="", sec="E"),
                row(sec=""),
                row(width=39),
                row(system_date="", system_time=""),
                row(system_date="bad", system_time="bad",
                    trading_date="bad", trading_time="bad"),
                row(trading_date="bad", trading_time="bad",
                    system_date="01-01-2024", system_time="00:10:00"),
                row(symbol="BAD", system_time="00:00:01", last="10"),
                row(system_time="00:11:00", last="-1"),
            ])
            result = reconcile_replay.scan_source(source, ZoneInfo("UTC"))

        self.assertEqual(result["rows_read"], 15)
        self.assertEqual(result["loader"]["emitted"], 10)
        self.assertEqual(result["loader"]["dropped_width"], 1)
        self.assertEqual(result["loader"]["dropped_no_symbol"], 2)
        self.assertEqual(result["loader"]["dropped_no_time"], 1)
        self.assertEqual(result["loader"]["dropped_timestamp"], 1)
        self.assertEqual(result["loader"]["malformed_prices"], 2)
        self.assertEqual(result["loader"]["fallback_events"], 1)
        self.assertEqual(result["query"]["eligible_trades"], 5)
        self.assertEqual(result["query"]["invalid_symbol_rows"], 1)
        self.assertEqual(result["query"]["nonpositive_or_missing_last_rows"], 4)
        self.assertEqual(result["reversals"]["events"], 3)
        self.assertEqual(result["reversals"]["eligible_trades"], 2)
        self.assertEqual(result["reversals"]["max_ms"], 2000)
        self.assertEqual(result["reversals"]["eligible_max_ms"], 623)
        self.assertEqual(result["windows"]["final_nonempty"], 1704067800000)
        self.assertEqual(result["windows"]["final_event"], 1704067800000)
        self.assertEqual(result["windows"]["counts"]["1704067200000"]["trades"], 4)
        self.assertEqual(result["windows"]["counts"]["1704067800000"]["trades"], 1)

    def test_reversal_risk_is_counted_against_the_disorder_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "slice.csv"
            write_csv(source, [
                row(system_time="00:00:10", last="10"),
                row(system_time="00:00:01", last="11"),
                row(system_time="00:00:09", last="12"),
            ])
            result = reconcile_replay.scan_source(source, ZoneInfo("UTC"), max_out_of_order_ms=5000)
        self.assertEqual(result["reversals"]["events"], 2)
        self.assertEqual(result["reversals"]["beyond_disorder_budget"], 1)
        self.assertEqual(result["reversals"]["max_ms"], 9000)

    def test_missing_or_bad_arguments_are_errors(self):
        with self.assertRaises(ValueError):
            reconcile_replay.scan_source("/nonexistent.csv", ZoneInfo("UTC"))


class SelectTests(unittest.TestCase):
    def test_selects_contiguous_capture_interval_and_keeps_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, output = Path(tmp) / "source.csv", Path(tmp) / "slice.csv"
            write_csv(source, [
                row(symbol="A.X", system_time="08:59:59"),
                row(symbol="B.X", system_time="09:00:00"),
                row(symbol="C.X", system_time="09:04:59"),
                row(symbol="D.X", system_time="09:05:00"),
                row(symbol="E.X", system_time="09:09:59"),
                row(symbol="F.X", system_time="09:10:00"),
                row(symbol="G.X", system_time="09:10:01"),
            ])
            result = reconcile_replay.select_capture_slice(
                source, output, "09:05:00", "09:10:00", ZoneInfo("UTC"))
            text = output.read_text()
        self.assertEqual(result["start_row"], 4)
        self.assertEqual(result["end_row"], 6)
        self.assertEqual(result["rows"], 2)
        self.assertTrue(text.startswith(HEADER))
        symbols = [line.split(",")[0] for line in text.splitlines()[13:]]
        self.assertEqual(symbols, ["D.X", "E.X"])

    def test_refuses_an_empty_or_reversed_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, output = Path(tmp) / "source.csv", Path(tmp) / "slice.csv"
            write_csv(source, [row(system_time="09:00:00")])
            with self.assertRaises(ValueError):
                reconcile_replay.select_capture_slice(
                    source, output, "10:00:00", "10:05:00", ZoneInfo("UTC"))
            with self.assertRaises(ValueError):
                reconcile_replay.select_capture_slice(
                    source, output, "10:05:00", "10:00:00", ZoneInfo("UTC"))


class ExtractTests(unittest.TestCase):
    def test_copies_a_one_based_data_row_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, output = Path(tmp) / "source.csv", Path(tmp) / "slice.csv"
            write_csv(source, [row(symbol=f"S{i}.X", system_time=f"00:00:0{i}") for i in range(5)])
            result = reconcile_replay.extract_rows(source, output, 2, 4)
            text = output.read_text()
        self.assertEqual(result["rows"], 2)
        self.assertTrue(text.startswith(HEADER))
        symbols = [line.split(",")[0] for line in text.splitlines()[13:]]
        self.assertEqual(symbols, ["S1.X", "S2.X"])

    def test_rejects_an_empty_or_out_of_range_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, output = Path(tmp) / "source.csv", Path(tmp) / "slice.csv"
            write_csv(source, [row(symbol="S0.X")])
            for start, end in ((0, 1), (2, 1), (1, 3)):
                with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                    reconcile_replay.extract_rows(source, output, start, end)


class SortTests(unittest.TestCase):
    def test_sort_is_stable_on_equal_event_time_and_preserves_multiset(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, output = Path(tmp) / "slice.csv", Path(tmp) / "sorted.csv"
            db = Path(tmp) / "sort.sqlite"
            write_csv(source, [
                row(symbol="B.X", system_time="00:00:02", last="2"),
                row(symbol="A.X", system_time="00:00:01", last="1"),
                row(symbol="A.X", system_time="00:00:02", last="3"),
                row(symbol="A.X", system_time="00:00:02", last="4"),
                row(width=39),
            ])
            result = reconcile_replay.sort_event_time(
                source, output, db, ZoneInfo("UTC"))
            original = reconcile_replay.scan_source(source, ZoneInfo("UTC"))
            sorted_scan = reconcile_replay.scan_source(output, ZoneInfo("UTC"))
            text = output.read_text()

        self.assertTrue(text.startswith(HEADER))
        ordered = [line.split(",")[0] + ":" + line.split(",")[21]
                   for line in text.splitlines()[13:]]
        self.assertEqual(ordered, ["A.X:1", "B.X:2", "A.X:3", "A.X:4"])
        self.assertEqual(result["emitted"], 4)
        self.assertEqual(result["dropped_width"], 1)
        self.assertGreater(result["scratch_bytes"], 0)
        self.assertEqual(sorted_scan["loader"]["emitted"], original["loader"]["emitted"])
        self.assertEqual(sorted_scan["query"]["eligible_trades"], original["query"]["eligible_trades"])
        self.assertEqual(sorted_scan["reversals"]["events"], 0)


class NormalizeTests(unittest.TestCase):
    def records(self):
        return [
            {"timestamp_emit": 1704067200000, "ticker": "A.X", "type": "price", "value": 39},
            {"timestamp_emit": 1704067200000, "ticker": "A.X", "type": "EMA38", "value": 2.0},
            {"timestamp_emit": 1704067200000, "ticker": "A.X", "type": "EMA100", "value": 0.7722772277227723},
            {"timestamp_emit": 1704067200000, "ticker": "A.X", "type": "BUY", "value": 39},
            {"timestamp_emit": 1704067500000, "ticker": "A.X", "type": "price", "value": 40},
            {"timestamp_emit": 1704067500000, "ticker": "A.X", "type": "EMA38", "value": 3.0},
            {"timestamp_emit": 1704067500000, "ticker": "A.X", "type": "EMA100", "value": 1.0},
            {"timestamp_emit": 1704067500000, "ticker": "A.X", "type": "SELL", "value": 40},
        ]

    def test_combines_ema_components_and_keeps_advisories(self):
        normalizer = reconcile_replay.OutputNormalizer()
        emitted = []
        for record in self.records():
            emitted.extend(normalizer.feed(record))
        emitted.extend(normalizer.finish())
        self.assertEqual(emitted[0]["type"], "EMA")
        self.assertEqual(emitted[0]["window"], 1704067200000)
        self.assertAlmostEqual(emitted[0]["ema38"], 2.0)
        self.assertEqual(emitted[1]["type"], "BUY")
        self.assertEqual(emitted[2]["type"], "EMA")
        self.assertEqual(emitted[3]["type"], "SELL")
        self.assertEqual(normalizer.counts["prices"], 2)
        self.assertEqual(normalizer.counts["ema_pairs"], 2)
        self.assertEqual(normalizer.counts["advisories"], 2)
        self.assertEqual(normalizer.counts["final_window"], 1704067500000)

    def test_duplicate_components_reach_the_comparator_as_duplicates(self):
        normalizer = reconcile_replay.OutputNormalizer()
        emitted = []
        for record in self.records() * 2:
            emitted.extend(normalizer.feed(record))
        emitted.extend(normalizer.finish())
        ema = [record for record in emitted if record["type"] == "EMA"]
        self.assertEqual(len(ema), 4)

    def test_incomplete_pairs_unknown_types_and_bad_fields_fail(self):
        for record in (
            {"timestamp_emit": 1704067200000, "ticker": "A.X", "type": "EMA38", "value": 1.0},
            {"timestamp_emit": 1704067200000, "ticker": "A.X", "type": "HOLD", "value": 1.0},
            {"timestamp_emit": -1, "ticker": "A.X", "type": "price", "value": 1.0},
            {"timestamp_emit": 1, "ticker": "", "type": "price", "value": 1.0},
            {"timestamp_emit": 1, "ticker": "A.X", "type": "price", "value": float("nan")},
            {"timestamp_emit": 1, "ticker": "A.X", "type": "EMA38", "value": -1.0},
            {"timestamp_emit": 1, "ticker": "A.X", "type": "price", "value": 1, "extra": 1},
        ):
            with self.subTest(record=record):
                normalizer = reconcile_replay.OutputNormalizer()
                with self.assertRaises(ValueError):
                    normalizer.feed(record)
                    normalizer.finish()


class CompareTests(unittest.TestCase):
    def write(self, path, records):
        Path(path).write_text("".join(json.dumps(record) + "\n" for record in records))

    def test_reports_all_mismatch_kinds_with_bounded_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            expected, actual, db = (Path(tmp) / name for name in
                                    ("expected.jsonl", "actual.jsonl", "compare.sqlite"))
            ema = {"symbol": "ABC", "window": 1000, "type": "EMA",
                   "ema38": 1.0, "ema100": 100.0, "timestamp_ms": 2000}
            wrong = ema | {"ema38": 2e-9, "ema100": 100.0000002}
            buy = {"symbol": "ABC", "window": 1000, "type": "BUY"}
            sell = {"symbol": "ABC", "window": 1000, "type": "SELL"}
            self.write(expected, [ema, buy, buy])
            self.write(actual, [wrong, sell, sell])
            report = reconcile_replay.compare_jsonl(expected, actual, db, sample_limit=2)
            self.assertTrue(db.exists())

        self.assertFalse(report["match"])
        self.assertEqual(report["counts"], {"missing": 1, "extra": 1, "incorrect": 1, "duplicates": 2})
        self.assertEqual([sample["type"] for sample in report["missing"]], ["BUY"])
        self.assertEqual([sample["type"] for sample in report["extra"]], ["SELL"])
        self.assertEqual(sorted(sample["field"] for sample in report["incorrect"]), ["ema100", "ema38"])
        self.assertEqual({sample["source"] for sample in report["duplicates"]}, {"expected", "actual"})

    def test_accepts_within_tolerance_and_rejects_invalid_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            expected, actual, db = (Path(tmp) / name for name in
                                    ("expected.jsonl", "actual.jsonl", "compare.sqlite"))
            self.write(expected, [{"symbol": "ABC", "window": 1000, "type": "EMA",
                                   "ema38": 1.0, "ema100": 100.0}])
            self.write(actual, [{"symbol": "ABC", "window": 1000, "type": "EMA",
                                 "ema38": 1.0000000005, "ema100": 100.00000005}])
            self.assertTrue(reconcile_replay.compare_jsonl(expected, actual, db)["match"])
            actual.write_text("{}\n")
            with self.assertRaisesRegex(ValueError, "actual.jsonl:1:"):
                reconcile_replay.compare_jsonl(expected, actual, db)


class GateTests(unittest.TestCase):
    def manifest(self):
        return {
            "mode": "event-time-ordered",
            "emitter": {"failed": 0, "emitted": 10, "acknowledged": 10, "complete": True},
            "input": {"records": 10, "complete": True},
            "audit": {"late": 0, "rejected": 0, "complete": True},
            "source": {"eligible_trades": 5},
            "output": {"prices": 5, "final_window": 600000, "complete": True},
            "expected_final_window": 600000,
            "comparison": {"match": True},
            "errors": [],
        }

    def test_passes_only_when_every_accounting_and_coverage_check_holds(self):
        passed, checks = reconcile_replay.evaluate(self.manifest())
        self.assertTrue(passed, checks)
        self.assertTrue(all(checks.values()), checks)

    def test_each_omission_or_error_fails_the_gate(self):
        queued = []
        manifest = self.manifest()
        manifest["emitter"]["failed"] = 1
        queued.append(manifest)
        manifest = self.manifest()
        manifest["emitter"]["acknowledged"] = 9
        queued.append(manifest)
        manifest = self.manifest()
        manifest["input"]["records"] = 9
        queued.append(manifest)
        manifest = self.manifest()
        manifest["input"]["complete"] = False
        queued.append(manifest)
        manifest = self.manifest()
        manifest["audit"]["late"] = 1
        queued.append(manifest)
        manifest = self.manifest()
        manifest["audit"]["rejected"] = 1
        queued.append(manifest)
        manifest = self.manifest()
        manifest["audit"]["complete"] = False
        queued.append(manifest)
        manifest = self.manifest()
        manifest["output"]["prices"] = 4
        queued.append(manifest)
        manifest = self.manifest()
        manifest["output"]["final_window"] = 300000
        queued.append(manifest)
        manifest = self.manifest()
        manifest["output"]["complete"] = False
        queued.append(manifest)
        manifest = self.manifest()
        manifest["comparison"] = {"match": False}
        queued.append(manifest)
        manifest = self.manifest()
        manifest["comparison"] = {"match": True, "error": "broken"}
        queued.append(manifest)
        manifest = self.manifest()
        manifest["expected_final_window"] = None
        queued.append(manifest)
        manifest = self.manifest()
        manifest["mode"] = "unsorted"
        queued.append(manifest)
        manifest = self.manifest()
        manifest["errors"] = ["boom"]
        queued.append(manifest)
        manifest = self.manifest()
        del manifest["emitter"]
        queued.append(manifest)

        for manifest in queued:
            with self.subTest(manifest=manifest):
                passed, checks = reconcile_replay.evaluate(manifest)
                self.assertFalse(passed, checks)


class CliTests(unittest.TestCase):
    def test_scan_and_compare_commands_expose_json_and_exit_codes(self):
        script = Path(__file__).resolve().parents[1] / "reconcile_replay.py"
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "slice.csv"
            write_csv(source, [row(system_time="00:00:01", last="39")])
            scan = subprocess.run([sys.executable, str(script), "scan", "--csv", str(source),
                                   "--timezone", "UTC"], capture_output=True, text=True)
            self.assertEqual(scan.returncode, 0, scan.stderr)
            self.assertEqual(json.loads(scan.stdout)["query"]["eligible_trades"], 1)
            expected, actual, db = (Path(tmp) / name for name in
                                    ("expected.jsonl", "actual.jsonl", "compare.sqlite"))
            expected.write_text(json.dumps({"symbol": "A.X", "window": 0, "type": "BUY"}) + "\n")
            actual.write_text(json.dumps({"symbol": "A.X", "window": 0, "type": "SELL"}) + "\n")
            compare = subprocess.run([sys.executable, str(script), "compare",
                                      "--expected", str(expected), "--actual", str(actual),
                                      "--db", str(db)], capture_output=True, text=True)
            self.assertEqual(compare.returncode, 1, compare.stderr)
            self.assertFalse(json.loads(compare.stdout)["match"])


if __name__ == "__main__":
    unittest.main()

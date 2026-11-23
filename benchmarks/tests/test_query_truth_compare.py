import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "query_truth_compare.py"


class QueryTruthCompareTests(unittest.TestCase):
    def run_compare(self, expected, actual, *extra):
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ("expected.jsonl", "actual.jsonl")]
            for path, records in zip(paths, (expected, actual)):
                path.write_text("".join(json.dumps(record) + "\n" for record in records))
            return subprocess.run([sys.executable, str(SCRIPT), *map(str, paths), *extra],
                                  capture_output=True, text=True)

    def test_matching_records_and_tolerances(self):
        expected = [{"symbol": "ABC", "window": 1000, "type": "EMA", "ema38": 1.0,
                     "ema100": 100.0, "timestamp_ms": 2000},
                    {"symbol": "ABC", "window": 1000, "type": "BUY"}]
        actual = [{"symbol": "ABC", "window": 1000, "type": "BUY"},
                  {"symbol": "ABC", "window": 1000, "type": "EMA", "ema38": 1.0000000005,
                   "ema100": 100.00000005, "timestamp_ms": 3000}]
        result = self.run_compare(expected, actual)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["match"])

    def test_reports_missing_extra_duplicates_and_incorrect_values(self):
        ema = {"symbol": "ABC", "window": 1000, "type": "EMA", "ema38": 0.0, "ema100": 100.0}
        buy = {"symbol": "ABC", "window": 1000, "type": "BUY"}
        sell = {"symbol": "ABC", "window": 1000, "type": "SELL"}
        incorrect = ema | {"ema38": 2e-9, "ema100": 100.0000002}
        result = self.run_compare([ema, buy, buy], [incorrect, incorrect, sell])
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        self.assertFalse(report["match"])
        self.assertEqual(report["missing"], [buy])
        self.assertEqual(report["extra"], [sell])
        self.assertEqual(report["duplicates"], {"expected": [buy], "actual": [
            {"symbol": "ABC", "window": 1000, "type": "EMA"}]})
        self.assertEqual([item["field"] for item in report["incorrect"]], ["ema38", "ema100"])

    def test_rejects_invalid_records(self):
        for invalid in [None, {}, {"symbol": "A", "window": True, "type": "BUY"},
                        {"symbol": "A", "window": 0, "type": "EMA", "ema38": float("nan"), "ema100": 1},
                        {"symbol": "A", "window": 0, "type": "EMA", "ema38": 1},
                        {"symbol": "A", "window": 0, "type": "HOLD"}]:
            with self.subTest(invalid=invalid):
                result = self.run_compare([], [invalid])
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("actual.jsonl:1:", result.stderr)

    def test_disk_backed_mode_reports_all_mismatch_kinds(self):
        ema = {"symbol": "ABC", "window": 1000, "type": "EMA", "ema38": 0.0, "ema100": 100.0}
        buy = {"symbol": "ABC", "window": 1000, "type": "BUY"}
        sell = {"symbol": "ABC", "window": 1000, "type": "SELL"}
        incorrect = ema | {"ema38": 2e-9, "ema100": 100.0000002}
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_compare([ema, buy, buy], [incorrect, incorrect, sell],
                                      "--db", str(Path(directory) / "compare.sqlite"))
            self.assertTrue((Path(directory) / "compare.sqlite").exists())
        self.assertEqual(result.returncode, 1, result.stderr)
        report = json.loads(result.stdout)
        self.assertFalse(report["match"])
        self.assertTrue(report["bounded"])
        self.assertEqual(report["counts"], {"missing": 1, "extra": 1, "incorrect": 1, "duplicates": 2})
        self.assertEqual(report["duplicates"]["expected"][0]["type"], "BUY")
        self.assertEqual(report["duplicates"]["actual"][0]["type"], "EMA")
        self.assertEqual(sorted(item["field"] for item in report["incorrect"]),
                         ["ema100", "ema38"])


if __name__ == "__main__":
    unittest.main()

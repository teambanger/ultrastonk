import csv
import datetime as dt
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "query_truth.py"


def row(symbol, time, price):
    cells = [""] * 40
    cells[0], cells[1] = symbol, "S"
    cells[2], cells[3] = "01-01-2024", time
    cells[21] = str(price)
    return cells


class OracleTest(unittest.TestCase):
    def test_millisecond_tie_uses_last_source_row_and_timestamp_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "input.csv"
            first = row("ABC.X", "00:00:01.123456789", 1)
            last = row("ABC.X", "00:00:01.123999999", 39)
            last[23], last[26] = "invalid", "01-01-2024"
            with source.open("w", newline="") as f:
                f.write("header\n" * 13)
                csv.writer(f).writerows([first, last])
            result = subprocess.run([sys.executable, str(SCRIPT), "--timezone", "UTC", str(source)],
                                    check=True, capture_output=True, text=True)
            records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(records[0]["timestamp_ms"], 1704067201123)
        self.assertEqual(records[0]["ema38"], 2.0)
        self.assertEqual(records[1]["type"], "BUY")

    def test_first_window_and_final_flush(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "input.csv"
            with source.open("w", newline="") as f:
                f.write("header\n" * 13)
                writer = csv.writer(f)
                writer.writerows([row("ABC.X", "00:00:01", 39), row("ABC.X", "00:04:59", 39)])
            result = subprocess.run([sys.executable, str(SCRIPT), "--timezone", "UTC", str(source)],
                                    check=True, capture_output=True, text=True)
            records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(records, [
            {"symbol": "ABC.X", "window": 1704067200000, "type": "EMA", "ema38": 2.0,
             "ema100": 0.7722772277227723, "timestamp_ms": 1704067499000},
            {"symbol": "ABC.X", "window": 1704067200000, "type": "BUY"},
        ])

    def test_crossovers_ties_empty_windows_and_symbol_isolation(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "input.csv"
            rows = []
            start = dt.datetime(2024, 1, 1)
            for i in range(33):
                time = (start + dt.timedelta(minutes=5 * i)).strftime("%H:%M:%S")
                rows.append(row("ABC.X", time, 39 if i == 0 else 0.01 if i < 32 else 1000))
            rows.extend([row("ABC.Y", "00:00:00", 39), row("ABC.X", "00:00:00", 39),
                         row("ABC.X", "00:10:00", 0), row("ABC.X", "03:00:00", "")])
            with source.open("w", newline="") as f:
                f.write("header\n" * 13)
                csv.writer(f).writerows(rows)
            result = subprocess.run([sys.executable, str(SCRIPT), "--timezone", "UTC", str(source)],
                                    check=True, capture_output=True, text=True)
            records = [json.loads(line) for line in result.stdout.splitlines()]
        signals = [(r["symbol"], (r["window"] - 1704067200000) // 300000, r["type"])
                   for r in records if r["type"] != "EMA"]
        self.assertEqual(signals, [("ABC.X", 0, "BUY"), ("ABC.X", 30, "SELL"),
                                   ("ABC.X", 32, "BUY"), ("ABC.Y", 0, "BUY")])
        ema = [r for r in records if r["symbol"] == "ABC.X" and r["type"] == "EMA"]
        self.assertEqual(len(ema), 33)
        self.assertAlmostEqual(ema[30]["ema38"], 0.42017254521453756, places=12)
        self.assertAlmostEqual(ema[30]["ema100"], 0.42833824515313035, places=12)
        self.assertAlmostEqual(ema[32]["ema38"], 51.66072071952578, places=12)
        self.assertAlmostEqual(ema[32]["ema100"], 20.213716610209374, places=12)


if __name__ == "__main__":
    unittest.main()

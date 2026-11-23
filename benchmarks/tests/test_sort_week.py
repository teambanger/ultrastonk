"""Tests for the event-time-ordered reference sorter (ticket 06)."""
import csv
import tempfile
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

from benchmarks import sort_week

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


def write_csv(path, rows):
    with Path(path).open("w", newline="", encoding="utf-8") as stream:
        stream.write(HEADER)
        csv.writer(stream).writerows(rows)


class SortWeekTests(unittest.TestCase):
    def test_sorts_by_event_time_then_source_ordinal_and_keeps_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "debs2022-gc-trading-day-08-11-21.csv"
            output_dir = Path(tmp) / "sorted"
            write_csv(source, [
                row(symbol="B.X", system_time="00:00:03", last="3"),
                row(symbol="A.X", system_time="00:00:01", last="1"),
                row(symbol="A.X", system_time="00:00:01", last="2"),
                row(symbol="C.X", system_time="00:00:02", last="4"),
                row(width=39),
                row(symbol="", sec="E"),
            ])
            result = sort_week.sort_week_file(source, output_dir, ZoneInfo("UTC"), tmp)

            text = (output_dir / source.name).read_text()
        self.assertTrue(text.startswith(HEADER))
        ordered = [(line.split(",")[0], line.split(",")[21])
                   for line in text.splitlines()[13:]]
        self.assertEqual(ordered, [("A.X", "1"), ("A.X", "2"), ("C.X", "4"), ("B.X", "3")])
        self.assertEqual(result["emitted"], 4)
        self.assertEqual(result["rows_read"], 6)

    def test_matches_the_oracle_eligibility_multiset(self):
        from benchmarks import query_truth
        from benchmarks import reconcile_replay
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "debs2022-gc-trading-day-08-11-21.csv"
            output_dir = Path(tmp) / "sorted"
            write_csv(source, [
                row(symbol="A.X", system_time="00:00:01", last="1"),
                row(system_time="00:00:02", last="0"),          # nonpositive price: loader emits
                row(symbol="", system_time="00:00:03", last="5"),  # no symbol: dropped
                row(symbol="B.X", system_time="00:00:04", last="6"),
            ])
            result = sort_week.sort_week_file(source, output_dir, ZoneInfo("UTC"), tmp)
            rescan = reconcile_replay.scan_source(output_dir / source.name, ZoneInfo("UTC"))
        self.assertEqual(result["emitted"], 3)
        self.assertEqual(rescan["reversals"]["events"], 0)
        self.assertEqual(rescan["loader"]["emitted"], 3)


if __name__ == "__main__":
    unittest.main()

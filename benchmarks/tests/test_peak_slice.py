import csv
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks import peak_slice


DATE = "09-11-2021"


def row(symbol, time, last="10.0", width=40):
    cells = [""] * width
    if width >= 4:
        cells[0] = symbol
        cells[1] = "E"
        cells[2] = DATE
        cells[3] = time
    if width >= 22:
        cells[21] = last
    return cells


def write_csv(path, rows):
    with Path(path).open("w", newline="") as target:
        target.write("header\n" * 13)
        csv.writer(target).writerows(rows)


class CaptureParsingTests(unittest.TestCase):
    def test_capture_ms_truncates_fraction_to_milliseconds(self):
        base = peak_slice.capture_ms(DATE, "15:58:01.000")
        self.assertEqual(peak_slice.capture_ms(DATE, "15:58:01.000499"), base)
        self.assertEqual(peak_slice.capture_ms(DATE, "15:58:01.000999"), base)
        self.assertEqual(peak_slice.capture_ms(DATE, "15:58:01.001"), base + 1)

    def test_capture_ms_accepts_absent_fraction(self):
        whole = peak_slice.capture_ms(DATE, "15:58:01")
        self.assertEqual(whole, peak_slice.capture_ms(DATE, "15:58:01.000"))

    def test_capture_ms_rejects_malformed_values(self):
        for date, time in [("bad", "15:58:01"), (DATE, "15:58"), (DATE, "15:58:01.abc")]:
            with self.assertRaises(ValueError):
                peak_slice.capture_ms(date, time)

    def test_exchange_of_uses_last_dot_suffix(self):
        self.assertEqual(peak_slice.exchange_of("A2ASZ7.ETR"), "ETR")
        self.assertEqual(peak_slice.exchange_of("A.B.FR"), "FR")
        self.assertIsNone(peak_slice.exchange_of("PLAIN"))
        self.assertIsNone(peak_slice.exchange_of(".FR"))

    def test_parse_iso_ms_matches_capture_ms(self):
        self.assertEqual(peak_slice.parse_iso_ms("2021-11-09T15:58:01.000000000"),
                         peak_slice.capture_ms(DATE, "15:58:01.000"))


class SliceWindowTests(unittest.TestCase):
    def test_warm_window_prepends_pre_roll_and_appends_trailing(self):
        start, end = 1_000, 2_000
        self.assertEqual(peak_slice.slice_window(start, end, 100, 300, "warm"), (900, 2300))

    def test_cold_window_drops_pre_roll(self):
        self.assertEqual(peak_slice.slice_window(1_000, 2_000, 100, 300, "cold"), (1_000, 2300))

    def test_rejects_bad_state_and_negative_spans(self):
        with self.assertRaises(ValueError):
            peak_slice.slice_window(1_000, 2_000, 0, 0, "tepid")
        with self.assertRaises(ValueError):
            peak_slice.slice_window(1_000, 2_000, -1, 0)
        with self.assertRaises(ValueError):
            peak_slice.slice_window(2_000, 1_000, 0, 0)


class EmitterOffsetTests(unittest.TestCase):
    def test_offsets_anchor_on_the_pre_roll_start(self):
        preroll, measure = peak_slice.emitter_offsets(900, 1_000, 2_000)
        self.assertEqual((preroll, measure), (100, 1_000))

    def test_offsets_start_at_zero_when_no_pre_roll_row_precedes_the_start(self):
        preroll, measure = peak_slice.emitter_offsets(1_050, 1_000, 2_000)
        self.assertEqual((preroll, measure), (0, 950))

    def test_segment_labels_are_half_open(self):
        anchor, preroll, measure = 900, 100, 1_000
        self.assertEqual(peak_slice.label_segment(900, anchor, preroll, measure), "PRE_ROLL")
        self.assertEqual(peak_slice.label_segment(999, anchor, preroll, measure), "PRE_ROLL")
        self.assertEqual(peak_slice.label_segment(1_000, anchor, preroll, measure), "MEASURED")
        self.assertEqual(peak_slice.label_segment(1_999, anchor, preroll, measure), "MEASURED")
        self.assertEqual(peak_slice.label_segment(2_000, anchor, preroll, measure), "TRAILING")


class ExtractSliceTests(unittest.TestCase):
    def fixture_rows(self):
        return [
            row("AAA.FR", "10:00:00.000"),
            row("BBB.NL", "10:00:05.000"),
            row("CCC.ETR", "10:00:10.000"),
            row("ZZZ.XX", "10:00:12.000"),           # exchange not FR/NL/ETR
            row("BAD", "10:00:13.000", width=13),    # wrong column count
            row("DDD.FR", "10:00:15.000"),
            row("EEE.NL", "10:00:20.000"),
            row("FFF.FR", "10:00:20.000"),
            row("GGG.FR", "10:00:30.000"),
            row("HHH.FR", "10:00:35.000"),           # at slice end (exclusive)
        ]

    def build(self):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        source = root / "day.csv"
        write_csv(source, self.fixture_rows())
        out = root / "slice.csv"
        start = peak_slice.capture_ms(DATE, "10:00:10.000")
        end = peak_slice.capture_ms(DATE, "10:00:20.000")
        manifest = peak_slice.extract_slice(source, out, start, end, pre_roll_ms=10_000,
                                            trailing_ms=15_000, state="warm")
        return temp, root, out, manifest

    def test_segments_and_physical_order_are_preserved(self):
        temp, root, out, manifest = self.build()
        self.addCleanup(temp.cleanup)
        self.assertEqual(manifest["segments"], {"PRE_ROLL": 2, "MEASURED": 2, "TRAILING": 3})
        self.assertEqual(manifest["measured_by_exchange"], {"FR": 1, "NL": 0, "ETR": 1})
        with out.open(newline="") as stream:
            rows = list(csv.reader(stream))
        self.assertEqual(len(rows), 13 + 7)
        symbols = [r[0] for r in rows[13:]]
        self.assertEqual(symbols, ["AAA.FR", "BBB.NL", "CCC.ETR", "DDD.FR", "EEE.NL", "FFF.FR", "GGG.FR"])

    def test_emitter_offsets_reproduce_the_labeled_measured_segment(self):
        temp, root, out, manifest = self.build()
        self.addCleanup(temp.cleanup)
        self.assertEqual(manifest["emitter"]["PACING"], "capture")
        self.assertEqual(manifest["emitter"]["REPLAY_PREROLL_MS"], 10_000)
        self.assertEqual(manifest["emitter"]["REPLAY_MEASURE_MS"], 10_000)
        self.assertEqual(manifest["labeled_measured"], manifest["segments"]["MEASURED"])

    def test_boundary_row_at_slice_end_is_excluded(self):
        temp, root, out, manifest = self.build()
        self.addCleanup(temp.cleanup)
        with out.open(newline="") as stream:
            rows = list(csv.reader(stream))
        self.assertNotIn("HHH.FR", [r[0] for r in rows[13:]])

    def test_cold_state_offers_the_peak_without_pre_roll(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        source = root / "day.csv"
        write_csv(source, self.fixture_rows())
        start = peak_slice.capture_ms(DATE, "10:00:10.000")
        end = peak_slice.capture_ms(DATE, "10:00:20.000")
        manifest = peak_slice.extract_slice(source, root / "cold.csv", start, end,
                                            pre_roll_ms=10_000, trailing_ms=15_000, state="cold")
        self.assertEqual(manifest["segments"]["PRE_ROLL"], 0)
        self.assertEqual(manifest["segments"]["MEASURED"], 2)
        self.assertEqual(manifest["emitter"]["REPLAY_PREROLL_MS"], 0)
        self.assertEqual(manifest["emitter"]["REPLAY_MEASURE_MS"], 10_000)

    def test_empty_measured_segment_is_rejected(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        source = root / "day.csv"
        write_csv(source, self.fixture_rows())
        start = peak_slice.capture_ms(DATE, "11:00:00.000")
        end = peak_slice.capture_ms(DATE, "11:00:10.000")
        with self.assertRaises(ValueError):
            peak_slice.extract_slice(source, root / "empty.csv", start, end)


class VerifyFixtureTests(unittest.TestCase):
    def manifest(self):
        return {
            "segments": {"PRE_ROLL": 2, "MEASURED": 2, "TRAILING": 3},
            "measured_by_exchange": {"FR": 1, "NL": 0, "ETR": 1},
            "first_measured_ms": peak_slice.capture_ms(DATE, "10:00:10.000"),
            "last_measured_ms": peak_slice.capture_ms(DATE, "10:00:15.000"),
        }

    def fixture(self):
        return {
            "raw_peak": {"start": "2021-11-09T10:00:10.000000000",
                         "end_exclusive": "2021-11-09T10:00:20.000000000", "rows": 2},
            "counts_by_exchange": {"FR": 1, "NL": 0, "ETR": 1},
            "latest": {"timestamp": "2021-11-09T10:00:15.000000000"},
        }

    def test_matching_manifest_verifies(self):
        verdict = peak_slice.verify_against_fixture(self.manifest(), self.fixture())
        self.assertTrue(verdict["verified"], verdict)

    def test_row_count_mismatch_fails(self):
        manifest = self.manifest()
        manifest["segments"]["MEASURED"] = 99
        verdict = peak_slice.verify_against_fixture(manifest, self.fixture())
        self.assertFalse(verdict["verified"])
        self.assertTrue(any("measured rows" in problem for problem in verdict["problems"]))

    def test_exchange_mismatch_fails(self):
        manifest = self.manifest()
        manifest["measured_by_exchange"]["ETR"] = 7
        verdict = peak_slice.verify_against_fixture(manifest, self.fixture())
        self.assertFalse(verdict["verified"])
        self.assertTrue(any("ETR" in problem for problem in verdict["problems"]))

    def test_last_measured_mismatch_fails(self):
        manifest = self.manifest()
        manifest["last_measured_ms"] += 1
        verdict = peak_slice.verify_against_fixture(manifest, self.fixture())
        self.assertFalse(verdict["verified"])
        self.assertTrue(any("last measured" in problem for problem in verdict["problems"]))


class LoadFixtureTests(unittest.TestCase):
    def test_load_fixture_parses_peak_bounds(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "fixture.json"
            path.write_text(json.dumps({
                "raw_peak": {"start": "2021-11-09T15:58:01.000000000",
                             "end_exclusive": "2021-11-09T16:03:01.000000000", "rows": 1}}))
            fixture, start, end = peak_slice.load_fixture(path)
            self.assertEqual(end - start, 300_000)
            self.assertEqual(start, peak_slice.capture_ms(DATE, "15:58:01.000"))

    def test_load_fixture_rejects_missing_peak(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "fixture.json"
            path.write_text(json.dumps({}))
            with self.assertRaises(ValueError):
                peak_slice.load_fixture(path)


if __name__ == "__main__":
    unittest.main()

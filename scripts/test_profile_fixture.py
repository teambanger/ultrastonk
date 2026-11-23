import csv
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("profile_fixture.py")


class ProfileFixtureTest(unittest.TestCase):
    def test_rejects_source_that_changed_after_profile(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "data.csv").write_text("header\n" * 13)
            (root / "manifest.json").write_text(json.dumps({"files": [{"key": "data.csv"}]}))
            peak = {"start": "2021-11-08T10:00:00.000000000",
                    "end_exclusive": "2021-11-08T10:05:00.000000000", "rows": 1}
            (root / "profile.json").write_text(json.dumps({"raw_peak": peak,
                "manifest_sha256": hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest(),
                "files": [{"key": "data.csv", "sha256": "0" * 64}]}))
            run = subprocess.run([sys.executable, str(SCRIPT), "--profile", str(root / "profile.json"),
                "--manifest", str(root / "manifest.json"), "--data-dir", str(root),
                "--output", str(root / "fixture.json")], capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)
            self.assertIn("source checksum", run.stderr)

    def test_selects_half_open_peak_in_manifest_order(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = []
            for symbol, time in [("A.FR", "09:59:59"), ("B.NL", "10:00:00"),
                                 ("C.ETR", "10:04:59.999999999"), ("D.FR", "10:05:00")]:
                row = [""] * 40
                row[0:4] = [symbol, "E", "08-11-2021", time]
                rows.append(row)
            with (root / "data.csv").open("w", newline="") as target:
                target.write("header\n" * 13)
                csv.writer(target).writerows(rows)
            (root / "manifest.json").write_text(json.dumps({"files": [{"key": "data.csv"}]}))
            peak = {"start": "2021-11-08T10:00:00.000000000",
                    "end_exclusive": "2021-11-08T10:05:00.000000000", "rows": 2}
            manifest_hash = hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
            (root / "profile.json").write_text(json.dumps({"raw_peak": peak, "manifest_sha256": manifest_hash,
                "files": [{"key": "data.csv", "sha256": hashlib.sha256((root / "data.csv").read_bytes()).hexdigest()}]}))
            output = root / "fixture.json"
            subprocess.run([sys.executable, str(SCRIPT), "--profile", str(root / "profile.json"),
                            "--manifest", str(root / "manifest.json"), "--data-dir", str(root),
                            "--output", str(output)], check=True)
            result = json.loads(output.read_text())
            self.assertEqual(result["raw_peak"], peak)
            self.assertEqual(result["manifest_sha256"], manifest_hash)
            self.assertEqual(result["profile_sha256"], hashlib.sha256((root / "profile.json").read_bytes()).hexdigest())
            self.assertEqual(result["counts_by_exchange"], {"FR": 0, "NL": 1, "ETR": 1})
            self.assertEqual(result["earliest"]["source"], {"file": "data.csv", "row": 2, "ordinal": 2})
            self.assertEqual(result["latest"]["source"], {"file": "data.csv", "row": 3, "ordinal": 3})
            self.assertEqual(result["pre_roll"], {"timestamp": "2021-11-08T09:59:59.000000000", "rows": 1})
            self.assertEqual(result["trailing"], {"timestamp": "2021-11-08T10:05:00.000000000", "rows": 1})
            self.assertEqual(result["context_available"], {
                "pre_roll_rows": 1, "earliest_pre_roll": "2021-11-08T09:59:59.000000000",
                "trailing_rows": 1, "latest_trailing": "2021-11-08T10:05:00.000000000"})

    def test_disorder_exclusions_and_cross_file_ordinals(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            def row(symbol, time):
                fields = [""] * 40
                fields[0:4] = [symbol, "E", "08-11-2021", time]
                return fields
            files = [
                [row("A.FR", "10:00:02"), row("B.US", "10:00:00"),
                 ["short"], row("A.FR", "invalid"), row("A.FR", "09:59:59")],
                [row("B.NL", "10:00:00"), row("C.ETR", "10:00:02"),
                 row("D.FR", "10:00:02"), row("A.NL", "09:59:59"), row("A.FR", "10:0:00")],
            ]
            for index, rows in enumerate(files):
                with (root / f"{index}.csv").open("w", newline="") as target:
                    target.write("header\n" * 13)
                    csv.writer(target).writerows(rows)
            (root / "manifest.json").write_text(json.dumps({"files": [{"key": "0.csv"}, {"key": "1.csv"}]}))
            peak = {"start": "2021-11-08T10:00:00.000000000",
                    "end_exclusive": "2021-11-08T10:05:00.000000000", "rows": 4}
            profile = {"raw_peak": peak, "manifest_sha256": hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest(),
                       "files": [{"key": f"{i}.csv", "sha256": hashlib.sha256((root / f"{i}.csv").read_bytes()).hexdigest()} for i in range(2)]}
            (root / "profile.json").write_text(json.dumps(profile))
            output = root / "fixture.json"
            command = [sys.executable, str(SCRIPT), "--profile", str(root / "profile.json"),
                       "--manifest", str(root / "manifest.json"), "--data-dir", str(root), "--output", str(output)]
            subprocess.run(command, check=True)
            result = json.loads(output.read_text())
            self.assertEqual(result["counts_by_exchange"], {"FR": 2, "NL": 1, "ETR": 1})
            self.assertEqual(result["earliest"]["source"], {"file": "1.csv", "row": 1, "ordinal": 6})
            self.assertEqual(result["latest"]["source"], {"file": "0.csv", "row": 1, "ordinal": 1})
            self.assertEqual(result["capture_order"], {"adjacent_pairs": 3, "decreases": 1,
                "equal_timestamps": 1, "max_forward_gap_ns": 2000000000, "max_backward_gap_ns": 2000000000})
            self.assertEqual(result["timestamp_order"], {"adjacent_pairs": 3, "equal_timestamps": 2,
                "max_gap_ns": 2000000000, "tie_order": "source ordinal"})
            self.assertEqual(result["pre_roll"]["rows"], 2)
            self.assertIsNone(result["trailing"])
            peak["rows"] = 5
            (root / "profile.json").write_text(json.dumps(profile))
            output.unlink()
            run = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)
            self.assertIn("do not match", run.stderr)
            self.assertFalse(output.exists())

    def test_rejects_profile_without_provenance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "manifest.json").write_text(json.dumps({"files": []}))
            (root / "profile.json").write_text(json.dumps({"raw_peak": {"start": "2021-11-08T10:00:00.000000000",
                "end_exclusive": "2021-11-08T10:05:00.000000000", "rows": 1}}))
            run = subprocess.run([sys.executable, str(SCRIPT), "--profile", str(root / "profile.json"),
                "--manifest", str(root / "manifest.json"), "--data-dir", str(root),
                "--output", str(root / "fixture.json")], capture_output=True, text=True)
            self.assertIn("profile manifest checksum", run.stderr)


if __name__ == "__main__":
    unittest.main()

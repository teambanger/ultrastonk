import hashlib
import json
import csv
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("profile_week.py")


class ProfileWeekTest(unittest.TestCase):
    def test_verified_manifest_records_provider_and_local_hashes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data = b"header\n" * 13
            (root / "sample.csv").write_bytes(data)
            manifest = {"doi": "test", "license": "test-license", "files": [{
                "key": "sample.csv", "size": len(data),
                "checksum": "md5:" + hashlib.md5(data).hexdigest(),
            }]}
            (root / "manifest.json").write_text(json.dumps(manifest))
            output = root / "output.json"
            subprocess.run([sys.executable, str(SCRIPT), "--manifest", str(root / "manifest.json"),
                            "--data-dir", str(root), "--output", str(output)], check=True)
            result = json.loads(output.read_text())
            self.assertEqual(result["files"][0]["sha256"], hashlib.sha256(data).hexdigest())
            self.assertEqual(result["files"][0]["rows"], 0)
            self.assertEqual(result["license"], "test-license")

    def test_provider_mismatch_is_rejected_without_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "sample.csv").write_bytes(b"different")
            (root / "manifest.json").write_text(json.dumps({"doi": "test", "license": "test", "files": [
                {"key": "sample.csv", "size": 9, "checksum": "md5:" + "0" * 32}]}))
            output = root / "output.json"
            run = subprocess.run([sys.executable, str(SCRIPT), "--manifest", str(root / "manifest.json"),
                                  "--data-dir", str(root), "--output", str(output)], capture_output=True)
            self.assertNotEqual(run.returncode, 0)
            self.assertFalse(output.exists())

    def test_capture_peak_and_row_categories(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "sample.csv"
            def row(symbol, time, last):
                fields = [""] * 40
                fields[0:4] = [symbol, "E", "08-11-2021", time]
                fields[21] = last
                return fields
            with path.open("w", newline="") as target:
                target.write("header\n" * 13)
                writer = csv.writer(target)
                writer.writerows([row("ABC.FR", "10:00:00.000", "1"),
                                  row("XYZ.NL", "10:04:59.999", ""),
                                  row("ABC.FR", "10:05:00.000", "2")])
            data = path.read_bytes()
            manifest = {"doi": "test", "license": "test", "files": [{"key": path.name,
                "size": len(data), "checksum": "md5:" + hashlib.md5(data).hexdigest()}]}
            (root / "manifest.json").write_text(json.dumps(manifest))
            output = root / "output.json"
            subprocess.run([sys.executable, str(SCRIPT), "--manifest", str(root / "manifest.json"),
                            "--data-dir", str(root), "--output", str(output)], check=True)
            result = json.loads(output.read_text())
            self.assertEqual(result["row_categories"], {"trade": 2, "non_trade": 1})
            self.assertEqual(result["exchanges"], {"FR": 2, "NL": 1})
            self.assertEqual(result["raw_peak"]["rows"], 2)
            self.assertEqual(result["raw_peak"]["start"], "2021-11-08T10:00:00.000000000")
            self.assertEqual(result["security_types"], {"E": 3})
            self.assertEqual(result["field_presence"]["last"], 2)
            self.assertEqual(result["symbol_ranks"][0], {"symbol": "ABC.FR", "rows": 2, "share": 2 / 3})
            self.assertEqual(result["one_second_peak"]["rows"], 1)

    def test_trade_peak_starts_at_first_trade_not_earlier_quote(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "sample.csv"
            rows = []
            for time, price in [("10:00:00", ""), ("10:01:00", "2")]:
                row = [""] * 40
                row[0:4] = ["ABC.FR", "E", "08-11-2021", time]
                row[21] = price
                rows.append(row)
            with path.open("w", newline="") as target:
                target.write("header\n" * 13)
                csv.writer(target).writerows(rows)
            data = path.read_bytes()
            (root / "manifest.json").write_text(json.dumps({"doi": "test", "license": "test", "files": [
                {"key": path.name, "size": len(data), "checksum": "md5:" + hashlib.md5(data).hexdigest()}]}))
            output = root / "output.json"
            subprocess.run([sys.executable, str(SCRIPT), "--manifest", str(root / "manifest.json"),
                            "--data-dir", str(root), "--output", str(output)], check=True)
            self.assertEqual(json.loads(output.read_text())["trade_peak"]["start"], "2021-11-08T10:01:00.000000000")

    def test_capture_exclusions_and_exchange_scope_are_reported(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "sample.csv"
            def row(symbol, time):
                fields = [""] * 40
                fields[0:4] = [symbol, "E", "08-11-2021", time]
                fields[21] = "3"
                return fields
            with path.open("w", newline="") as target:
                target.write("header\n" * 13)
                csv.writer(target).writerows([row("A.FR", "10:00:00"), row("A.FR", ""),
                                              row("A.US", "10:00:00")])
            data = path.read_bytes()
            (root / "manifest.json").write_text(json.dumps({"doi": "test", "license": "test", "files": [
                {"key": path.name, "size": len(data), "checksum": "md5:" + hashlib.md5(data).hexdigest()}]}))
            output = root / "output.json"
            subprocess.run([sys.executable, str(SCRIPT), "--manifest", str(root / "manifest.json"),
                            "--data-dir", str(root), "--output", str(output)], check=True)
            result = json.loads(output.read_text())
            self.assertEqual(result["capture_dispositions"], {"included": 1, "invalid_capture_time": 1, "other_exchange": 1})
            self.assertEqual(result["raw_peak"]["rows"], 1)


if __name__ == "__main__":
    unittest.main()

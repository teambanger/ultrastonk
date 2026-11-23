import csv
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class ProfileRatesTest(unittest.TestCase):
    def test_output_cannot_overwrite_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "day.csv"
            source.write_bytes(b"header\n" * 13)
            data = source.read_bytes()
            (root / "manifest.json").write_text(json.dumps({"doi": "test", "license": "test", "files": [{
                "key": source.name, "size": len(data), "checksum": "md5:" + hashlib.md5(data).hexdigest()}]}))
            script = Path(__file__).with_name("profile_week.py")
            result = subprocess.run([sys.executable, str(script), "--manifest", str(root / "manifest.json"),
                "--data-dir", str(root), "--output", str(source)], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(source.read_bytes(), data)

    def test_sparse_second_series_and_rolling_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "day.csv"
            with source.open("w", newline="") as f:
                f.write("header\n" * 13)
                writer = csv.writer(f)
                for timestamp, price in [("10:00:00", "2"), ("10:00:00.5", ""), ("10:05:00", "3")]:
                    row = [""] * 40
                    row[0:4] = ["ABC.FR", "E", "08-11-2021", timestamp]
                    row[21] = price
                    writer.writerow(row)
                unqualified = [""] * 40
                unqualified[0:4] = ["FR", "E", "08-11-2021", "10:00:00"]
                writer.writerow(unqualified)
            data = source.read_bytes()
            manifest = {"doi": "test", "license": "test", "files": [{"key": source.name,
                "size": len(data), "checksum": "md5:" + hashlib.md5(data).hexdigest()}]}
            (root / "manifest.json").write_text(json.dumps(manifest))
            output, series = root / "output.json", root / "rates.jsonl"
            script = Path(__file__).with_name("profile_week.py")
            subprocess.run([sys.executable, str(script), "--manifest", str(root / "manifest.json"),
                            "--data-dir", str(root), "--output", str(output), "--series", str(series)], check=True)
            rows = [json.loads(line) for line in series.read_text().splitlines()]
            self.assertEqual([(r["raw"], r["trade"], r["rolling_raw"], r["rolling_trade"]) for r in rows],
                             [(2, 1, 2, 1), (1, 1, 1, 1)])
            profile = json.loads(output.read_text())
            self.assertEqual(profile["series_sha256"], hashlib.sha256(series.read_bytes()).hexdigest())
            self.assertEqual(profile["files"][0]["row_categories"], {"trade": 2, "non_trade": 2})
            self.assertEqual(profile["capture_dispositions"]["other_exchange"], 1)


if __name__ == "__main__":
    unittest.main()

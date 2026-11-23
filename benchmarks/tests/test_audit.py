import csv
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from benchmarks import matrix, verify


BENCHMARKS = Path(__file__).resolve().parents[1]


class MatrixSafetyTests(unittest.TestCase):
    def test_existing_explicit_project_is_not_built_stopped_or_removed(self):
        for resource in ("ps", "network", "volume"):
            with self.subTest(resource=resource):
                commands = []

                def docker(command, **kwargs):
                    commands.append(command)
                    if "config" in command:
                        output = '{}'
                    elif command[1] == resource:
                        output = 'existing\n'
                    else:
                        output = ''
                    return subprocess.CompletedProcess(command, 0, stdout=output)

                with patch.object(sys, "argv", ["matrix.py", "--project", "bench-test-audit"]), \
                        patch("subprocess.run", side_effect=docker), \
                        self.assertRaisesRegex(SystemExit, "already has Docker resources"):
                    matrix.main()
                self.assertEqual([command[1:2] for command in commands if command[1] == "compose"], [["compose"]])
                self.assertIn("config", commands[0])
                self.assertIn("label=com.docker.compose.project=bench-test-audit", commands[-1])


class VerifyTests(unittest.TestCase):
    def test_application_and_cross_project_topics_are_refused(self):
        env = dict(COMPOSE_PROJECT_NAME="ultrastonk-verify-ab12", KAFKA_INPUT_TOPIC="verify-ab12-in",
                   KAFKA_OUTPUT_TOPIC="verify-ab12-out", KAFKA_REJECTED_TOPIC="verify-ab12-audit")
        self.assertEqual(verify.isolated_topics(env), ("verify-ab12-in", "verify-ab12-out", "verify-ab12-audit"))
        for key, invalid in [("COMPOSE_PROJECT_NAME", "ultrastonk"), ("KAFKA_INPUT_TOPIC", "price-events"),
                             ("KAFKA_OUTPUT_TOPIC", "verify-other-out"), ("KAFKA_REJECTED_TOPIC", "verify-ab12-out")]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                verify.isolated_topics(env | {key: invalid})

    def test_marker_covers_large_out_of_order_and_grace(self):
        # The window ending at t=2000 fires at watermark 17999 with 16000 ms grace.
        # With 11000 ms disorder, that watermark needs an event at t=29000.
        self.assertEqual(verify.flush_timestamp(2000, 11000, 16000), 29000)


class ReportTests(unittest.TestCase):
    def test_failed_runs_are_counted_but_do_not_contribute_measurements(self):
        with tempfile.TemporaryDirectory() as temp:
            matrix_csv = Path(temp) / "matrix.csv"
            with matrix_csv.open("w") as stream:
                rows = csv.writer(stream)
                rows.writerow(["quality", "dedup", "late", "parallelism", "target_rate", "exit_code",
                               "price_correctness_passed", "committed_prices_per_second", "latency_p99_ms"])
                for code, correct, rate, latency in [(0, True, 50, 10), (0, True, 70, 20),
                                                     (1, True, 900, 90), (0, False, 800, 80)]:
                    rows.writerow(["mixed", "NONE", "DROP", 1, 100, code, correct, rate, latency])
                rows.writerow(["mixed", "NONE", "DROP", 1, 200, 1, False, 999, 999])
            result = subprocess.run([sys.executable, str(BENCHMARKS / "report.py"), str(matrix_csv)],
                                    text=True, capture_output=True, check=True)
            self.assertIn("| 100 | 2/4 | 60.00 | 14.14 | 15.00 |", result.stdout)
            self.assertIn("| 200 | 0/1 | n/a | n/a | n/a |", result.stdout)


if __name__ == "__main__":
    unittest.main()

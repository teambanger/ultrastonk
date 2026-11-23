"""Tests for the read-only stack dashboard.

Run either way:

    python3 scripts/test_stack_tui.py
    python3 -m unittest scripts.test_stack_tui -v
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import stack_tui


COMPLETE_LOG = """\
Picked up JAVA_TOOL_OPTIONS: -XX:MaxRAMPercentage=75.0
Replaying [/data/debs2022-gc-trading-day-08-11-21.csv, /data/debs2022-gc-trading-day-09-11-21.csv] -> topic 'demo-in' via kafka:9092 at 1000.00x (event zone Europe/Berlin, price filter none, pacing EVENT)
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
Replaying /data/debs2022-gc-trading-day-08-11-21.csv
... 1000000 events sent (500000 ev/s, 2.0 s elapsed)
... 171000000 events sent (539875 ev/s, 316.7 s elapsed)
  debs2022-gc-trading-day-08-11-21.csv: read 53,994,554 -> emitted 53,982,555 (0.0222% dropped: 0 bad width, 0 no symbol, 12,000 no event time, 0 filtered out by none, 1 unparsable timestamp; 0 malformed prices nulled)
Replaying /data/debs2022-gc-trading-day-09-11-21.csv
... 289000000 events sent (546629 ev/s, 528.7 s elapsed)
  debs2022-gc-trading-day-09-11-21.csv: read 235,122,254 -> emitted 235,067,197 (0.0234% dropped: 0 bad width, 0 no symbol, 55,000 no event time, 0 filtered out by none, 6 unparsable timestamp; 0 malformed prices nulled)
Done: 289049752 submitted, 0 failed, 462406 ev/s, first paced time 1636329667000
Total: read 289,116,808 -> emitted 289,049,752 (0.0232% dropped: 0 bad width, 0 no symbol, 67,049 no event time, 0 filtered out by none, 7 unparsable timestamp; 0 malformed prices nulled)
"""

MISSING_TOTAL_LOG = """\
Replaying [/data/a.csv, /data/b.csv] -> topic 'demo-in' via kafka:9092 at 1000.00x (event zone Europe/Berlin, price filter none, pacing EVENT)
Replaying /data/a.csv
... 100 events sent (50 ev/s, 2.0 s elapsed)
  a.csv: read 1,000 -> emitted 999 (0.1000% dropped: 0 bad width, 0 no symbol, 1 no event time, 0 filtered out by none, 0 unparsable timestamp; 0 malformed prices nulled)
Replaying /data/b.csv
... 2,000 events sent (100 ev/s, 20.0 s elapsed)
  b.csv: read 2,000 -> emitted 1,998 (0.1000% dropped: 0 bad width, 0 no symbol, 2 no event time, 0 filtered out by none, 0 unparsable timestamp; 0 malformed prices nulled)
Done: 2,000 submitted, 0 failed, 100 ev/s, first paced time 1636329667000
"""

TRUNCATED_LOG = """\
... 171000000 events sent (539875 ev/s, 316.7 s elapsed)
  debs2022-gc-trading-day-08-11-21.csv: read 53,994,554 -> emitted 53,982,555 (0.0222% dropped: 0 bad width, 0 no symbol, 12,000 no event time, 0 filtered out by none, 1 unparsable timestamp; 0 malformed prices nulled)
this is a garbage line that should be ignored
"""


class EmitterParserTest(unittest.TestCase):
    def test_complete_log(self):
        progress = stack_tui.parse_emitter_log(COMPLETE_LOG)
        self.assertEqual(progress["banner"]["topic"], "demo-in")
        self.assertEqual(progress["banner"]["speed_multiplier"], 1000.0)
        self.assertEqual(progress["events_sent"], 289000000)
        self.assertEqual(progress["rate_ev_s"], 546629.0)
        self.assertEqual(progress["current_file"], "debs2022-gc-trading-day-09-11-21.csv")
        self.assertTrue(progress["done"])
        self.assertEqual(progress["total"]["read"], 289116808)
        self.assertEqual(progress["total"]["emitted"], 289049752)
        self.assertEqual(progress["expected_total"], 289116808)
        self.assertEqual(progress["expected_total_source"], "total")
        self.assertTrue(progress["expected_total_complete"])
        self.assertEqual(progress["done_stats"]["submitted"], 289049752)
        view = stack_tui.derive_emitter_view(progress)
        self.assertTrue(view["finished"])
        self.assertIsNotNone(view["run_percent"])
        self.assertEqual(view["eta_current_s"], 0.0)
        self.assertEqual(view["eta_total_s"], 0.0)
        self.assertEqual(len(view["files"]), 2)

    def test_truncated_log(self):
        progress = stack_tui.parse_emitter_log(TRUNCATED_LOG)
        self.assertEqual(progress["events_sent"], 171000000)
        self.assertEqual(progress["rate_ev_s"], 539875.0)
        self.assertIsNone(progress["banner"])
        self.assertEqual(progress["total"], None)
        self.assertIn("debs2022-gc-trading-day-08-11-21.csv", progress["files"])
        self.assertEqual(progress["files"]["debs2022-gc-trading-day-08-11-21.csv"]["read"], 53994554)
        self.assertEqual(progress["expected_total"], 53994554)
        self.assertEqual(progress["expected_total_source"], "sum-file-reads")
        self.assertFalse(progress["expected_total_complete"])

    def test_garbage_lines_are_ignored(self):
        progress = stack_tui.parse_emitter_log("garbage\n\n" + COMPLETE_LOG + "\nnot a replay line\n")
        self.assertEqual(progress["banner"]["topic"], "demo-in")
        self.assertTrue(progress["done"])

    def test_missing_total_learns_sum_of_file_reads(self):
        progress = stack_tui.parse_emitter_log(MISSING_TOTAL_LOG)
        self.assertIsNone(progress["total"])
        self.assertEqual(progress["expected_total"], 3000)
        self.assertEqual(progress["expected_total_source"], "sum-file-reads")
        self.assertTrue(progress["expected_total_complete"])

    def test_comma_formatted_numbers(self):
        progress = stack_tui.parse_emitter_log(COMPLETE_LOG)
        self.assertEqual(progress["files"]["debs2022-gc-trading-day-08-11-21.csv"]["read"], 53994554)
        self.assertEqual(progress["files"]["debs2022-gc-trading-day-08-11-21.csv"]["emitted"], 53982555)
        self.assertEqual(progress["total"]["emitted"], 289049752)

    def test_done_and_total_present(self):
        progress = stack_tui.parse_emitter_log(COMPLETE_LOG)
        self.assertTrue(progress["done"])
        self.assertIsNotNone(progress["total"])
        self.assertEqual(progress["done_stats"]["failed"], 0)
        self.assertEqual(progress["done_stats"]["rate_ev_s"], 462406.0)


class KafkaParserTest(unittest.TestCase):
    def test_offsets_multi_topic_multi_partition(self):
        text = (
            "demo-in:0:100\n"
            "demo-in:1:200\n"
            "demo-out:0:50\n"
            "demo-out:2:75\n"
        )
        offsets = stack_tui.parse_kafka_offsets(text)
        self.assertEqual(offsets["demo-in"], {0: 100, 1: 200})
        self.assertEqual(offsets["demo-out"], {0: 50, 2: 75})
        self.assertEqual(stack_tui.total_offsets(offsets), {"demo-in": 300, "demo-out": 125})

    def test_offsets_empty(self):
        self.assertEqual(stack_tui.parse_kafka_offsets(""), {})
        self.assertEqual(stack_tui.parse_kafka_offsets("not an offset line\n"), {})

    def test_offsets_delta(self):
        previous = {"demo-in": {0: 100, 1: 200}}
        current = {"demo-in": {0: 150, 1: 180}}
        self.assertEqual(stack_tui.offset_deltas(previous, current), {"demo-in": {0: 50, 1: -20}})
        self.assertIsNone(stack_tui.offset_deltas(None, current))

    def test_topic_filtering(self):
        topics = "demo-in\ndemo-out\ndemo-audit\nother-in\n__consumer_offsets\n"
        self.assertEqual(stack_tui.parse_kafka_topics(topics, "demo"), ["demo-in", "demo-out", "demo-audit"])

    def test_consumer_group_parse(self):
        text = (
            "Consumer group 'demo-flink' has no active members.\n"
            "GROUP      TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID\n"
            "demo-flink demo-in  0          100             200             100  -            -     -\n"
            "demo-flink demo-in  1          50              80              30   -            -     -\n"
        )
        group = stack_tui.parse_consumer_group(text)
        self.assertTrue(group["available"])
        self.assertTrue(group["exists"])
        self.assertFalse(group["active"])
        self.assertEqual(group["total_lag"], 130)
        self.assertEqual(len(group["partitions"]), 2)

    def test_consumer_group_missing(self):
        group = stack_tui.parse_consumer_group("Error: Consumer group 'demo-flink' does not exist.\n")
        self.assertFalse(group["exists"])
        self.assertEqual(group["note"], "group not found")


class DockerParserTest(unittest.TestCase):
    def test_docker_port(self):
        text = "29092/tcp -> 127.0.0.1:29093\n8081/tcp -> 127.0.0.1:28082\n"
        mappings = stack_tui.parse_docker_port(text)
        self.assertEqual(len(mappings), 2)
        self.assertEqual(mappings[0]["container_port"], 29092)
        self.assertEqual(mappings[0]["host_ip"], "127.0.0.1")
        self.assertEqual(mappings[1]["host_port"], 28082)

    def test_docker_stats(self):
        text = (
            '{"BlockIO":"19MB / 137MB","CPUPerc":"195.18%","Container":"abc",'
            '"MemPerc":"2.81%","MemUsage":"891.4MiB / 30.94GiB","Name":"demo-taskmanager-1"}\n'
        )
        stats = stack_tui.parse_docker_stats(text)
        self.assertEqual(stats["demo-taskmanager-1"]["cpu_percent"], 195.18)
        self.assertEqual(stats["demo-taskmanager-1"]["mem_usage"], "891.4MiB / 30.94GiB")

    def test_docker_ps_labels_with_commas(self):
        text = (
            '{"ID":"abc123","Names":"demo-kafka-1","Image":"apache/kafka:3.9.1",'
            '"Status":"Up 2 minutes","Labels":"com.docker.compose.project=demo,'
            'com.docker.compose.project.config_files=/a/compose.yaml,/b/compose.yaml,'
            'com.docker.compose.service=kafka"}\n'
        )
        containers = stack_tui.parse_docker_ps(text)
        self.assertEqual(len(containers), 1)
        self.assertEqual(containers[0]["project"], "demo")
        self.assertEqual(containers[0]["service"], "kafka")

    def test_df_rows(self):
        text = (
            "Filesystem Size Used Avail Use% Mounted on\n"
            "/dev/mapper/shared 791G 606G 145G 81% /mnt/shared\n"
            "tmpfs 16G 137M 16G 1% /tmp\n"
        )
        rows = stack_tui.parse_df_rows(text)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["target"], "/mnt/shared")
        self.assertEqual(rows[1]["percent"], 1)


class MathTest(unittest.TestCase):
    def test_percent_edges(self):
        self.assertEqual(stack_tui.safe_percent(5, 10), 50.0)
        self.assertIsNone(stack_tui.safe_percent(5, 0))
        self.assertIsNone(stack_tui.safe_percent(0, 0))
        self.assertIsNone(stack_tui.safe_percent(None, 10))

    def test_eta_edges(self):
        self.assertEqual(stack_tui.eta_seconds(10, 2), 5.0)
        self.assertEqual(stack_tui.eta_seconds(0, 2), 0.0)
        self.assertIsNone(stack_tui.eta_seconds(10, 0))
        self.assertIsNone(stack_tui.eta_seconds(10, None))
        self.assertIsNone(stack_tui.eta_seconds(None, 2))

    def test_rate_edges(self):
        self.assertEqual(stack_tui.compute_rate(10, 2), 5.0)
        self.assertEqual(stack_tui.compute_rate(0, 2), 0.0)
        self.assertIsNone(stack_tui.compute_rate(10, 0))
        self.assertIsNone(stack_tui.compute_rate(10, None))

    def test_zero_rate_view(self):
        progress = stack_tui.parse_emitter_log(
            "Replaying [/data/a.csv] -> topic 'demo-in' via kafka:9092 at 1000.00x "
            "(event zone Europe/Berlin, price filter none, pacing EVENT)\n"
            "Replaying /data/a.csv\n"
            "... 10 events sent (0 ev/s, 0.0 s elapsed)\n"
        )
        view = stack_tui.derive_emitter_view(progress)
        self.assertIsNone(view["eta_total_s"])
        self.assertIsNone(view["eta_current_s"])


class RendererTest(unittest.TestCase):
    def test_nothing_running(self):
        snapshot = {
            "timestamp": 1700000000,
            "projects": [],
            "host": {"load": (0.1, 0.2, 0.3), "filesystems": [], "tmpfs": None},
        }
        text = stack_tui.render_plain_text(snapshot)
        self.assertIn("no Compose stacks running", text)
        self.assertIn("host:", text)

    def test_one_project(self):
        progress = stack_tui.parse_emitter_log(COMPLETE_LOG)
        snapshot = {
            "timestamp": 1700000000,
            "projects": [{
                "name": "demo",
                "containers": [{
                    "service": "kafka",
                    "short_id": "abc123",
                    "image": "apache/kafka:3.9.1",
                    "status": "Up 2 minutes",
                    "health": "healthy",
                    "running_for": "2 minutes",
                    "ports": [{"container_port": 29092, "protocol": "tcp", "host_ip": "127.0.0.1", "host_port": 29093}],
                    "stats": {"cpu_percent": 1.0, "mem_usage": "100MiB / 1GiB"},
                }],
                "emitter": {"available": True, "progress": progress},
                "kafka": {
                    "available": True,
                    "bootstrap": "localhost:9092",
                    "topics": ["demo-in"],
                    "offsets": {"demo-in": {0: 100, 1: 200}},
                    "totals": {"demo-in": 300},
                    "deltas": None,
                    "groups": [{"name": "demo-flink", "available": True, "exists": True, "active": False,
                                "total_lag": 50, "partitions": [{"lag": 50}]}],
                    "errors": [],
                },
                "flink": {
                    "available": True,
                    "stale": False,
                    "summary": {
                        "name": "Demo job",
                        "state": "RUNNING",
                        "start_time": 1700000000000,
                        "duration": 120000,
                        "vertices": [{"name": "Source", "num_records_in": 10, "num_records_out": 9, "backpressure": 0}],
                    },
                },
            }],
            "artifacts": {
                "dirs": [{"path": "/tmp/demo", "available": True}],
                "files": [{"path": "/tmp/demo/emitter.log", "size": 1234, "lines": 10, "growth_bps": 12.5}],
                "errors": [],
            },
            "host": {"load": (0.1, 0.2, 0.3), "filesystems": [{"target": "/mnt/shared", "used": "1G", "size": "10G", "percent": 10}], "tmpfs": None},
        }
        text = stack_tui.render_plain_text(snapshot)
        for needle in ("=== project demo ===", "[Containers]", "[Replay]", "[Kafka]", "[Flink]", "[Artifacts]", "host:"):
            self.assertIn(needle, text)
        self.assertIn("Done:", text)
        self.assertIn("Total:", text)
        self.assertIn("demo-flink", text)


if __name__ == "__main__":
    unittest.main()

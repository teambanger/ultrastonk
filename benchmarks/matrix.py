#!/usr/bin/env python3
"""Run isolated, comparable policy/resource experiments without deleting application data."""
import argparse
import csv
import itertools
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--rates", default="1000,10000")
    parser.add_argument("--parallelism", default="1,2")
    parser.add_argument("--dedup", default="NONE,EXACT,TIMESTAMP")
    parser.add_argument("--late", default="DROP,SIDE_OUTPUT,GRACE")
    parser.add_argument("--qualities", default="mixed", help="comma-separated baseline,duplicates,late,mixed")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--symbols", type=int, default=1000)
    parser.add_argument("--skew", type=float, default=1.2)
    parser.add_argument("--duplicate-rate", type=float, default=.1)
    parser.add_argument("--late-rate", type=float, default=.05)
    parser.add_argument("--late-delay-ms", type=int, default=2000)
    parser.add_argument("--window-ms", type=int, default=1000)
    parser.add_argument("--grace-ms", type=int, default=2000)
    parser.add_argument("--project", default="ultrastonk-benchmark")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--monitoring", action="store_true")
    parser.add_argument("--keep-running", action="store_true")
    parser.add_argument("--kafka-port", type=int, default=19092)
    parser.add_argument("--flink-port", type=int, default=18081)
    parser.add_argument("--prometheus-port", type=int, default=19090)
    args = parser.parse_args()
    if args.project == "ultrastonk":
        parser.error("use a separate benchmark project to avoid stopping the application")
    if args.repetitions <= 0:
        parser.error("repetitions must be positive")
    qualities = args.qualities.split(",")
    if not set(qualities) <= {"baseline", "duplicates", "late", "mixed"}:
        parser.error("qualities must be baseline, duplicates, late, or mixed")
    results_dir = ROOT / "benchmarks/results"
    results_dir.mkdir(exist_ok=True)
    session = uuid.uuid4().hex[:8]
    source_paths = [*ROOT.glob("compose*.yaml"), *ROOT.glob("flink/src/**/*.java"),
                    *ROOT.glob("emitter/*.py"), *ROOT.glob("benchmarks/*.py"), ROOT / "flink/pom.xml"]
    manifest = {"session": session, "arguments": vars(args), "started_at_epoch_seconds": time.time(),
                "sources_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}}
    for key, command_line in {"docker_server_version": ["docker", "version", "--format", "{{.Server.Version}}"],
                              "host_cpu_count_and_memory_bytes": ["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"],
                              "git_commit": ["git", "rev-parse", "HEAD"]}.items():
        completed = subprocess.run(command_line, cwd=ROOT, capture_output=True, text=True)
        manifest[key] = completed.stdout.strip() if completed.returncode == 0 else "unavailable"
    (results_dir / (session + "-manifest.json")).write_text(json.dumps(manifest, indent=2) + "\n")
    env = os.environ | {"COMPOSE_PROJECT_NAME": args.project, "KAFKA_PORT": str(args.kafka_port),
        "FLINK_UI_PORT": str(args.flink_port), "PROMETHEUS_PORT": str(args.prometheus_port),
        "GRAFANA_PORT": "13000", "SOURCE_IDLE_TIMEOUT_MS": "1000", "MAX_OUT_OF_ORDER_MS": "100",
        "CHECKPOINT_INTERVAL_MS": "1000", "FLINK_RESTORE_PATH": "", "KAFKA_PARTITIONS": "6",
        "WINDOW_SIZE_MS": str(args.window_ms), "ALLOWED_LATENESS_MS": str(args.grace_ms),
        "DEDUP_TTL_MS": str(int((args.duration + args.warmup + 300) * 1000)),
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "FLINK_CHECKPOINTS_URI": "file:///opt/flink/state/checkpoints",
        "FLINK_SAVEPOINTS_URI": "file:///opt/flink/state/savepoints"}
    compose = ["docker", "compose", "-p", args.project, "-f", "compose.yaml"]
    if args.monitoring:
        compose += ["-f", "compose.monitoring.yaml"]
    def command(*parts, check=True, capture=False):
        return subprocess.run(compose + list(parts), cwd=ROOT, env=env, check=check,
                              text=True, capture_output=capture)
    combinations = list(itertools.product(qualities, args.dedup.split(","), args.late.split(","),
                                         args.parallelism.split(","), args.rates.split(","), range(args.repetitions)))
    # Randomize execution order to reduce confounding by thermal/cache drift.
    import random
    random.Random(args.seed).shuffle(combinations)
    rows = []
    try:
        command("build", "jobmanager", "benchmark")
        for index, (quality, dedup, late, parallelism, rate, repetition) in enumerate(combinations):
            run_id = f"{session}-{index:03d}"
            env.update({"DEDUP_STRATEGY": dedup, "LATE_EVENT_STRATEGY": late, "FLINK_PARALLELISM": parallelism,
                        "KAFKA_INPUT_TOPIC": f"bench-{run_id}-in", "KAFKA_OUTPUT_TOPIC": f"bench-{run_id}-out",
                        "KAFKA_REJECTED_TOPIC": f"bench-{run_id}-rejected", "KAFKA_GROUP_ID": f"bench-{run_id}",
                        "KAFKA_TRANSACTION_PREFIX": f"bench-{run_id}-"})
            command("stop", "jobmanager", "taskmanager", check=False)
            command("up", "-d", "--scale", "taskmanager=" + parallelism,
                    *(["kafka", "kafka-init", "jobmanager", "taskmanager", "prometheus", "kafka-exporter", "loki", "alloy", "grafana"]
                      if args.monitoring else ["kafka", "kafka-init", "jobmanager", "taskmanager"]))
            print(f"Run {index + 1}/{len(combinations)}: quality={quality}, {dedup}/{late}, p={parallelism}, rate={rate}, repetition={repetition}", flush=True)
            started = time.time()
            completed = command("run", "--rm", "--no-deps", "benchmark", "--run-id", run_id,
                    "--duration", str(args.duration), "--warmup", str(args.warmup), "--rate", rate,
                    "--symbols", str(args.symbols), "--skew", str(args.skew), "--seed", str(args.seed + repetition),
                    "--duplicate-rate", str(args.duplicate_rate if quality in {"duplicates", "mixed"} else 0),
                    "--late-rate", str(args.late_rate if quality in {"late", "mixed"} else 0),
                    "--late-delay-ms", str(args.late_delay_ms), check=False, capture=True)
            (results_dir / (run_id + ".driver.log")).write_text(completed.stdout + completed.stderr)
            log = command("logs", "--no-color", "jobmanager", "taskmanager", capture=True)
            (results_dir / (run_id + ".containers.log")).write_text(log.stdout)
            stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                                   capture_output=True, text=True, check=True)
            (results_dir / (run_id + ".resources.jsonl")).write_text("\n".join(
                line for line in stats.stdout.splitlines() if args.project in json.loads(line).get("Name", "")) + "\n")
            if args.monitoring:
                # Preserve raw time series so findings can be checked after containers stop.
                query = urllib.parse.urlencode({"query": '{job=~"flink-.*|kafka"}',
                                               "start": started, "end": time.time(), "step": 5})
                try:
                    with urllib.request.urlopen(f"http://localhost:{args.prometheus_port}/api/v1/query_range?" + query, timeout=30) as response:
                        (results_dir / (run_id + ".prometheus.json")).write_bytes(response.read())
                except OSError as error:
                    (results_dir / (run_id + ".prometheus-error.txt")).write_text(str(error))
            path = results_dir / (run_id + ".json")
            result = json.loads(path.read_text()) if path.exists() else {}
            measured = result.get("measurement", {})
            latency = measured.get("visible_roundtrip_ms", {})
            rows.append({"run_id": run_id, "quality": quality, "dedup": dedup, "late": late, "parallelism": parallelism,
                "target_rate": rate, "repetition": repetition, "exit_code": completed.returncode,
                "price_correctness_passed": result.get("price_correctness_passed", False),
                **{k: v for k, v in measured.items() if not isinstance(v, dict)},
                **{"latency_" + k + "_ms": latency.get(k) for k in ["p50", "p95", "p99", "max"]}})
            # Write after every run so interruptions retain completed experiments.
            columns = list(dict.fromkeys(key for row in rows for key in row))
            with (results_dir / (session + "-matrix.csv")).open("w") as stream:
                writer = csv.DictWriter(stream, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
            print(json.dumps(rows[-1]), flush=True)
    finally:
        if not args.keep_running:
            command("down", check=False)
    if any(row["exit_code"] or not row["price_correctness_passed"] for row in rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

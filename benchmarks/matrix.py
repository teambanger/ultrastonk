#!/usr/bin/env python3
"""Run isolated, comparable policy/resource experiments without deleting application data."""
import argparse
import csv
import fcntl
import itertools
import hashlib
import json
import os
from pathlib import Path
import random
import re
import subprocess
import tempfile
import time
import uuid
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MONITORING_SERVICES = ("prometheus", "kafka-exporter", "resource-exporter", "loki", "alloy", "grafana")


def docker_list(*args):
    return subprocess.run(["docker", *args], cwd=ROOT, check=True, text=True,
                          capture_output=True).stdout.splitlines()


def check_project_available(project, config):
    label = f"label=com.docker.compose.project={project}"
    for list_args in (("ps", "-a"), ("network", "ls"), ("volume", "ls")):
        if docker_list(*list_args, "--filter", label, "--quiet"):
            raise SystemExit(f"Compose project {project!r} already has Docker resources; refusing to use it")
    # Compose can adopt an unlabeled volume with a matching name. Reject name collisions too.
    for resource, kind in (("networks", "network"), ("volumes", "volume")):
        names = {entry["name"] for entry in config.get(resource, {}).values()}
        for name in names:
            if name in docker_list(kind, "ls", "--filter", f"name=^{re.escape(name)}$", "--format", "{{.Name}}"):
                raise SystemExit(f"Docker {kind} {name!r} already exists; refusing to use project {project!r}")


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
    parser.add_argument("--project", help="unused Compose project name (default: ultrastonk-benchmark-<session>)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--monitoring", action="store_true")
    parser.add_argument("--keep-running", action="store_true")
    parser.add_argument("--kafka-port", type=int, help="publish Kafka on this host port (default: internal only)")
    parser.add_argument("--flink-port", type=int, default=0, help="Flink host port (default: Docker-assigned)")
    parser.add_argument("--prometheus-port", type=int, default=0, help="Prometheus host port (default: Docker-assigned)")
    parser.add_argument("--grafana-port", type=int, default=0, help="Grafana host port (default: Docker-assigned)")
    args = parser.parse_args()
    if args.project is not None and (args.project == "ultrastonk" or args.project.startswith("ultrastonk-verify-")
                                     or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,79}", args.project)):
        parser.error("use a separate, valid Compose project name outside the ultrastonk-verify-* namespace")
    if args.kafka_port is not None and not 1 <= args.kafka_port <= 65535:
        parser.error("--kafka-port must be in 1..65535")
    if not all(0 <= port <= 65535 for port in (args.flink_port, args.prometheus_port, args.grafana_port)):
        parser.error("host ports must be in 0..65535")
    if args.repetitions <= 0:
        parser.error("repetitions must be positive")
    qualities = args.qualities.split(",")
    if not set(qualities) <= {"baseline", "duplicates", "late", "mixed"}:
        parser.error("qualities must be baseline, duplicates, late, or mixed")
    session = uuid.uuid4().hex[:12]
    args.project = args.project or f"ultrastonk-benchmark-{session}"
    # Serialize ownership checks and cleanup for concurrent local invocations of the same project.
    lock_path = Path(tempfile.gettempdir()) / f"ultrastonk-matrix-{args.project}.lock"
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error(f"Compose project {args.project!r} is already in use by another matrix invocation")
        run_matrix(args, qualities, session)


def run_matrix(args, qualities, session):
    env = os.environ | {"COMPOSE_PROJECT_NAME": args.project, "KAFKA_PORT": str(args.kafka_port or 9092),
        "FLINK_UI_PORT": str(args.flink_port), "PROMETHEUS_PORT": str(args.prometheus_port),
        "GRAFANA_PORT": str(args.grafana_port), "SOURCE_IDLE_TIMEOUT_MS": "1000", "MAX_OUT_OF_ORDER_MS": "100",
        "CHECKPOINT_INTERVAL_MS": "1000", "FLINK_RESTORE_PATH": "", "FLINK_BOUNDED_REPLAY": "false",
        "KAFKA_PARTITIONS": "6",
        "KAFKA_REPLICATION_FACTOR": "1", "KAFKA_MIN_ISR": "1",
        "WINDOW_SIZE_MS": str(args.window_ms), "ALLOWED_LATENESS_MS": str(args.grace_ms),
        "DEDUP_TTL_MS": str(int((args.duration + args.warmup + 300) * 1000)),
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "FLINK_CHECKPOINTS_URI": "file:///opt/flink/state/checkpoints",
        "FLINK_SAVEPOINTS_URI": "file:///opt/flink/state/savepoints"}
    compose = ["docker", "compose", "-p", args.project, "-f", "compose.yaml"]
    if args.monitoring:
        compose += ["-f", "deploy/compose.monitoring.yaml"]
    # Keep a benchmark build from replacing another project's local Flink image tag.
    flink_image = f"ultrastonk-matrix-flink:{args.project}"
    override = f"services:\n  jobmanager:\n    image: {flink_image}\n  taskmanager:\n    image: {flink_image}\n"
    if args.kafka_port is None:
        # Kafka clients run inside Compose; don't bind a host port unless explicitly requested.
        override += "  kafka:\n    ports: !reset []\n"
    compose += ["-f", "-", "--profile", "benchmark"]

    def command(*parts, check=True, capture=False):
        return subprocess.run(compose + list(parts), cwd=ROOT, env=env, check=check,
                              input=override, text=True, capture_output=capture)

    config = json.loads(command("config", "--format", "json", capture=True).stdout)
    check_project_available(args.project, config)
    image_names = [flink_image, f"{args.project}-benchmark:latest"]
    if args.monitoring:
        image_names.append(f"{args.project}-resource-exporter:latest")
    for name in image_names:
        if docker_list("image", "ls", "--filter", f"reference={name}", "--quiet"):
            raise SystemExit(f"benchmark image {name!r} already exists; refusing to overwrite it")
    print(f"Benchmark session {session}: Compose project {args.project}", flush=True)
    results_dir = ROOT / "benchmarks/results"
    results_dir.mkdir(exist_ok=True)
    source_patterns = ("compose.yaml", "deploy/compose*.yaml", "flink/src/**/*.java", "flink/pom.xml", "flink/Dockerfile",
                       "flink/*.sh", "flink/*.properties",
                       "event-emitter/src/**/*.java", "event-emitter/pom.xml", "event-emitter/Dockerfile",
                       "benchmarks/*.py", "benchmarks/Dockerfile", "benchmarks/requirements.txt",
                       "benchmarks/synthetic-emitter/**/*.py", "benchmarks/synthetic-emitter/configs/*.json",
                       "monitoring/**/*.yaml", "monitoring/**/*.json", "monitoring/**/*.alloy",
                       "monitoring/resource-exporter/*.py", "monitoring/resource-exporter/Dockerfile")
    source_paths = sorted({path for pattern in source_patterns for path in ROOT.glob(pattern) if path.is_file()})
    manifest = {"session": session, "arguments": vars(args), "started_at_epoch_seconds": time.time(),
                 "sources_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in source_paths}}
    for key, command_line in {"docker_server_version": ["docker", "version", "--format", "{{.Server.Version}}"],
                              "host_cpu_count_and_memory_bytes": ["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"],
                              "git_commit": ["git", "rev-parse", "HEAD"]}.items():
        completed = subprocess.run(command_line, cwd=ROOT, capture_output=True, text=True)
        manifest[key] = completed.stdout.strip() if completed.returncode == 0 else "unavailable"
    (results_dir / (session + "-manifest.json")).write_text(json.dumps(manifest, indent=2) + "\n")
    combinations = list(itertools.product(qualities, args.dedup.split(","), args.late.split(","),
                                         args.parallelism.split(","), args.rates.split(","), range(args.repetitions)))
    # Randomize execution order to reduce confounding by thermal/cache drift.
    random.Random(args.seed).shuffle(combinations)
    rows = []
    try:
        command("build", "jobmanager", "benchmark", *(["resource-exporter"] if args.monitoring else []))
        for index, (quality, dedup, late, parallelism, rate, repetition) in enumerate(combinations):
            run_id = f"{session}-{index:03d}"
            env.update({"DEDUP_STRATEGY": dedup, "LATE_EVENT_STRATEGY": late, "FLINK_PARALLELISM": parallelism,
                        "EXPECTED_TASKMANAGERS": parallelism,
                        "KAFKA_INPUT_TOPIC": f"bench-{run_id}-in", "KAFKA_OUTPUT_TOPIC": f"bench-{run_id}-out",
                        "KAFKA_REJECTED_TOPIC": f"bench-{run_id}-rejected", "KAFKA_GROUP_ID": f"bench-{run_id}",
                        "KAFKA_TRANSACTION_PREFIX": f"bench-{run_id}-"})
            if index:
                command("stop", "jobmanager", "taskmanager")
            command("up", "-d", "--scale", "taskmanager=" + parallelism,
                    "kafka", "kafka-init", "jobmanager", "taskmanager", *(MONITORING_SERVICES if args.monitoring else ()))
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
            containers = command("ps", "--quiet", capture=True).stdout.split()
            stats = docker_list("stats", "--no-stream", "--format", "{{json .}}", *containers) if containers else []
            (results_dir / (run_id + ".resources.jsonl")).write_text("".join(line + "\n" for line in stats))
            if args.monitoring:
                # Preserve raw time series so findings can be checked after containers stop.
                query = urllib.parse.urlencode({"query": '{job=~"flink-.*|kafka|resources"}',
                                               "start": started, "end": time.time(), "step": 5})
                try:
                    endpoint = command("port", "prometheus", "9090", capture=True).stdout.strip()
                    with urllib.request.urlopen(f"http://{endpoint}/api/v1/query_range?" + query, timeout=30) as response:
                        (results_dir / (run_id + ".prometheus.json")).write_bytes(response.read())
                except (OSError, subprocess.CalledProcessError) as error:
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
            command("down", "--volumes", "--rmi", "local", check=False)
            if docker_list("image", "ls", "--filter", f"reference={flink_image}", "--quiet"):
                subprocess.run(["docker", "image", "rm", flink_image], cwd=ROOT, check=False)
    if any(row["exit_code"] or not row["price_correctness_passed"] for row in rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

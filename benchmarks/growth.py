#!/usr/bin/env python3
"""Measure symbol-population effects and subscriber fan-out (tickets 09 and 10).

Two short, single-repetition comparisons at the documented passing baseline from
the capacity bracket (4,000 offered/s, one TaskManager, fixed per-worker caps):

* Ticket 09 — uniform traffic versus the week's measured symbol shares at 1,000
  symbols, then the same measured shares extended to 2,000 symbols. Offered load,
  exchanges, policies and resources are held fixed; only the weight distribution or
  the tail length moves. Delivery, latency and per-partition input load are exported.
* Ticket 10 — the same workload and symbol population served by one gateway to two
  and then four live subscribers. Each subscriber is verified against the committed
  output, so growth is measured as fan-out rather than coverage.

Results are directional: one repetition per condition shows whether skew, symbol
count or subscriber count moved delivery, latency or gateway pressure at this
baseline. It does not establish a limit or a scaling law. Pure helpers are unit
tested in ``benchmarks/tests/test_growth.py``.
"""
import argparse
import csv
import json
from pathlib import Path
import re
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE = Path("/mnt/shared/data/official-week-Ij8jlfeA/results/week-profile.json")

TICKET09 = [
    {"name": "uniform-1000", "symbols": 1000, "skew": 0.0, "distribution": None},
    {"name": "longtail-1000", "symbols": 1000, "skew": None, "distribution": 1000},
    {"name": "longtail-2000", "symbols": 2000, "skew": None, "distribution": 2000},
]
TICKET10 = [
    {"name": "clients-2", "symbols": 200, "skew": None, "distribution": 200, "clients": 2},
    {"name": "clients-4", "symbols": 200, "skew": None, "distribution": 200, "clients": 4},
]


# ---------------------------------------------------------------------------
# Pure helpers

def coverage(shares):
    """Share of traffic covered by the hottest 1, 10 and 100 symbols and all symbols."""
    total = sum(shares)
    if not total:
        raise ValueError("weights must sum to a positive value")
    cumulative, result = 0.0, {}
    for rank, share in enumerate(shares, start=1):
        cumulative += share / total
        if rank in (1, 10, 100):
            result[f"top{rank}"] = cumulative
    for rank in (1, 10, 100):
        result.setdefault(f"top{rank}", cumulative)
    result["symbols"] = len(shares)
    result["all"] = 1.0
    return result


def rank_weights(profile, count):
    """Normalized per-symbol weights and names for the hottest ``count`` symbols.

    The week profile's ``symbol_ranks`` are ordered by descending row count, and
    ``share`` is the fraction of all valid-width rows. Taking a prefix keeps the hot
    symbols identical between the 1,000- and 2,000-symbol conditions; extending the
    prefix adds tail symbols with their measured weights, so the tail mass grows the
    way a larger symbol population grows it in the data.
    """
    ranks = profile.get("symbol_ranks") or []
    if count < 1 or len(ranks) < count:
        raise ValueError(f"profile has {len(ranks)} ranked symbols; cannot take {count}")
    chosen = ranks[:count]
    shares = [float(entry["share"]) for entry in chosen]
    total = sum(shares)
    return {
        "symbols": [entry["symbol"] for entry in chosen],
        "weights": [share / total for share in shares],
        "coverage": coverage(shares),
        "source_rows": sum(int(entry.get("rows") or 0) for entry in chosen),
    }


def imbalance(partition_records):
    """Per-partition input load spread; ``partition_records`` maps partition -> count."""
    if not partition_records:
        return None
    ordered = sorted(partition_records.items(), key=lambda item: int(item[0]))
    values = [int(value) for _, value in ordered]
    total = sum(values)
    if not total:
        return None
    mean = total / len(values)
    shares = {str(key): value / total for key, value in ordered}
    return {
        "partitions": len(values),
        "records": total,
        "shares": shares,
        "max_share": max(shares.values()),
        "min_share": min(shares.values()),
        "max_over_mean": max(values) / mean,
        "coefficient_of_variation": statistics.pstdev(values) / mean if mean else None,
    }


def gateway_health(telemetry):
    """Last observed gateway /health totals: fan-out, slow-client drops, RSS."""
    latest = None
    for sample in telemetry:
        health = sample.get("gateway")
        if isinstance(health, dict) and "totals" in health:
            latest = health
    if latest is None:
        return None
    totals = latest.get("totals") or {}
    return {"in": totals.get("in"), "forwarded": totals.get("forwarded"),
            "dropped": totals.get("dropped"), "invalid": totals.get("invalid"),
            "clients": latest.get("clients"), "tickers": latest.get("tickers"),
            "memory_mb": latest.get("memoryMb")}


def prometheus_services(document):
    """Per-service maxima from the resource-exporter range query (monitored runs only)."""
    result = ((document or {}).get("data") or {}).get("result") or []
    keep = {"pipeline_container_cpu_utilization_ratio", "pipeline_container_cpu_used_cores",
            "pipeline_container_cpu_throttled_periods_ratio",
            "pipeline_container_memory_working_set_bytes", "pipeline_container_memory_utilization_ratio"}

    def series_max(values):
        numbers = [float(value) for _, value in values or [] if value is not None]
        return max(numbers) if numbers else None

    services = {}
    for series in result:
        name = series.get("metric", {}).get("__name__")
        service = series.get("metric", {}).get("service")
        if name in keep and service:
            value = series_max(series.get("values"))
            if value is not None:
                bucket = services.setdefault(service, {})
                bucket[name] = max(bucket.get(name, 0.0), value)
    return services


def parse_bytes(text):
    match = re.fullmatch(r"([0-9.]+)\s*([kmgt]?i?b)?", (text or "").strip(), re.IGNORECASE)
    if not match:
        return None
    factor = {"": 1, "b": 1, "kb": 1e3, "kib": 2 ** 10, "mb": 1e6, "mib": 2 ** 20,
              "gb": 1e9, "gib": 2 ** 30, "tb": 1e12, "tib": 2 ** 40}[((match.group(2) or "")).lower()]
    return float(match.group(1)) * factor


def container_stats(resources, needle):
    """CPU percent and memory bytes for the container whose name contains ``needle``."""
    rows = [row for row in resources or [] if isinstance(row, dict) and needle in (row.get("Name") or "")]
    if not rows:
        return None
    cpu = [float(row["CPUPerc"].rstrip("%")) for row in rows if row.get("CPUPerc")]
    memory = [parsed for row in rows if (parsed := parse_bytes((row.get("MemUsage") or "").split("/")[0]))]
    return {"samples": len(rows), "cpu_percent": cpu[0] if cpu else None,
            "memory_bytes": memory[0] if memory else None}


def run_metrics(driver):
    """Normalize one driver artifact into the fields the evidence pack quotes."""
    measurement = driver.get("measurement") or {}
    latency = measurement.get("visible_roundtrip_ms") or {}
    arguments = driver.get("arguments") or {}
    try:
        achieved = float(measurement.get("achieved_originals_per_second"))
    except (TypeError, ValueError):
        achieved = None
    metrics = {
        "symbols": arguments.get("symbols"),
        "distribution_file": arguments.get("distribution_file"),
        # matrix always passes --skew, but a distribution file supersedes it.
        "skew": arguments.get("skew") if not arguments.get("distribution_file") else None,
        "achieved_originals_per_second": achieved,
        "committed_prices_per_second": measurement.get("committed_prices_per_second"),
        "delivered_prices": measurement.get("observed_prices"),
        "expected_prices": measurement.get("expected_prices"),
        "missing_prices": measurement.get("missing_prices"),
        "unexpected_prices": measurement.get("unexpected_prices"),
        "drain_seconds": measurement.get("drain_seconds"),
        "latency_ms": {key: latency.get(key) for key in ("p50", "p95", "p99", "max")},
        "latency_samples": latency.get("count"),
        "input_partition_imbalance": imbalance(measurement.get("input_partition_records")),
        "price_correctness_passed": driver.get("price_correctness_passed"),
        "errors": list(driver.get("errors") or [])[:5],
    }
    if driver.get("gateway_correctness_passed") is not None:
        clients = measurement.get("clients") or []
        metrics["gateway"] = {
            "clients": len(clients),
            "delivered_exact_all_clients": driver.get("gateway_correctness_passed"),
            "fanout_frames_per_second": measurement.get("fanout_frames_per_second"),
            "per_client_prices_per_second": measurement.get("websocket_delivered_prices_per_second"),
            "websocket_frames": measurement.get("websocket_frames"),
            "client_latency_ms": [{"p50": client["latency_ms"]["p50"], "p99": client["latency_ms"]["p99"]}
                                  for client in clients],
            "client_missing_prices": [client["missing_prices"] for client in clients],
            "client_missing_emas": [client["missing_emas"] for client in clients],
            "client_errors": [client["errors"] for client in clients],
            "health": gateway_health(driver.get("telemetry") or []),
        }
    return metrics


def resolve(record, path):
    value = record
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def compare(lower, higher, paths):
    """Paired deltas for one condition against its baseline; ratios, not laws."""
    result = {}
    for path in paths:
        base, grown = resolve(lower, path), resolve(higher, path)
        if isinstance(base, (int, float)) and isinstance(grown, (int, float)) and base:
            result[path] = {"baseline": base, "changed": grown, "ratio": grown / base}
    return result


def condition_metrics(driver, resources, matrix_row, prometheus=None):
    metrics = run_metrics(driver)
    metrics["gateway_container"] = container_stats(resources, "-gateway-")
    metrics["benchmark_container"] = container_stats(resources, "-driver-")
    metrics["prometheus_services"] = prometheus_services(prometheus) if prometheus else None
    metrics["run_id"] = driver.get("run_id")
    metrics["exit_code"] = matrix_row.get("exit_code")
    return metrics


# ---------------------------------------------------------------------------
# Orchestration through matrix.py

def matrix_command(matrix, condition, *, project, duration, warmup, rate, profile_spec, gateway=False,
                   monitoring=False):
    command = ["python3", str(matrix), "--project", project,
               "--duration", str(duration), "--warmup", str(warmup), "--rates", str(rate),
               "--parallelism", "1", "--dedup", "NONE", "--late", "DROP", "--qualities", "baseline",
               "--repetitions", "1", "--duplicate-rate", "0", "--late-rate", "0",
               "--symbols", str(condition["symbols"]), "--seed", "42",
               "--resource-profile", f"perworker={profile_spec}"]
    if condition.get("distribution"):
        command += ["--distribution-file", f"/results/growth-weights-{condition['distribution']}.json"]
    else:
        command += ["--skew", str(condition.get("skew", 0.0))]
    if gateway:
        command += ["--gateway", "--clients", str(condition["clients"])]
    if monitoring:
        command.append("--monitoring")
    return command


def parse_session(stdout):
    match = re.search(r"Benchmark session ([0-9a-f]+): Compose project (\S+)", stdout or "")
    return match.group(1) if match else None


def read_condition(results_dir, session):
    results_dir = Path(results_dir)
    with (results_dir / f"{session}-matrix.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != 1:
        raise SystemExit(f"expected one run in session {session}, got {len(rows)}")
    row = rows[0]
    driver_path = results_dir / f"{row['run_id']}.json"
    driver = json.loads(driver_path.read_text()) if driver_path.exists() else {}
    resources_path = results_dir / f"{row['run_id']}.resources.jsonl"
    resources = ([json.loads(line) for line in resources_path.read_text().splitlines() if line.strip()]
                 if resources_path.exists() else [])
    prometheus_path = results_dir / f"{row['run_id']}.prometheus.json"
    prometheus = json.loads(prometheus_path.read_text()) if prometheus_path.exists() else None
    return driver, resources, row, prometheus


def write_weight_files(profile, evidence_dir, results_dir, counts):
    """Publish the distribution inputs and the container-visible copies matrix needs."""
    results_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    for count in counts:
        payload = rank_weights(profile, count)
        body = {"source": "official-week week-profile.json symbol_ranks prefix",
                "ranked_symbols": payload["symbols"], "weights": payload["weights"],
                "coverage": payload["coverage"], "source_rows": payload["source_rows"]}
        text = json.dumps(body, indent=2) + "\n"
        (results_dir / f"growth-weights-{count}.json").write_text(text)
        (evidence_dir / f"symbol-weights-{count}.json").write_text(text)
        written[str(count)] = {"coverage": payload["coverage"], "source_rows": payload["source_rows"]}
    return written


def run_batch(matrix, conditions, args, results_dir, profile_spec, label, gateway=False, monitoring=False):
    records, sessions, commands = {}, [], {}
    for condition in conditions:
        project = f"{args.project_prefix}-{label}-{condition['name']}"
        command = matrix_command(matrix, condition, project=project, duration=args.duration,
                                 warmup=args.warmup, rate=args.rate, profile_spec=profile_spec,
                                 gateway=gateway, monitoring=monitoring)
        commands[condition["name"]] = " ".join(command)
        print(f"[{label}] {condition['name']}: {commands[condition['name']]}", flush=True)
        completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
        session = parse_session(completed.stdout)
        if session is None:
            raise SystemExit(f"{condition['name']} produced no matrix session:\n"
                             f"{completed.stdout[-2000:]}\n{completed.stderr[-2000:]}")
        driver, resources, row, prometheus = read_condition(results_dir, session)
        sessions.append(session)
        records[condition["name"]] = condition_metrics(driver, resources, row, prometheus)
    return records, sessions, commands


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", default=str(ROOT / "benchmarks/matrix.py"))
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE), help="official week profile with symbol_ranks")
    parser.add_argument("--results-dir", default=str(ROOT / "benchmarks/results"))
    parser.add_argument("--evidence-root", default=str(ROOT / "benchmarks/evidence"))
    parser.add_argument("--duration", type=float, default=20)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--rate", type=int, default=4000)
    parser.add_argument("--project-prefix", default="ultrastonk-growth")
    parser.add_argument("--tickets", default="09,10", help="comma-separated tickets to run")
    parser.add_argument("--monitoring", action="store_true", help="add Prometheus resource series to the pack")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not Path(args.profile).exists():
        raise SystemExit(f"missing week profile {args.profile}; tickets 09/10 need its symbol ranks")

    tickets = [ticket.strip() for ticket in args.tickets.split(",")]
    unknown = set(tickets) - {"09", "10"}
    if unknown:
        raise SystemExit(f"unknown tickets: {sorted(unknown)}")
    profile = json.loads(Path(args.profile).read_text())
    results_dir = Path(args.results_dir)
    baseline = {"offered_per_second": args.rate, "workers": 1, "duration_seconds": args.duration,
                "warmup_seconds": args.warmup, "repetitions": 1,
                "basis": "capacity bracket: 1 worker at 4,000 offered/s passed delivery exactly"}

    if args.dry_run:
        plan = {"baseline": baseline, "commands": {}}
        if "09" in tickets:
            for condition in TICKET09:
                plan["commands"][f"09/{condition['name']}"] = " ".join(matrix_command(
                    args.matrix, condition, project=f"{args.project_prefix}-09-{condition['name']}",
                    duration=args.duration, warmup=args.warmup, rate=args.rate,
                    profile_spec="taskmanager:1:2g,kafka:2:2g,benchmark:2:1g"))
        if "10" in tickets:
            for condition in TICKET10:
                plan["commands"][f"10/{condition['name']}"] = " ".join(matrix_command(
                    args.matrix, condition, project=f"{args.project_prefix}-10-{condition['name']}",
                    duration=args.duration, warmup=args.warmup, rate=args.rate,
                    profile_spec="taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g", gateway=True))
        print(json.dumps(plan, indent=2))
        return

    started = time.monotonic()
    outputs = {}
    if "09" in tickets:
        evidence = Path(args.evidence_root) / "long-tail"
        weights = write_weight_files(profile, evidence, results_dir, (1000, 2000))
        records, sessions, commands = run_batch(
            args.matrix, TICKET09, args, results_dir,
            "taskmanager:1:2g,kafka:2:2g,benchmark:2:1g", "09")
        outputs["09"] = {
            "baseline": baseline, "distribution_inputs": weights,
            "conditions": records, "source_sessions": sessions, "commands": commands,
            "comparisons": {
                "longtail_vs_uniform_1000": compare(
                    records.get("uniform-1000", {}), records.get("longtail-1000", {}),
                    ("committed_prices_per_second", "latency_ms.p50", "latency_ms.p95", "latency_ms.p99",
                     "input_partition_imbalance.coefficient_of_variation",
                     "input_partition_imbalance.max_over_mean")),
                "symbols_2000_vs_1000": compare(
                    records.get("longtail-1000", {}), records.get("longtail-2000", {}),
                    ("committed_prices_per_second", "latency_ms.p50", "latency_ms.p95", "latency_ms.p99",
                     "input_partition_imbalance.coefficient_of_variation",
                     "input_partition_imbalance.max_over_mean")),
            },
            "scope": "single repetition; directional; processing path only, no gateway",
        }
        (evidence / "runs").mkdir(parents=True, exist_ok=True)
        for name, record in records.items():
            (evidence / "runs" / f"{name}.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    if "10" in tickets:
        evidence = Path(args.evidence_root) / "subscribers"
        weights = write_weight_files(profile, evidence, results_dir, (200,))
        records, sessions, commands = run_batch(
            args.matrix, TICKET10, args, results_dir,
            "taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g", "10", gateway=True,
            monitoring=args.monitoring)
        outputs["10"] = {
            "baseline": baseline, "distribution_inputs": weights,
            "conditions": records, "source_sessions": sessions, "commands": commands,
            "comparison": compare(records.get("clients-2", {}), records.get("clients-4", {}),
                                  ("committed_prices_per_second", "latency_ms.p99",
                                   "gateway.fanout_frames_per_second", "gateway.per_client_prices_per_second",
                                   "gateway.health.dropped")),
            "scope": "single repetition; directional; one gateway instance; upstream unchanged",
        }
        (evidence / "runs").mkdir(parents=True, exist_ok=True)
        for name, record in records.items():
            (evidence / "runs" / f"{name}.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")

    for ticket, payload in outputs.items():
        payload["wall_seconds"] = time.monotonic() - started
        directory = Path(args.evidence_root) / ("long-tail" if ticket == "09" else "subscribers")
        (directory / "summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps({ticket: {name: {"delivered": (metrics.get("price_correctness_passed"),
                                                    (metrics.get("gateway") or {}).get("delivered_exact_all_clients")),
                                 "committed_per_second": metrics.get("committed_prices_per_second"),
                                 "latency_p99_ms": (metrics.get("latency_ms") or {}).get("p99")}
                             for name, metrics in payload["conditions"].items()}
                      for ticket, payload in outputs.items()}, indent=2))


if __name__ == "__main__":
    main()
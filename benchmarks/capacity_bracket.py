#!/usr/bin/env python3
"""Bracket the Sustainable limit for one and two TaskManagers.

Preparation-only tooling for ticket 08. It reuses the synthetic workload through
``benchmarks/matrix.py`` with the live query revision, holds partitions, policies,
workload shape, per-worker resource caps and instrumentation fixed across both
worker counts, steps the offered load coarse-to-fine, and runs longer and repeats
near the passing/failing boundary. It exports the highest-passing and first-failing
bounds, or an explicit ``unbracketed`` outcome when no separation is found.

Acceptance rules are implemented in code and pre-registered: ``default_rules()`` is
locked to ``benchmarks/evidence/sustainable-limit/pre-registered-rules.json`` by a
unit test, so the thresholds cannot drift after scoring starts. A run whose offered
load was not actually achieved (generator shortfall) or whose required telemetry is
missing is marked INCONCLUSIVE rather than counted as a pass or a fail.

The evaluation, bracket, schedule and summary functions are pure so they can be
exercised against controlled fixtures; ``main()`` orchestrates ``matrix.py`` runs
against a disposable Compose project. ``--dry-run`` prints exactly what the scored
phase will execute without starting any container or generating any load.
"""
import argparse
import collections
import csv
import json
import math
from pathlib import Path
import re
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]

# Pre-registered acceptance rules. Changing any value invalidates prior brackets;
# the value is also pinned in benchmarks/evidence/sustainable-limit/pre-registered-rules.json.
PASS, FAIL, INCONCLUSIVE = "PASS", "FAIL", "INCONCLUSIVE"
DEFAULT_RULES = {
    "version": "sustainable-limit/1",
    "min_achieved_input_ratio": 0.98,
    "require_gateway_delivery": True,
    "require_limits": True,
    "require_hardware": True,
    "require_worker_cpu": True,
    "min_progress_samples": 5,
    "backlog_growth_slack": 0.0,
    "backlog_terminal_floor": 0.0,
    "ignore_load_window_slack_s": 2.0,
    "stretch_capacity_gain": 1.5,
}


def default_rules():
    return dict(DEFAULT_RULES)


def _number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _integer(value):
    number = _number(value)
    return int(number) if number is not None else None


def _bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return None


# ---------------------------------------------------------------------------
# Normalization: matrix CSV row + driver artifacts -> one comparable run record

def effective_limits(limits):
    """Actual Docker-inspected per-worker caps, summed for the worker pool."""
    if not limits:
        return None
    containers = limits.get("containers") or []
    workers = [container for container in containers if "-taskmanager-" in (container.get("name") or "")]
    cpu = [container["nano_cpus"] for container in workers if container.get("nano_cpus")]
    memory = [container["memory_bytes"] for container in workers if container.get("memory_bytes")]
    return {
        "profile": limits.get("profile"),
        "requested": limits.get("requested") or {},
        "workers": workers,
        "worker_count": len(workers),
        "aggregate_worker_cpu_quota_cores": sum(cpu) / 1e9 if cpu else None,
        "aggregate_worker_memory_bytes": sum(memory) if memory else None,
        "oom_killed": any(container.get("oom_killed") for container in workers),
    }


def host_hardware(manifest):
    if not manifest:
        return None
    cpu = memory = None
    raw = manifest.get("host_cpu_count_and_memory_bytes")
    if isinstance(raw, str) and raw.strip():
        parts = raw.split()
        if len(parts) == 2:
            cpu, memory = _integer(parts[0]), _integer(parts[1])
    return {"cpu_count": cpu, "memory_bytes": memory,
            "docker_server_version": manifest.get("docker_server_version"),
            "git_commit": manifest.get("git_commit")}


def _parse_bytes(text):
    match = re.fullmatch(r"([0-9.]+)\s*([kmgt]?i?b)?", (text or "").strip(), re.IGNORECASE)
    if not match:
        return None
    factor = {"": 1, "b": 1, "kb": 1e3, "kib": 2 ** 10, "mb": 1e6, "mib": 2 ** 20,
              "gb": 1e9, "gib": 2 ** 30, "tb": 1e12, "tib": 2 ** 40}[(match.group(2) or "").lower()]
    return float(match.group(1)) * factor


def resources_summary(resources):
    """Aggregate the post-run ``docker stats`` snapshot for the worker pool."""
    rows = [row for row in (resources or []) if not isinstance(row, str)]
    if not rows:
        return None
    workers = [row for row in rows if "-taskmanager-" in (row.get("Name") or "")]
    if not workers:
        return None
    cpu = [float(row["CPUPerc"].rstrip("%")) for row in workers if row.get("CPUPerc")]
    memory = [bytes_ for row in workers if (bytes_ := _parse_bytes((row.get("MemUsage") or "").split("/")[0]))]
    return {"source": "docker_stats", "workers": len(workers), "samples": len(rows),
            "cpu_percent_sum": sum(cpu) if cpu else None,
            "memory_bytes_sum": sum(memory) if memory else None}


def _series_max(values):
    numbers = [_number(value) for _, value in values or []]
    numbers = [number for number in numbers if number is not None]
    return max(numbers) if numbers else None


def prometheus_summary(document):
    """Aggregate worker CPU quotas, utilization, throttling and memory series."""
    if not document or not isinstance(document.get("data"), dict):
        return None
    result = document["data"].get("result") or []
    if not result:
        return None
    by_name = collections.defaultdict(list)
    for series in result:
        by_name[series["metric"].get("__name__")].append(series)

    def maxima(name, service="taskmanager"):
        values = [_series_max(series.get("values")) for series in by_name.get(name, [])
                  if (series["metric"].get("service") or service) == service]
        return [value for value in values if value is not None]

    used = maxima("pipeline_container_cpu_used_cores")
    utilization = maxima("pipeline_container_cpu_utilization_ratio")
    throttled = maxima("pipeline_container_cpu_throttled_periods_ratio")
    working_set = maxima("pipeline_container_memory_working_set_bytes")
    memory_ratio = maxima("pipeline_container_memory_utilization_ratio")
    points = sum(len(series.get("values") or []) for series in result)
    return {
        "source": "prometheus",
        "workers": len(used) or len(utilization),
        "samples": points,
        "used_cores_max": sum(used) if used else None,
        "utilization_ratio_max": max(utilization) if utilization else None,
        "throttled_periods_ratio_max": max(throttled) if throttled else None,
        "memory_working_set_bytes_max": sum(working_set) if working_set else None,
        "memory_utilization_ratio_max": max(memory_ratio) if memory_ratio else None,
    }


def worker_cpu(run):
    """Prefer exporter Prometheus series; fall back to the Docker stats snapshot."""
    prometheus = run.get("prometheus")
    if prometheus and prometheus.get("samples"):
        return prometheus
    return run.get("resources")


def normalize_run(row, driver, limits=None, manifest=None, resources=None, prometheus=None):
    """Build one comparable run record from matrix/driver artifacts (pure, no IO)."""
    driver = driver or {}
    arguments = driver.get("arguments") or {}
    settings = driver.get("settings") or {}
    workers = _integer(row.get("parallelism")) or _integer(arguments.get("flink_parallelism")) \
        or _integer(settings.get("FLINK_PARALLELISM")) or 1
    offered = _number(row.get("target_rate")) or _number(arguments.get("rate"))
    gateway_present = "gateway_correctness_passed" in row or "gateway_correctness_passed" in driver
    record = {
        "run_id": row.get("run_id") or driver.get("run_id"),
        "workers": workers,
        "offered": offered,
        "repetition": _integer(row.get("repetition")) or 0,
        "quality": row.get("quality"),
        "dedup": row.get("dedup") or settings.get("DEDUP_STRATEGY"),
        "late": row.get("late") or settings.get("LATE_EVENT_STRATEGY"),
        "resource_profile": row.get("resource_profile"),
        "exit_code": _integer(row.get("exit_code")) if row.get("exit_code") is not None else None,
        "arguments": arguments,
        "settings": {
            "window_ms": _integer(settings.get("WINDOW_SIZE_MS")),
            "max_out_of_order_ms": _integer(settings.get("MAX_OUT_OF_ORDER_MS")),
            "allowed_lateness_ms": _integer(settings.get("ALLOWED_LATENESS_MS")),
            "checkpoint_interval_ms": _integer(settings.get("CHECKPOINT_INTERVAL_MS")),
            "flink_parallelism": _integer(settings.get("FLINK_PARALLELISM")) or workers,
        },
        "measurement": driver.get("measurement") or {},
        "counters": driver.get("counters") or {},
        "progress": driver.get("progress") or [],
        "telemetry": driver.get("telemetry") or [],
        "errors": driver.get("errors") or [],
        "limits": effective_limits(limits),
        "hardware": host_hardware(manifest),
        "resources": resources_summary(resources),
        "prometheus": prometheus_summary(prometheus),
        "gateway_present": gateway_present,
        "price_correctness_passed": _bool(row.get("price_correctness_passed")) if row.get("price_correctness_passed") is not None
            else _bool(driver.get("price_correctness_passed")),
        "gateway_correctness_passed": _bool(driver.get("gateway_correctness_passed"))
            if "gateway_correctness_passed" in driver else (_bool(row.get("gateway_correctness_passed")) if gateway_present else None),
    }
    record["worker_cpu"] = worker_cpu(record)
    return record


# ---------------------------------------------------------------------------
# Stage backlog and checkpoint-batching suppression

def stage_backlogs(progress, load_window_s=None, slack_s=2.0):
    """Stage backlog series within the measured load window, before drain.

    ``input_to_committed`` is the Flink ingestion backlog
    (expected measured prices minus committed measured prices); ``committed_to_websocket``
    is the gateway backlog. Samples after the producer stops are excluded because they
    measure drain, not keeping up under load.
    """
    stages = collections.defaultdict(list)
    for sample in progress or []:
        elapsed = _number(sample.get("elapsed_seconds"))
        if elapsed is None or elapsed < 0:
            continue
        if load_window_s is not None and elapsed > load_window_s + slack_s:
            continue
        expected = _integer(sample.get("expected_measured_prices"))
        committed = _integer(sample.get("committed_measured_prices"))
        websocket = _integer(sample.get("websocket_measured_prices"))
        if expected is not None and committed is not None:
            stages["input_to_committed"].append((elapsed, expected - committed))
        if committed is not None and websocket is not None:
            stages["committed_to_websocket"].append((elapsed, committed - websocket))
    return dict(stages)


def _sample_interval(points):
    if len(points) < 2:
        return 1.0
    deltas = [b[0] - a[0] for a, b in zip(points, points[1:]) if b[0] > a[0]]
    return statistics.median(deltas) if deltas else 1.0


def backlog_trend(points, checkpoint_interval_ms=1000, slack=0.0, floor=0.0):
    """Sustained-growth test that ignores checkpoint-batching spikes.

    Committed output is transactional, so the ingestion backlog sawtooths: it climbs
    between checkpoints and drops at commit. A transient batching spike is therefore
    ignored by taking the per-checkpoint-interval minimum (the trough). Sustained
    overload is the trough itself climbing across the load window.
    """
    interval = _sample_interval(points)
    width = max(1, round((checkpoint_interval_ms / 1000.0) / interval)) if checkpoint_interval_ms else 1
    troughs = [min(value for _, value in points[offset:offset + width])
               for offset in range(0, len(points), width)]
    third = max(1, len(troughs) // 3)
    first = sum(troughs[:third]) / third
    last = sum(troughs[-third:]) / third
    final = troughs[-1]
    growing = final > floor and (last - first) > slack
    return {
        "samples": len(points),
        "checkpoint_buckets": len(troughs),
        "bucket_width_samples": width,
        "troughs": troughs,
        "first_third_mean": first,
        "last_third_mean": last,
        "final": final,
        "max": max((value for _, value in points), default=0),
        "spikes_suppressed": len(points) - len(troughs),
        "growing": growing,
    }


# ---------------------------------------------------------------------------
# Acceptance rules

def telemetry_check(run, rules):
    missing = []
    measurement = run.get("measurement") or {}
    settings = run.get("settings") or {}
    if not measurement:
        missing.append("driver measurement")
    if _number(measurement.get("achieved_originals_per_second")) is None:
        missing.append("achieved input rate")
    if run.get("offered") is None or run["offered"] <= 0:
        missing.append("offered load")
    if run.get("price_correctness_passed") is None:
        missing.append("price correctness")
    if rules["require_gateway_delivery"] and run.get("gateway_present") and run.get("gateway_correctness_passed") is None:
        missing.append("gateway correctness")
    if len(run.get("progress") or []) < rules["min_progress_samples"]:
        missing.append("progress samples")
    if settings.get("window_ms") is None:
        missing.append("window size")
    if settings.get("checkpoint_interval_ms") is None:
        missing.append("checkpoint interval")
    if rules["require_limits"] and not (run.get("limits") and run["limits"].get("worker_count")):
        missing.append("effective limits")
    if rules["require_hardware"] and not (run.get("hardware") and run["hardware"].get("cpu_count")
                                          and run["hardware"].get("memory_bytes")):
        missing.append("host hardware")
    if rules["require_worker_cpu"] and not (run.get("worker_cpu") and run["worker_cpu"].get("samples")):
        missing.append("aggregate worker CPU")
    return {"passed": not missing, "missing": missing,
            "reason": None if not missing else "missing telemetry: " + ", ".join(missing)}


def achieved_input_check(run, rules):
    measurement = run.get("measurement") or {}
    achieved = _number(measurement.get("achieved_originals_per_second"))
    offered = run.get("offered")
    if achieved is None or not offered:
        return {"passed": False, "ratio": None, "reason": "no achieved/offered rate"}
    ratio = achieved / offered
    passed = ratio >= rules["min_achieved_input_ratio"]
    return {"passed": passed, "ratio": ratio, "required_ratio": rules["min_achieved_input_ratio"],
            "achieved_per_second": achieved, "offered_per_second": offered,
            "producer_queue_full": _integer((run.get("counters") or {}).get("producer_queue_full")),
            "reason": None if passed else f"generator shortfall: achieved {ratio:.4f} < "
                                          f"{rules['min_achieved_input_ratio']:.4f}"}


def delivery_check(run, rules):
    problems = []
    if run.get("price_correctness_passed") is not True:
        problems.append("committed Kafka delivery is not correct")
    if rules["require_gateway_delivery"] and run.get("gateway_present") and run.get("gateway_correctness_passed") is not True:
        problems.append("WebSocket delivery is not correct")
    measurement = run.get("measurement") or {}
    if (measurement.get("missing_prices") or 0) or (measurement.get("unexpected_prices") or 0):
        problems.append("missing or extra committed prices")
    return {"passed": not problems, "reason": None if not problems else "delivery failed: " + "; ".join(problems),
            "missing_prices": measurement.get("missing_prices"),
            "unexpected_prices": measurement.get("unexpected_prices"),
            "missing_emas": measurement.get("websocket_missing_emas")}


def backlog_check(run, rules):
    load_window = _number((run.get("arguments") or {}).get("duration"))
    stages = stage_backlogs(run.get("progress") or [], load_window, rules["ignore_load_window_slack_s"])
    checkpoint = (run.get("settings") or {}).get("checkpoint_interval_ms") or 1000
    trends = {stage: backlog_trend(points, checkpoint, rules["backlog_growth_slack"],
                                   rules["backlog_terminal_floor"])
              for stage, points in stages.items()}
    growing = [stage for stage, trend in trends.items() if trend["growing"]]
    return {"passed": not growing, "growing_stages": growing, "stages": trends,
            "reason": None if not growing else "sustained stage backlog growth: " + ", ".join(growing)}


def evaluate_run(run, rules=None):
    """PASS, FAIL, or INCONCLUSIVE with the reasons behind the verdict."""
    rules = rules or default_rules()
    checks = {"telemetry": telemetry_check(run, rules)}
    if not checks["telemetry"]["passed"]:
        verdict, reasons = INCONCLUSIVE, [checks["telemetry"]["reason"]]
    else:
        checks["achieved_input"] = achieved_input_check(run, rules)
        checks["delivery"] = delivery_check(run, rules)
        checks["backlog"] = backlog_check(run, rules)
        if not checks["achieved_input"]["passed"]:
            verdict = INCONCLUSIVE
            reasons = [checks["achieved_input"]["reason"]]
        elif not checks["delivery"]["passed"]:
            verdict, reasons = FAIL, [checks["delivery"]["reason"]]
        elif not checks["backlog"]["passed"]:
            verdict, reasons = FAIL, [checks["backlog"]["reason"]]
        else:
            verdict, reasons = PASS, []
    return {"run_id": run.get("run_id"), "workers": run.get("workers"), "offered": run.get("offered"),
            "repetition": run.get("repetition"), "verdict": verdict, "reasons": reasons,
            "checks": checks, "measurements": run_measurements(run), "resource_profile": run.get("resource_profile")}


def steady_throughput(run):
    """Committed and WebSocket prices per second while the producer is still running."""
    measured = [sample for sample in run.get("progress") or [] if _number(sample.get("elapsed_seconds"))]
    if not measured:
        return {"steady_committed_per_second": None, "steady_websocket_per_second": None, "window_seconds": None}
    cut = max((sample for sample in measured if _number(sample.get("elapsed_seconds"))),
              key=lambda sample: sample.get("originals") or 0)
    elapsed = _number(cut.get("elapsed_seconds"))
    committed = _integer(cut.get("committed_measured_prices"))
    websocket = _integer(cut.get("websocket_measured_prices"))
    return {
        "steady_committed_per_second": committed / elapsed if committed is not None and elapsed else None,
        "steady_websocket_per_second": websocket / elapsed if websocket is not None and elapsed else None,
        "window_seconds": elapsed,
    }


def run_measurements(run):
    measurement = run.get("measurement") or {}
    counters = run.get("counters") or {}
    latency = measurement.get("visible_roundtrip_ms") or {}
    websocket_latency = measurement.get("websocket_roundtrip_ms") or {}
    backlog = backlog_check(run, default_rules())["stages"] if run.get("progress") else {}
    return {
        "achieved_originals_per_second": _number(measurement.get("achieved_originals_per_second")),
        "committed_prices_per_second": _number(measurement.get("committed_prices_per_second")),
        "websocket_prices_per_second": _number(measurement.get("websocket_prices_per_second")),
        **steady_throughput(run),
        "visible_latency_ms": {key: _number(latency.get(key)) for key in ("p50", "p95", "p99", "max")},
        "websocket_latency_ms": {key: _number(websocket_latency.get(key)) for key in ("p50", "p95", "p99", "max")},
        "drain_seconds": _number(measurement.get("drain_seconds")),
        "producer_queue_full": _integer(counters.get("producer_queue_full")),
        "input_to_committed_max": (backlog.get("input_to_committed") or {}).get("max"),
        "committed_to_websocket_max": (backlog.get("committed_to_websocket") or {}).get("max"),
        "worker_cpu_used_cores_max": (run.get("worker_cpu") or {}).get("used_cores_max"),
        "worker_cpu_utilization_ratio_max": (run.get("worker_cpu") or {}).get("utilization_ratio_max"),
        "worker_throttled_periods_ratio_max": (run.get("worker_cpu") or {}).get("throttled_periods_ratio_max"),
        "worker_memory_working_set_bytes_max": (run.get("worker_cpu") or {}).get("memory_working_set_bytes_max"),
    }


# ---------------------------------------------------------------------------
# Bracket and search

def point_verdict(verdicts):
    """Worst verdict wins: one inconclusive repeat makes the point inconclusive."""
    if INCONCLUSIVE in verdicts:
        return INCONCLUSIVE
    if FAIL in verdicts:
        return FAIL
    return PASS


def bracket(records):
    """Highest passing and first failing offered load per worker count, or unbracketed."""
    groups = collections.defaultdict(list)
    for record in records:
        groups[(record["workers"], record["offered"])].append(record)
    points = {}
    for (workers, offered), group in groups.items():
        points.setdefault(workers, {})[offered] = point_verdict([record["verdict"] for record in group])
    result = {}
    for workers, offers in points.items():
        ordered = sorted(offers)
        passing = [offer for offer in ordered if offers[offer] == PASS]
        failing = [offer for offer in ordered if offers[offer] == FAIL]
        highest, first = (max(passing) if passing else None), (min(failing) if failing else None)
        non_monotonic = bool(passing and failing and max(failing) < max(passing))
        separated = highest is not None and first is not None and first > highest
        if not passing:
            reason = "unbracketed: no passing offered load"
        elif not failing:
            reason = "unbracketed: no failing offered load"
        elif not separated:
            reason = "unbracketed: first failing is below the highest passing load"
        else:
            reason = None
        result[workers] = {
            "points": {offer: offers[offer] for offer in ordered},
            "highest_passing": highest,
            "first_failing": first,
            "separated": separated,
            "non_monotonic": non_monotonic,
            "unbracketed_reason": reason,
        }
    return result


def coarse_rates(start, stop, factor):
    """Geometric coarse ladder from start up to stop inclusive-ish."""
    if start <= 0 or factor <= 1 or stop < start:
        raise ValueError("coarse_rates needs 0 < start <= stop and factor > 1")
    rates, rate = [], float(start)
    while rate <= stop + 1e-9:
        rates.append(int(round(rate)))
        rate *= factor
    if rates[-1] != int(round(stop)):
        rates.append(int(round(stop)))
    return sorted(set(rates))


def boundary_rates(low, high, count):
    """Evenly spaced fine rates in the open interval between the coarse bounds."""
    if low is None or high is None or high <= low or count < 1:
        return []
    step = (high - low) / (count + 1)
    return sorted({int(round(low + step * index)) for index in range(1, count + 1)})


def build_coarse_plan(workers, rates, duration, warmup, repetitions):
    return {"phase": "coarse", "workers": workers, "rates": rates, "duration": duration,
            "warmup": warmup, "repetitions": repetitions}


def build_fine_plans(coarse, workers_labeled, count, duration, warmup, repetitions, fraction=0.25):
    """One longer, repeated plan per worker count, centred on its coarse boundary."""
    plans = []
    for workers in workers_labeled:
        bounds = coarse.get(workers) or {}
        highest, first = bounds.get("highest_passing"), bounds.get("first_failing")
        if highest is None or first is None or first <= highest:
            continue
        low = highest * (1 - fraction)
        high = first * (1 + fraction)
        rates = boundary_rates(low, high, count)
        if not rates:
            continue
        plans.append({"phase": "fine", "workers": workers, "rates": rates, "duration": duration,
                      "warmup": warmup, "repetitions": repetitions})
    return plans


# ---------------------------------------------------------------------------
# Summary, capacity gain and bottleneck evidence

def _spread(values):
    values = [value for value in values if value is not None]
    if not values:
        return {"median": None, "min": None, "max": None, "stdev": None, "spread": None, "count": 0}
    return {"median": statistics.median(values), "min": min(values), "max": max(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
            "spread": max(values) - min(values), "count": len(values)}


def summarize(records):
    """Per (workers, offered) medians and repeat spread for passing runs."""
    groups = collections.defaultdict(list)
    for record in records:
        groups[(record["workers"], record["offered"])].append(record)
    points = []
    for (workers, offered), group in sorted(groups.items()):
        passing = [record for record in group if record["verdict"] == PASS]
        measurements = [record["measurements"] for record in passing]
        def series(key):
            return [measurement.get(key) for measurement in measurements]
        points.append({
            "workers": workers, "offered": offered,
            "passed": len(passing), "runs": len(group),
            "verdict": point_verdict([record["verdict"] for record in group]),
            "steady_committed_per_second": _spread(series("steady_committed_per_second")),
            "steady_websocket_per_second": _spread(series("steady_websocket_per_second")),
            "visible_latency_p99_ms": _spread([(measurement.get("visible_latency_ms") or {}).get("p99")
                                               for measurement in measurements]),
            "websocket_latency_p99_ms": _spread([(measurement.get("websocket_latency_ms") or {}).get("p99")
                                                 for measurement in measurements]),
            "drain_seconds": _spread(series("drain_seconds")),
            "input_to_committed_max": _spread(series("input_to_committed_max")),
            "committed_to_websocket_max": _spread(series("committed_to_websocket_max")),
            "producer_queue_full": _spread(series("producer_queue_full")),
            "worker_cpu_utilization_ratio_max": _spread(series("worker_cpu_utilization_ratio_max")),
            "worker_throttled_periods_ratio_max": _spread(series("worker_throttled_periods_ratio_max")),
            "worker_memory_working_set_bytes_max": _spread(series("worker_memory_working_set_bytes_max")),
        })
    return points


def worker_capacity_gain(points, rules=None, flink_limiter=False):
    """Worker-capacity gain at a shared passing load, reported with total resources."""
    rules = rules or default_rules()
    by_offer = collections.defaultdict(dict)
    for point in points:
        if point["verdict"] == PASS and point["steady_committed_per_second"]["median"] is not None:
            by_offer[point["offered"]][point["workers"]] = point
    gains = []
    for offered, workers in sorted(by_offer.items()):
        if len(workers) < 2:
            continue
        low, high = min(workers), max(workers)
        base = workers[low]["steady_committed_per_second"]["median"]
        grown = workers[high]["steady_committed_per_second"]["median"]
        if not base:
            continue
        gain = grown / base
        gains.append({"offered": offered, "baseline_workers": low, "workers": high,
                      "baseline_per_second": base, "scaled_per_second": grown, "gain": gain,
                      "total_resource_multiplier": high / low,
                      "stretch_gain": rules["stretch_capacity_gain"], "flink_limiter": flink_limiter,
                      "stretch_met": gain >= rules["stretch_capacity_gain"] and flink_limiter})
    return gains


def flink_limiter_evidence(point):
    """Advisory gate: only call a gain a stretch check when Flink is the limiter."""
    cpu = point.get("worker_cpu_utilization_ratio_max", {})
    throttled = point.get("worker_throttled_periods_ratio_max", {})
    backpressure = point.get("producer_queue_full", {})
    utilization = cpu.get("median")
    throttle = throttled.get("median")
    generator_kept_up = point.get("verdict") == PASS or point.get("verdict") == FAIL
    flink_bound = bool((throttle and throttle > 0) or (utilization is not None and utilization >= 0.9))
    return {"flink_bound": flink_bound, "worker_cpu_utilization_ratio": utilization,
            "worker_throttled_periods_ratio": throttle, "producer_queue_full": backpressure.get("median"),
            "generator_kept_up": generator_kept_up,
            "reason": "Flink-bound" if flink_bound else "no worker CPU saturation in telemetry"}


def stage_evidence(record):
    """Per-stage evidence for a claimed bottleneck: backlog plus worker CPU pressure."""
    measurements = record.get("measurements") or run_measurements(record)
    return {"input_to_committed_max": measurements.get("input_to_committed_max"),
            "committed_to_websocket_max": measurements.get("committed_to_websocket_max"),
            "worker_cpu_utilization_ratio_max": measurements.get("worker_cpu_utilization_ratio_max"),
            "worker_throttled_periods_ratio_max": measurements.get("worker_throttled_periods_ratio_max")}


def one_budget_change(base, changed, budget_item):
    """Validate a paired experiment that moves exactly one resource budget.

    ``budget_item`` is one of ``taskmanager_cpu``, ``taskmanager_memory`` or
    ``parallelism``. A valid pair shares offered load, workload shape and policy and
    differs only in that one budget, so a stage move can be attributed to it.
    """
    def budget(record, item):
        limits = record.get("limits") or {}
        return {"taskmanager_cpu": limits.get("aggregate_worker_cpu_quota_cores"),
                "taskmanager_memory": limits.get("aggregate_worker_memory_bytes"),
                "parallelism": record.get("workers")}.get(item)

    base_value, changed_value = budget(base, budget_item), budget(changed, budget_item)
    same_context = all(base.get(key) == changed.get(key) for key in ("offered", "dedup", "late"))
    affected = [key for key in ("taskmanager_cpu", "taskmanager_memory", "parallelism")
                if budget(base, key) != budget(changed, key)]
    valid = bool(same_context and base_value is not None and changed_value is not None
                 and base_value != changed_value and affected == [budget_item])
    return {"budget_item": budget_item, "base_value": base_value, "changed_value": changed_value,
            "same_offered_and_policy": same_context, "differing_budgets": affected, "valid": valid,
            "base": stage_evidence(base), "changed": stage_evidence(changed)}


# ---------------------------------------------------------------------------
# Persistent ledger: failed attempts are retained, never deleted

class Ledger:
    def __init__(self, path):
        self.path = Path(path)

    def records(self):
        if not self.path.exists():
            return []
        entries = []
        for line in self.path.read_text().splitlines():
            if line.strip():
                entries.append(json.loads(line))
        return entries

    def known_ids(self):
        return {entry.get("run_id") for entry in self.records()}

    def append(self, entries):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        known = self.known_ids()
        appended = 0
        with self.path.open("a") as stream:
            for entry in entries:
                if entry.get("run_id") in known:
                    continue
                record = dict(entry)
                record.setdefault("recorded_at_epoch_seconds", time.time())
                stream.write(json.dumps(record) + "\n")
                known.add(entry.get("run_id"))
                appended += 1
        return appended


# ---------------------------------------------------------------------------
# Orchestration through matrix.py (dry-run prints the exact scored commands)

def matrix_command(matrix, *, workers, rates, duration, warmup, repetitions, profile_name, profile_spec,
                   project, symbols=None, skew=None, window_ms=None, grace_ms=None,
                   gateway=True, monitoring=True, seed=42, python=None):
    command = [python or "python3", str(matrix),
               "--project", project,
               "--duration", str(duration), "--warmup", str(warmup),
               "--rates", ",".join(str(rate) for rate in rates),
               "--parallelism", ",".join(str(worker) for worker in ([workers] if isinstance(workers, int) else workers)),
               "--dedup", "NONE", "--late", "DROP", "--qualities", "baseline",
               "--repetitions", str(repetitions),
               "--duplicate-rate", "0", "--late-rate", "0",
               "--resource-profile", f"{profile_name}={profile_spec}",
               "--seed", str(seed)]
    if symbols is not None:
        command += ["--symbols", str(symbols)]
    if skew is not None:
        command += ["--skew", str(skew)]
    if window_ms is not None:
        command += ["--window-ms", str(window_ms)]
    if grace_ms is not None:
        command += ["--grace-ms", str(grace_ms)]
    if gateway:
        command.append("--gateway")
    if monitoring:
        command.append("--monitoring")
    return command


def parse_session(stdout):
    match = re.search(r"Benchmark session ([0-9a-f]+): Compose project (\S+)", stdout or "")
    return match.group(1) if match else None


def read_session(results_dir, session):
    """Load and normalize every run row written by one matrix session."""
    results_dir = Path(results_dir)
    manifest = json.loads((results_dir / f"{session}-manifest.json").read_text())
    rows = list(csv.DictReader((results_dir / f"{session}-matrix.csv").open()))
    records = []
    for row in rows:
        run_id = row["run_id"]
        driver_path = results_dir / f"{run_id}.json"
        driver = json.loads(driver_path.read_text()) if driver_path.exists() else {}
        limits_path = results_dir / f"{run_id}.limits.json"
        limits = json.loads(limits_path.read_text()) if limits_path.exists() else None
        resources_path = results_dir / f"{run_id}.resources.jsonl"
        resources = ([json.loads(line) for line in resources_path.read_text().splitlines() if line.strip()]
                     if resources_path.exists() else None)
        prometheus_path = results_dir / f"{run_id}.prometheus.json"
        prometheus = json.loads(prometheus_path.read_text()) if prometheus_path.exists() else None
        records.append(normalize_run(row, driver, limits, manifest, resources, prometheus))
    return records


def collect_batch(matrix, args, spec, session_suffix):
    command = matrix_command(matrix, workers=spec["workers"], rates=spec["rates"], duration=spec["duration"],
                             warmup=spec["warmup"], repetitions=spec["repetitions"],
                             profile_name=args.profile_name, profile_spec=args.profile_spec,
                             project=f"{args.project_prefix}-{session_suffix}",
                             symbols=args.symbols, skew=args.skew, window_ms=args.window_ms,
                             grace_ms=args.grace_ms, gateway=not args.no_gateway, monitoring=not args.no_monitoring,
                             seed=args.seed)
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    session = parse_session(completed.stdout)
    if session is None:
        raise SystemExit(f"could not find a matrix session in output:\n{completed.stdout[-2000:]}")
    return session, command


def evaluate_records(records, rules, ledger):
    evaluated = [evaluate_run(record, rules) for record in records]
    ledger.append([dict(record) for record in evaluated])
    return evaluated


def write_outputs(evidence_dir, evaluated, rules, coarse, final, source_sessions):
    evidence_dir = Path(evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    points = summarize(evaluated)
    limiter = any(flink_limiter_evidence(point)["flink_bound"] for point in points if point["verdict"] in {PASS, FAIL})
    gain = worker_capacity_gain(points, rules, flink_limiter=limiter)
    payload = {"rules": rules, "generated_at_epoch_seconds": time.time(), "source_sessions": source_sessions,
               "bracket": {str(workers): bounds for workers, bounds in final.items()},
               "coarse_bracket": {str(workers): bounds for workers, bounds in coarse.items()},
               "flink_limiter": limiter, "points": points, "worker_capacity_gain": gain, "runs": evaluated}
    (evidence_dir / "bracket.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", default=str(ROOT / "benchmarks/matrix.py"))
    parser.add_argument("--results-dir", default=str(ROOT / "benchmarks/results"))
    parser.add_argument("--evidence-dir", default=str(ROOT / "benchmarks/evidence/sustainable-limit"))
    parser.add_argument("--ledger", default=str(ROOT / "benchmarks/evidence/sustainable-limit/run-ledger.jsonl"))
    parser.add_argument("--workers", default="1,2", help="comma-separated TaskManager counts")
    parser.add_argument("--coarse-start", type=int, default=1000)
    parser.add_argument("--coarse-stop", type=int, default=32000)
    parser.add_argument("--coarse-factor", type=float, default=2.0)
    parser.add_argument("--duration-coarse", type=float, default=60)
    parser.add_argument("--duration-fine", type=float, default=120)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--repetitions-coarse", type=int, default=1)
    parser.add_argument("--repetitions-fine", type=int, default=3)
    parser.add_argument("--fine-points", type=int, default=3)
    parser.add_argument("--fine-fraction", type=float, default=0.25)
    parser.add_argument("--profile-name", default="perworker")
    parser.add_argument("--profile-spec", required=True,
                        help="per-worker caps held fixed across worker counts, e.g. taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g")
    parser.add_argument("--symbols", type=int, default=1000)
    parser.add_argument("--skew", type=float, default=1.2)
    parser.add_argument("--window-ms", type=int, default=1000)
    parser.add_argument("--grace-ms", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--project-prefix", default="ultrastonk-capacity")
    parser.add_argument("--no-gateway", action="store_true")
    parser.add_argument("--no-monitoring", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="print the scored commands without running anything")
    parser.add_argument("--evaluate-session", action="append", default=[],
                        help="evaluate an existing matrix session id instead of running")
    args = parser.parse_args()

    rules = default_rules()
    ledger = Ledger(args.ledger)
    workers = [int(worker) for worker in args.workers.split(",")]
    coarse = build_coarse_plan(workers, coarse_rates(args.coarse_start, args.coarse_stop, args.coarse_factor),
                               args.duration_coarse, args.warmup, args.repetitions_coarse)

    if args.dry_run:
        command = matrix_command(args.matrix, workers=coarse["workers"], rates=coarse["rates"],
                                 duration=coarse["duration"], warmup=coarse["warmup"],
                                 repetitions=coarse["repetitions"], profile_name=args.profile_name,
                                 profile_spec=args.profile_spec, project=f"{args.project_prefix}-coarse",
                                 symbols=args.symbols, skew=args.skew, window_ms=args.window_ms,
                                 grace_ms=args.grace_ms, gateway=not args.no_gateway,
                                 monitoring=not args.no_monitoring, seed=args.seed)
        plan = {"rules": rules, "coarse": coarse,
                "fine": "derived per worker from the coarse bracket: longer runs at "
                        f"{args.fine_points} rates between highest-passing and first-failing, "
                        f"{args.repetitions_fine} repetitions, {args.duration_fine}s each",
                "coarse_command": command}
        print(json.dumps(plan, indent=2, sort_keys=True))
        return

    if args.evaluate_session:
        sessions = list(args.evaluate_session)
        evaluated = []
        for session in sessions:
            evaluated += evaluate_records(read_session(args.results_dir, session), rules, ledger)
        final = bracket(evaluated)
        payload = write_outputs(args.evidence_dir, evaluated, rules, final, final, sessions)
        print(json.dumps({"bracket": payload["bracket"], "source_sessions": sessions}, indent=2))
        return

    evaluated, sessions = [], []
    session, _ = collect_batch(args.matrix, args, coarse, "coarse")
    sessions.append(session)
    evaluated += evaluate_records(read_session(args.results_dir, session), rules, ledger)
    coarse_bracket = bracket(evaluated)
    for index, spec in enumerate(build_fine_plans(coarse_bracket, workers, args.fine_points,
                                                  args.duration_fine, args.warmup, args.repetitions_fine,
                                                  args.fine_fraction)):
        session, _ = collect_batch(args.matrix, args, spec, f"fine{index}")
        sessions.append(session)
        evaluated += evaluate_records(read_session(args.results_dir, session), rules, ledger)
    final = bracket(evaluated)
    payload = write_outputs(args.evidence_dir, evaluated, rules, coarse_bracket, final, sessions)
    print(json.dumps({"bracket": payload["bracket"], "source_sessions": sessions}, indent=2))


if __name__ == "__main__":
    main()

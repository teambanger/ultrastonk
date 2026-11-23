#!/usr/bin/env python3
"""Live peak-load runner for ticket 07.

This is the experiment harness behind `docs/live-peak.md`. It offers ticket 02's
peak slice to the capture-paced replay at 1x and 1.25x, three valid repeats per
headline condition, in a warm or forced-cold state, on a fresh isolated Compose
project. It collects achieved pacing, expected versus delivered counts per
subscription, stage backlog, latency quantiles with sample sizes, restarts and
drain, compares live results against the independent CSV oracle, and keeps a run
ledger that retains failed and invalid attempts.

The measurement logic is pure and unit-tested; `client` runs inside the
benchmark container and `run` orchestrates Compose on the host.
"""
import argparse
import collections
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import urllib.request
import uuid

try:
    from benchmarks import capture_pilot, peak_slice
except ImportError:  # when executed from inside benchmarks/
    import capture_pilot  # type: ignore
    import peak_slice  # type: ignore

PACING_LEVELS = (1.0, 1.25)
STATES = ("warm", "cold")
DEFAULT_REPEATS = 3
DEFAULT_PARALLELISM = 2
DEFAULT_PARTITIONS = 6
FROZEN_CHECKPOINT_INTERVAL_MS = 10_000
# Project targets, explicitly not rubric requirements.
PROJECT_TARGET_P99_MS = 1000
# A live advisory p99 needs enough crossover receipts to be meaningful.
ADVISORY_MIN_SAMPLES = 20
TRADE_MIN_SAMPLES = 30
SUBSCRIBE_BATCH = 500

ISOLATION_FIELDS = ("COMPOSE_PROJECT_NAME", "KAFKA_INPUT_TOPIC", "KAFKA_OUTPUT_TOPIC",
                    "KAFKA_REJECTED_TOPIC", "FLINK_UI_PORT", "EXPECTED_TASKMANAGERS",
                    "FLINK_BOUNDED_REPLAY")


# ---------------------------------------------------------------------------
# Planning


def plan_runs(pacings=PACING_LEVELS, states=("warm",), repeats=DEFAULT_REPEATS,
              include_rollover=False):
    """Return the pre-registered run list for the headline conditions.

    Pacing level is offered as `SPEED_MULTIPLIER` (`1x` and `1.25x`). Warm is the
    default headline state; `states=("warm", "cold")` runs both, and the rollover
    condition is always a separate labeled fixture.
    """
    if repeats <= 0:
        raise ValueError("repeats must be positive")
    runs = []
    for state in states:
        for pacing in pacings:
            for repeat in range(1, repeats + 1):
                runs.append({
                    "run_id": f"peak-{state}-{pacing:g}x-r{repeat}",
                    "kind": "headline",
                    "condition": f"{state}@{pacing:g}x",
                    "state": state,
                    "pacing_level": float(pacing),
                    "speed_multiplier": float(pacing),
                    "repeat": repeat,
                })
    if include_rollover:
        runs.append({"run_id": "peak-rollover-r1", "kind": "rollover", "condition": "rollover",
                     "state": "warm", "pacing_level": 1.0, "speed_multiplier": 1.0, "repeat": 1})
    return runs


def _topic_suffix(project):
    """Short unique suffix shared by the run's input/output/rejected topics."""
    tail = project.rpartition("-")[2]
    return tail or project


def compose_env(run, slice_manifest, project, parallelism=DEFAULT_PARALLELISM,
                partitions=DEFAULT_PARTITIONS, subscribe_limit=None):
    """Build the isolation env for one run, following the verifier isolation contract.

    Fresh project, input/output/rejected topics that share the project's unique
    suffix, `FLINK_UI_PORT=0` so Docker assigns a collision-free port, a TaskManager
    count equal to the Flink parallelism, and `FLINK_BOUNDED_REPLAY=false` because
    this measures a live paced replay rather than the finite preload-and-process
    mode. The capture emitter offsets come from the slice manifest.
    """
    suffix = _topic_suffix(project)
    env = {
        "COMPOSE_PROJECT_NAME": project,
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_INPUT_TOPIC": f"peak-{suffix}-in",
        "KAFKA_OUTPUT_TOPIC": f"peak-{suffix}-out",
        "KAFKA_REJECTED_TOPIC": f"peak-{suffix}-rejected",
        "KAFKA_GROUP_ID": f"peak-flink-{suffix}",
        "KAFKA_TRANSACTION_PREFIX": f"peak-{suffix}-",
        "GATEWAY_KAFKA_GROUP_ID": f"peak-gateway-{suffix}",
        "KAFKA_PARTITIONS": str(partitions),
        "KAFKA_REPLICATION_FACTOR": "1",
        "KAFKA_MIN_ISR": "1",
        "FLINK_UI_PORT": "0",
        "GATEWAY_PORT": "0",
        "PROMETHEUS_PORT": "0",
        "FLINK_BOUNDED_REPLAY": "false",
        "FLINK_PARALLELISM": str(parallelism),
        "EXPECTED_TASKMANAGERS": str(parallelism),
        "CHECKPOINT_INTERVAL_MS": str(FROZEN_CHECKPOINT_INTERVAL_MS),
        "SOURCE_IDLE_TIMEOUT_MS": "30000",
        "WINDOW_SIZE_MS": "300000",
        "MAX_OUT_OF_ORDER_MS": "5000",
        "DEDUP_STRATEGY": "NONE",
        "LATE_EVENT_STRATEGY": "DROP",
        "ALLOWED_LATENESS_MS": "0",
        "DEDUP_TTL_MS": "3600000",
        "PACING": "capture",
        "MISSING_CAPTURE_TIME": "fail",
        "REPLAY_PREROLL_MS": str(slice_manifest["emitter"]["REPLAY_PREROLL_MS"]),
        "REPLAY_MEASURE_MS": str(slice_manifest["emitter"]["REPLAY_MEASURE_MS"]),
        "SPEED_MULTIPLIER": str(run["speed_multiplier"]),
        "MAX_SLEEP_MS": "0",
        "PACING_REPORT_PATH": f"/results/{run['run_id']}-pacing.json",
        "PACING_SAMPLE_LIMIT": "5000",
        "CSV_PATH": "/data/peak-slice.csv",
        "GATEWAY_KAFKA_FROM_BEGINNING": "true",
        "GATEWAY_HISTORY_LIMIT": "0",
    }
    if subscribe_limit:
        env["MAX_TICKERS_PER_CLIENT"] = str(subscribe_limit)
        env["_GATEWAY_OVERRIDE_MAX_TICKERS"] = str(subscribe_limit)
    return env


def contract_violations(env):
    """Return the isolation-contract fields that a run env fails to satisfy."""
    problems = []
    project = env.get("COMPOSE_PROJECT_NAME")
    if not project or project == "ultrastonk" or project.startswith("ultrastonk-verify-"):
        problems.append("COMPOSE_PROJECT_NAME must be a fresh project outside the verify namespace")
    if env.get("FLINK_UI_PORT") != "0":
        problems.append("FLINK_UI_PORT must be 0 for collision-free binding")
    if str(env.get("FLINK_BOUNDED_REPLAY")).lower() != "false":
        problems.append("FLINK_BOUNDED_REPLAY must be false for a live measurement")
    if env.get("FLINK_PARALLELISM") != env.get("EXPECTED_TASKMANAGERS"):
        problems.append("EXPECTED_TASKMANAGERS must equal FLINK_PARALLELISM")
    suffix = _topic_suffix(project) if project else ""
    topics = {key: str(env.get(key, "")) for key in
              ("KAFKA_INPUT_TOPIC", "KAFKA_OUTPUT_TOPIC", "KAFKA_REJECTED_TOPIC")}
    for key, topic in topics.items():
        if suffix and suffix not in topic:
            problems.append(f"{key} must contain the project suffix {suffix!r}")
    if len(set(topics.values())) != len(topics):
        problems.append("input, output and rejected topics must be distinct")
    return problems


def run_plan_json(fixture_path, slice_manifests, runs, parallelism=DEFAULT_PARALLELISM,
                  subscribe="all"):
    """Assemble the pre-registered plan document (written before any scored run)."""
    return {
        "ticket": "07-measure-live-peak",
        "fixture": str(fixture_path),
        "pacing_levels": [run["pacing_level"] for run in runs if run["kind"] == "headline"],
        "repeats": max((run["repeat"] for run in runs), default=0),
        "states": sorted({run["state"] for run in runs}),
        "parallelism": parallelism,
        "checkpoint_interval_ms": FROZEN_CHECKPOINT_INTERVAL_MS,
        "project_targets": {"trade_enqueue_to_websocket_p99_ms": PROJECT_TARGET_P99_MS,
                            "window_finalization_to_advisory_receipt_p99_ms": PROJECT_TARGET_P99_MS,
                            "note": "project targets, not rubric requirements"},
        "advisory_min_samples": ADVISORY_MIN_SAMPLES,
        "subscribe": subscribe,
        "slices": {state: manifest for state, manifest in slice_manifests.items()},
        "runs": runs,
    }


# ---------------------------------------------------------------------------
# Metrics


def _growing(gaps):
    if not gaps:
        return None
    third = max(1, len(gaps) // 3)
    return gaps[-1] > 0 and (sum(gaps[-third:]) / third) > (sum(gaps[:third]) / third)


def delivered_by_subscription(run):
    """Expected, committed and WebSocket price counts, overall and per ticker."""
    expected = collections.Counter(record["ticker"] for record in run.get("input") or []
                                   if capture_pilot._is_trade(record))
    committed = collections.Counter(record["ticker"] for record in
                                    capture_pilot._prices(run.get("committed") or []))
    websocket = collections.Counter(record["ticker"] for record in
                                    capture_pilot._prices(run.get("websocket") or []))
    tickers = sorted(set(expected) | set(committed) | set(websocket))
    return {
        "expected": sum(expected.values()),
        "committed": sum(committed.values()),
        "websocket": sum(websocket.values()),
        "by_ticker": {ticker: {"expected": expected[ticker], "committed": committed[ticker],
                               "websocket": websocket[ticker]} for ticker in tickers},
    }


def stage_backlog(run):
    """Stage backlog across Kafka (broker lag), Flink (input-minus-output) and gateway."""
    result = {"kafka_lag": None, "gateway_lag": None, "flink_backlog": None,
              "gateway_delivery_gap": None, "flink_backlog_growing": None, "notes": []}
    lag = run.get("consumer_lag") or {}
    result["kafka_lag"] = lag.get("flink_ingest")
    result["gateway_lag"] = lag.get("gateway")
    if result["kafka_lag"] is None:
        result["notes"].append("no broker consumer-group lag telemetry")
    if result["gateway_lag"] is None:
        result["notes"].append("no gateway consumer-group lag telemetry")
    progress = run.get("progress") or []
    if progress:
        input_gaps = [sample.get("input", 0) - sample.get("output", 0) for sample in progress]
        result["flink_backlog"] = {"max": max(input_gaps), "final": input_gaps[-1]}
        result["flink_backlog_growing"] = _growing(input_gaps)
        output_gaps = [sample.get("output", 0) - sample.get("websocket", 0) for sample in progress]
        result["gateway_delivery_gap"] = {"max": max(output_gaps), "final": output_gaps[-1],
                                          "growing": _growing(output_gaps)}
    else:
        result["notes"].append("no progress samples")
    return result


def restart_oom_summary(run):
    """OOM kills and task restart counts, when the telemetry exists."""
    limits = run.get("limits")
    containers = limits.get("containers") if isinstance(limits, dict) else None
    ooms = [container.get("name") for container in containers or [] if container.get("oom_killed")]
    if containers is None and isinstance(limits, dict):
        containers = []
    return {"oom_killed": ooms, "restarts": run.get("restarts"),
            "available": containers is not None and len(containers) > 0}


def drain_summary(run):
    """Seconds from the first committed-input peak to the first delivery peak."""
    if run.get("drain_seconds") is not None:
        return {"available": True, "seconds": run["drain_seconds"]}
    progress = run.get("progress") or []
    if not progress:
        return {"available": False, "seconds": None}

    def first_full(field):
        peak = max(sample.get(field, 0) for sample in progress)
        for sample in progress:
            if sample.get(field, 0) == peak:
                return sample.get("wall_ms", 0), peak
        return None, peak

    input_at, input_peak = first_full("input")
    output_at, output_peak = first_full("output")
    websocket_at, websocket_peak = first_full("websocket")
    if input_peak <= 0 or (output_peak <= 0 and websocket_peak <= 0):
        return {"available": False, "seconds": None}
    delivered_at = max(value for value in (output_at, websocket_at) if value is not None)
    return {"available": True, "seconds": max(0.0, (delivered_at - input_at) / 1000.0),
            "input_full_ms": input_at, "delivered_full_ms": delivered_at}


def checkpoint_cadence(run, tuned=None):
    """Record the frozen checkpoint cadence and, if tuned, its matched comparison."""
    settings = run.get("settings") or {}
    interval = settings.get("checkpoint_interval_ms")
    return {"interval_ms": interval, "frozen": interval == FROZEN_CHECKPOINT_INTERVAL_MS,
            "tuned": bool(tuned), "tuning_comparison": tuned}


def cadence_tuning_comparison(baseline, candidate):
    """Matched visibility/throughput/checkpoint-cost comparison for a cadence change.

    Ticket 05 deferred any latency-driven checkpoint tuning to ticket 07; this
    records the comparison only when a tune was actually made. It never infers a
    win from a single run.
    """
    def metric(summary, name):
        return ((summary.get("measurements") or {}).get(name) or {}).get("p99")

    def throughput(summary):
        values = []
        for record in summary.get("runs") or []:
            value = (record.get("measurements") or {}).get("transport_ms") or {}
            if value.get("count"):
                values.append(value["count"])
        return None

    return {
        "baseline_checkpoint_interval_ms": (baseline.get("checkpoint") or {}).get("interval_ms"),
        "candidate_checkpoint_interval_ms": (candidate.get("checkpoint") or {}).get("interval_ms"),
        "visibility_p99_ms": {"baseline": metric(baseline, "transport_ms"),
                              "candidate": metric(candidate, "transport_ms")},
        "checkpoint_cost": {"baseline": baseline.get("checkpoint_cost"),
                            "candidate": candidate.get("checkpoint_cost")},
        "decision": candidate.get("decision"),
    }


# ---------------------------------------------------------------------------
# Independent latency targets


def _eligibility_lower_bound(records):
    """Return a function mapping a window start to its deterministic eligibility time.

    Mirrors `capture_pilot.window_check`: the watermark can only pass a window
    timer after an input exists whose event time can advance it, so the earliest
    enqueue of such an input is a deterministic lower bound.
    """
    prepared = sorted((int(record["event_ms"]), int(record["emit_ms"]))
                      for record in records
                      if record.get("emit_ms") is not None and record.get("event_ms") is not None)

    import bisect
    events = [event for event, _ in prepared]
    suffix_min = [None] * (len(prepared) + 1)
    for index in range(len(prepared) - 1, -1, -1):
        emit = prepared[index][1]
        later = suffix_min[index + 1]
        suffix_min[index] = emit if later is None else min(emit, later)

    def bound(window_start):
        required = window_start
        index = bisect.bisect_left(events, required)
        return suffix_min[index] if index < len(prepared) else None

    return bound


def advisory_window_latencies(run):
    """Window-finalization-to-advisory-receipt samples, keyed off the window timer."""
    settings = run.get("settings") or {}
    window = int(settings.get("window_ms") or 0)
    if window <= 0:
        return []
    disorder = int(settings.get("max_out_of_order_ms") or 0)
    grace = int(settings.get("allowed_lateness_ms") or 0) if settings.get("late") == "GRACE" else 0
    raw_bound = _eligibility_lower_bound(run.get("input") or [])

    def bound(window_start):
        required = window_start + window + disorder + grace
        return raw_bound(required)

    samples = []
    for record in run.get("websocket") or []:
        if record.get("type") not in ("BUY", "SELL") or record.get("wall_ms") is None:
            continue
        window_start = int(record["event_ms"]) // window * window
        eligible = bound(window_start)
        if eligible is not None:
            samples.append(record["wall_ms"] - eligible)
    return samples


def independence_latencies(run):
    """The two project-target latencies, measured independently."""
    boundaries = capture_pilot._boundary_latencies(run)
    trade = boundaries["enqueue_to_websocket_ms"] or boundaries["enqueue_to_committed_ms"]
    return {"trade_enqueue_to_websocket_ms": list(trade),
            "window_finalization_to_advisory_ms": advisory_window_latencies(run)}


def latency_verdict(samples, min_samples, target_ms=PROJECT_TARGET_P99_MS):
    """Report a latency quantile, or explicitly inconclusive when too sparse."""
    values = [value for value in samples if value is not None]
    if len(values) < min_samples:
        return {"verdict": "inconclusive", "samples": len(values), "min_samples": min_samples,
                "reason": f"too few samples ({len(values)} < {min_samples})", "target_ms": target_ms}
    quantiles = capture_pilot.quantiles(values)
    passed = quantiles["p99"] is not None and quantiles["p99"] <= target_ms
    return {"verdict": "pass" if passed else "fail", "samples": len(values), "target_ms": target_ms,
            "p50": quantiles["p50"], "p95": quantiles["p95"], "p99": quantiles["p99"],
            "max": quantiles["max"]}


def keeping_up_verdict(run, delivery=None, backlog=None, restarts=None):
    """Judge keeping up on correct delivery and no sustained backlog growth.

    Deliberately *not* on eventual parity: a run that only catches up after the
    peak has no headroom. Exact delivery is reported alongside the verdict so the
    two can be read separately.
    """
    delivery = delivery or delivered_by_subscription(run)
    backlog = backlog or stage_backlog(run)
    restarts = restarts or restart_oom_summary(run)
    exact = (delivery["committed"] == delivery["expected"]
             and delivery["websocket"] == delivery["committed"])
    growing = bool(backlog.get("flink_backlog_growing")) or \
        bool((backlog.get("gateway_delivery_gap") or {}).get("growing"))
    pacing = ((run.get("pacing") or {}).get("segments", {}).get("MEASURED", {})
              .get("achieved_ratio"))
    paced = pacing is None or pacing >= 0.98
    oom = bool(restarts.get("oom_killed"))
    kept_up = exact and not growing and paced and not oom
    return {"verdict": "kept_up" if kept_up else "did_not_keep_up",
            "exact_delivery": exact, "sustained_backlog_growth": growing,
            "measured_pacing": pacing, "oom_killed": oom,
            "note": "correct delivery and no sustained backlog growth, not eventual parity"}


def missing_evidence(base):
    """The telemetry the run did not provide, surfaced explicitly."""
    checks = (base or {}).get("checks") or {}
    return list((checks.get("telemetry") or {}).get("missing") or [])


def decisions(run):
    """Report the two project targets independently and never fudge sparse advisories."""
    latencies = independence_latencies(run)
    trade = latency_verdict(latencies["trade_enqueue_to_websocket_ms"], TRADE_MIN_SAMPLES)
    advisory = latency_verdict(latencies["window_finalization_to_advisory_ms"], ADVISORY_MIN_SAMPLES)
    rollover = run.get("rollover")
    return {
        "trade_enqueue_to_websocket": trade,
        "window_finalization_to_advisory_receipt": advisory,
        "live_advisory_p99_ms": (advisory["p99"] if advisory["verdict"] in ("pass", "fail")
                                 else "inconclusive"),
        "rollover_fixture": rollover,
        "note": "1 s p99 is a project target, not a rubric requirement; live advisories are "
                "reported separately from any controlled rollover fixture",
    }


def summarize_run(run, require_limits=True, min_pacing_ratio=0.98):
    """Evaluate one live run and attach the ticket-07 metric groups."""
    base = capture_pilot.evaluate(run, min_pacing_ratio=min_pacing_ratio,
                                  require_limits=require_limits)
    return {
        "run_id": run.get("run_id"),
        "overall_passed": base["overall_passed"],
        "checks": base["checks"],
        "measurements": base["measurements"],
        "settings": base["settings"],
        "limits": base["limits"],
        "clock": base["clock"],
        "failures": base["failures"],
        "subscriptions": delivered_by_subscription(run),
        "stage_backlog": stage_backlog(run),
        "restarts": restart_oom_summary(run),
        "drain": drain_summary(run),
        "checkpoint": checkpoint_cadence(run),
        "decisions": decisions(run),
        "keeping_up": keeping_up_verdict(run),
        "missing_evidence": missing_evidence(base),
    }


def aggregate_repeats(summaries):
    """Median and spread per condition, keeping correctness failures visible."""
    groups = collections.defaultdict(list)
    for summary in summaries:
        groups[(summary.get("condition"), summary.get("state"),
                summary.get("pacing_level"))].append(summary)
    rows = []
    for (condition, state, pacing), group in sorted(groups.items(), key=lambda item: str(item[0])):
        passed = [item for item in group if item.get("overall_passed")]
        trade_p99 = [_p99(item, "trade_enqueue_to_websocket") for item in passed]
        advisory_p99 = [_p99(item, "window_finalization_to_advisory_receipt") for item in passed]
        advisories = [item["decisions"]["live_advisory_p99_ms"] for item in group]
        rows.append({
            "condition": condition, "state": state, "pacing_level": pacing,
            "valid": len(group), "passes": len(passed),
            "trade_p99_ms": _median([value for value in trade_p99 if value is not None]),
            "trade_p99_spread": _spread([value for value in trade_p99 if value is not None]),
            "advisory_p99_ms": _median([value for value in advisory_p99 if value is not None]),
            "advisory_p99_spread": _spread([value for value in advisory_p99 if value is not None]),
            "advisory_verdicts": sorted(set(advisories)),
            "failures": [failure for item in group for failure in item.get("failures", [])],
        })
    return rows


def _p99(summary, decision):
    verdict = (summary.get("decisions") or {}).get(decision) or {}
    return verdict.get("p99") if verdict.get("verdict") in ("pass", "fail") else None


def _median(values):
    return statistics.median(values) if values else None


def _spread(values):
    return (max(values) - min(values)) if len(values) > 1 else (0.0 if values else None)


# ---------------------------------------------------------------------------
# Oracle comparison and deviation split


def _window_key(record, window_ms):
    return record["ticker"], int(record["event_ms"]) // window_ms * window_ms


def count_eligible_by_window(records, window_ms):
    """Eligible trade counts keyed by (ticker, window start)."""
    counts = collections.Counter()
    for record in records:
        if capture_pilot._is_trade(record):
            counts[_window_key(record, window_ms)] += 1
    return counts


def classify_oracle_deviations(report, expected_input, committed_input):
    """Split oracle deviations into policy-related and transport loss.

    A window whose committed inputs are short of the eligible inputs means trades
    never reached the query (transport loss). When the committed inputs match, a
    deviation is the live policy's own decision (for example lateness or dedup),
    not a transport fault.
    """
    policy, transport, unexplained = [], [], []

    def status(key):
        expected, committed = expected_input.get(key, 0), committed_input.get(key, 0)
        if committed < expected:
            return "transport"
        if committed > expected:
            return "policy"
        return "policy"

    def route(key, item, missing=False):
        kind = status(key)
        if kind == "transport":
            transport.append(item | {"classification": "transport loss"})
        elif missing and expected_input.get(key, 0) == 0:
            unexplained.append(item | {"classification": "unexplained (no eligible input)"})
        else:
            policy.append(item | {"classification": "policy-related"})

    for item in report.get("missing", []):
        route((item["symbol"], item["window"]), item, missing=True)
    for item in report.get("extra", []):
        item = item | {"classification": None}
        if committed_input.get((item["symbol"], item["window"]), 0) > expected_input.get(
                (item["symbol"], item["window"]), 0):
            policy.append(item | {"classification": "policy-related (duplicate input)"})
        else:
            unexplained.append(item | {"classification": "unexplained extra"})
    for item in report.get("incorrect", []):
        key = (item["symbol"], item["window"])
        bucket = transport if status(key) == "transport" else policy
        bucket.append(item | {"classification": "transport loss" if status(key) == "transport"
                              else "policy-related"})
    return {
        "match": report.get("match"),
        "policy": policy,
        "transport": transport,
        "unexplained": unexplained,
        "counts": {"policy": len(policy), "transport": len(transport), "unexplained": len(unexplained)},
        "note": "deviations from complete-input truth are reported against the ideal, not as a "
                "live-policy expectation; no extrapolation to full-week capture order",
    }


def count_slice_eligible(slice_path, window_ms, timezone="Europe/Berlin", skip_lines=peak_slice.SKIP_LINES):
    """Per-window eligible-trade counts for the offered slice, using the oracle's rule.

    Mirrors `benchmarks/query_truth.py`: 40 columns, nonblank security type, an
    exchange-qualified symbol, a finite strictly positive `last`, and a trading
    timestamp (columns 26/23) falling back to the system timestamp (2/3), on the
    same zone as the run's event clock.
    """
    import csv
    import math
    import re
    from zoneinfo import ZoneInfo

    zone = ZoneInfo(timezone)
    symbol = re.compile(r"[^\s.]+(?:\.[^\s.]+)*\.[^\s.]+")
    counts = collections.Counter()

    def parse(date, time, fallback_date, fallback_time):
        for date_value, time_value in ((date, time), (fallback_date, fallback_time)):
            if not date_value or not time_value:
                continue
            whole, _, fraction = time_value.partition(".")
            if len(fraction) > 9 or (fraction and not fraction.isdigit()):
                continue
            try:
                moment = __import__("datetime").datetime.strptime(
                    date_value + " " + whole, "%d-%m-%Y %H:%M:%S").replace(tzinfo=zone)
            except ValueError:
                continue
            return int(moment.timestamp() * 1000) + int((fraction + "000000000")[:3])
        return None

    with Path(slice_path).open(newline="", encoding="utf-8") as stream:
        for _ in range(skip_lines):
            next(stream, None)
        for row in csv.reader(stream):
            if len(row) != 40 or not row[1].strip() or not symbol.fullmatch(row[0].strip()):
                continue
            try:
                price = float(row[21])
            except ValueError:
                continue
            if not math.isfinite(price) or price <= 0:
                continue
            stamp = parse(row[26].strip(), row[23].strip(), row[2].strip(), row[3].strip())
            if stamp is None or stamp < 0:
                continue
            counts[(row[0].strip(), stamp // window_ms * window_ms)] += 1
    return counts


def compare_with_oracle(run, slice_path, oracle_path, actual_path, window_ms=300_000,
                        db=None, sample_limit=20):
    """Compare live output to complete-input truth and split the deviations.

    `actual_path` is committed output normalized to the oracle JSONL schema (use
    `reconcile_replay.py normalize`). Deviations are split into transport loss
    (offered eligible trades missing from the committed input) and policy-related
    differences (inputs reached the query).
    """
    try:
        from benchmarks import query_truth_compare
    except ImportError:
        import query_truth_compare  # type: ignore
    report = (query_truth_compare.bounded_compare(oracle_path, actual_path, db, sample_limit)
              if db else query_truth_compare.compare(oracle_path, actual_path))
    expected_input = count_slice_eligible(slice_path, window_ms)
    committed_input = count_eligible_by_window(run.get("input") or [], window_ms)
    return {"oracle_report": report,
            "classification": classify_oracle_deviations(report, expected_input, committed_input)}


# ---------------------------------------------------------------------------
# Run ledger


def new_ledger(phase, created_at=None):
    return {"phase": phase, "created_at_epoch_seconds": created_at or time.time(), "entries": []}


def ledger_entry(run, status, reasons=None, artifacts=None, started=None, finished=None):
    """One attempt. `status` is valid, invalid or failed and every attempt is kept."""
    if status not in ("valid", "invalid", "failed"):
        raise ValueError("status must be valid, invalid or failed")
    return {
        "run_id": run.get("run_id"),
        "condition": run.get("condition"),
        "kind": run.get("kind"),
        "state": run.get("state"),
        "pacing_level": run.get("pacing_level"),
        "repeat": run.get("repeat"),
        "status": status,
        "reasons": list(reasons or []),
        "artifacts": artifacts or {},
        "started_at_epoch_seconds": started,
        "finished_at_epoch_seconds": finished,
    }


def append_entry(ledger, entry):
    ledger.setdefault("entries", []).append(entry)
    return entry


def ledger_status_counts(ledger):
    counts = collections.Counter(entry["status"] for entry in ledger.get("entries", []))
    return {status: counts.get(status, 0) for status in ("valid", "invalid", "failed")}


def write_ledger(ledger, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledger, indent=2) + "\n")


def load_ledger(path):
    return json.loads(Path(path).read_text())


# ---------------------------------------------------------------------------
# Live collection (container client and host orchestrator)


def _request(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def client_collect(args):
    """In-container collector: batched subscription across committed input/output/WebSocket."""
    from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
    from websockets.sync.client import connect

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "peak-in")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "peak-out")
    tickers = [ticker for ticker in (args.tickers or "").split(",") if ticker]
    if not tickers:
        raise SystemExit("--tickers is required so the subscriber watches the slice before publication")

    capture_pilot.wait_for_pipeline(args.gateway_url)
    socket = connect(args.gateway_url, open_timeout=30, close_timeout=1)
    for offset in range(0, len(tickers), SUBSCRIBE_BATCH):
        batch = tickers[offset:offset + SUBSCRIBE_BATCH]
        socket.send(json.dumps({"type": "subscribe", "tickers": batch}))
        pending = set(batch)
        deadline = time.monotonic() + 30
        while pending:
            frame = json.loads(socket.recv(timeout=max(0, deadline - time.monotonic())))
            if frame.get("type") != "history" or frame.get("ticker") not in pending:
                raise RuntimeError(f"unexpected subscription frame: {frame!r}")
            pending.remove(frame["ticker"])

    consumer = Consumer({"bootstrap.servers": brokers, "group.id": "peak-collect-" + args.run_id,
                         "enable.auto.commit": False, "isolation.level": "read_committed",
                         "auto.offset.reset": "earliest"})
    metadata = consumer.list_topics(timeout=15)
    consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)
                     for topic in (input_topic, output_topic)
                     for partition in metadata.topics[topic].partitions])

    def integer(name):
        value = os.getenv(name)
        return int(value) if value not in (None, "") else None

    speed = os.getenv("SPEED_MULTIPLIER")
    run = {
        "run_id": args.run_id,
        "settings": {"window_ms": integer("WINDOW_SIZE_MS"),
                     "max_out_of_order_ms": integer("MAX_OUT_OF_ORDER_MS"),
                     "allowed_lateness_ms": integer("ALLOWED_LATENESS_MS"),
                     "checkpoint_interval_ms": integer("CHECKPOINT_INTERVAL_MS"),
                     "flink_parallelism": integer("FLINK_PARALLELISM"),
                     "dedup": os.getenv("DEDUP_STRATEGY"),
                     "late": os.getenv("LATE_EVENT_STRATEGY"),
                     "speed_multiplier": float(speed) if speed else None},
        "input": [], "committed": [], "websocket": [], "ws_errors": [],
        "watermark_samples": [], "progress": [], "job_running": False,
        "limits": json.loads(Path(args.limits).read_text()) if args.limits else None,
        "clock": {"basis": "same_host_wall", "max_clock_error_ms": args.max_clock_error_ms},
        "consumer_lag": {},
    }

    def read_message(timeout):
        message = consumer.poll(timeout)
        if message is None or message.error():
            return
        value = json.loads(message.value())
        now = time.time() * 1000
        if message.topic() == input_topic:
            if value.get("id") in tickers:
                run["input"].append({"ticker": value["id"], "event_ms": value["eventTimestampMs"],
                                     "value": value.get("last"), "emit_ms": value.get("emitTimestampMs"),
                                     "wall_ms": now})
        elif set(value) == {"timestamp_emit", "ticker", "type", "value"} and value["ticker"] in tickers:
            run["committed"].append({"ticker": value["ticker"], "type": value["type"],
                                     "event_ms": value["timestamp_emit"], "value": value["value"],
                                     "wall_ms": now})

    def read_websocket():
        try:
            raw = socket.recv(timeout=1)
        except TimeoutError:
            return
        value = json.loads(raw)
        if value.get("ticker") in tickers and value.get("type") not in {"history", "error"}:
            run["websocket"].append({"ticker": value["ticker"], "type": value["type"],
                                     "event_ms": value["timestamp_emit"], "value": value["value"],
                                     "wall_ms": time.time() * 1000})
        elif value.get("type") == "error":
            run["ws_errors"].append(value)

    def sample_watermark():
        observed_at = time.time() * 1000
        try:
            for job in _request("http://jobmanager:8081/jobs/overview")["jobs"]:
                run["job_running"] = run["job_running"] or job["state"] == "RUNNING"
                if job["state"] != "RUNNING":
                    continue
                for vertex in _request("http://jobmanager:8081/jobs/" + job["jid"])["vertices"]:
                    values = _request(f"http://jobmanager:8081/jobs/{job['jid']}/vertices/{vertex['id']}/watermarks")
                    for value in values:
                        observed = int(value["value"])
                        if 0 < observed < 9_000_000_000_000_000_000:
                            run["watermark_samples"].append({"wall_ms": observed_at, "watermark": observed})
        except (OSError, ValueError):
            pass

    import threading
    stop = threading.Event()

    def consume_loop():
        while not stop.is_set():
            try:
                read_message(.1)
            except Exception as error:
                run["ws_errors"].append(f"consumer: {error!r}")
                return

    def websocket_loop():
        while not stop.is_set():
            try:
                read_websocket()
            except Exception as error:
                run["ws_errors"].append(f"websocket: {error!r}")
                return

    threads = [threading.Thread(target=consume_loop, daemon=True),
               threading.Thread(target=websocket_loop, daemon=True)]
    for thread in threads:
        thread.start()

    deadline = time.monotonic() + args.timeout
    last_data = time.monotonic()
    next_progress = next_watermark = time.monotonic()
    counts = (len(run["input"]), len(run["committed"]), len(run["websocket"]))
    timed_out = True
    while time.monotonic() < deadline:
        now = time.monotonic()
        current = (len(run["input"]), len(run["committed"]), len(run["websocket"]))
        if current != counts:
            counts = current
            last_data = now
        if now >= next_progress:
            run["progress"].append({"wall_ms": time.time() * 1000,
                                    **dict(zip(("input", "output", "websocket"), counts))})
            next_progress = now + 1
        if now >= next_watermark:
            sample_watermark()
            next_watermark = now + 1
        if args.pacing_report and Path(args.pacing_report).exists() and now - last_data >= args.settle:
            timed_out = False
            break
        time.sleep(.05)
    stop.set()
    for thread in threads:
        thread.join(2)
    consumer.close()
    socket.close()
    if timed_out:
        raise RuntimeError("peak run timed out before the replay settled")
    if args.pacing_report and Path(args.pacing_report).exists():
        run["pacing"] = json.loads(Path(args.pacing_report).read_text())
    if args.restarts is not None:
        run["restarts"] = json.loads(Path(args.restarts).read_text())
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / (args.run_id + "-run.json")).write_text(json.dumps(run, indent=2) + "\n")
    return run


def slice_tickers(path, limit=None):
    """Distinct subscribed tickers present in a slice, most frequent first."""
    with Path(path).open(newline="", encoding="utf-8") as stream:
        for _ in range(peak_slice.SKIP_LINES):
            next(stream, None)
        counts = collections.Counter()
        import csv as _csv
        for row in _csv.reader(stream):
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                counts[row[0].strip()] += 1
    ordered = [ticker for ticker, _ in counts.most_common()]
    return ordered[:limit] if limit else ordered


def gateway_override(project, subscribe_limit):
    """Compose override that raises the gateway's per-client ticker cap for the peak slice."""
    return {"services": {"gateway": {"environment": {"MAX_TICKERS_PER_CLIENT": str(subscribe_limit)}}}}


def host_commands(run, env, slice_path, project, override_path, results_dir, tickers):
    """The ordered host steps for one live run (no Compose state is touched here)."""
    compose = ["docker", "compose", "-p", project, "-f", "compose.yaml", "-f", str(override_path)]
    base = {**os.environ, **env}
    limits_path = Path(results_dir) / f"{run['run_id']}-limits.json"
    steps = [
        {"note": "start the isolated pipeline with fresh topics",
         "argv": compose + ["up", "-d", "--build", "kafka", "kafka-init", "jobmanager",
                            "taskmanager", "gateway"], "env": base},
        {"note": "build the collector image",
         "argv": compose + ["--profile", "benchmark", "build", "benchmark"], "env": base},
        {"note": "capture effective container limits before the run",
         "argv": ["bash", "-c", f"docker compose -p {project} ps -q | xargs docker inspect > {limits_path}"],
         "env": base},
        {"note": "subscribe the client to the whole slice before publication",
         "argv": compose + ["--profile", "benchmark", "run", "-d", "--name", f"{project}-client",
                            "--no-deps", "-v", f"{Path.cwd()}/benchmarks:/benchmarks:ro",
                            "--entrypoint", "python", "benchmark",
                            "/benchmarks/live_peak.py", "client", "--run-id", run["run_id"],
                            "--tickers", ",".join(tickers),
                            "--pacing-report", env["PACING_REPORT_PATH"],
                            "--limits", f"/results/{run['run_id']}-limits.json"],
         "env": base, "detached": True},
        {"note": "offer the slice at capture pace",
         "argv": compose + ["--profile", "emitter", "run", "--rm", "--user",
                            f"{os.getuid()}:{os.getgid()}", "event-emitter"],
         "env": {**base, "CSV_PATH": f"/data/{Path(slice_path).name}",
                 "REPLAY_DATA_DIR": str(Path(slice_path).parent)}},
        {"note": "wait for the collector and keep its log",
         "argv": ["bash", "-c", f"docker wait {project}-client; docker logs {project}-client"],
         "env": base},
    ]
    return steps


def execute_run(run, env, slice_path, project, override_path, results_dir, tickers):
    """Write the override, run the steps, and return per-step outcomes."""
    Path(override_path).parent.mkdir(parents=True, exist_ok=True)
    Path(override_path).write_text(json.dumps(gateway_override(project, len(tickers))))
    outcomes = []
    for step in host_commands(run, env, slice_path, project, override_path, results_dir, tickers):
        completed = subprocess.run(step["argv"], cwd=Path.cwd(), env=step["env"], text=True,
                                   capture_output=True)
        outcomes.append({"note": step["note"], "exit_code": completed.returncode,
                         "stdout_tail": completed.stdout[-4000:], "stderr_tail": completed.stderr[-4000:]})
        if completed.returncode != 0:
            break
    artifact = Path(results_dir) / f"{run['run_id']}-run.json"
    if artifact.exists():
        recorded = json.loads(artifact.read_text())
        recorded.setdefault("condition", run.get("condition"))
        recorded.setdefault("state", run.get("state"))
        recorded.setdefault("pacing_level", run.get("pacing_level"))
        recorded.setdefault("kind", run.get("kind"))
        artifact.write_text(json.dumps(recorded, indent=2) + "\n")
    return outcomes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="write the pre-registered run plan")
    plan.add_argument("--fixture", type=Path, required=True)
    plan.add_argument("--slice-manifest", action="append", required=True,
                      help="STATE=path to a slice manifest produced by peak_slice.py")
    plan.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    plan.add_argument("--states", default="warm")
    plan.add_argument("--include-rollover", action="store_true")
    plan.add_argument("--output", type=Path)

    client = commands.add_parser("client", help="run the in-container collector")
    client.add_argument("--run-id", required=True)
    client.add_argument("--tickers", required=True)
    client.add_argument("--pacing-report")
    client.add_argument("--limits")
    client.add_argument("--restarts")
    client.add_argument("--gateway-url", default="ws://gateway:8080/")
    client.add_argument("--output-dir", default="/results")
    client.add_argument("--max-clock-error-ms", type=int, default=250)
    client.add_argument("--settle", type=float, default=15)
    client.add_argument("--timeout", type=float, default=3600)

    summarize = commands.add_parser("summarize", help="evaluate collected run artifacts")
    summarize.add_argument("--run", action="append", required=True, help="path to RUN-run.json")
    summarize.add_argument("--ledger", type=Path)
    summarize.add_argument("--output", type=Path)

    execute = commands.add_parser("run", help="execute the planned runs against isolated Compose projects")
    execute.add_argument("--fixture", type=Path, required=True)
    execute.add_argument("--slice", action="append", required=True, help="STATE=path/to/slice.csv")
    execute.add_argument("--slice-manifest", action="append", required=True, help="STATE=path/to/manifest.json")
    execute.add_argument("--results-dir", type=Path, default=Path("benchmarks/evidence/live-peak/runs"))
    execute.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    execute.add_argument("--states", default="warm")
    execute.add_argument("--include-rollover", action="store_true")
    execute.add_argument("--parallelism", type=int, default=DEFAULT_PARALLELISM)
    execute.add_argument("--partitions", type=int, default=DEFAULT_PARTITIONS)
    execute.add_argument("--subscribe", default="all", help="all or topN")
    execute.add_argument("--dry-run", action="store_true")
    execute.add_argument("--ledger", type=Path, default=Path("benchmarks/evidence/live-peak/ledger.json"))

    compare = commands.add_parser("compare", help="compare live output to the CSV oracle")
    compare.add_argument("--run", type=Path, required=True, help="RUN-run.json from the collector")
    compare.add_argument("--slice", type=Path, required=True, help="the offered slice CSV")
    compare.add_argument("--oracle", type=Path, required=True, help="query_truth.py JSONL over the slice")
    compare.add_argument("--actual", type=Path, required=True, help="normalized committed output JSONL")
    compare.add_argument("--window-ms", type=int, default=300_000)
    compare.add_argument("--db", type=Path, help="disk-backed comparison database for slice-scale files")
    compare.add_argument("--output", type=Path)

    args = parser.parse_args(argv)
    if args.command == "plan":
        fixture, start, end = peak_slice.load_fixture(args.fixture)
        states = tuple(state for state in args.states.split(",") if state)
        if args.include_rollover:
            states = states or ("warm",)
        runs = plan_runs(states=states, repeats=args.repeats,
                         include_rollover=args.include_rollover)
        manifests = {}
        for item in args.slice_manifest:
            state, _, path = item.partition("=")
            manifests[state] = json.loads(Path(path).read_text())["slice"]
        document = run_plan_json(args.fixture, manifests, runs)
        text = json.dumps(document, indent=2) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text)
        print(text)
        return 0
    if args.command == "client":
        client_collect(args)
        return 0
    if args.command == "summarize":
        summaries = []
        for path in args.run:
            run = json.loads(Path(path).read_text())
            summary = summarize_run(run)
            summary["condition"] = run.get("condition")
            summary["state"] = run.get("state")
            summary["pacing_level"] = run.get("pacing_level")
            summaries.append(summary)
        report = {"runs": summaries, "aggregate": aggregate_repeats(summaries)}
        if args.ledger:
            ledger = new_ledger("scored")
            for summary in summaries:
                status = "valid" if summary["overall_passed"] else "invalid"
                append_entry(ledger, ledger_entry(summary, status,
                                                  reasons=[failure.get("reason") for failure in summary["failures"]]))
            write_ledger(ledger, args.ledger)
            report["ledger"] = str(args.ledger)
        text = json.dumps(report, indent=2) + "\n"
        if args.output:
            args.output.write_text(text)
        print(text)
        return 0
    if args.command == "run":
        fixture, start, end = peak_slice.load_fixture(args.fixture)
        slices = dict(item.partition("=")[::2] for item in args.slice)
        manifests = {item.partition("=")[0]: json.loads(Path(item.partition("=")[2]).read_text())["slice"]
                     for item in args.slice_manifest}
        states = tuple(state for state in args.states.split(",") if state) or ("warm",)
        runs = plan_runs(states=states, repeats=args.repeats, include_rollover=args.include_rollover)
        limit = int(args.subscribe[3:]) if args.subscribe.startswith("top") else None
        args.results_dir.mkdir(parents=True, exist_ok=True)
        ledger = new_ledger("scored")
        for run in runs:
            slice_path = slices[run["state"]]
            manifest = manifests[run["state"]]
            project = f"ultrastonk-peak-{uuid.uuid4().hex[:10]}"
            tickers = slice_tickers(slice_path, limit)
            env = compose_env(run, manifest, project, args.parallelism, args.partitions,
                              subscribe_limit=len(tickers))
            violations = contract_violations(env)
            override_path = args.results_dir / f"{project}-override.yaml"
            if violations:
                append_entry(ledger, ledger_entry(run, "invalid", reasons=violations))
                continue
            if args.dry_run:
                print(json.dumps({"run_id": run["run_id"], "project": project,
                                  "tickers": len(tickers), "env": env,
                                  "steps": host_commands(run, env, slice_path, project, override_path,
                                                         args.results_dir, tickers)},
                                 indent=2))
                append_entry(ledger, ledger_entry(run, "invalid", reasons=["dry run only"]))
                continue
            started = time.time()
            outcomes = execute_run(run, env, slice_path, project, override_path, args.results_dir, tickers)
            finished = time.time()
            artifacts = {"run_json": str(args.results_dir / f"{run['run_id']}-run.json"),
                         "project": project, "outcomes": outcomes}
            status = "failed" if any(step["exit_code"] for step in outcomes) else "invalid"
            append_entry(ledger, ledger_entry(run, status, reasons=[step["note"] for step in outcomes
                                                                    if step["exit_code"]], artifacts=artifacts,
                                              started=started, finished=finished))
        if args.dry_run and args.ledger:
            write_ledger(ledger, args.ledger)
        return 0
    if args.command == "compare":
        run = json.loads(args.run.read_text())
        result = compare_with_oracle(run, args.slice, args.oracle, args.actual,
                                     args.window_ms, args.db)
        text = json.dumps(result, indent=2) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text)
        print(text)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Predictive capacity model for the Ultrastonk week workload (pure analysis).

This is the predictive half of the evaluation. It reads only evidence that
already exists on disk, fits a small and deliberately honest model to the
measured points, and prints (and optionally writes) a prediction of wall time
and resource need for a stated workload volume. It starts no container and
generates no load.

Measured points it reads (every number is traced to a path in
``docs/capacity-model.md``):

* the ticket 02 week profile (row volumes, the recorded five-minute raw peak,
  the trade share, the top-symbol concentration);
* the ticket 06 run-2 whole-week job (the only direct whole-week end-to-end
  measurement: input rows, ingestion wall time, processing wall time, output
  volume, broker footprint);
* the ticket 08 sustainable-limit ladder under
  ``benchmarks/evidence/sustainable-limit/`` and ``benchmarks/results/<session>-*``.
  When those files are absent every 08-derived number is reported as
  ``PENDING`` and the code path stays ready;
* the ticket 03 and 11 artifacts, restated only as measured penalties.

The maths is implemented as side-effect-free functions so it can be unit tested
without any artifact; ``main()`` is the only IO and the only place a report is
emitted. Run ``python3 -m unittest discover -s benchmarks/tests`` for the tests.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import re
from pathlib import Path

PENDING = "PENDING"

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_WEEK_PROFILE = Path("/mnt/shared/data/official-week-Ij8jlfeA/results/week-profile.json")
DEFAULT_WEEK_RUN = Path("/mnt/shared/data/ultrastonk-full-week/run2")
DEFAULT_BRACKET = ROOT / "benchmarks/evidence/sustainable-limit/bracket.json"
DEFAULT_LEDGER = ROOT / "benchmarks/evidence/sustainable-limit/run-ledger.jsonl"
DEFAULT_RESULTS = ROOT / "benchmarks/results"
DEFAULT_DROP_GRACE = ROOT / "benchmarks/evidence/drop-vs-grace/comparison.json"

# The measured configuration ran two TaskManagers (ticket 06, FLINK_PARALLELISM=2).
WEEK_RUN_WORKERS = 2
# Ticket 08 fixes a per-worker cap, so total worker resources grow with worker
# count. Per-worker caps mean total resources grow with worker count; every
# resource prediction states the aggregate, not just the per-worker cap.
PER_WORKER_CPU_CORES = 1.0
PER_WORKER_MEMORY_BYTES = 2 * 1024 ** 3  # 2 GiB, ticket 08 fixed profile

# Directional band applied to a single-repetition point estimate.
DIRECTIONAL_FRACTION = 0.20


# ---------------------------------------------------------------------------
# Pure maths


def safe_ratio(numerator, denominator):
    """``numerator / denominator`` or ``None`` when the inputs cannot carry it."""
    if numerator is None or denominator is None:
        return None
    if not isinstance(denominator, (int, float)) or denominator <= 0:
        return None
    return numerator / denominator


def rows_per_second(rows, seconds):
    """Achieved rows per second, or ``None`` for a degenerate input."""
    if rows is None or rows < 0:
        return None
    return safe_ratio(rows, seconds)


def seconds_for_volume(rows, per_second):
    """Wall seconds to move ``rows`` at ``per_second``, or ``None``."""
    if rows is None or rows < 0:
        return None
    return safe_ratio(rows, per_second)


def per_worker_rate(rate, workers):
    """Aggregate rate divided by worker count (linear-scaling-implied per worker)."""
    if rate is None or not workers or workers <= 0:
        return None
    return rate / workers


def scaling_efficiency(rate, base_rate, workers, base_workers=1):
    """Achieved gain divided by the resource gain, relative to a baseline.

    ``1.0`` means throughput grew exactly with the worker budget; ``0.5`` means
    half of the added worker budget became throughput.
    """
    if rate is None or base_rate is None or not workers or not base_workers:
        return None
    if base_rate <= 0 or workers <= 0 or base_workers <= 0:
        return None
    return (rate / base_rate) / (workers / base_workers)


def scaling_bounds(rows, aggregate_rate, measured_workers, workers):
    """Bound the wall time at ``workers`` from one measured point.

    Two monotonicity assumptions hold for a real pipeline and need no second
    point: more workers cannot lower aggregate throughput (``R_p >= R_m`` for
    ``p >= m``), and adding workers cannot more than proportionally raise it
    (``R_p <= (p/m) R_m``). Those give a bracket around ``rows / R_m``:

    * ``p >= m``: wall in ``[T_m * m/p, T_m]``;
    * ``p <= m``: wall in ``[T_m, T_m * m/p]``.
    """
    if rows is None or aggregate_rate is None or not measured_workers or not workers:
        return None
    base = seconds_for_volume(rows, aggregate_rate)
    if base is None:
        return None
    ratio = measured_workers / workers
    return {"low": base * min(1.0, ratio), "high": base * max(1.0, ratio)}


def monotone_fit(ys):
    """Non-decreasing isotonic (PAVA) fit of a sequence.

    Used to keep a throughput-vs-load fit monotone when measured points are
    noisy. For an already non-decreasing input it is the identity.
    """
    if not ys:
        return []
    blocks = []  # each [value, weight]
    for value in ys:
        blocks.append([float(value), 1])
        while len(blocks) > 1 and blocks[-2][0] > blocks[-1][0]:
            v2, w2 = blocks.pop()
            v1, w1 = blocks.pop()
            blocks.append([(v1 * w1 + v2 * w2) / (w1 + w2), w1 + w2])
    fitted = []
    for value, weight in blocks:
        fitted.extend([value] * weight)
    return fitted


def interpolate_linear(x, points):
    """Piecewise-linear interpolation of ``points`` = [(x, y), ...] at ``x``.

    Values outside the measured range clamp to the nearest measured point, which
    is the honest behaviour for a model with no calibration beyond them.
    """
    if not points:
        return None
    ordered = sorted(points, key=lambda pair: pair[0])
    if len(ordered) == 1:
        return ordered[0][1]
    if x <= ordered[0][0]:
        return ordered[0][1]
    if x >= ordered[-1][0]:
        return ordered[-1][1]
    for (x0, y0), (x1, y1) in zip(ordered, ordered[1:]):
        if x0 <= x <= x1:
            if x1 == x0:
                return y1
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return ordered[-1][1]


def throughput_at(offered, points):
    """Interpolated achieved input rate at an offered rate from ladder points."""
    pairs = [(point["offered"], point["achieved_per_second"])
             for point in points
             if point.get("offered") is not None and point.get("achieved_per_second") is not None]
    return interpolate_linear(offered, pairs)


def latency_at(offered, points, key="p99"):
    """Interpolated latency percentile at an offered rate from ladder points."""
    pairs = []
    for point in points:
        latency = point.get("latency_ms") or {}
        if point.get("offered") is not None and latency.get(key) is not None:
            pairs.append((point["offered"], latency[key]))
    return interpolate_linear(offered, pairs)


def directional_range(value, fraction=DIRECTIONAL_FRACTION):
    """A single-repetition uncertainty band around a point estimate."""
    if value is None:
        return {"point": None, "low": None, "high": None, "fraction": fraction}
    return {"point": value, "low": value * (1 - fraction), "high": value * (1 + fraction),
            "fraction": fraction}


def prediction_range(seconds, bounds, measured, fraction=DIRECTIONAL_FRACTION):
    """The honest range for a prediction.

    At the measured worker count the monotone bracket is degenerate, so a
    single-repetition band is reported instead. Away from it, the monotone
    bracket is the range.
    """
    if seconds is None:
        return {"point": None, "low": None, "high": None, "basis": "none"}
    if bounds and bounds["low"] != bounds["high"]:
        return {"point": seconds, "low": bounds["low"], "high": bounds["high"], "basis": "monotone-bounds"}
    band = directional_range(seconds, fraction)
    return {"point": seconds, "low": band["low"], "high": band["high"], "basis": "one-repetition-band"}


def resource_envelope(rows, workers, *, per_worker_cpu_cores, per_worker_memory_bytes,
                      disk_bytes_per_row=None, output_bytes_per_row=None,
                      output_records_per_row=None):
    """Worker CPU/memory quota and disk footprint for a volume of ``rows``."""
    total_cpu = None if (workers is None or per_worker_cpu_cores is None) else workers * per_worker_cpu_cores
    total_memory = None if (workers is None or per_worker_memory_bytes is None) else workers * per_worker_memory_bytes
    return {
        "rows": rows,
        "workers": workers,
        "per_worker_cpu_cores": per_worker_cpu_cores,
        "per_worker_memory_bytes": per_worker_memory_bytes,
        "total_worker_cpu_cores": total_cpu,
        "total_worker_memory_bytes": total_memory,
        "disk_bytes_per_million_rows": None if disk_bytes_per_row is None else disk_bytes_per_row * 1e6,
        "disk_bytes": None if (rows is None or disk_bytes_per_row is None) else rows * disk_bytes_per_row,
        "output_bytes": None if (rows is None or output_bytes_per_row is None) else rows * output_bytes_per_row,
        "output_records": None if (rows is None or output_records_per_row is None) else rows * output_records_per_row,
    }


def peak_headroom(peak_rate, capacity_rate):
    """How many times the measured capacity exceeds a peak arrival rate."""
    if peak_rate is None or capacity_rate is None or peak_rate <= 0:
        return None
    return capacity_rate / peak_rate


def human_bytes(value):
    if value is None:
        return PENDING
    step = 1024.0
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < step or unit == "TiB":
            return f"{value:.2f} {unit}"
        value /= step
    return f"{value:.2f} TiB"


def human_seconds(value):
    if value is None:
        return PENDING
    if value < 120:
        return f"{value:.1f} s"
    minutes, seconds = divmod(value, 60)
    if minutes < 120:
        return f"{int(minutes)} min {seconds:.0f} s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)} h {int(minutes)} min"


# ---------------------------------------------------------------------------
# Artifact loaders (IO; not unit tested against live hosts)


def _read_json(path):
    path = Path(path)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _strip_z(value):
    return value[:-1] + "+00:00" if value.endswith("Z") else value


def _parse_iso(value):
    if not value:
        return None
    try:
        return _dt.datetime.fromisoformat(_strip_z(value))
    except ValueError:
        return None


def load_week_profile(path=DEFAULT_WEEK_PROFILE):
    """Ticket 02 workload shapes, or a PENDING skeleton when the file is absent."""
    document = _read_json(path)
    if not document:
        return {"path": str(path), "present": False}
    files = document.get("files") or []
    rows_total = sum(file_.get("rows", 0) for file_ in files)
    peak = document.get("raw_peak") or {}
    peak_rows = peak.get("rows")
    trade = (document.get("row_categories") or {}).get("trade")
    emitted = (document.get("replay_screening") or {}).get("emitted")
    return {
        "path": str(path),
        "present": True,
        "rows_read": rows_total,
        "rows_emitted": emitted,
        "peak_rows": peak_rows,
        "peak_rate_per_second": rows_per_second(peak_rows, 300) if peak_rows else None,
        "peak_start": peak.get("start"),
        "trade_rows": trade,
        "trade_share": safe_ratio(trade, emitted),
        "top_symbol_shares": document.get("top_symbol_shares") or {},
        "days": [{"key": file_.get("key"), "rows": file_.get("rows"), "size": file_.get("size")}
                 for file_ in files],
    }


def load_week_run(path=DEFAULT_WEEK_RUN):
    """Ticket 06 run-2 whole-week measurement, or a PENDING skeleton."""
    root = Path(path)
    if not root.exists():
        return {"path": str(root), "present": False}

    emitter = {"submitted": None, "ev_per_second": None, "read_rows": None, "emitted_rows": None}
    emitter_log = root / "logs/emitter.log"
    if emitter_log.exists():
        text = emitter_log.read_text(errors="replace")
        done = re.search(r"Done:\s*([\d,]+)\s*submitted,\s*(\d+)\s*failed,\s*([\d,]+)\s*ev/s", text)
        total = re.search(r"Total:\s*read\s*([\d,]+)\s*->\s*emitted\s*([\d,]+)", text)
        if done:
            emitter["submitted"] = int(done.group(1).replace(",", ""))
            emitter["ev_per_second"] = int(done.group(3).replace(",", ""))
        if total:
            emitter["read_rows"] = int(total.group(1).replace(",", ""))
            emitter["emitted_rows"] = int(total.group(2).replace(",", ""))

    emitter_wall = None
    inspect = _read_json(root / "logs/emitter_inspect.json")
    if isinstance(inspect, list):
        inspect = inspect[0] if inspect else None
    if inspect:
        state = inspect.get("State") or {}
        started, finished = _parse_iso(state.get("StartedAt")), _parse_iso(state.get("FinishedAt"))
        if started and finished:
            emitter_wall = (finished - started).total_seconds()

    job_seconds = None
    jobmanager = root / "logs/jobmanager.log"
    if jobmanager.exists():
        text = jobmanager.read_text(errors="replace")
        running = re.search(r'"timestamp":"([^"]+)"[^\n]*switched from state CREATED to RUNNING', text)
        finished = re.search(r'"timestamp":"([^"]+)"[^\n]*switched from state RUNNING to FINISHED', text)
        if running and finished:
            start, end = _parse_iso(running.group(1)), _parse_iso(finished.group(1))
            if start and end:
                job_seconds = (end - start).total_seconds()

    normalize = _read_json(root / "evidence/normalize.json") or {}
    counts = normalize.get("counts") or {}
    output_path = root / "evidence/output.jsonl"
    output_bytes = output_path.stat().st_size if output_path.exists() else None

    dump_seconds = None
    dump = _read_json(root / "logs/dump-output.log")
    if dump:
        dump_seconds = dump.get("seconds")

    kafka_bytes = None
    kafka_file = root / "logs/kafka_data_bytes.txt"
    if kafka_file.exists():
        first = kafka_file.read_text().split()[0]
        if first.isdigit():
            kafka_bytes = int(first)

    submitted = emitter.get("submitted")
    return {
        "path": str(root),
        "present": True,
        "workers": WEEK_RUN_WORKERS,
        "submitted": submitted,
        "emitter_ev_per_second": emitter.get("ev_per_second"),
        "emitter_wall_seconds": emitter_wall,
        "processing_seconds": job_seconds,
        "processing_rate_per_second": rows_per_second(submitted, job_seconds) if job_seconds else None,
        "dump_seconds": dump_seconds,
        "output_records": counts.get("records"),
        "output_prices": counts.get("prices"),
        "output_bytes": output_bytes,
        "broker_bytes": kafka_bytes,
    }


def _bracket_points(bracket):
    points = []
    for entry in bracket.get("points") or []:
        steady = entry.get("steady_committed_per_second") or {}
        visible = entry.get("visible_latency_ms") or {}
        websocket = entry.get("websocket_latency_p99_ms") or {}
        points.append({
            "workers": entry.get("workers"),
            "offered": entry.get("offered"),
            "verdict": entry.get("verdict"),
            "achieved_per_second": steady.get("median"),
            "latency_ms": {"p50": visible.get("p50"), "p95": visible.get("p95"),
                           "p99": visible.get("p99") if visible.get("p99") is not None else websocket.get("median")},
            "drain_seconds": (entry.get("drain_seconds") or {}).get("median"),
            "worker_cpu_utilization_ratio": (entry.get("worker_cpu_utilization_ratio_max") or {}).get("median"),
            "worker_throttled_periods_ratio": (entry.get("worker_throttled_periods_ratio_max") or {}).get("median"),
            "worker_memory_working_set_bytes": (entry.get("worker_memory_working_set_bytes_max") or {}).get("median"),
        })
    return points


def _to_number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    number = _to_number(value)
    return int(number) if number is not None else None


def load_matrix_points(results_dir, sessions):
    """Minimal ticket 08 points from raw ``<session>-matrix.csv`` files.

    Used only when ``bracket.json`` has not been written yet and the caller names
    the capacity sessions explicitly, so nothing is guessed from unrelated runs.
    The bracket's richer aggregates stay the primary source.
    """
    if not sessions:
        return []
    points = []
    for session in sessions:
        path = Path(results_dir) / f"{session}-matrix.csv"
        if not path.exists():
            continue
        with path.open() as stream:
            for row in csv.DictReader(stream):
                passed = (row.get("price_correctness_passed") or "").strip().lower() == "true"
                points.append({
                    "workers": _to_int(row.get("parallelism")),
                    "offered": _to_number(row.get("target_rate")),
                    "verdict": "PASS" if passed else None,
                    "achieved_per_second": _to_number(row.get("committed_prices_per_second"))
                    or _to_number(row.get("achieved_originals_per_second")),
                    "latency_ms": {"p50": _to_number(row.get("latency_p50_ms")),
                                   "p95": _to_number(row.get("latency_p95_ms")),
                                   "p99": _to_number(row.get("latency_p99_ms"))},
                    "drain_seconds": _to_number(row.get("drain_seconds")),
                    "worker_cpu_utilization_ratio": None,
                    "worker_throttled_periods_ratio": None,
                    "worker_memory_working_set_bytes": None,
                    "source": str(path),
                })
    return points


def load_bracket(bracket_path=DEFAULT_BRACKET, ledger_path=DEFAULT_LEDGER, results_dir=DEFAULT_RESULTS,
                 sessions=None):
    """Ticket 08 ladder points, or a PENDING marker when no scored run exists.

    Prefers ``bracket.json`` (the canonical output of ``capacity_bracket.py``).
    If it is absent and the caller names the scored ``--session`` ids, the raw
    ``<session>-matrix.csv`` rows are used instead. Otherwise everything derived
    from ticket 08 stays ``PENDING``.
    """
    bracket = _read_json(bracket_path)
    if not bracket:
        points = load_matrix_points(results_dir, sessions)
        return {"path": str(bracket_path), "present": bool(points), "points": points,
                "source_sessions": list(sessions or []), "bracket": {}, "worker_capacity_gain": []}
    return {
        "path": str(bracket_path),
        "present": True,
        "points": _bracket_points(bracket),
        "source_sessions": bracket.get("source_sessions") or [],
        "bracket": bracket.get("bracket") or {},
        "worker_capacity_gain": bracket.get("worker_capacity_gain") or [],
        "flink_limiter": bracket.get("flink_limiter"),
        "plan_decisions": bracket.get("plan_decisions") or [],
        "results_dir": str(results_dir),
    }


def load_policy_penalties(drop_grace_path=DEFAULT_DROP_GRACE):
    """Ticket 11 measured drop-versus-grace penalties, or PENDING."""
    comparison = _read_json(drop_grace_path)
    if not comparison:
        return {"path": str(drop_grace_path), "present": False}
    lag = comparison.get("finalization_lag_ms") or {}
    return {
        "path": str(drop_grace_path),
        "present": True,
        "finalization_lag_ms": {"DROP": lag.get("DROP"), "GRACE": lag.get("GRACE")},
        "vs_truth": comparison.get("vs_truth"),
        "note": "measured policy penalty, not a new capacity claim",
    }


# ---------------------------------------------------------------------------
# Model assembly and prediction


def build_model(week_profile, week_run, bracket, policy_penalties=None):
    """Combine the measured points into the fitted model (pure)."""
    weeks = []
    if week_run.get("present") and week_run.get("processing_rate_per_second"):
        weeks.append({
            "name": "week-run2",
            "regime": "offline-preload",
            "workers": week_run["workers"],
            "rows": week_run["submitted"],
            "processing_seconds": week_run["processing_seconds"],
            "rate_per_second": week_run["processing_rate_per_second"],
            "source": week_run["path"],
        })

    ladder = bracket.get("points", [])
    ladder_rates = [point for point in ladder if point.get("workers") and point.get("achieved_per_second")]

    per_worker = None
    efficiency = None
    aggregate = weeks[0]["rate_per_second"] if weeks else None
    measured_workers = weeks[0]["workers"] if weeks else None
    if aggregate is not None:
        per_worker = per_worker_rate(aggregate, measured_workers)
        # A single point cannot separate the per-worker rate from scaling
        # efficiency; efficiency stays PENDING until ticket 08 adds a worker count.
        efficiency = None

    # With two or more ladder worker counts, fit per-worker rate and efficiency.
    if len(ladder_rates) >= 2:
        ladder_rates.sort(key=lambda point: point["workers"])
        baseline = ladder_rates[0]
        efficiency = scaling_efficiency(ladder_rates[-1]["achieved_per_second"],
                                        baseline["achieved_per_second"],
                                        ladder_rates[-1]["workers"], baseline["workers"])

    return {
        "week_profile": week_profile,
        "week_run": week_run,
        "bracket": bracket,
        "policy_penalties": policy_penalties or {"present": False},
        "weeks": weeks,
        "ladder": ladder,
        "ladder_present": bool(ladder_rates),
        "per_worker_rate_per_second": per_worker,
        "measured_aggregate_rate_per_second": aggregate,
        "measured_workers": measured_workers,
        "scaling_efficiency": efficiency,
    }


def predict(model, rows, workers, *, disk_bytes_per_row=None, output_bytes_per_row=None,
            output_records_per_row=None):
    """Predict wall time and resources for ``rows`` at ``workers`` (pure).

    The point estimate scales the measured per-worker batch rate linearly with
    the worker count. It deliberately does **not** use the ladder's scaling
    efficiency: the ladder measures the live regime, the week anchor measures the
    batch regime, and mixing them would double-count. The monotone bracket bounds
    the linear point instead.
    """
    per_worker = model.get("per_worker_rate_per_second")

    processing_seconds = None
    if per_worker is not None and workers and workers > 0:
        processing_seconds = seconds_for_volume(rows, per_worker * workers)

    bounds = scaling_bounds(rows, model.get("measured_aggregate_rate_per_second"),
                            model.get("measured_workers"), workers)

    envelope = resource_envelope(
        rows, workers,
        per_worker_cpu_cores=PER_WORKER_CPU_CORES,
        per_worker_memory_bytes=PER_WORKER_MEMORY_BYTES,
        disk_bytes_per_row=disk_bytes_per_row,
        output_bytes_per_row=output_bytes_per_row,
        output_records_per_row=output_records_per_row,
    )
    return {
        "rows": rows,
        "workers": workers,
        "predicted_processing_seconds": processing_seconds,
        "predicted_processing_range": prediction_range(
            processing_seconds, bounds, workers == model.get("measured_workers")),
        "predicted_processing_bounds": bounds,
        "resources": envelope,
    }


# ---------------------------------------------------------------------------
# Reporting


def default_disk_rates(week_run):
    """Per-row disk and output rates derived from the week run (or all ``None``)."""
    rows = week_run.get("submitted")
    return {
        "disk_bytes_per_row": safe_ratio(week_run.get("broker_bytes"), rows),
        "output_bytes_per_row": safe_ratio(week_run.get("output_bytes"), rows),
        "output_records_per_row": safe_ratio(week_run.get("output_records"), rows),
    }


def _notes(model):
    notes = [
        "One repetition (ticket 06) anchors the fit: per-worker rate and every wall time are directional, not validated.",
        "The week run is preload-and-process: its processing rate is a batch drain ceiling, not a live arrival ceiling.",
        "Throughput is assumed independent of volume; a much larger volume may change cache/state behaviour.",
        "Peak headroom is compared against the batch drain rate; the direct peak test is ticket 07 (pending).",
    ]
    if not model.get("ladder_present"):
        notes.insert(1, "All ticket 08 ladder numbers (offered sweep, throttling, scaling gain) are PENDING.")
    if model.get("scaling_efficiency") is None and model.get("ladder_present"):
        notes.insert(1, "Scaling efficiency needs at least two measured worker counts.")
    return notes


def format_report(model, predictions, *, peak_fit=None, notes=None):
    """Human-readable report. ``predictions`` is a list of labelled dicts."""
    lines = []
    lines.append("Ultrastonk capacity model (predictive; pure analysis of existing artifacts)")
    lines.append("=" * 78)

    profile = model["week_profile"]
    lines.append("Workload shapes (ticket 02)")
    if profile.get("present"):
        lines.append(f"  rows read (week)       : {profile['rows_read']:,}")
        lines.append(f"  rows emitted (week)    : {profile['rows_emitted']:,}")
        lines.append(f"  recorded raw peak      : {profile['peak_rate_per_second']:.1f} rows/s"
                     f" ({profile['peak_rows']:,} rows in 300 s, {profile['peak_start']})")
        if profile.get("trade_share") is not None:
            lines.append(f"  trade share            : {profile['trade_share'] * 100:.2f}% of emitted rows")
        shares = profile.get("top_symbol_shares") or {}
        if shares:
            lines.append(f"  top-symbol share       : top1 {shares.get('1', 0) * 100:.2f}%,"
                         f" top10 {shares.get('10', 0) * 100:.2f}%, top100 {shares.get('100', 0) * 100:.2f}%")
    else:
        lines.append(f"  {PENDING}: {profile.get('path')}")

    run = model["week_run"]
    lines.append("")
    lines.append("Whole-week measured point (ticket 06 run 2)")
    if run.get("present"):
        lines.append(f"  input rows             : {run['submitted']:,}")
        lines.append(f"  emitter wall           : {human_seconds(run['emitter_wall_seconds'])}"
                     f" (generator-side; {run['emitter_ev_per_second']:,} ev/s offered)")
        lines.append(f"  Flink processing wall  : {human_seconds(run['processing_seconds'])}")
        lines.append(f"  processing rate        : {run['processing_rate_per_second']:,.0f} rows/s"
                     f" at {run['workers']} workers")
        lines.append(f"  output records         : {run['output_records']:,}"
                     f" ({human_bytes(run['output_bytes'])} raw JSONL)")
        lines.append(f"  broker footprint       : {human_bytes(run['broker_bytes'])}")
    else:
        lines.append(f"  {PENDING}: {run.get('path')}")

    lines.append("")
    lines.append("Fit")
    per_worker = model.get("per_worker_rate_per_second")
    if per_worker is not None:
        lines.append(f"  per-worker service rate: {per_worker:,.0f} rows/s"
                     f" (implied by the {model['measured_workers']}-worker point; linear-scaling implied)")
    else:
        lines.append(f"  per-worker service rate: {PENDING}")
    if model.get("scaling_efficiency") is not None:
        lines.append(f"  scaling efficiency     : {model['scaling_efficiency']:.3f}"
                     f" (fitted from {len(model['ladder'])} ladder points)")
    else:
        lines.append(f"  scaling efficiency     : {PENDING} (ticket 08 ladder absent)")

    for prediction in predictions:
        lines.append("")
        lines.append(prediction["label"])
        seconds = prediction.get("predicted_processing_seconds")
        band = prediction.get("predicted_processing_range") or {}
        if seconds is None:
            lines.append(f"  processing wall        : {PENDING}")
        else:
            lines.append(f"  processing wall        : {human_seconds(seconds)}"
                         f" (range {human_seconds(band.get('low'))} .. {human_seconds(band.get('high'))},"
                         f" {band.get('basis')})")
        resources = prediction.get("resources") or {}
        lines.append(f"  worker CPU quota       : {resources.get('total_worker_cpu_cores', PENDING)} cores"
                     f" ({resources.get('workers', PENDING)} x {resources.get('per_worker_cpu_cores', PENDING)})")
        lines.append(f"  worker memory quota    : {human_bytes(resources.get('total_worker_memory_bytes'))}")
        lines.append(f"  disk (broker)          : {human_bytes(resources.get('disk_bytes'))}")
        output_records = resources.get("output_records")
        output_note = f" ({output_records:,.0f} records)" if output_records is not None else ""
        lines.append(f"  output bytes (raw)     : {human_bytes(resources.get('output_bytes'))}{output_note}")

    if peak_fit:
        lines.append("")
        lines.append("Recorded-peak headroom")
        lines.append(f"  peak rate              : {peak_fit['peak_rate']:.1f} rows/s")
        lines.append(f"  measured batch capacity: {peak_fit['capacity_rate']:,.0f} rows/s"
                     f" -> headroom {peak_fit['headroom']:.1f}x")
        lines.append(f"  direct headroom test   : {peak_fit['verdict']}")

    if notes:
        lines.append("")
        lines.append("What this cannot support")
        for note in notes:
            lines.append(f"  - {note}")
    return "\n".join(lines)


def json_report(model, predictions, *, peak_fit=None, notes=None):
    """Machine-readable report; only JSON-safe values."""
    return {
        "model": {
            "week_profile": model.get("week_profile"),
            "week_run": model.get("week_run"),
            "per_worker_rate_per_second": model.get("per_worker_rate_per_second"),
            "measured_aggregate_rate_per_second": model.get("measured_aggregate_rate_per_second"),
            "measured_workers": model.get("measured_workers"),
            "ladder_present": model.get("ladder_present"),
            "scaling_efficiency": model.get("scaling_efficiency"),
            "per_worker_cpu_cores": PER_WORKER_CPU_CORES,
            "per_worker_memory_bytes": PER_WORKER_MEMORY_BYTES,
            "policy_penalties": model.get("policy_penalties"),
        },
        "predictions": predictions,
        "peak_fit": peak_fit,
        "notes": notes or [],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--week-profile", default=str(DEFAULT_WEEK_PROFILE))
    parser.add_argument("--week-run", default=str(DEFAULT_WEEK_RUN))
    parser.add_argument("--bracket", default=str(DEFAULT_BRACKET))
    parser.add_argument("--results-dir", default=str(DEFAULT_RESULTS))
    parser.add_argument("--session", action="append", default=[],
                        help="ticket 08 matrix session id to read if bracket.json is absent")
    parser.add_argument("--volume", type=int, help="target input row count")
    parser.add_argument("--fraction", type=float, help="fraction of the official week volume (e.g. 0.5)")
    parser.add_argument("--multiple", type=float, help="multiple of the official week volume (e.g. 3)")
    parser.add_argument("--workers", default="1,2", help="comma-separated worker counts to predict")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a human report")
    parser.add_argument("--out", help="also write the report (JSON if --json) to this path")
    args = parser.parse_args(argv)

    profile = load_week_profile(args.week_profile)
    week_run = load_week_run(args.week_run)
    bracket = load_bracket(args.bracket, results_dir=args.results_dir, sessions=args.session)
    penalties = load_policy_penalties()
    model = build_model(profile, week_run, bracket, penalties)

    rates = default_disk_rates(week_run)
    week_rows = profile.get("rows_emitted") or week_run.get("submitted")

    targets = []
    if args.volume is not None:
        targets.append(("target volume", args.volume))
    if args.fraction is not None and week_rows:
        targets.append((f"{args.fraction:g}x week volume", week_rows * args.fraction))
    if args.multiple is not None and week_rows:
        targets.append((f"{args.multiple:g}x week volume", week_rows * args.multiple))
    if not targets and week_rows:
        targets.append(("official week volume", week_rows))
        targets.append(("3x week volume", week_rows * 3))

    workers = [int(worker) for worker in args.workers.split(",")]
    predictions = []
    for label, rows in targets:
        for worker_count in workers:
            prediction = predict(model, rows, worker_count, **rates)
            prediction["label"] = f"{label} at {worker_count} worker(s)"
            predictions.append(prediction)

    peak_fit = None
    capacity = model.get("measured_aggregate_rate_per_second")
    peak_rate = profile.get("peak_rate_per_second")
    if capacity and peak_rate:
        peak_fit = {
            "peak_rate": peak_rate,
            "capacity_rate": capacity,
            "headroom": peak_headroom(peak_rate, capacity),
            "verdict": "fits by the week-point batch bound; direct confirmation pending ticket 07/08",
        }

    notes = _notes(model)
    if args.json:
        payload = json_report(model, predictions, peak_fit=peak_fit, notes=notes)
        text = json.dumps(payload, indent=2, sort_keys=True)
    else:
        text = format_report(model, predictions, peak_fit=peak_fit, notes=notes)

    print(text)
    if args.out:
        Path(args.out).write_text(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

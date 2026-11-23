#!/usr/bin/env python3
"""Full-week reference reconciliation for the production Flink query (ticket 06).

This tool turns one completed capture-order (or event-time-ordered) bounded Historical
replay into a fail-closed reconciliation:

* source accounting per day and per exchange, from `reconcile_replay.scan`;
* a disk-backed comparison of committed Kafka output against the independent CSV oracle
  at `(symbol, window, type)`, including the final window, with explicit missing, extra,
  numeric, duplicate and wrong-direction error counts;
* separate preprocessing, ingestion, processing and drain timings read from the run's
  timestamp markers;
* a disk/retention preflight with the measured broker and checkpoint footprint.

Raw CSVs, Kafka data and scratch databases stay outside Git. Only the summary document
and the machine-readable artifacts under `benchmarks/evidence/full-week/` are committed.
"""
import argparse
import collections
import concurrent.futures
import json
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from zoneinfo import ZoneInfo

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks import reconcile_replay

exchange_of = reconcile_replay.exchange_of

MODES = reconcile_replay.MODES
DEFAULT_WINDOW_MS = reconcile_replay.DEFAULT_WINDOW_MS

LOADER_KEYS = ("emitted", "dropped_width", "dropped_no_symbol", "dropped_no_time",
               "dropped_timestamp", "malformed_prices", "fallback_events")
QUERY_KEYS = ("eligible_trades", "invalid_symbol_rows", "nonpositive_or_missing_last_rows")
REVERSAL_KEYS = ("events", "eligible_trades", "beyond_disorder_budget", "max_ms",
                 "eligible_max_ms")
ERROR_KEYS = ("missing", "extra", "numeric", "duplicate", "wrong_direction")


def window_day(window_ms, zone):
    """ISO date of a five-minute window start in the event-time zone."""
    return datetime.fromtimestamp(window_ms // 1000, tz=zone).date().isoformat()


# ---------------------------------------------------------------------------
# Source accounting aggregation

def _empty_source():
    return {
        "rows_read": 0,
        "loader": {key: 0 for key in LOADER_KEYS},
        "query": {key: 0 for key in QUERY_KEYS},
        "reversals": {key: 0 for key in REVERSAL_KEYS},
        "windows": {"count": 0, "final_nonempty": None, "counts": {}},
    }


def _empty_exchange():
    return {"emitted": 0,
            "query": {key: 0 for key in QUERY_KEYS},
            "windows": {}}


def _accumulate_source(acc, scan):
    acc["rows_read"] += scan.get("rows_read", 0)
    for key in LOADER_KEYS:
        acc["loader"][key] += scan.get("loader", {}).get(key, 0)
    for key in QUERY_KEYS:
        acc["query"][key] += scan.get("query", {}).get(key, 0)
    reversals = scan.get("reversals", {})
    for key in REVERSAL_KEYS:
        value = reversals.get(key, 0) or 0
        if key in ("max_ms", "eligible_max_ms"):
            acc["reversals"][key] = max(acc["reversals"][key], value)
        else:
            acc["reversals"][key] += value
    windows = scan.get("windows", {})
    counts = windows.get("counts", {})
    acc["windows"]["count"] += len(counts)
    for window, window_counts in counts.items():
        slot = acc["windows"]["counts"].setdefault(window, {"rows": 0, "trades": 0})
        slot["rows"] += window_counts.get("rows", 0)
        slot["trades"] += window_counts.get("trades", 0)
    final = windows.get("final_nonempty")
    if final is not None:
        acc["windows"]["final_nonempty"] = max(acc["windows"]["final_nonempty"] or final, final)


def _accumulate_exchange(acc, exchange):
    acc["emitted"] += exchange.get("emitted", 0)
    acc["query"]["eligible_trades"] += exchange.get("eligible_trades", 0)
    acc["query"]["invalid_symbol_rows"] += exchange.get("invalid_symbol_rows", 0)
    acc["query"]["nonpositive_or_missing_last_rows"] += exchange.get(
        "nonpositive_or_missing_last_rows", 0)
    for window, window_counts in exchange.get("windows", {}).items():
        slot = acc["windows"].setdefault(window, {"rows": 0, "trades": 0})
        slot["rows"] += window_counts.get("rows", 0)
        slot["trades"] += window_counts.get("trades", 0)


def merge_source_scans(entries):
    """Merge per-file scans into totals, per-day and per-exchange reconciliation.

    `entries` is a list of `{"day": ISO date, "scan": scan_source result}`. Distinct symbol
    counts are sums over files, so `query.eligible_symbols` is an upper bound; use it only
    as a size indicator.
    """
    totals = _empty_source()
    per_day = {}
    per_exchange = {}
    for entry in entries:
        day = entry["day"]
        scan = entry["scan"]
        _accumulate_source(totals, scan)
        _accumulate_source(per_day.setdefault(day, _empty_source()), scan)
        for exchange, counts in scan.get("exchanges", {}).items():
            _accumulate_exchange(per_exchange.setdefault(exchange, _empty_exchange()), counts)
    return {"totals": totals, "per_day": per_day, "per_exchange": per_exchange}


# ---------------------------------------------------------------------------
# Full comparison with error taxonomy and per-group reconciliation

def _empty_counts():
    return {"expected": 0, "actual": 0, "missing": 0, "extra": 0,
            "numeric": 0, "duplicate": 0, "wrong_direction": 0}


def _validate_and_load(path, connection, source, zone):
    with Path(path).open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("record must be an object")
                symbol, window, kind = (record[field] for field in ("symbol", "window", "type"))
                if not isinstance(symbol, str) or not symbol or type(window) is not int:
                    raise ValueError("symbol must be a nonempty string and window an integer")
                if kind not in ("EMA", "BUY", "SELL"):
                    raise ValueError("type must be EMA, BUY, or SELL")
                day = window_day(window, zone)
                exchange = exchange_of(symbol)
                if kind == "EMA":
                    for field in ("ema38", "ema100"):
                        value = record[field]
                        if type(value) not in (int, float) or not math.isfinite(value):
                            raise ValueError(f"{field} must be a finite number")
                    connection.execute(
                        "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?) "
                        "ON CONFLICT(source, symbol, window, type) DO UPDATE SET n = n + 1",
                        (source, symbol, window, kind, record["ema38"], record["ema100"], day, exchange))
                else:
                    connection.execute(
                        "INSERT INTO records VALUES (?, ?, ?, ?, NULL, NULL, 1, ?, ?) "
                        "ON CONFLICT(source, symbol, window, type) DO UPDATE SET n = n + 1",
                        (source, symbol, window, kind, day, exchange))
            except (ValueError, KeyError, TypeError, OverflowError) as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc


def _sample(connection, sql, parameters):
    return [dict(zip(("symbol", "window", "type", "count"), row))
            for row in connection.execute(sql, parameters)]


def _group_stats(connection, column):
    stats = {}

    def entry(group):
        return stats.setdefault(group, _empty_counts())

    for group, source, total in connection.execute(
            f"SELECT {column}, source, COALESCE(SUM(n), 0) FROM records "
            f"GROUP BY {column}, source"):
        entry(group)["expected" if source == "expected" else "actual"] += total
    for group, count in connection.execute(
            f"SELECT e.{column}, COUNT(*) FROM records e WHERE e.source='expected' AND NOT EXISTS "
            "(SELECT 1 FROM records a WHERE a.source='actual' AND a.symbol=e.symbol "
            f"AND a.window=e.window AND a.type=e.type) GROUP BY e.{column}"):
        entry(group)["missing"] += count
    for group, count in connection.execute(
            f"SELECT a.{column}, COUNT(*) FROM records a WHERE a.source='actual' AND NOT EXISTS "
            "(SELECT 1 FROM records e WHERE e.source='expected' AND e.symbol=a.symbol "
            f"AND e.window=a.window AND e.type=a.type) GROUP BY a.{column}"):
        entry(group)["extra"] += count
    for group, count in connection.execute(
            f"SELECT {column}, COUNT(*) FROM records WHERE n > 1 GROUP BY {column}"):
        entry(group)["duplicate"] += count
    for group, e38, e100, a38, a100 in connection.execute(
            f"SELECT e.{column}, e.ema38, e.ema100, a.ema38, a.ema100 FROM records e JOIN records a "
            "ON a.source='actual' AND a.symbol=e.symbol AND a.window=e.window AND a.type=e.type "
            "WHERE e.source='expected' AND e.type='EMA'"):
        if (not math.isclose(e38, a38, rel_tol=1e-9, abs_tol=1e-9)
                or not math.isclose(e100, a100, rel_tol=1e-9, abs_tol=1e-9)):
            entry(group)["numeric"] += 1
    for group, count in connection.execute(
            f"SELECT e.{column}, COUNT(*) FROM records e JOIN records a ON a.source='actual' "
            "AND a.symbol=e.symbol AND a.window=e.window AND a.type IN ('BUY','SELL') "
            "WHERE e.source='expected' AND e.type IN ('BUY','SELL') AND e.type <> a.type "
            f"GROUP BY e.{column}"):
        entry(group)["wrong_direction"] += count
    return stats


def compare_full(expected, actual, db_path, zone, sample_limit=20):
    """Disk-backed comparison at (symbol, window, type) with per-day/per-exchange grouping."""
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute(
        "CREATE TABLE records (source TEXT NOT NULL, symbol TEXT NOT NULL, window INTEGER NOT NULL, "
        "type TEXT NOT NULL, ema38 REAL, ema100 REAL, n INTEGER NOT NULL, day TEXT NOT NULL, "
        "exchange TEXT NOT NULL, PRIMARY KEY (source, symbol, window, type))")
    try:
        _validate_and_load(str(expected), connection, "expected", zone)
        _validate_and_load(str(actual), connection, "actual", zone)
        connection.commit()
    except Exception:
        connection.close()
        raise

    per_day = _group_stats(connection, "day")
    per_exchange = _group_stats(connection, "exchange")
    counts = {key: sum(stats[key] for stats in per_day.values()) for key in ERROR_KEYS}
    records = {source: connection.execute(
        "SELECT COALESCE(SUM(n), 0) FROM records WHERE source = ?", (source,)).fetchone()[0]
        for source in ("expected", "actual")}

    missing = _sample(
        connection,
        "SELECT e.symbol, e.window, e.type, e.n FROM records e WHERE e.source='expected' AND "
        "NOT EXISTS (SELECT 1 FROM records a WHERE a.source='actual' AND a.symbol=e.symbol AND "
        "a.window=e.window AND a.type=e.type) ORDER BY e.window, e.symbol, e.type LIMIT ?",
        (sample_limit,))
    extra = _sample(
        connection,
        "SELECT a.symbol, a.window, a.type, a.n FROM records a WHERE a.source='actual' AND "
        "NOT EXISTS (SELECT 1 FROM records e WHERE e.source='expected' AND e.symbol=a.symbol AND "
        "e.window=a.window AND e.type=a.type) ORDER BY a.window, a.symbol, a.type LIMIT ?",
        (sample_limit,))
    incorrect = []
    for symbol, window, expected38, expected100, actual38, actual100 in connection.execute(
            "SELECT e.symbol, e.window, e.ema38, e.ema100, a.ema38, a.ema100 FROM records e JOIN "
            "records a ON a.source='actual' AND a.symbol=e.symbol AND a.window=e.window AND "
            "a.type=e.type WHERE e.source='expected' AND e.type='EMA' ORDER BY e.window, e.symbol"):
        if (not math.isclose(expected38, actual38, rel_tol=1e-9, abs_tol=1e-9)
                or not math.isclose(expected100, actual100, rel_tol=1e-9, abs_tol=1e-9)):
            if len(incorrect) < sample_limit:
                incorrect.append({"symbol": symbol, "window": window, "type": "EMA",
                                  "expected": [expected38, expected100],
                                  "actual": [actual38, actual100]})
    wrong_direction = [dict(zip(("symbol", "window", "expected", "actual"), row))
                       for row in connection.execute(
            "SELECT e.symbol, e.window, e.type, a.type FROM records e JOIN records a ON "
            "a.source='actual' AND a.symbol=e.symbol AND a.window=e.window AND "
            "a.type IN ('BUY','SELL') WHERE e.source='expected' AND e.type IN ('BUY','SELL') AND "
            "e.type <> a.type ORDER BY e.window, e.symbol LIMIT ?", (sample_limit,))]
    duplicates = [{"source": source, "symbol": symbol, "window": window, "type": kind, "count": count}
                  for source, symbol, window, kind, count in connection.execute(
            "SELECT source, symbol, window, type, n FROM records WHERE n > 1 ORDER BY source, "
            "window, symbol, type LIMIT ?", (sample_limit,))]
    expected_final = connection.execute(
        "SELECT MAX(window) FROM records WHERE source='expected' AND type='EMA'").fetchone()[0]
    actual_final = connection.execute(
        "SELECT MAX(window) FROM records WHERE source='actual' AND type='EMA'").fetchone()[0]
    connection.close()

    return {
        "match": not any(counts.values()),
        "counts": counts,
        "records": records,
        "expected_final_window": expected_final,
        "actual_final_window": actual_final,
        "per_day": per_day,
        "per_exchange": per_exchange,
        "missing": missing,
        "extra": extra,
        "incorrect": incorrect,
        "duplicates": duplicates,
        "wrong_direction": wrong_direction,
        "sample_limit": sample_limit,
    }


# ---------------------------------------------------------------------------
# Fail-closed gate

def evaluate(manifest):
    """Fail closed unless accounting, coverage and zero-error comparison all hold."""
    def section(name):
        value = manifest.get(name)
        return value if isinstance(value, dict) else {}

    source, emitter, output = section("source"), section("emitter"), section("output")
    audit, comparison, errors = section("audit"), section("comparison"), manifest.get("errors")
    input_section = section("input")
    counts = comparison.get("counts") or {}
    expected_final = manifest.get("expected_final_window")
    checks = {
        "mode_declared": manifest.get("mode") in MODES,
        "source_nonempty": source.get("rows_read", 0) > 0 and source.get("eligible_trades", 0) > 0,
        "emitter_complete": (emitter.get("complete") is True and emitter.get("failed") == 0
                             and emitter.get("emitted") == emitter.get("acknowledged")),
        "input_complete": (input_section.get("complete") is True
                           and input_section.get("records") == emitter.get("emitted")),
        "audit_complete": audit.get("complete") is True,
        "zero_late_exclusions": audit.get("late") == 0,
        "zero_rejected_inputs": audit.get("rejected") == 0,
        "committed_prices_match": output.get("prices") == source.get("eligible_trades"),
        "output_complete": output.get("complete") is True,
        "oracle_exact": (comparison.get("match") is True and "error" not in comparison
                         and all(counts.get(key) == 0 for key in ERROR_KEYS)),
        "final_window_present": (expected_final is not None
                                 and output.get("final_window") == expected_final),
        "no_errors": not errors,
    }
    return all(checks.values()), checks


# ---------------------------------------------------------------------------
# Parallel per-partition drain

def dump_parallel(bootstrap, topic, output_path, timeout=1800, slack=4):
    """Consume a committed topic with one reader per partition, then concatenate in order.

    Mirrors reconcile_replay.dump_topic's read_committed completeness rule per partition:
    transaction control records occupy offsets but are not delivered, so completion allows a
    small control tail. A separate consumer per partition removes the single-consumer drain
    bottleneck for the 60M+ record out topic.
    """
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as exc:
        raise ValueError("confluent_kafka is required for dump; run inside the benchmark image") from exc

    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": "fullweek-probe-" + uuid.uuid4().hex[:8]})
    try:
        metadata = probe.list_topics(topic, timeout=15)
        if topic not in metadata.topics:
            raise ValueError(f"topic does not exist: {topic}")
        partitions = sorted(metadata.topics[topic].partitions)
        bounds = {partition: probe.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                  for partition in partitions}
    finally:
        probe.close()

    results = {}
    errors = {}
    started = time.monotonic()

    def worker(partition):
        low, high = bounds[partition]
        consumer = Consumer({"bootstrap.servers": bootstrap,
                             "group.id": f"fullweek-part-{uuid.uuid4().hex[:8]}",
                             "enable.auto.commit": False, "isolation.level": "read_committed",
                             "auto.offset.reset": "earliest"})
        path = f"{output_path}.p{partition}"
        records = 0
        last = low
        last_progress = time.monotonic()
        deadline = started + timeout
        complete = drained = False
        try:
            consumer.assign([TopicPartition(topic, partition, low)])
            with Path(path).open("w", encoding="utf-8") as target:
                while time.monotonic() < deadline:
                    message = consumer.poll(0.5)
                    if message is None:
                        reached = last >= max(low, high - slack)
                        if reached and drained:
                            complete = True
                            break
                        drained = reached
                        if time.monotonic() - last_progress > 30:
                            break
                        continue
                    drained = False
                    if message.error():
                        raise ValueError(str(message.error()))
                    target.write(message.value().decode("utf-8") + "\n")
                    records += 1
                    last = max(last, message.offset() + 1)
                    last_progress = time.monotonic()
        except Exception as exc:  # surfaced after join
            errors[partition] = str(exc)
        finally:
            consumer.close()
        if not complete:
            complete = last >= max(low, high - slack)
        results[partition] = {"low": low, "high": high, "records": records, "last": last,
                              "complete": complete, "output": path}

    threads = [threading.Thread(target=worker, args=(partition,)) for partition in partitions]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise ValueError(f"parallel dump failed: {errors}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with Path(output_path).open("w", encoding="utf-8") as target:
        for partition in partitions:
            total += results[partition]["records"]
            with Path(results[partition]["output"]).open(encoding="utf-8") as part:
                shutil.copyfileobj(part, target)
            Path(results[partition]["output"]).unlink()
    return {"topic": topic, "output": str(output_path),
            "partitions": {str(p): {k: results[p][k] for k in
                                    ("low", "high", "records", "last", "complete")}
                           for p in partitions},
            "records": total,
            "high_total": sum(value[1] for value in bounds.values()),
            "low_total": sum(value[0] for value in bounds.values()),
            "control_slack": slack,
            "complete": all(results[p]["complete"] for p in partitions),
            "seconds": round(time.monotonic() - started, 3)}


# ---------------------------------------------------------------------------
# CLI

def _print(value):
    print(json.dumps(value, indent=2, allow_nan=False, default=str))


# ---------------------------------------------------------------------------
# Source scanning (per day, in parallel) and report assembly

FILENAME_DAY = re.compile(r"day-(\d{2})-(\d{2})-(\d{2})\.csv$")
EMITTER_FILE = re.compile(r"^\s+(\S+\.csv): read ([\d,]+) -> emitted ([\d,]+)")
EMITTER_TOTAL = re.compile(r"^Total: read ([\d,]+) -> emitted ([\d,]+)")
EMITTER_DONE = re.compile(r"^Done: (\d+) submitted, (\d+) failed")
EMITTER_ACCOUNTING = re.compile(
    r"([\d,]+) bad width, ([\d,]+) no symbol, ([\d,]+) no event time, "
    r"([\d,]+) filtered out by (\S+?), ([\d,]+) unparsable timestamp; "
    r"([\d,]+) malformed prices nulled")
JOB_RUNNING = re.compile(r'"timestamp":"([^"]+)".*switched from state CREATED to RUNNING')
JOB_FINISHED = re.compile(r'"timestamp":"([^"]+)".*switched from state RUNNING to FINISHED')


def day_from_filename(name):
    match = FILENAME_DAY.search(name)
    if not match:
        raise ValueError(f"cannot derive day from {name}")
    day, month, year = match.groups()
    return f"20{year}-{month}-{day}"


def _scan_one(job):
    path, timezone, window_ms, max_out_of_order_ms = job
    started = time.perf_counter()
    scan = reconcile_replay.scan_source(path, ZoneInfo(timezone), window_ms, max_out_of_order_ms)
    return {"file": os.path.basename(path), "day": day_from_filename(path), "scan": scan,
            "seconds": time.perf_counter() - started}


def scan_directory(data_dir, timezone, window_ms, max_out_of_order_ms, jobs, output_dir):
    files = sorted(str(p) for p in Path(data_dir).glob("debs2022-gc-trading-day-*.csv"))
    if not files:
        raise ValueError(f"no week CSVs under {data_dir}")
    started = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=jobs) as pool:
        results = list(pool.map(_scan_one, [(f, timezone, window_ms, max_out_of_order_ms)
                                            for f in files]))
    wall = time.perf_counter() - started
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for result in sorted(results, key=lambda item: item["day"]):
        (output_dir / f"{result['day']}.json").write_text(
            json.dumps(result["scan"], indent=2, default=str) + "\n")
        entries.append({"day": result["day"], "scan": result["scan"]})
    return entries, {"files": [{"file": r["file"], "day": r["day"], "seconds": r["seconds"]}
                               for r in results],
                     "parallel_jobs": jobs, "wall_seconds": wall}


def parse_emitter_log(text):
    files, total, done, accounting = [], None, None, None
    for line in text.splitlines():
        file_match = EMITTER_FILE.match(line)
        if file_match:
            files.append({"file": file_match.group(1),
                          "read": int(file_match.group(2).replace(",", "")),
                          "emitted": int(file_match.group(3).replace(",", ""))})
        total_match = EMITTER_TOTAL.match(line)
        if total_match:
            total = {"read": int(total_match.group(1).replace(",", "")),
                     "emitted": int(total_match.group(2).replace(",", ""))}
            found = EMITTER_ACCOUNTING.search(line)
            if found:
                values = [int(value.replace(",", "")) for value in found.groups()[:3]]
                values.append(int(found.group(4).replace(",", "")))
                values.append(int(found.group(6).replace(",", "")))
                values.append(int(found.group(7).replace(",", "")))
                accounting = {
                    "dropped_width": values[0], "dropped_no_symbol": values[1],
                    "dropped_no_time": values[2], "dropped_price_filter": values[3],
                    "dropped_timestamp": values[4], "malformed_prices": values[5],
                    "price_filter": found.group(5)}
        done_match = EMITTER_DONE.match(line)
        if done_match:
            done = {"submitted": int(done_match.group(1)), "failed": int(done_match.group(2))}
    if total is None or done is None:
        raise ValueError("emitter log is missing its completion summary")
    return {"files": files, "total": total, "done": done, "accounting": accounting}


def classify_audit(path):
    late = rejected = 0
    for line in Path(path).read_text().splitlines():
        if not line.strip():
            continue
        try:
            reason = json.loads(line).get("reason", "")
        except json.JSONDecodeError:
            reason = ""
        if reason == "window already finalized":
            late += 1
        else:
            rejected += 1
    return {"late": late, "rejected": rejected}


def _read_json(path, default=None):
    path = Path(path)
    return json.loads(path.read_text()) if path.exists() else default


def _read_marker(path):
    path = Path(path)
    return float(path.read_text().strip()) if path.exists() else None


def _inspect_epoch(path, field):
    data = _read_json(path)
    if not data:
        return None
    value = data[0].get("State", {}).get(field)
    if not value or value.startswith("0001"):
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _job_times(log_text):
    running = JOB_RUNNING.search(log_text)
    finished = JOB_FINISHED.search(log_text)
    to_epoch = lambda ts: datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() if ts else None
    return to_epoch(running.group(1) if running else None), to_epoch(finished.group(1) if finished else None)


def _git(worktree, *args):
    try:
        return subprocess.run(["git", "-C", worktree, *args], capture_output=True, text=True,
                              check=True).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return None


def _sanitize(value):
    """Replace absolute host paths with basenames so committed artifacts carry no host layout."""
    if isinstance(value, dict):
        return {key: _sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, str) and value.startswith("/"):
        return os.path.basename(value) or value
    return value


def export_evidence(manifest, output_dir, gate):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    clean = _sanitize(manifest)
    (output_dir / "manifest.json").write_text(json.dumps(clean, indent=2) + "\n")
    (output_dir / "gate.json").write_text(json.dumps(gate, indent=2) + "\n")
    (output_dir / "comparison.json").write_text(
        json.dumps(clean.get("comparison", {}), indent=2) + "\n")
    (output_dir / "source.json").write_text(json.dumps({
        "totals": clean.get("source"), "reconciliation": clean.get("reconciliation"),
        "per_day_source": {day: value for day, value in
                           (manifest.get("reconciliation", {}).get("per_day") or {}).items()},
    }, indent=2) + "\n")
    (output_dir / "timings.json").write_text(json.dumps(clean.get("timings", {}), indent=2) + "\n")
    (output_dir / "retention.json").write_text(
        json.dumps({"preflight": clean.get("preflight"), "retention": clean.get("retention")},
                   indent=2) + "\n")


def build_manifest(run_dir, data_dir, timezone, worktree, scan_timing, window_ms=DEFAULT_WINDOW_MS):
    run_dir = Path(run_dir)
    logs, evidence = run_dir / "logs", run_dir / "evidence"
    source = _read_json(run_dir / "source.json")
    emitter_log = parse_emitter_log((logs / "emitter.log").read_text())
    offsets = _read_json(evidence / "offsets.json", {})
    dump_output = _read_json(logs / "dump-output.log", {})
    dump_audit = _read_json(logs / "dump-audit.log", {})
    comparison = _read_json(evidence / "compare.json")
    expected_records = comparison["records"]["expected"]
    input_topic = next((topic for topic in offsets if topic.endswith("-in")), None)
    input_records = sum(max(0, part["high"] - part["low"])
                        for part in offsets.get(input_topic, {}).values()) if input_topic else None
    audit_counts = classify_audit(evidence / "audit.jsonl")
    job_running, job_finished = _job_times((logs / "jobmanager.log").read_text())

    up_start = _read_marker(logs / "compose_up_start.txt")
    up_done = _read_marker(logs / "compose_up_done.txt")
    oracle_start = _read_marker(logs / "oracle_start.txt")
    oracle_done = _read_marker(logs / "oracle_done.txt")
    dump_start = _read_marker(logs / "dump_start.txt")
    dump_done = _read_marker(logs / "dump_done.txt")
    emitter_start = _inspect_epoch(logs / "emitter_inspect.json", "StartedAt")
    emitter_finish = _inspect_epoch(logs / "emitter_inspect.json", "FinishedAt")

    submitted, failed = emitter_log["done"]["submitted"], emitter_log["done"]["failed"]
    acknowledged = submitted - failed
    accounting = emitter_log["accounting"] or {}
    reconciled_dispositions = {
        "scan_loader": source["totals"]["loader"] if source else None,
        "emitter_accounting": accounting,
        "emitter_rows_read": emitter_log["total"]["read"],
        "scan_rows_read": source["totals"]["rows_read"] if source else None,
    }
    errors = []
    if source and accounting:
        if accounting.get("dropped_width", 0) != source["totals"]["loader"]["dropped_width"]:
            errors.append("emitter and scan disagree on dropped_width")

    timings = {
        "unit": "seconds",
        "preprocessing": {
            "oracle_wall": (oracle_done - oracle_start) if oracle_start and oracle_done else None,
            "source_scan_wall": scan_timing["wall_seconds"],
            "source_scan_parallel_jobs": scan_timing["parallel_jobs"],
            "source_scan_per_file": scan_timing["files"],
            "note": "analysis steps; source_scan ran in parallel after the replay",
        },
        "ingestion": {
            "emitter_wall": (emitter_finish - emitter_start) if emitter_start and emitter_finish else None,
            "compose_up_to_emitter_done": (up_done - up_start) if up_start and up_done else None,
            "note": "emitter publication; also bounded by the replay pacer at SPEED_MULTIPLIER",
        },
        "processing": {
            "flink_job_wall": (job_finished - job_running) if job_running and job_finished else None,
            "job_running_epoch": job_running,
            "job_finished_epoch": job_finished,
            "note": "bounded Flink job from RUNNING to FINISHED",
        },
        "drain": {
            "dump_output_seconds": dump_output.get("seconds"),
            "dump_audit_seconds": dump_audit.get("seconds"),
            "dump_wall": (dump_done - dump_start) if dump_start and dump_done else None,
            "note": "committed-output drain and audit read",
        },
    }

    emit_final_window = _read_json(evidence / "normalize.json", {}).get("counts", {}).get("final_window")
    kafka_bytes = (logs / "kafka_data_bytes.txt").read_text().split()[0] if (logs / "kafka_data_bytes.txt").exists() else None
    flink_bytes = (logs / "flink_state_bytes.txt").read_text().split()[0] if (logs / "flink_state_bytes.txt").exists() else None

    manifest = {
        "ticket": "06-verify-full-week-queries",
        "mode": "capture-order",
        "scope": "seven official working/weekend files replayed in manifest order through the "
                 "production Flink query with fresh topics and state; correctness reference run, "
                 "not a capacity or live-latency measurement",
        "revision": {"commit": _git(worktree, "rev-parse", "HEAD"),
                     "branch": _git(worktree, "branch", "--show-current"),
                     "describe": _git(worktree, "describe", "--always", "--dirty"),
                     "worktree": os.path.basename(worktree)},
        "configuration": {
            "window_ms": window_ms,
            "max_out_of_order_ms": 5000,
            "event_time_zone": timezone,
            "speed_multiplier": 1000,
            "pacing": "event",
            "dedup_strategy": "NONE",
            "late_event_strategy": "SIDE_OUTPUT",
            "flink_parallelism": 2,
        },
        "source": {
            "rows_read": source["totals"]["rows_read"] if source else 0,
            "eligible_trades": source["totals"]["query"]["eligible_trades"] if source else 0,
            "final_window": source["totals"]["windows"]["final_nonempty"] if source else None,
            "loader": source["totals"]["loader"] if source else None,
        },
        "emitter": {
            "emitted": emitter_log["total"]["emitted"],
            "acknowledged": acknowledged,
            "submitted": submitted,
            "failed": failed,
            "complete": True,
            "accounting": accounting,
            "per_file": emitter_log["files"],
        },
        "input": {"records": input_records, "complete": input_records == acknowledged,
                  "offsets": offsets.get(input_topic)},
        "audit": {"late": audit_counts["late"], "rejected": audit_counts["rejected"],
                  "records": dump_audit.get("records"), "complete": dump_audit.get("complete") is True,
                  "offsets": offsets.get(next((t for t in offsets if t.endswith("-audit")), ""), {})},
        "output": {"prices": (_read_json(evidence / "normalize.json", {}).get("counts", {})
                              .get("prices")),
                   "final_window": emit_final_window,
                   "records": dump_output.get("records"), "complete": dump_output.get("complete") is True,
                   "offsets": offsets.get(next((t for t in offsets if t.endswith("-out")), ""), {})},
        "comparison": comparison,
        "expected_final_window": comparison["expected_final_window"],
        "expected_records": expected_records,
        "disposition_reconciliation": reconciled_dispositions,
        "reconciliation": _reconcile(source, comparison),
        "timings": timings,
        "preflight": _read_json(logs / "preflight.json", {}),
        "retention": {
            "configured_retention_ms": 604800000,
            "measured_kafka_data_bytes": int(kafka_bytes) if kafka_bytes else None,
            "measured_flink_state_bytes": int(flink_bytes) if flink_bytes else None,
            "note": "fresh topics, 6 partitions, no prior data; measured after the run",
        },
        "errors": errors,
    }
    return manifest


def _reconcile(source, comparison):
    per_day = {}
    for day, stats in (comparison.get("per_day") or {}).items():
        source_day = (source or {}).get("per_day", {}).get(day, {})
        per_day[day] = {
            "rows_read": source_day.get("rows_read"),
            "emitted": (source_day.get("loader") or {}).get("emitted"),
            "eligible_trades": (source_day.get("query") or {}).get("eligible_trades"),
            "committed_expected": stats.get("expected"),
            "committed_actual": stats.get("actual"),
            **{key: stats.get(key) for key in ERROR_KEYS},
        }
    per_exchange = {}
    for exchange, stats in (comparison.get("per_exchange") or {}).items():
        source_exchange = (source or {}).get("per_exchange", {}).get(exchange, {})
        per_exchange[exchange] = {
            "emitted": source_exchange.get("emitted"),
            "eligible_trades": (source_exchange.get("query") or {}).get("eligible_trades"),
            "committed_expected": stats.get("expected"),
            "committed_actual": stats.get("actual"),
            **{key: stats.get(key) for key in ERROR_KEYS},
        }
    return {"per_day": per_day, "per_exchange": per_exchange}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    compare = commands.add_parser("compare", help="full-week comparison with error taxonomy")
    compare.add_argument("--expected", required=True)
    compare.add_argument("--actual", required=True)
    compare.add_argument("--db", required=True)
    compare.add_argument("--timezone", default="Europe/Berlin")
    compare.add_argument("--sample-limit", type=int, default=20)
    compare.add_argument("--output")

    merge = commands.add_parser("merge-source", help="merge per-file source scans")
    merge.add_argument("--scans", required=True,
                       help="JSON file: [{day, scan}, ...] or a directory of <day>.json scans")
    merge.add_argument("--output")

    scan = commands.add_parser("scan", help="scan every week file into per-day dispositions")
    scan.add_argument("--data-dir", required=True)
    scan.add_argument("--output-dir", required=True)
    scan.add_argument("--timezone", default="Europe/Berlin")
    scan.add_argument("--window-ms", type=int, default=DEFAULT_WINDOW_MS)
    scan.add_argument("--max-out-of-order-ms", type=int, default=5000)
    scan.add_argument("--jobs", type=int, default=7)
    scan.add_argument("--timing")

    report = commands.add_parser("report", help="assemble and gate a completed run")
    report.add_argument("--run-dir", required=True)
    report.add_argument("--data-dir", required=True)
    report.add_argument("--scan-dir", required=True)
    report.add_argument("--timezone", default="Europe/Berlin")
    report.add_argument("--worktree", default="")
    report.add_argument("--jobs", type=int, default=7)
    report.add_argument("--output", required=True)
    report.add_argument("--evidence-dir")

    gate = commands.add_parser("gate", help="fail-closed full-week gate")
    gate.add_argument("--manifest", required=True)

    export = commands.add_parser("export", help="write sanitized committed evidence pack")
    export.add_argument("--manifest", required=True)
    export.add_argument("--output-dir", required=True)

    dump = commands.add_parser("dump-parallel", help="drain a committed topic per partition")
    dump.add_argument("--bootstrap", required=True)
    dump.add_argument("--topic", required=True)
    dump.add_argument("--output", required=True)
    dump.add_argument("--timeout", type=int, default=1800)
    dump.add_argument("--slack", type=int, default=4)

    args = parser.parse_args(argv)
    try:
        if args.command == "dump-parallel":
            meta = dump_parallel(args.bootstrap, args.topic, args.output, args.timeout, args.slack)
            _print({key: meta[key] for key in ("topic", "records", "high_total",
                                               "complete", "seconds")})
            return 0 if meta["complete"] else 1
        if args.command == "compare":
            report_value = compare_full(args.expected, args.actual, args.db,
                                        ZoneInfo(args.timezone), args.sample_limit)
            if args.output:
                Path(args.output).write_text(json.dumps(report_value, indent=2, default=str) + "\n")
            _print({key: report_value[key] for key in
                    ("match", "counts", "records", "expected_final_window", "actual_final_window")})
            return 0 if report_value["match"] else 1
        if args.command == "merge-source":
            path = Path(args.scans)
            if path.is_dir():
                entries = [{"day": item.stem, "scan": json.loads(item.read_text())}
                           for item in sorted(path.glob("*.json"))]
            else:
                entries = json.loads(path.read_text())
            merged = merge_source_scans(entries)
            if args.output:
                Path(args.output).write_text(json.dumps(merged, indent=2, default=str) + "\n")
            _print({key: merged[key] for key in ("totals", "per_exchange")})
            return 0
        if args.command == "scan":
            entries, timing = scan_directory(args.data_dir, args.timezone, args.window_ms,
                                             args.max_out_of_order_ms, args.jobs, args.output_dir)
            merged = merge_source_scans(entries)
            Path(args.output_dir, "source.json").write_text(
                json.dumps(merged, indent=2, default=str) + "\n")
            if args.timing:
                Path(args.timing).write_text(json.dumps(timing, indent=2, default=str) + "\n")
            _print({"files": len(entries), "wall_seconds": timing["wall_seconds"],
                    "rows_read": merged["totals"]["rows_read"]})
            return 0
        if args.command == "report":
            run_dir = Path(args.run_dir)
            scan_timing = _read_json(run_dir / "scan-timing.json", {"wall_seconds": None,
                                                                    "parallel_jobs": None, "files": []})
            manifest = build_manifest(run_dir, args.data_dir, args.timezone,
                                      args.worktree or str(Path.cwd()), scan_timing)
            Path(args.output).write_text(json.dumps(manifest, indent=2, default=str) + "\n")
            passed, checks = evaluate(manifest)
            if args.evidence_dir:
                export_evidence(manifest, args.evidence_dir, {"passed": passed, "checks": checks})
            _print({"passed": passed, "checks": checks,
                    "counts": manifest["comparison"]["counts"]})
            return 0 if passed else 1
        if args.command == "export":
            manifest = json.loads(Path(args.manifest).read_text())
            passed, checks = evaluate(manifest)
            export_evidence(manifest, args.output_dir, {"passed": passed, "checks": checks})
            _print({"passed": passed, "exported_to": args.output_dir})
            return 0 if passed else 1
        if args.command == "gate":
            manifest = json.loads(Path(args.manifest).read_text())
            passed, checks = evaluate(manifest)
            _print({"passed": passed, "checks": checks})
            return 0 if passed else 1
    except (OSError, ValueError, sqlite3.Error, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())

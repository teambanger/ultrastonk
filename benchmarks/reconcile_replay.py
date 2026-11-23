#!/usr/bin/env python3
"""Reconcile a bounded historical replay slice against the independent CSV oracle.

This tool is deliberately separate from the live benchmark path. It scans and
sorts raw DEBS CSV rows, normalizes committed Flink output into the oracle
schema, compares the two with a disk-backed index, and gates a run on complete
accounting, zero eligible lateness and final-window coverage.

Run `python3 benchmarks/reconcile_replay.py --help` for commands. Source data and
scratch databases must live on disk, outside Git.
"""
import argparse
import collections
import csv
import datetime as dt
import hashlib
import json
import math
import os
import re
import sqlite3
import sys
import time
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

SKIP_LINES = 13
COLUMNS = 40
DEFAULT_WINDOW_MS = 300000
DEFAULT_MAX_OUT_OF_ORDER_MS = 5000
PRICE_COLUMNS = (4, 6, 10, 21)
SYMBOL = re.compile(r"[^\s.]+(?:\.[^\s.]+)*\.[^\s.]+")
MODES = ("capture-order", "event-time-ordered")
KINDS = ("EMA", "BUY", "SELL", "price", "EMA38", "EMA100")


def _first_non_blank(*values):
    for value in values:
        if value is not None and value.strip():
            return value.strip()
    return ""


def _wall_time(date, time, zone):
    """Parse a DEBS wall-clock pair, truncating fractional seconds to milliseconds."""
    text = time[:15]
    pattern = "%d-%m-%Y %H:%M:%S.%f" if "." in text else "%d-%m-%Y %H:%M:%S"
    value = dt.datetime.strptime(date + " " + text, pattern)
    return int(value.replace(tzinfo=zone).timestamp() * 1000)


def event_time(row, zone):
    """Return (epoch milliseconds, used_system_fallback) exactly as the replay loader does."""
    primary_date = _first_non_blank(row[26], row[2])
    primary_time = _first_non_blank(row[23], row[3])
    try:
        return _wall_time(primary_date, primary_time, zone), False
    except ValueError:
        return _wall_time(_first_non_blank(row[2]), _first_non_blank(row[3]), zone), True


def parse_price(value):
    if value is None or not value.strip():
        return None
    try:
        price = float(value.strip())
    except ValueError:
        return None
    return price if math.isfinite(price) and price >= 0 else None


def malformed_price_cells(row):
    return sum(1 for column in PRICE_COLUMNS
               if row[column].strip() and parse_price(row[column]) is None)


def classify(row, zone):
    """Mirror DataLoader.PriceFilter.NONE: (disposition, event_time, malformed_prices, fallback)."""
    if len(row) != COLUMNS:
        return "dropped_width", None, 0, False
    if not row[0].strip() or not row[1].strip():
        return "dropped_no_symbol", None, 0, False
    if not _first_non_blank(row[26], row[2]) or not _first_non_blank(row[23], row[3]):
        return "dropped_no_time", None, 0, False
    malformed = malformed_price_cells(row)
    try:
        timestamp, fallback = event_time(row, zone)
    except (ValueError, OverflowError):
        return "dropped_timestamp", None, malformed, False
    return "emitted", timestamp, malformed, fallback


def _eligible_trade(row, timestamp):
    return (len(row) == COLUMNS and bool(row[1].strip())
            and bool(SYMBOL.fullmatch(row[0].strip()))
            and (parse_price(row[21]) or 0) > 0
            and timestamp is not None and timestamp >= 0)


def _iter_csv(path):
    with Path(path).open(newline="", encoding="utf-8") as source:
        for _ in range(SKIP_LINES):
            next(source, None)
        yield from csv.reader(source)


def _iter_lines(path):
    with Path(path).open(encoding="utf-8") as source:
        for _ in range(SKIP_LINES):
            next(source, None)
        for line in source:
            yield line


def _row_from_line(line):
    try:
        return next(csv.reader([line.rstrip("\r\n")]))
    except (csv.Error, StopIteration):
        return []


def scan_source(path, zone, window_ms=DEFAULT_WINDOW_MS,
                max_out_of_order_ms=DEFAULT_MAX_OUT_OF_ORDER_MS):
    """Streaming source accounting. Memory scales with distinct symbols and windows, not rows."""
    if not Path(path).is_file():
        raise ValueError(f"CSV file is not readable: {path}")
    loader = collections.Counter(rows_read=0, emitted=0, dropped_width=0, dropped_no_symbol=0,
                                 dropped_no_time=0, dropped_timestamp=0, malformed_prices=0,
                                 fallback_events=0)
    query = collections.Counter(eligible_trades=0, invalid_symbol_rows=0,
                                nonpositive_or_missing_last_rows=0)
    windows = collections.defaultdict(lambda: {"rows": 0, "trades": 0})
    reversals = collections.Counter(events=0, eligible_trades=0, beyond_disorder_budget=0)
    max_seen = {}
    reversal_max_ms = 0
    eligible_reversal_max_ms = 0
    eligible_symbols = set()
    for row in _iter_csv(path):
        loader["rows_read"] += 1
        disposition, timestamp, malformed, fallback = classify(row, zone)
        if disposition != "emitted":
            loader[disposition] += 1
            continue
        loader["emitted"] += 1
        loader["malformed_prices"] += malformed
        loader["fallback_events"] += int(fallback)
        symbol = row[0].strip()
        window = timestamp // window_ms * window_ms
        windows[window]["rows"] += 1
        previous = max_seen.get(symbol)
        if previous is not None and timestamp < previous:
            delta = previous - timestamp
            reversals["events"] += 1
            reversal_max_ms = max(reversal_max_ms, delta)
            if delta > max_out_of_order_ms:
                reversals["beyond_disorder_budget"] += 1
        max_seen[symbol] = max(previous or timestamp, timestamp)
        if not SYMBOL.fullmatch(symbol):
            query["invalid_symbol_rows"] += 1
            continue
        price = parse_price(row[21])
        if price is None or price <= 0:
            query["nonpositive_or_missing_last_rows"] += 1
            continue
        query["eligible_trades"] += 1
        eligible_symbols.add(symbol)
        windows[window]["trades"] += 1
        if previous is not None and timestamp < previous:
            reversals["eligible_trades"] += 1
            eligible_reversal_max_ms = max(eligible_reversal_max_ms, previous - timestamp)
    trading_windows = [window for window, counts in windows.items() if counts["trades"]]
    all_windows = list(windows)
    return {
        "csv": str(path),
        "window_ms": window_ms,
        "max_out_of_order_ms": max_out_of_order_ms,
        "rows_read": loader["rows_read"],
        "loader": {key: loader[key] for key in (
            "emitted", "dropped_width", "dropped_no_symbol", "dropped_no_time",
            "dropped_timestamp", "malformed_prices", "fallback_events")},
        "query": {
            "eligible_trades": query["eligible_trades"],
            "eligible_symbols": len(eligible_symbols),
            "invalid_symbol_rows": query["invalid_symbol_rows"],
            "nonpositive_or_missing_last_rows": query["nonpositive_or_missing_last_rows"],
        },
        "reversals": {
            "events": reversals["events"],
            "eligible_trades": reversals["eligible_trades"],
            "beyond_disorder_budget": reversals["beyond_disorder_budget"],
            "max_ms": reversal_max_ms,
            "eligible_max_ms": eligible_reversal_max_ms,
        },
        "windows": {
            "count": len(all_windows),
            "final_nonempty": max(trading_windows) if trading_windows else None,
            "final_event": max(all_windows) if all_windows else None,
            "counts": {str(window): counts for window, counts in sorted(windows.items())},
        },
    }


def _time_of_day_ms(text):
    whole, _, fraction = text.partition(".")
    moment = dt.datetime.strptime(whole, "%H:%M:%S")
    return ((moment.hour * 60 + moment.minute) * 60 + moment.second) * 1000 + int(
        (fraction + "000")[:3])


def _capture_time_of_day_ms(row):
    if len(row) < 4 or not row[2].strip() or not row[3].strip():
        return None
    try:
        return _wall_time(row[2].strip(), row[3].strip(), dt.timezone.utc) % 86_400_000
    except ValueError:
        return None


def select_capture_slice(source, output, start, end, zone):
    """Copy a contiguous physical row block whose system capture time falls in [start, end)."""
    start_ms, end_ms = _time_of_day_ms(start), _time_of_day_ms(end)
    if start_ms >= end_ms:
        raise ValueError("capture slice start must be before end")
    if not Path(source).is_file():
        raise ValueError(f"CSV file is not readable: {source}")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    start_row = end_row = None
    row_index = 0
    try:
        with Path(source).open(newline="", encoding="utf-8") as stream, \
                temporary.open("w", newline="", encoding="utf-8") as target:
            header = [next(stream, "") for _ in range(SKIP_LINES)]
            if not all(header):
                raise ValueError("source has fewer than 13 header lines")
            target.write("".join(header))
            writer = csv.writer(target)
            for row in csv.reader(stream):
                row_index += 1
                stamp = _capture_time_of_day_ms(row) if len(row) == COLUMNS else None
                if start_row is None:
                    if stamp is not None and stamp >= start_ms:
                        start_row = row_index
                elif stamp is not None and stamp >= end_ms:
                    end_row = row_index
                    break
                if start_row is not None:
                    writer.writerow(row)
        if start_row is None:
            raise ValueError("no row reached the requested start")
        if end_row is None:
            raise ValueError("no row reached the requested end")
        if end_row <= start_row:
            raise ValueError("selected capture interval is empty")
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {"source": str(source), "output": str(output), "start": start, "end": end,
            "start_row": start_row, "end_row": end_row,
            "rows": end_row - start_row, "timezone": str(zone)}


def extract_rows(source, output, start_row, end_row):
    """Copy the 1-based data row block [start_row, end_row) without reordering it."""
    if start_row < 1 or end_row <= start_row:
        raise ValueError("extract requires 1 <= start_row < end_row")
    if not Path(source).is_file():
        raise ValueError(f"CSV file is not readable: {source}")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    copied = 0
    try:
        with Path(source).open(newline="", encoding="utf-8") as stream, \
                temporary.open("w", newline="", encoding="utf-8") as target:
            header = [next(stream, "") for _ in range(SKIP_LINES)]
            if not all(header):
                raise ValueError("source has fewer than 13 header lines")
            target.write("".join(header))
            writer = csv.writer(target)
            for row_index, row in enumerate(csv.reader(stream), 1):
                if row_index >= end_row:
                    break
                if row_index >= start_row:
                    writer.writerow(row)
                    copied += 1
        if copied != end_row - start_row:
            raise ValueError("source ended before the requested row range")
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {"source": str(source), "output": str(output), "start_row": start_row,
            "end_row": end_row, "rows": copied}


def sort_event_time(source, output, db_path, zone):
    """Stable disk-backed sort by (event millisecond, original source ordinal).

    The row multiset emitted by DataLoader.PriceFilter.NONE is preserved: only
    rows the loader would drop are omitted, and equal timestamps keep their
    original ordinal order so the production last-arrival tie rule is unchanged.
    """
    if not Path(source).is_file():
        raise ValueError(f"CSV file is not readable: {source}")
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    loader = collections.Counter(rows_read=0, emitted=0, dropped_width=0, dropped_no_symbol=0,
                                 dropped_no_time=0, dropped_timestamp=0, malformed_prices=0,
                                 fallback_events=0)
    connection = sqlite3.connect(str(db_path))
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("CREATE TABLE rows (ts INTEGER NOT NULL, ordinal INTEGER NOT NULL, "
                       "line TEXT NOT NULL, PRIMARY KEY (ts, ordinal))")
    batch = []
    ordinal = 0
    with Path(source).open(encoding="utf-8") as stream:
        header = [next(stream, "") for _ in range(SKIP_LINES)]
        if not all(header):
            raise ValueError("source has fewer than 13 header lines")
        for line in stream:
            ordinal += 1
            row = _row_from_line(line)
            loader["rows_read"] += 1
            disposition, timestamp, malformed, fallback = classify(row, zone)
            if disposition != "emitted":
                loader[disposition] += 1
                continue
            loader["emitted"] += 1
            loader["malformed_prices"] += malformed
            loader["fallback_events"] += int(fallback)
            batch.append((timestamp, ordinal, line.rstrip("\r\n")))
            if len(batch) >= 10000:
                connection.executemany("INSERT INTO rows VALUES (?, ?, ?)", batch)
                batch.clear()
        if batch:
            connection.executemany("INSERT INTO rows VALUES (?, ?, ?)", batch)
        connection.commit()
        digest = hashlib.sha256()
        with Path(output).open("w", encoding="utf-8") as target:
            target.write("".join(header))
            digest.update("".join(header).encode())
            for (line,) in connection.execute("SELECT line FROM rows ORDER BY ts, ordinal"):
                target.write(line + "\n")
                digest.update((line + "\n").encode())
    connection.close()
    return {
        "source": str(source), "output": str(output), "db": str(db_path),
        "rows_read": loader["rows_read"], "emitted": loader["emitted"],
        "dropped_width": loader["dropped_width"], "dropped_no_symbol": loader["dropped_no_symbol"],
        "dropped_no_time": loader["dropped_no_time"], "dropped_timestamp": loader["dropped_timestamp"],
        "malformed_prices": loader["malformed_prices"], "fallback_events": loader["fallback_events"],
        "scratch_bytes": db_path.stat().st_size, "output_bytes": Path(output).stat().st_size,
        "sha256": digest.hexdigest(),
    }


class OutputNormalizer:
    """Turn committed four-field Kafka output into oracle-comparable records."""

    def __init__(self, window_ms=DEFAULT_WINDOW_MS):
        self.window_ms = window_ms
        self.pending = {}
        self.counts = collections.Counter(
            records=0, prices=0, ema_components=0, ema_pairs=0, advisories=0)
        self.counts["window_min"] = None
        self.counts["final_window"] = None
        self.symbols = set()

    def _validate(self, record):
        if not isinstance(record, dict):
            raise ValueError("record must be an object")
        if set(record) != {"timestamp_emit", "ticker", "type", "value"}:
            raise ValueError("output record must have exactly timestamp_emit, ticker, type, value")
        timestamp, ticker, kind, value = (record[field] for field in
                                          ("timestamp_emit", "ticker", "type", "value"))
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("timestamp_emit must be a nonnegative integer")
        if not isinstance(ticker, str) or not ticker:
            raise ValueError("ticker must be a nonempty string")
        if kind not in KINDS:
            raise ValueError("type must be EMA38, EMA100, BUY, SELL, or price")
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError("value must be a finite number")
        if kind != "price" and value < 0:
            raise ValueError("EMA and advisory values must be nonnegative")
        return timestamp, ticker, kind, value

    def feed(self, record):
        timestamp, ticker, kind, value = self._validate(record)
        self.counts["records"] += 1
        self.symbols.add(ticker)
        window = timestamp // self.window_ms * self.window_ms
        if self.counts["window_min"] is None:
            self.counts["window_min"] = window
        if kind == "price":
            self.counts["prices"] += 1
            return []
        if kind in ("BUY", "SELL"):
            self.counts["advisories"] += 1
            return [{"symbol": ticker, "window": window, "type": kind}]
        self.counts["ema_components"] += 1
        slot = self.pending.setdefault((ticker, timestamp), {"EMA38": [], "EMA100": []})
        slot[kind].append(value)
        emitted = []
        while slot["EMA38"] and slot["EMA100"]:
            short = slot["EMA38"].pop(0)
            long = slot["EMA100"].pop(0)
            self.counts["ema_pairs"] += 1
            self.counts["final_window"] = max(self.counts["final_window"] or window, window)
            emitted.append({"symbol": ticker, "window": window, "type": "EMA",
                            "ema38": short, "ema100": long, "timestamp_ms": timestamp})
        if not slot["EMA38"] and not slot["EMA100"]:
            del self.pending[(ticker, timestamp)]
        return emitted

    def finish(self):
        if self.pending:
            (ticker, timestamp), slot = next(iter(self.pending.items()))
            raise ValueError(f"incomplete EMA pair for {ticker!r} at {timestamp}: {slot}")
        return []


def normalize_jsonl(input_path, output_path, window_ms=DEFAULT_WINDOW_MS):
    normalizer = OutputNormalizer(window_ms)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with Path(input_path).open(encoding="utf-8") as source, \
            Path(output_path).open("w", encoding="utf-8") as target:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                emitted = normalizer.feed(record)
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                raise ValueError(f"{input_path}:{line_number}: {exc}") from exc
            for item in emitted:
                target.write(json.dumps(item, allow_nan=False) + "\n")
        try:
            normalizer.finish()
        except ValueError as exc:
            raise ValueError(f"{input_path}: {exc}") from exc
    return {"input": str(input_path), "output": str(output_path),
            "counts": dict(normalizer.counts), "symbols": len(normalizer.symbols),
            "output_bytes": Path(output_path).stat().st_size}


def _load_records(path, connection, source_name):
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
                if kind == "EMA":
                    for field in ("ema38", "ema100"):
                        value = record[field]
                        if type(value) not in (int, float) or not math.isfinite(value):
                            raise ValueError(f"{field} must be a finite number")
                    connection.execute(
                        "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, 1) "
                        "ON CONFLICT(source, symbol, window, type) DO UPDATE SET n = n + 1",
                        (source_name, symbol, window, kind, record["ema38"], record["ema100"]))
                else:
                    connection.execute(
                        "INSERT INTO records VALUES (?, ?, ?, ?, NULL, NULL, 1) "
                        "ON CONFLICT(source, symbol, window, type) DO UPDATE SET n = n + 1",
                        (source_name, symbol, window, kind))
            except (ValueError, KeyError, TypeError, OverflowError) as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc


def _sample(connection, sql, parameters):
    return [dict(zip(("symbol", "window", "type", "count"), row))
            for row in connection.execute(sql, parameters)]


def compare_jsonl(expected, actual, db_path, sample_limit=20):
    """Disk-backed comparison with exact keys, directions and isclose EMA tolerance."""
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("CREATE TABLE records (source TEXT NOT NULL, symbol TEXT NOT NULL, "
                       "window INTEGER NOT NULL, type TEXT NOT NULL, ema38 REAL, ema100 REAL, "
                       "n INTEGER NOT NULL, PRIMARY KEY (source, symbol, window, type))")
    try:
        _load_records(str(expected), connection, "expected")
        _load_records(str(actual), connection, "actual")
        connection.commit()
    except Exception:
        connection.close()
        raise
    missing_count = connection.execute(
        "SELECT COUNT(*) FROM records e WHERE e.source = 'expected' AND NOT EXISTS "
        "(SELECT 1 FROM records a WHERE a.source = 'actual' AND a.symbol = e.symbol "
        "AND a.window = e.window AND a.type = e.type)").fetchone()[0]
    extra_count = connection.execute(
        "SELECT COUNT(*) FROM records a WHERE a.source = 'actual' AND NOT EXISTS "
        "(SELECT 1 FROM records e WHERE e.source = 'expected' AND e.symbol = a.symbol "
        "AND e.window = a.window AND e.type = a.type)").fetchone()[0]
    missing = _sample(connection,
                      "SELECT e.symbol, e.window, e.type, e.n FROM records e WHERE e.source = 'expected' "
                      "AND NOT EXISTS (SELECT 1 FROM records a WHERE a.source = 'actual' "
                      "AND a.symbol = e.symbol AND a.window = e.window AND a.type = e.type) "
                      "ORDER BY e.window, e.symbol, e.type LIMIT ?",
                      (sample_limit,))
    extra = _sample(connection,
                    "SELECT a.symbol, a.window, a.type, a.n FROM records a WHERE a.source = 'actual' "
                    "AND NOT EXISTS (SELECT 1 FROM records e WHERE e.source = 'expected' "
                    "AND e.symbol = a.symbol AND e.window = a.window AND e.type = a.type) "
                    "ORDER BY a.window, a.symbol, a.type LIMIT ?",
                    (sample_limit,))
    incorrect = []
    incorrect_count = 0
    rows = connection.execute(
        "SELECT e.symbol, e.window, e.ema38, e.ema100, a.ema38, a.ema100 FROM records e "
        "JOIN records a ON a.source = 'actual' AND a.symbol = e.symbol AND a.window = e.window "
        "AND a.type = e.type WHERE e.source = 'expected' AND e.type = 'EMA' "
        "ORDER BY e.window, e.symbol")
    for symbol, window, expected38, expected100, actual38, actual100 in rows:
        bad = []
        if not math.isclose(expected38, actual38, rel_tol=1e-9, abs_tol=1e-9):
            bad.append(("ema38", expected38, actual38))
        if not math.isclose(expected100, actual100, rel_tol=1e-9, abs_tol=1e-9):
            bad.append(("ema100", expected100, actual100))
        if bad:
            incorrect_count += 1
            for field, left, right in bad:
                if len(incorrect) < sample_limit:
                    incorrect.append({"symbol": symbol, "window": window, "type": "EMA",
                                      "field": field, "expected": left, "actual": right})
    duplicate_pairs = connection.execute(
        "SELECT source, symbol, window, type, n FROM records WHERE n > 1 "
        "ORDER BY source, window, symbol, type").fetchall()
    duplicates = [{"source": source, "symbol": symbol, "window": window,
                   "type": kind, "count": count}
                  for source, symbol, window, kind, count in duplicate_pairs[:sample_limit]]
    duplicate_count = len(duplicate_pairs)
    totals = {source: connection.execute(
        "SELECT COALESCE(SUM(n), 0) FROM records WHERE source = ?", (source,)).fetchone()[0]
        for source in ("expected", "actual")}
    expected_max_window = connection.execute(
        "SELECT MAX(window) FROM records WHERE source = 'expected' AND type = 'EMA'").fetchone()[0]
    actual_max_window = connection.execute(
        "SELECT MAX(window) FROM records WHERE source = 'actual' AND type = 'EMA'").fetchone()[0]
    connection.close()
    counts = {"missing": missing_count, "extra": extra_count,
              "incorrect": incorrect_count, "duplicates": duplicate_count}
    return {"match": not any(counts.values()), "counts": counts,
            "missing": missing, "extra": extra, "incorrect": incorrect,
            "duplicates": duplicates,
            "records": {"expected": totals["expected"], "actual": totals["actual"]},
            "expected_final_window": expected_max_window,
            "actual_final_window": actual_max_window,
            "sample_limit": sample_limit}


def evaluate(manifest):
    """Fail closed unless publication, offsets, lateness, oracle and final window all agree."""
    def section(name):
        value = manifest.get(name)
        return value if isinstance(value, dict) else {}

    emitter, source, output = section("emitter"), section("source"), section("output")
    audit, comparison, errors = section("audit"), section("comparison"), manifest.get("errors")
    input_section = section("input")
    expected_final = manifest.get("expected_final_window")
    checks = {
        "mode_declared": manifest.get("mode") in MODES,
        "source_nonempty": source.get("eligible_trades", 0) > 0,
        "emitter_complete": (emitter.get("complete") is True and emitter.get("failed") == 0
                             and emitter.get("emitted") == emitter.get("acknowledged")),
        "input_complete": (input_section.get("complete") is True
                           and input_section.get("records") == emitter.get("emitted")),
        "audit_complete": audit.get("complete") is True,
        "zero_late_exclusions": audit.get("late") == 0,
        "zero_rejected_inputs": audit.get("rejected") == 0,
        "committed_prices_match": output.get("prices") == source.get("eligible_trades"),
        "output_complete": output.get("complete") is True,
        "oracle_exact": comparison.get("match") is True and "error" not in comparison,
        "final_window_present": (expected_final is not None
                                 and output.get("final_window") == expected_final),
        "no_errors": not errors,
    }
    return all(checks.values()), checks


def dump_topic(bootstrap, topic, output_path, timeout=600, group_id=None):
    """Consume committed records from earliest to the captured end offsets into JSON Lines."""
    try:
        from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
    except ImportError as exc:
        raise ValueError("confluent_kafka is required for dump/offsets; run inside the benchmark image") from exc
    consumer = Consumer({"bootstrap.servers": bootstrap,
                         "group.id": group_id or "reconcile-" + uuid.uuid4().hex[:12],
                         "enable.auto.commit": False,
                         "isolation.level": "read_committed",
                         "auto.offset.reset": "earliest"})
    try:
        metadata = consumer.list_topics(topic, timeout=15)
        if topic not in metadata.topics:
            raise ValueError(f"topic does not exist: {topic}")
        partitions = sorted(metadata.topics[topic].partitions)
        bounds = {}
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            bounds[partition] = {"low": low, "high": high}
        consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)
                         for partition in partitions])
        counts = collections.Counter()
        last_offsets = {partition: bounds[partition]["low"] for partition in partitions}
        # Transaction commit/end markers occupy log offsets but are not delivered as records under
        # read_committed. Allow a small control tail, then require a quiet drain before completing.
        control_slack = 4
        started = time.monotonic()
        deadline = started + timeout
        last_progress = started
        complete = False
        drained = False
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with Path(output_path).open("w", encoding="utf-8") as target:
            while time.monotonic() < deadline:
                message = consumer.poll(.5)
                if message is None:
                    reached = all(last_offsets[partition] >= max(bounds[partition]["low"],
                                                                  bounds[partition]["high"] - control_slack)
                                  for partition in partitions)
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
                counts[message.partition()] += 1
                last_offsets[message.partition()] = max(last_offsets[message.partition()],
                                                         message.offset() + 1)
                last_progress = time.monotonic()
        if not complete:
            complete = all(last_offsets[partition] >= max(bounds[partition]["low"],
                                                          bounds[partition]["high"] - control_slack)
                           for partition in partitions)
        meta = {"topic": topic, "output": str(output_path),
                "partitions": {str(partition): {"low": bounds[partition]["low"],
                                                "high": bounds[partition]["high"],
                                                "records": counts[partition],
                                                "last": last_offsets[partition]}
                               for partition in partitions},
                "records": sum(counts.values()),
                "high_total": sum(item["high"] for item in bounds.values()),
                "low_total": sum(item["low"] for item in bounds.values()),
                "control_slack": control_slack,
                "complete": complete, "seconds": round(time.monotonic() - started, 3)}
        return meta
    finally:
        consumer.close()


def topic_offsets(bootstrap, topics):
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as exc:
        raise ValueError("confluent_kafka is required for offsets; run inside the benchmark image") from exc
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "reconcile-offsets-" + uuid.uuid4().hex[:8]})
    try:
        metadata = consumer.list_topics(timeout=15)
        result = {}
        for topic in topics:
            if topic not in metadata.topics:
                raise ValueError(f"topic does not exist: {topic}")
            result[topic] = {}
            for partition in sorted(metadata.topics[topic].partitions):
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                result[topic][str(partition)] = {"low": low, "high": high}
        return result
    finally:
        consumer.close()


def _print(value):
    print(json.dumps(value, indent=2, allow_nan=False))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    scan = commands.add_parser("scan", help="account for source rows and reversals")
    scan.add_argument("--csv", required=True)
    scan.add_argument("--timezone", required=True)
    scan.add_argument("--window-ms", type=int, default=DEFAULT_WINDOW_MS)
    scan.add_argument("--max-out-of-order-ms", type=int, default=DEFAULT_MAX_OUT_OF_ORDER_MS)
    scan.add_argument("--output")

    select = commands.add_parser("select", help="copy a contiguous capture-time slice")
    select.add_argument("--csv", required=True)
    select.add_argument("--output", required=True)
    select.add_argument("--start", required=True, help="HH:MM:SS[.mmm] system capture time")
    select.add_argument("--end", required=True, help="HH:MM:SS[.mmm] system capture time")
    select.add_argument("--timezone", default="Europe/Berlin")

    extract = commands.add_parser("extract", help="copy a contiguous 1-based data row block")
    extract.add_argument("--csv", required=True)
    extract.add_argument("--output", required=True)
    extract.add_argument("--start-row", type=int, required=True)
    extract.add_argument("--end-row", type=int, required=True)

    sort = commands.add_parser("sort", help="stable event-time sort of loader-emitted rows")
    sort.add_argument("--csv", required=True)
    sort.add_argument("--output", required=True)
    sort.add_argument("--db", required=True)
    sort.add_argument("--timezone", required=True)

    normalize = commands.add_parser("normalize", help="normalize committed Kafka output")
    normalize.add_argument("--input", required=True)
    normalize.add_argument("--output", required=True)
    normalize.add_argument("--window-ms", type=int, default=DEFAULT_WINDOW_MS)

    compare = commands.add_parser("compare", help="disk-backed oracle comparison")
    compare.add_argument("--expected", required=True)
    compare.add_argument("--actual", required=True)
    compare.add_argument("--db", required=True)
    compare.add_argument("--sample-limit", type=int, default=20)

    gate = commands.add_parser("gate", help="fail-closed run eligibility gate")
    gate.add_argument("--manifest", required=True)

    dump = commands.add_parser("dump", help="dump a committed topic to JSON Lines")
    dump.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    dump.add_argument("--topic", required=True)
    dump.add_argument("--output", required=True)
    dump.add_argument("--timeout", type=float, default=600)

    offsets = commands.add_parser("offsets", help="show topic watermark offsets")
    offsets.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    offsets.add_argument("--topics", required=True, help="comma-separated topic names")
    offsets.add_argument("--output")

    args = parser.parse_args(argv)
    try:
        if args.command == "scan":
            result = scan_source(args.csv, ZoneInfo(args.timezone), args.window_ms,
                                 args.max_out_of_order_ms)
            if args.output:
                Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
            _print(result)
            return 0
        if args.command == "select":
            result = select_capture_slice(args.csv, args.output, args.start, args.end,
                                          ZoneInfo(args.timezone))
            _print(result)
            return 0
        if args.command == "extract":
            _print(extract_rows(args.csv, args.output, args.start_row, args.end_row))
            return 0
        if args.command == "sort":
            result = sort_event_time(args.csv, args.output, args.db, ZoneInfo(args.timezone))
            _print(result)
            return 0
        if args.command == "normalize":
            _print(normalize_jsonl(args.input, args.output, args.window_ms))
            return 0
        if args.command == "compare":
            report = compare_jsonl(args.expected, args.actual, args.db, args.sample_limit)
            _print(report)
            return 0 if report["match"] else 1
        if args.command == "gate":
            manifest = json.loads(Path(args.manifest).read_text())
            passed, checks = evaluate(manifest)
            _print({"passed": passed, "checks": checks})
            return 0 if passed else 1
        if args.command == "dump":
            _print(dump_topic(args.bootstrap, args.topic, args.output, args.timeout))
            return 0
        if args.command == "offsets":
            result = topic_offsets(args.bootstrap, [topic for topic in args.topics.split(",") if topic])
            if args.output:
                Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
            _print(result)
            return 0
    except (OSError, ValueError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Verify and profile a manifest-ordered set of DEBS CSV files."""

import argparse
import csv
import hashlib
import json
import math
import re
import sqlite3
import sys
import tempfile
from collections import Counter, deque
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

FIELDS = (("ask", 4), ("bid", 6), ("close", 10), ("last", 21), ("trading_time", 23), ("trading_date", 26))


@lru_cache(maxsize=8192)
def capture_ns(date, time):
    if not re.fullmatch(r"\d{2}-\d{2}-\d{4}", date) or not re.fullmatch(r"\d{2}:\d{2}:\d{2}(?:\.\d{0,9})?", time):
        raise ValueError("invalid date or time format")
    whole, _, fraction = time.partition(".")
    if len(fraction) > 9 or (fraction and not fraction.isdigit()):
        raise ValueError("invalid fractional seconds")
    moment = datetime.strptime(date + " " + whole, "%d-%m-%Y %H:%M:%S")
    return ((moment - datetime(1970, 1, 1)) // timedelta(seconds=1)) * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")


def replay_screen(row):
    if len(row) != 40:
        return "invalid_width", None
    if not row[0].strip() or not row[1].strip():
        return "missing_symbol_or_security", None
    system_date, system_time = row[2].strip(), row[3].strip()
    trading_date, trading_time = row[26].strip(), row[23].strip()
    date, time = trading_date or system_date, trading_time or system_time
    if not date or not time:
        return "missing_time", None
    try:
        capture_ns(date, time)
        source = "trading" if trading_date and trading_time else "mixed" if trading_date or trading_time else "system"
    except ValueError:
        try:
            capture_ns(system_date, system_time)
        except ValueError:
            return "invalid_timestamp", None
        source = "system_after_invalid_trading"
    return "emitted", source


def format_ns(value):
    seconds, nanos = divmod(value, 1_000_000_000)
    return (datetime(1970, 1, 1) + timedelta(seconds=seconds)).isoformat() + f".{nanos:09d}"


def peak(db, column):
    left = iter(db.execute(f"SELECT stamp, {column} FROM ticks WHERE {column} > 0 ORDER BY stamp"))
    earliest = next(left, None)
    total = 0
    best = None
    for stamp, count in db.execute(f"SELECT stamp, {column} FROM ticks WHERE {column} > 0 ORDER BY stamp"):
        total += count
        while earliest is not None and earliest[0] <= stamp - 300_000_000_000:
            total -= earliest[1]
            earliest = next(left, None)
        if total and (best is None or total > best["rows"]):
            best = {"start": format_ns(earliest[0]), "end_exclusive": format_ns(earliest[0] + 300_000_000_000), "rows": total}
    return best


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch-dir", type=Path, help="disk-backed directory for temporary timestamp index (default: output directory)")
    parser.add_argument("--series", type=Path, help="optional output JSONL for sparse per-second and trailing five-minute rates")
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    protected = {args.manifest.resolve(), *((args.data_dir / entry["key"]).resolve() for entry in manifest["files"])}
    outputs = [args.output.resolve()] + ([args.series.resolve()] if args.series else [])
    if any(path in protected for path in outputs) or len(set(outputs)) != len(outputs):
        parser.error("output paths must be distinct and must not overwrite inputs")
    result = {"doi": manifest["doi"], "license": manifest["license"], "files": [],
              "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
              "generation_command": sys.argv,
              "definitions": {
                  "scope": "All rows for category and rank counts; FR, NL and ETR with valid capture date and time for peaks.",
                  "capture_time": "CSV system Date and Time as naive local wall time, up to nanoseconds; no fallback or timezone conversion.",
                  "trade": "Last price parses as finite and strictly positive; a provisional price-bearing row, not a confirmed trade or query eligibility.",
                  "raw_peak": "Half-open 300-second interval starting at an observed capture timestamp; ties choose earliest start.",
                  "one_second_peak": "Midnight-aligned one-second bucket; ties choose earliest second.",
                  "field_presence": "Nonblank cells in valid-width rows; overlaps row categories.",
                  "replay_screening": "Exclusive default emitter (price filter none) outcomes, before downstream validation; includes all exchanges.",
                  "timestamp_fallback": "Emitted rows by trading/system date and time source; invalid selected pairs retry the system pair. Wall-clock parsing only, without timezone conversion.",
                  "symbol_ranks": "All valid-width rows, descending count then symbol; share denominator is all valid-width rows."
              }}
    categories, exchanges, dispositions, security_types, presence = (Counter() for _ in range(5))
    screening, fallback = Counter(), Counter()
    with tempfile.TemporaryDirectory(prefix="profile-week-", dir=args.scratch_dir or args.output.parent) as temp:
      db = sqlite3.connect(str(Path(temp) / "timestamps.sqlite"))
      db.execute("CREATE TABLE ticks (stamp INTEGER PRIMARY KEY, raw INTEGER, trade INTEGER)")
      db.execute("CREATE TABLE symbols (symbol TEXT PRIMARY KEY, rows INTEGER)")
      for entry in manifest["files"]:
        path = args.data_dir / entry["key"]
        md5, sha256 = hashlib.md5(), hashlib.sha256()
        size = 0
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                size += len(chunk)
                md5.update(chunk)
                sha256.update(chunk)
        if size != entry["size"] or "md5:" + md5.hexdigest() != entry["checksum"]:
            raise ValueError(f"Provider verification failed: {path.name}")
        with path.open(newline="", encoding="utf-8") as source:
            for _ in range(13):
                next(source)
            rows = 0
            day_exchanges, day_security, day_presence = Counter(), Counter(), Counter({name: 0 for name, _ in FIELDS})
            day_screening, day_fallback = Counter(), Counter()
            day_categories = Counter()
            day_capture = Counter()
            batch = Counter()
            symbols = Counter()
            for row in csv.reader(source):
                rows += 1
                outcome, timestamp_source = replay_screen(row)
                screening[outcome] += 1
                day_screening[outcome] += 1
                if timestamp_source:
                    fallback[timestamp_source] += 1
                    day_fallback[timestamp_source] += 1
                if len(row) != 40:
                    categories["invalid_width"] += 1
                    day_categories["invalid_width"] += 1
                    dispositions["invalid_width"] += 1
                    day_capture["invalid_width"] += 1
                    continue
                base, separator, exchange = row[0].strip().rpartition(".")
                if not separator or not base:
                    exchange = ""
                exchanges[exchange or "unknown"] += 1
                day_exchanges[exchange or "unknown"] += 1
                security_types[row[1].strip() or "unknown"] += 1
                day_security[row[1].strip() or "unknown"] += 1
                symbols[row[0].strip() or "unknown"] += 1
                for name, index in FIELDS:
                    if row[index].strip():
                        presence[name] += 1
                        day_presence[name] += 1
                if len(symbols) >= 50000:
                    db.executemany("INSERT INTO symbols VALUES (?, ?) ON CONFLICT(symbol) DO UPDATE SET rows=rows+excluded.rows", symbols.items())
                    symbols.clear()
                try:
                    price = float(row[21])
                    trade = math.isfinite(price) and price > 0
                except ValueError:
                    trade = False
                categories["trade" if trade else "non_trade"] += 1
                day_categories["trade" if trade else "non_trade"] += 1
                if exchange not in {"FR", "NL", "ETR"}:
                    dispositions["other_exchange"] += 1
                    day_capture["other_exchange"] += 1
                    continue
                try:
                    stamp = capture_ns(row[2].strip(), row[3].strip())
                except ValueError:
                    dispositions["invalid_capture_time"] += 1
                    day_capture["invalid_capture_time"] += 1
                    continue
                dispositions["included"] += 1
                day_capture["included"] += 1
                batch[(stamp, trade)] += 1
                if len(batch) >= 50000:
                    db.executemany("INSERT INTO ticks VALUES (?, ?, ?) ON CONFLICT(stamp) DO UPDATE SET raw=raw+excluded.raw, trade=trade+excluded.trade", ((t, n, n if trade else 0) for (t, trade), n in batch.items()))
                    batch.clear()
            db.executemany("INSERT INTO ticks VALUES (?, ?, ?) ON CONFLICT(stamp) DO UPDATE SET raw=raw+excluded.raw, trade=trade+excluded.trade", ((t, n, n if trade else 0) for (t, trade), n in batch.items()))
            db.executemany("INSERT INTO symbols VALUES (?, ?) ON CONFLICT(symbol) DO UPDATE SET rows=rows+excluded.rows", symbols.items())
        result["files"].append({**entry, "sha256": sha256.hexdigest(), "rows": rows,
                                "exchanges": dict(day_exchanges), "security_types": dict(day_security),
                                 "field_presence": dict(day_presence), "replay_screening": dict(day_screening),
                                 "timestamp_fallback": dict(day_fallback), "row_categories": dict(day_categories),
                                 "capture_dispositions": dict(day_capture)})
      symbol_total = db.execute("SELECT COALESCE(SUM(rows), 0) FROM symbols").fetchone()[0]
      result["top_symbol_shares"] = {str(n): db.execute("SELECT COALESCE(SUM(rows), 0) FROM (SELECT rows FROM symbols ORDER BY rows DESC, symbol LIMIT ?)", (n,)).fetchone()[0] / symbol_total if symbol_total else 0.0 for n in (1, 10, 100)}
      second = db.execute("SELECT stamp / 1000000000 AS second, SUM(raw) AS n FROM ticks GROUP BY second ORDER BY n DESC, second LIMIT 1").fetchone()
      result.update(row_categories=dict(categories), exchanges=dict(exchanges), security_types=dict(security_types), field_presence={name: presence[name] for name, _ in FIELDS}, replay_screening=dict(screening), timestamp_fallback=dict(fallback), capture_dispositions=dict(dispositions), raw_peak=peak(db, "raw"), trade_peak=peak(db, "trade"), one_second_peak={"start": format_ns(second[0] * 1000000000), "rows": second[1]} if second else None)
      if args.series:
        result["definitions"]["series"] = "Sparse nonempty one-second buckets; trailing counts include the current and previous 299 aligned seconds; rate is count/300. Exact nanosecond rolling peaks are reported separately."
        window = deque()
        raw_total = trade_total = 0
        digest = hashlib.sha256()
        with args.series.open("wb") as target:
            for stamp, raw, trade in db.execute("SELECT stamp / 1000000000 AS second, SUM(raw), SUM(trade) FROM ticks GROUP BY second ORDER BY second"):
                while window and window[0][0] <= stamp - 300:
                    _, old_raw, old_trade = window.popleft()
                    raw_total -= old_raw
                    trade_total -= old_trade
                window.append((stamp, raw, trade))
                raw_total += raw
                trade_total += trade
                record = {"start": format_ns(stamp * 1000000000), "raw": raw, "trade": trade,
                          "rolling_raw": raw_total, "rolling_trade": trade_total,
                          "rolling_raw_per_second": raw_total / 300, "rolling_trade_per_second": trade_total / 300}
                line = (json.dumps(record, separators=(",", ":")) + "\n").encode()
                target.write(line)
                digest.update(line)
        result["series_sha256"] = digest.hexdigest()
      with args.output.open("w", encoding="utf-8") as target:
        target.write(json.dumps(result, indent=2)[:-2] + ',\n  "symbol_ranks": [')
        separator = ""
        for symbol, count in db.execute("SELECT symbol, rows FROM symbols ORDER BY rows DESC, symbol"):
            target.write(separator + json.dumps({"symbol": symbol, "rows": count, "share": count / symbol_total}))
            separator = ","
        target.write("]\n}\n")
      db.close()


if __name__ == "__main__":
    main()

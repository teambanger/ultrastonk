#!/usr/bin/env python3
"""Complete-input reference for the raw DEBS CSV query, independent of Flink."""
import argparse
import csv
import datetime as dt
import json
import math
import re
import sqlite3
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo


def timestamp(date, time, zone):
    # Python parses at microsecond precision; the replay truncates to milliseconds.
    value = dt.datetime.strptime(date + " " + time[:15], "%d-%m-%Y %H:%M:%S.%f" if "." in time else "%d-%m-%Y %H:%M:%S")
    return int(value.replace(tzinfo=zone).timestamp() * 1000)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timezone", required=True, help="IANA zone of CSV timestamps")
    parser.add_argument("--skip-lines", type=int, default=13)
    parser.add_argument("csv", nargs="+", type=Path, help="raw files in original source order")
    args = parser.parse_args()
    zone = ZoneInfo(args.timezone)
    with tempfile.TemporaryDirectory(prefix="query-truth-") as tmp:
        db = sqlite3.connect(str(Path(tmp) / "closes.sqlite"))
        db.execute("CREATE TABLE closes (symbol TEXT, window INTEGER, ts INTEGER, ordinal INTEGER, price REAL, PRIMARY KEY(symbol, window))")
        ordinal = 0
        for path in args.csv:
            with path.open(newline="", encoding="utf-8") as f:
                for _ in range(args.skip_lines):
                    next(f, None)
                for row in csv.reader(f):
                    ordinal += 1
                    if len(row) != 40 or not row[1].strip() or not re.fullmatch(r"[^\s.]+(?:\.[^\s.]+)*\.[^\s.]+", row[0].strip()):
                        continue
                    try:
                        price = float(row[21])
                        if not math.isfinite(price) or price <= 0:
                            continue
                        try:
                            ts = timestamp(row[26].strip() or row[2].strip(), row[23].strip() or row[3].strip(), zone)
                        except ValueError:
                            ts = timestamp(row[2].strip(), row[3].strip(), zone)
                        if ts < 0:
                            continue
                    except (ValueError, OverflowError):
                        continue
                    db.execute("""INSERT INTO closes VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(symbol, window) DO UPDATE SET ts=excluded.ts, ordinal=excluded.ordinal, price=excluded.price
                        WHERE (excluded.ts, excluded.ordinal) > (closes.ts, closes.ordinal)""",
                        (row[0].strip(), ts // 300000 * 300000, ts, ordinal, price))
        db.commit()
        symbol = None
        for current, window, ts, price in db.execute("SELECT symbol, window, ts, price FROM closes ORDER BY symbol, window"):
            if current != symbol:
                symbol, fast, slow = current, 0.0, 0.0
            previous = fast - slow
            fast += (2.0 / 39.0) * (price - fast)
            slow += (2.0 / 101.0) * (price - slow)
            print(json.dumps(dict(symbol=symbol, window=window, type="EMA", ema38=fast, ema100=slow, timestamp_ms=ts)))
            direction = "BUY" if previous <= 0 and fast > slow else "SELL" if previous >= 0 and fast < slow else None
            if direction:
                print(json.dumps(dict(symbol=symbol, window=window, type=direction)))


if __name__ == "__main__":
    main()

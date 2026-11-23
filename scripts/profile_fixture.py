#!/usr/bin/env python3
"""Describe the raw peak fixture without exporting source data."""

import argparse
import csv
import hashlib
import json
import sqlite3
import tempfile
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from profile_week import capture_ns


EPOCH = datetime(1970, 1, 1)
SECOND = 1_000_000_000


def parse_iso(value):
    date, time = value.split("T")
    return capture_ns(datetime.strptime(date, "%Y-%m-%d").strftime("%d-%m-%Y"), time)


def format_ns(value):
    seconds, nanos = divmod(value, SECOND)
    return (EPOCH + timedelta(seconds=seconds)).isoformat() + f".{nanos:09d}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch-dir", type=Path, help="disk-backed directory for timestamp index")
    args = parser.parse_args()
    profile_bytes = args.profile.read_bytes()
    profile = json.loads(profile_bytes)
    peak = profile["raw_peak"]
    if not isinstance(peak, dict):
        raise ValueError("profile has no raw peak")
    start, end = parse_iso(peak["start"]), parse_iso(peak["end_exclusive"])
    if end - start != 300 * SECOND or not isinstance(peak["rows"], int) or peak["rows"] <= 0:
        raise ValueError("invalid raw peak")
    manifest_bytes = args.manifest.read_bytes()
    manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
    if profile.get("manifest_sha256") != manifest_hash:
        raise ValueError("profile manifest checksum does not match")
    manifest = json.loads(manifest_bytes)
    protected = {args.profile.resolve(), args.manifest.resolve(), *((args.data_dir / entry["key"]).resolve() for entry in manifest["files"])}
    if args.output.resolve() in protected:
        parser.error("output path must not overwrite an input")
    if not isinstance(profile.get("files"), list) or [entry["key"] for entry in profile["files"]] != [entry["key"] for entry in manifest["files"]]:
        raise ValueError("profile and manifest file order do not match")
    for entry in profile["files"]:
        digest = hashlib.sha256()
        with (args.data_dir / entry["key"]).open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        if digest.hexdigest() != entry.get("sha256"):
            raise ValueError("source checksum does not match profile: " + entry["key"])
    counts = dict.fromkeys(("FR", "NL", "ETR"), 0)
    temp = tempfile.TemporaryDirectory(prefix="profile-fixture-", dir=args.scratch_dir or args.output.parent)
    db = sqlite3.connect(str(Path(temp.name) / "timestamps.sqlite"))
    db.execute("CREATE TABLE ticks (stamp INTEGER PRIMARY KEY, rows INTEGER)")
    batch = Counter()
    earliest = latest = pre = trailing = previous = None
    ordinal = selected = decreases = equal = max_gap = max_backward = 0
    pre_count = trailing_count = 0
    earliest_pre = latest_trailing = None
    for entry in manifest["files"]:
        with (args.data_dir / entry["key"]).open(newline="", encoding="utf-8") as source:
            for _ in range(13):
                next(source)
            for row_number, row in enumerate(csv.reader(source), 1):
                ordinal += 1
                if len(row) != 40:
                    continue
                base, separator, exchange = row[0].strip().rpartition(".")
                if not separator or not base:
                    exchange = ""
                if exchange not in counts:
                    continue
                try:
                    stamp = capture_ns(row[2].strip(), row[3].strip())
                except ValueError:
                    continue
                if stamp < start:
                    pre_count += 1
                    earliest_pre = stamp if earliest_pre is None else min(earliest_pre, stamp)
                    if pre is None or stamp > pre[0]:
                        pre = (stamp, 1)
                    elif stamp == pre[0]:
                        pre = (stamp, pre[1] + 1)
                elif stamp >= end:
                    trailing_count += 1
                    latest_trailing = stamp if latest_trailing is None else max(latest_trailing, stamp)
                    if trailing is None or stamp < trailing[0]:
                        trailing = (stamp, 1)
                    elif stamp == trailing[0]:
                        trailing = (stamp, trailing[1] + 1)
                else:
                    selected += 1
                    batch[stamp] += 1
                    if len(batch) >= 50000:
                        db.executemany("INSERT INTO ticks VALUES (?, ?) ON CONFLICT(stamp) DO UPDATE SET rows=rows+excluded.rows", batch.items())
                        batch.clear()
                    counts[exchange] += 1
                    location = {"timestamp": format_ns(stamp), "source": {"file": entry["key"], "row": row_number, "ordinal": ordinal}}
                    if earliest is None or stamp < earliest[0]:
                        earliest = (stamp, location)
                    if latest is None or stamp > latest[0]:
                        latest = (stamp, location)
                    if previous is not None:
                        delta = stamp - previous
                        decreases += delta < 0
                        equal += delta == 0
                        max_gap = max(max_gap, delta)
                        max_backward = max(max_backward, -delta)
                    previous = stamp
    if selected != peak["rows"] or earliest is None or earliest[0] != start:
        raise ValueError("selected rows do not match profile raw_peak")
    db.executemany("INSERT INTO ticks VALUES (?, ?) ON CONFLICT(stamp) DO UPDATE SET rows=rows+excluded.rows", batch.items())
    sorted_previous = None
    sorted_equal = sorted_max_gap = 0
    for stamp, rows in db.execute("SELECT stamp, rows FROM ticks ORDER BY stamp"):
        sorted_equal += rows - 1
        if sorted_previous is not None:
            sorted_max_gap = max(sorted_max_gap, stamp - sorted_previous)
        sorted_previous = stamp
    db.close()
    temp.cleanup()
    result = {
        "manifest_sha256": manifest_hash,
        "profile_sha256": hashlib.sha256(profile_bytes).hexdigest(),
        "definitions": {
            "selection": "40-column rows with exchange FR, NL or ETR and valid system capture time in [start,end_exclusive); manifest file order, then CSV row order.",
            "source": "One-based row within the file after 13 header lines and one-based ordinal across all manifest data rows, including excluded rows.",
            "extrema": "Earliest and latest capture timestamps; ties choose first source row.",
            "capture_order": "Adjacent selected rows in source order; gaps are signed timestamp differences, reported in nanoseconds. A decrease is an adjacent negative gap.",
            "timestamp_order": "Ascending system timestamp, then original source ordinal for ties; gaps in nanoseconds.",
            "context": "Nearest eligible capture timestamp strictly before start or at/after end, across all manifest files; rows counts ties at that timestamp.",
        },
        "raw_peak": peak, "counts_by_exchange": counts,
        "earliest": earliest[1], "latest": latest[1],
        "capture_order": {"adjacent_pairs": max(0, selected - 1), "decreases": decreases, "equal_timestamps": equal,
                          "max_forward_gap_ns": max_gap, "max_backward_gap_ns": max_backward},
        "timestamp_order": {"adjacent_pairs": max(0, selected - 1), "equal_timestamps": sorted_equal,
                            "max_gap_ns": sorted_max_gap, "tie_order": "source ordinal"},
        "pre_roll": {"timestamp": format_ns(pre[0]), "rows": pre[1]} if pre else None,
        "trailing": {"timestamp": format_ns(trailing[0]), "rows": trailing[1]} if trailing else None,
        "context_available": {"pre_roll_rows": pre_count, "earliest_pre_roll": format_ns(earliest_pre) if earliest_pre is not None else None,
                              "trailing_rows": trailing_count, "latest_trailing": format_ns(latest_trailing) if latest_trailing is not None else None},
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()

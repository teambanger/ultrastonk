#!/usr/bin/env python3
"""Event-time-ordered reference input for the full-week replay (ticket 06).

Ordinary capture-order replay drops eligible trades when source order is not event-time
order, which breaks exact reference correctness. This tool produces the agreed separately
labeled event-time-ordered input: each week CSV is stably sorted by (event millisecond,
original source ordinal), preserving the loader's emitted multiset and the production
last-arrival tie rule, and never precomputing closes. Sorting cost is reported separately.

The sort key reuses `reconcile_replay.classify`, so the ordering key is the exact event
time the replay loader and the batch oracle use. `sort` runs with LC_ALL=C over fixed-width
zero-padded keys, so a plain byte sort reproduces numeric (timestamp, ordinal) order.
"""
import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks import reconcile_replay

KEY_WIDTH = 22  # 13-digit milliseconds + 9-digit ordinal


def write_keyed(path, keyed_path, zone):
    """Stream one CSV into `<ts><ordinal> <line>` rows for the emitted multiset."""
    ordinal = 0
    rows_read = emitted = 0
    with Path(path).open(encoding="utf-8") as source, \
            Path(keyed_path).open("w", encoding="utf-8") as out:
        header = [next(source, "") for _ in range(reconcile_replay.SKIP_LINES)]
        if not all(header):
            raise ValueError(f"{path}: fewer than {reconcile_replay.SKIP_LINES} header lines")
        for line in source:
            ordinal += 1
            rows_read += 1
            disposition, timestamp, _malformed, _fallback = reconcile_replay.classify(
                reconcile_replay._row_from_line(line), zone)
            if disposition != "emitted":
                continue
            emitted += 1
            out.write(f"{timestamp:013d}{ordinal:09d} {line.rstrip(chr(10)).rstrip(chr(13))}\n")
    return {"header": header, "rows_read": rows_read, "emitted": emitted}


def sort_keyed(keyed_path, sorted_path, scratch, memory="2G"):
    environment = dict(os.environ, LC_ALL="C")
    subprocess.run(["sort", "-S", memory, "-T", str(scratch), "-o", str(sorted_path),
                    str(keyed_path)], check=True, env=environment)


def strip_and_write(header, sorted_path, output_path):
    with Path(output_path).open("w", encoding="utf-8") as out:
        out.write("".join(header))
        with Path(sorted_path).open(encoding="utf-8") as keyed:
            for line in keyed:
                out.write(line.split(" ", 1)[1])


def sort_week_file(path, output_dir, zone, scratch, memory="2G"):
    path = Path(path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    keyed = output_dir / f"{path.name}.keyed"
    sorted_keyed = output_dir / f"{path.name}.sorted.keyed"
    output = output_dir / path.name
    started = time.perf_counter()
    try:
        stats = write_keyed(path, keyed, zone)
        sort_keyed(keyed, sorted_keyed, scratch, memory)
        strip_and_write(stats["header"], sorted_keyed, output)
    finally:
        keyed.unlink(missing_ok=True)
        sorted_keyed.unlink(missing_ok=True)
    return {"source": str(path), "output": str(output), "rows_read": stats["rows_read"],
            "emitted": stats["emitted"], "output_bytes": output.stat().st_size,
            "seconds": time.perf_counter() - started}


def _job(args):
    return sort_week_file(*args)


def sort_week(data_dir, output_dir, zone, jobs, scratch, memory="2G", timing_path=None):
    files = sorted(Path(data_dir).glob("debs2022-gc-trading-day-*.csv"))
    if not files:
        raise ValueError(f"no week CSVs under {data_dir}")
    Path(scratch).mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=jobs) as pool:
        results = list(pool.map(_job, [(str(f), output_dir, zone, scratch, memory)
                                       for f in files]))
    results.sort(key=lambda item: Path(item["source"]).name)
    timing = {"parallel_jobs": jobs, "wall_seconds": time.perf_counter() - started,
              "files": results}
    if timing_path:
        Path(timing_path).write_text(json.dumps(timing, indent=2, default=str) + "\n")
    return timing


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timezone", default="Europe/Berlin")
    parser.add_argument("--jobs", type=int, default=7)
    parser.add_argument("--scratch", required=True)
    parser.add_argument("--memory", default="2G")
    parser.add_argument("--timing")
    args = parser.parse_args(argv)
    try:
        timing = sort_week(args.data_dir, args.output_dir, ZoneInfo(args.timezone), args.jobs,
                           args.scratch, args.memory, args.timing)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"wall_seconds": timing["wall_seconds"],
                      "files": [{"file": Path(f["source"]).name, "emitted": f["emitted"],
                                 "seconds": round(f["seconds"], 1)} for f in timing["files"]]},
                     indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

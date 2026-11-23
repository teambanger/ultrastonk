#!/usr/bin/env python3
"""Summarize repeated matrix runs; keep correctness failures visible."""
import argparse
import collections
import csv
import math
from pathlib import Path
import statistics


def measurements(rows, key):
    for row in rows:
        try:
            value = float(row.get(key) or "nan")
        except ValueError:
            continue
        if math.isfinite(value) and value >= 0:
            yield value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("matrix_csv", type=Path)
    args = parser.parse_args()
    with args.matrix_csv.open() as stream:
        reader = csv.DictReader(stream)
        websocket = "gateway_correctness_passed" in (reader.fieldnames or [])
        rows = list(reader)
    groups = collections.defaultdict(list)
    for row in rows:
        groups[(row.get("quality", "mixed"), *tuple(row[key] for key in ["dedup", "late", "parallelism", "target_rate"]))].append(row)
    lines = ["# Benchmark comparison", "",
             "Throughput and latency medians and throughput sample standard deviation use passing runs only.",
             "Latency includes Kafka transaction visibility. Failed runs still count toward total runs.", "",
             "| Quality | Dedup | Late policy | Workers | Target originals/s | Passes/runs | Committed prices/s | Std dev | p99 visible latency (ms) |",
             "|---|---|---|---:|---:|---:|---:|---:|---:|"]
    if websocket:
        lines[-2] += " WebSocket prices/s | p99 end-to-end latency (ms) |"
        lines[-1] += "---:|---:|"
    for key, group in sorted(groups.items()):
        passed = [row for row in group if row["exit_code"] == "0" and row["price_correctness_passed"] == "True"
                  and (not websocket or row["gateway_correctness_passed"] == "True")]
        rates = list(measurements(passed, "committed_prices_per_second"))
        latencies = list(measurements(passed, "latency_p99_ms"))
        fmt = lambda value: f"{value:.2f}" if value is not None else "n/a"
        cells = [*key, f"{len(passed)}/{len(group)}",
            fmt(statistics.median(rates) if rates else None), fmt(statistics.stdev(rates) if len(rates) > 1 else None),
            fmt(statistics.median(latencies) if latencies else None)]
        if websocket:
            for metric in ("websocket_prices_per_second", "websocket_latency_p99_ms"):
                values = list(measurements(passed, metric))
                cells.append(fmt(statistics.median(values) if values else None))
        lines.append("| " + " | ".join(cells) + " |")
    result = "\n".join(lines) + "\n"
    output = args.matrix_csv.with_suffix(".md")
    output.write_text(result)
    print(result)


if __name__ == "__main__":
    main()

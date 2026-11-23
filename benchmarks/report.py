#!/usr/bin/env python3
"""Summarize repeated matrix runs; keep correctness failures visible."""
import argparse
import collections
import csv
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("matrix_csv", type=Path)
    args = parser.parse_args()
    with args.matrix_csv.open() as stream:
        rows = list(csv.DictReader(stream))
    groups = collections.defaultdict(list)
    for row in rows:
        groups[(row.get("quality", "mixed"), *tuple(row[key] for key in ["dedup", "late", "parallelism", "target_rate"]))].append(row)
    lines = ["# Benchmark comparison", "",
             "Throughput and latency are medians across repetitions. Throughput spread is sample standard deviation.",
             "Latency includes Kafka transaction visibility. Failed runs remain in the table.", "",
             "| Quality | Dedup | Late policy | Workers | Target originals/s | Passes/runs | Committed prices/s | Std dev | p99 visible latency (ms) |",
             "|---|---|---|---:|---:|---:|---:|---:|---:|"]
    for key, group in sorted(groups.items()):
        rates = [float(row["committed_prices_per_second"]) for row in group if row.get("committed_prices_per_second")]
        latencies = [float(row["latency_p99_ms"]) for row in group if row.get("latency_p99_ms")]
        passed = sum(row["exit_code"] == "0" and row["price_correctness_passed"] == "True" for row in group)
        fmt = lambda value: f"{value:.2f}" if value is not None else "n/a"
        lines.append("| " + " | ".join([*key, f"{passed}/{len(group)}",
            fmt(statistics.median(rates) if rates else None), fmt(statistics.stdev(rates) if len(rates) > 1 else None),
            fmt(statistics.median(latencies) if latencies else None)]) + " |")
    result = "\n".join(lines) + "\n"
    output = args.matrix_csv.with_suffix(".md")
    output.write_text(result)
    print(result)


if __name__ == "__main__":
    main()

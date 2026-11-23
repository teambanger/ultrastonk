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
        resources = "resource_profile" in (reader.fieldnames or [])
        rows = list(reader)
    groups = collections.defaultdict(list)
    for row in rows:
        groups[(row.get("resource_profile", "default"), row.get("quality", "mixed"),
                *(row[key] for key in ("dedup", "late", "parallelism", "target_rate")))].append(row)
    lines = ["# Benchmark comparison", "",
             "Throughput and latency medians and throughput sample standard deviation use passing runs only.",
             "Latency includes Kafka transaction visibility. Failed runs still count toward total runs.", "",
             "| Quality | Dedup | Late policy | Workers | Target originals/s | Passes/runs | Committed prices/s | Std dev | p99 visible latency (ms) |",
             "|---|---|---|---:|---:|---:|---:|---:|---:|"]
    if websocket:
        lines[-2] += " WebSocket prices/s | p99 end-to-end latency (ms) |"
        lines[-1] += "---:|---:|"
    if resources:
        lines[-2] = "| Resource profile " + lines[-2]
        lines[-1] = "|---" + lines[-1]
    scaling = collections.defaultdict(dict)
    for key, group in sorted(groups.items()):
        passed = [row for row in group if row["exit_code"] == "0" and row["price_correctness_passed"] == "True"
                  and (not websocket or row["gateway_correctness_passed"] == "True")]
        rates = list(measurements(passed, "committed_prices_per_second"))
        latencies = list(measurements(passed, "latency_p99_ms"))
        fmt = lambda value: f"{value:.2f}" if value is not None else "n/a"
        cells = [*(key if resources else key[1:]), f"{len(passed)}/{len(group)}",
            fmt(statistics.median(rates) if rates else None), fmt(statistics.stdev(rates) if len(rates) > 1 else None),
            fmt(statistics.median(latencies) if latencies else None)]
        if websocket:
            for metric in ("websocket_prices_per_second", "websocket_latency_p99_ms"):
                values = list(measurements(passed, metric))
                cells.append(fmt(statistics.median(values) if values else None))
        lines.append("| " + " | ".join(cells) + " |")
        profile, quality, dedup, late, workers, offered = key
        boundary = list(measurements(passed, "websocket_prices_per_second")) if websocket else rates
        scaling[profile, quality, dedup, late, offered][int(workers)] = statistics.median(boundary) if boundary else None
    if any(len(group) > 1 for group in scaling.values()):
        lines.extend(["", "## Worker scaling", "",
                      "Compares median delivery at the same offered load and resource profile; this is not a capacity estimate.",
                      "Efficiency is delivery ratio divided by worker-count ratio. Per-worker limits stay fixed, so total resources may grow.", "",
                      "| Profile | Quality | Dedup | Late policy | Target originals/s | Workers | Baseline workers | Delivery ratio | Worker efficiency |",
                      "|---|---|---|---|---:|---:|---:|---:|---:|"])
        for key, group in sorted(scaling.items()):
            if len(group) < 2:
                continue
            baseline_workers = min(group)
            baseline = group[baseline_workers]
            for workers, rate in sorted(group.items()):
                if workers == baseline_workers:
                    continue
                ratio = rate / baseline if rate is not None and baseline else None
                ratio_text = f"{ratio:.2f}x" if ratio is not None else "n/a"
                efficiency = f"{ratio * baseline_workers / workers:.1%}" if ratio is not None else "n/a"
                lines.append("| " + " | ".join([*key, str(workers), str(baseline_workers), ratio_text, efficiency]) + " |")
    result = "\n".join(lines) + "\n"
    output = args.matrix_csv.with_suffix(".md")
    output.write_text(result)
    print(result)


if __name__ == "__main__":
    main()

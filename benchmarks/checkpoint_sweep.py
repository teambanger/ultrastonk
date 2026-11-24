"""Summarize latency and measured-window checkpoint work from matrix artifacts."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import statistics


COMPLETION = re.compile(
    r"Completed checkpoint (\d+) for job (\w+) "
    r"\((\d+) bytes, checkpointDuration=(\d+) ms, finalizationTime=\d+ ms\)\."
)


def checkpoint_work(log, start, end):
    """Count unique successful completions in the driver's measured wall interval."""
    checkpoints = {}
    for line in log.splitlines():
        match = COMPLETION.search(line)
        if not match or " | " not in line:
            continue
        entry = json.loads(line.split(" | ", 1)[1])
        if entry["logger"] != "org.apache.flink.runtime.checkpoint.CheckpointCoordinator":
            continue
        if start <= datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00")).timestamp() < end:
            checkpoint, job, size, duration = match.groups()
            checkpoints[job, checkpoint] = (int(size), int(duration))
    if not checkpoints:
        raise ValueError("no checkpoint completions in the measured interval")
    total = sum(size for size, _ in checkpoints.values())
    return {
        "completed": len(checkpoints),
        "reported_bytes": total,
        "reported_bytes_per_second": total / (end - start),
        "median_duration_ms": statistics.median(duration for _, duration in checkpoints.values()),
    }


def summarize(results, run_id):
    raw = (results / f"{run_id}.json").read_bytes()
    log = (results / f"{run_id}.containers.log").read_bytes()
    run = json.loads(raw)
    settings, args, measured = run["settings"], run["arguments"], run["measurement"]
    duration = args["duration"]
    # The final progress point is collected after drain. Use the driver's
    # measured-start clock and nominal duration, not container start/stop times.
    first = run["progress"][0]
    start = first["epoch_seconds"] - first["elapsed_seconds"]
    session = run_id.rsplit("-", 1)[0]
    manifest = json.loads((results / f"{session}-manifest.json").read_text())
    return {
        "run_id": run_id,
        "project": manifest["arguments"]["project"],
        "checkpoint_ms": int(settings["CHECKPOINT_INTERVAL_MS"]),
        "watermark_allowance_ms": int(settings["MAX_OUT_OF_ORDER_MS"]),
        "offered_originals_per_second": args["rate"],
        "warmup_seconds": args["warmup"],
        "achieved_originals_per_second": round(measured["achieved_originals_per_second"], 3),
        "measured_seconds": duration,
        "measured_start_epoch_seconds": start,
        "expected_prices": measured["expected_prices"],
        "committed_prices": measured["observed_prices"],
        "websocket_prices": measured["websocket_prices"],
        "price_correctness_passed": run["price_correctness_passed"],
        "gateway_correctness_passed": run["gateway_correctness_passed"],
        "enqueue_to_committed_ms": {k: round(v, 3) for k, v in measured["visible_roundtrip_ms"].items()},
        "enqueue_to_websocket_ms": {k: round(v, 3) for k, v in measured["websocket_roundtrip_ms"].items()},
        "checkpoint_work": checkpoint_work(log.decode(), start, start + duration),
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
        "log_sha256": hashlib.sha256(log).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=Path("benchmarks/results"))
    parser.add_argument("--output", type=Path, help="write the evidence JSON here instead of stdout")
    parser.add_argument("run_ids", nargs="+")
    args = parser.parse_args()
    output = json.dumps({"runs": [summarize(args.results, run_id) for run_id in args.run_ids]}, indent=2) + "\n"
    if args.output:
        args.output.write_text(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()

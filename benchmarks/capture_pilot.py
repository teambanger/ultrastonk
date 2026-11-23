"""Capture-paced End-to-end pilot: prove the measured path, not historical event age.

This tool consumes one isolated capture-paced Historical replay at three boundaries:
committed input (enqueue payload), committed output, and one live WebSocket subscriber.
It correlates the same event identity across those boundaries, checks that the emitter
actually achieved its capture schedule, and refuses to pass when required telemetry,
pacing, or delivery is missing.

The evaluation functions are pure so they can be exercised against controlled fixtures;
`main()` performs the live collection against a disposable Compose project.
"""
import argparse
import collections
import json
import math
import os
from pathlib import Path
import time
import urllib.request


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}

    def q(p):
        i = p * (len(values) - 1)
        lo, hi = math.floor(i), math.ceil(i)
        return values[lo] + (i - lo) * (values[hi] - values[lo])

    return {"count": len(values), "p50": q(.5), "p95": q(.95), "p99": q(.99), "max": values[-1]}


def identity(record):
    return record["ticker"], int(record["event_ms"]), round(float(record["value"]), 9)


def ema_identity(record):
    return record["ticker"], int(record["event_ms"]), record["type"], round(float(record["value"]), 9)


def _records(counter):
    return [{"ticker": ticker, "event_ms": event_ms, "value": value, "count": count}
            for (ticker, event_ms, value), count in sorted(counter.items())]


def _ema_records(counter):
    return [{"ticker": ticker, "event_ms": event_ms, "type": kind, "value": value, "count": count}
            for (ticker, event_ms, kind, value), count in sorted(counter.items())]


def _is_trade(record):
    value = record.get("value")
    return value is not None and isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def _prices(records):
    return [record for record in records if record.get("type", "price") == "price"]


def _emas(records):
    return [record for record in records if record.get("type") in {"EMA38", "EMA100"}]


def _matched_latencies(start_records, end_records, start_field, end_field):
    queues = collections.defaultdict(collections.deque)
    for record in start_records:
        queues[identity(record)].append(record)
    values = []
    for record in end_records:
        queue = queues.get(identity(record))
        if queue:
            source = queue.popleft()
            if source.get(start_field) is not None and record.get(end_field) is not None:
                values.append(record[end_field] - source[start_field])
    return values


def telemetry_check(run, require_limits=True):
    missing = []
    pacing = run.get("pacing")
    if not pacing:
        missing.append("pacing report")
    else:
        if pacing.get("mode") != "capture":
            missing.append("capture pacing mode")
        segments = pacing.get("segments") or {}
        for segment in ("PRE_ROLL", "MEASURED", "TRAILING"):
            if segment not in segments:
                missing.append(f"{segment.lower()} segment")
        if (segments.get("MEASURED") or {}).get("rows", 0) <= 0:
            missing.append("measured segment rows")
        if (pacing.get("clock_offset") or {}).get("samples", 0) <= 0:
            missing.append("capture/event clock correlation")
        if not pacing.get("samples"):
            missing.append("pacing samples")
        if not any(sample.get("ack_epoch_ms") is not None for sample in pacing.get("samples") or []):
            missing.append("enqueue/acknowledgement samples")
        if pacing.get("submitted") is None or pacing.get("failed") is None:
            missing.append("emitter submitted/failed counts")
    settings = run.get("settings") or {}
    for key in ("window_ms", "max_out_of_order_ms", "checkpoint_interval_ms", "flink_parallelism",
                "dedup", "late", "speed_multiplier"):
        if settings.get(key) is None:
            missing.append(f"run setting {key}")
    clock = run.get("clock") or {}
    if not clock.get("basis"):
        missing.append("clock basis")
    if require_limits and not run.get("limits"):
        missing.append("effective limits")
    if not run.get("job_running"):
        missing.append("running Flink job")
    if not run.get("watermark_samples"):
        missing.append("watermark samples")
    if not run.get("progress"):
        missing.append("progress samples")
    if not run.get("input"):
        missing.append("committed input records")
    if not run.get("committed"):
        missing.append("committed output records")
    if not run.get("websocket"):
        missing.append("websocket records")
    if run.get("ws_errors"):
        missing.append("websocket errors")
    return {"passed": not missing, "missing": missing,
            "reason": None if not missing else "missing telemetry: " + ", ".join(missing)}


def pacing_check(run, min_ratio=0.98):
    pacing = run.get("pacing")
    if not pacing:
        return {"passed": False, "reason": "missing pacing report"}
    if pacing.get("mode") != "capture":
        return {"passed": False, "reason": "pacing report is not capture mode"}
    measured = (pacing.get("segments") or {}).get("MEASURED") or {}
    ratio = measured.get("achieved_ratio")
    if measured.get("rows", 0) <= 0:
        return {"passed": False, "reason": "insufficient pacing: measured segment is empty"}
    if ratio is None:
        return {"passed": False, "reason": "insufficient pacing: no measurable scheduled span"}
    passed = ratio >= min_ratio
    return {"passed": passed, "achieved_ratio": ratio, "required_ratio": min_ratio,
            "max_lag_ms": measured.get("max_lag_ms"),
            "reason": None if passed else f"insufficient pacing: achieved {ratio:.4f} < {min_ratio:.4f}"}


def delivery_check(run):
    input_records = [record for record in run.get("input") or [] if _is_trade(record)]
    committed = _prices(run.get("committed") or [])
    websocket = _prices(run.get("websocket") or [])
    expected = collections.Counter(identity(record) for record in input_records)
    kafka = collections.Counter(identity(record) for record in committed)
    ws = collections.Counter(identity(record) for record in websocket)
    missing = expected - kafka
    extra = kafka - expected
    ws_missing = kafka - ws
    ws_extra = ws - kafka

    committed_emas = collections.Counter(ema_identity(record) for record in _emas(run.get("committed") or []))
    ws_emas = collections.Counter(ema_identity(record) for record in _emas(run.get("websocket") or []))
    ema_missing = committed_emas - ws_emas
    ema_extra = ws_emas - committed_emas

    pacing = run.get("pacing") or {}
    ingest_missing = ingest_extra = 0
    if pacing.get("submitted") is not None and pacing.get("failed") is not None:
        expected_inputs = int(pacing["submitted"]) - int(pacing["failed"])
        observed_inputs = len(run.get("input") or [])
        ingest_missing = max(0, expected_inputs - observed_inputs)
        ingest_extra = max(0, observed_inputs - expected_inputs)

    duplicates = {key: count - 1 for key, count in expected.items() if count > 1}
    by_timestamp = collections.defaultdict(set)
    for ticker, event_ms, value in expected:
        by_timestamp[(ticker, event_ms)].add(value)
    tied = sum(1 for values in by_timestamp.values() if len(values) > 1)

    missing_records = _records(missing) + _ema_records(ema_missing)
    extra_records = _records(extra) + _ema_records(ema_extra)
    passed = not any((missing, extra, ws_missing, ws_extra, ema_missing, ema_extra,
                      ingest_missing, ingest_extra))
    reason = None
    if not passed:
        reason = (f"delivery mismatch: {sum(missing.values())} missing and {sum(extra.values())} extra at Kafka, "
                  f"{sum(ws_missing.values())} missing and {sum(ws_extra.values())} extra at WebSocket, "
                  f"{ingest_missing} missing and {ingest_extra} extra at ingest")
    return {
        "passed": passed,
        "reason": reason,
        "missing": _records(missing + ws_missing),
        "extra": _records(extra + ws_extra),
        "ema_missing": _ema_records(ema_missing),
        "ema_extra": _ema_records(ema_extra),
        "ingest_missing": ingest_missing,
        "ingest_extra": ingest_extra,
        "expected_prices": sum(expected.values()),
        "committed_prices": sum(kafka.values()),
        "websocket_prices": sum(ws.values()),
        "duplicate_identities": len(duplicates),
        "duplicate_rows": sum(duplicates.values()),
        "tied_timestamps": tied,
    }


def _boundary_latencies(run):
    inputs = [record for record in run.get("input") or []
              if record.get("emit_ms") is not None and _is_trade(record)]
    committed = _prices(run.get("committed") or [])
    websocket = _prices(run.get("websocket") or [])
    return {
        "enqueue_to_committed_ms": _matched_latencies(inputs, committed, "emit_ms", "wall_ms"),
        "enqueue_to_websocket_ms": _matched_latencies(inputs, websocket, "emit_ms", "wall_ms"),
        "committed_to_websocket_ms": _matched_latencies(committed, websocket, "wall_ms", "wall_ms"),
    }


def identity_clock_check(run, observation_tolerance_ms=5000):
    clock = run.get("clock") or {}
    tolerance = max(0, int(clock.get("max_clock_error_ms") or 0))
    pacing = run.get("pacing") or {}
    ack_values = [sample["ack_epoch_ms"] - sample["enqueued_epoch_ms"]
                  for sample in pacing.get("samples") or []
                  if sample.get("ack_epoch_ms") is not None and sample.get("enqueued_epoch_ms") is not None]
    latency = _boundary_latencies(run)
    problems = []
    if not ack_values:
        problems.append("no enqueue/acknowledgement samples")
    if not latency["enqueue_to_committed_ms"]:
        problems.append("no enqueue-to-committed identity matches")
    if run.get("websocket") and not latency["enqueue_to_websocket_ms"]:
        problems.append("no enqueue-to-websocket identity matches")
    # Emitter-clock comparisons can only be negative through clock skew.
    emitter_clock = ack_values + latency["enqueue_to_committed_ms"] + latency["enqueue_to_websocket_ms"]
    skew = [value for value in emitter_clock if value < -tolerance]
    if skew:
        problems.append(f"{len(skew)} samples older than the documented {tolerance} ms clock error")
    # Committed observation and WebSocket receipt share the pilot clock. A negative value only
    # means the subscriber saw the record before this consumer polled the committed topic.
    negative_observation = sum(1 for value in latency["committed_to_websocket_ms"] if value < 0)
    if any(value < -observation_tolerance_ms for value in latency["committed_to_websocket_ms"]):
        problems.append(f"websocket receipt precedes committed observation by more than "
                        f"{observation_tolerance_ms} ms; the committed sample looks incomplete")
    reason = None
    if problems:
        reason = "boundary correlation failed: " + "; ".join(problems)
    return {"passed": not problems, "reason": reason, "clock_error_ms": tolerance,
            "observation_tolerance_ms": observation_tolerance_ms,
            "websocket_before_committed_observation": negative_observation,
            "samples": {"enqueue_to_ack_ms": len(ack_values),
                        **{name: len(values) for name, values in latency.items()}}}


def window_check(run, tolerance_ms=3000):
    settings = run.get("settings") or {}
    window = int(settings["window_ms"])
    disorder = int(settings["max_out_of_order_ms"])
    grace = int(settings.get("allowed_lateness_ms") or 0) if settings.get("late") == "GRACE" else 0
    committed = run.get("committed") or []
    ema_index = collections.defaultdict(set)
    for record in _emas(committed):
        ema_index[(record["ticker"], int(record["event_ms"]))].add(record["type"])
    receipt_at = {}
    for record in _emas(committed):
        key = (record["ticker"], int(record["event_ms"]))
        receipt_at[key] = min(record["wall_ms"], receipt_at.get(key, record["wall_ms"]))
    watermarks = run.get("watermark_samples") or []

    groups = collections.defaultdict(list)
    for record in run.get("input") or []:
        if _is_trade(record):
            event_ms = int(record["event_ms"])
            groups[(record["ticker"], event_ms // window * window)].append(record)

    latencies = []
    waits = []
    pre_observation = 0
    missing = []
    tied_windows = 0
    eligible_windows = 0
    not_eligible = 0
    for (ticker, window_start), records in sorted(groups.items()):
        closing = max(int(record["event_ms"]) for record in records)
        closing_values = {round(float(record["value"]), 9) for record in records
                          if int(record["event_ms"]) == closing}
        if len(closing_values) > 1:
            tied_windows += 1
            continue
        observed_threshold = window_start + window - 1 + grace
        observed = next((item for item in watermarks if item["watermark"] >= observed_threshold), None)
        if observed is None:
            not_eligible += 1
            continue
        # The watermark can only advance past the timer once an input record with a high enough
        # event time was enqueued. Its enqueue wall time is a deterministic lower bound for
        # eligibility and does not depend on the REST polling resolution.
        required_event = window_start + window + disorder + grace
        advancers = [record for record in run.get("input") or []
                     if record.get("emit_ms") is not None and int(record["event_ms"]) >= required_event]
        if not advancers:
            missing.append({"ticker": ticker, "window_start": window_start,
                            "reason": "observed watermark crossing without a committed input that can advance it"})
            continue
        eligible_at = min(int(record["emit_ms"]) for record in advancers)
        eligible_windows += 1
        if ema_index.get((ticker, closing)) != {"EMA38", "EMA100"}:
            missing.append({"ticker": ticker, "window_start": window_start, "closing_event_ms": closing,
                            "reason": "no finalized EMA pair at the window closing tick"})
            continue
        latency = receipt_at[(ticker, closing)] - eligible_at
        if receipt_at[(ticker, closing)] < observed["wall_ms"]:
            pre_observation += 1
        latencies.append(latency)
        last_enqueue = max(int(record.get("emit_ms") or 0) for record in records)
        waits.append(eligible_at - last_enqueue)

    problems = []
    if eligible_windows == 0:
        problems.append("no window became eligible for finalization inside the pilot")
    if missing:
        problems.append(f"{len(missing)} eligible windows produced no EMA pair")
    if any(value < -tolerance_ms for value in latencies):
        problems.append(f"window finalization receipt precedes observed watermark eligibility by more "
                        f"than {tolerance_ms} ms")
    reason = None
    if problems:
        reason = "window telemetry failed: " + "; ".join(problems)
    return {"passed": not problems, "reason": reason, "eligible_windows": eligible_windows,
            "tied_windows": tied_windows, "not_eligible": not_eligible, "missing": missing,
            "pre_observation_count": pre_observation, "tolerance_ms": tolerance_ms,
            "latencies_ms": [max(0, value) for value in latencies], "watermark_waits_ms": waits}


def backlog_measurement(progress):
    if not progress:
        return {"max": None, "final": None, "growing": None}
    gaps = [sample.get("input", 0) - sample.get("output", 0) for sample in progress]
    third = max(1, len(gaps) // 3)
    growing = gaps[-1] > 0 and (sum(gaps[-third:]) / third) > (sum(gaps[:third]) / third)
    return {"max": max(gaps), "final": gaps[-1], "growing": growing}


def evaluate(run, min_pacing_ratio=0.98, require_limits=True,
             watermark_tolerance_ms=3000, observation_tolerance_ms=5000):
    telemetry = telemetry_check(run, require_limits=require_limits)
    pacing = pacing_check(run, min_pacing_ratio)
    delivery = delivery_check(run)
    identity_clock = identity_clock_check(run, observation_tolerance_ms=observation_tolerance_ms)
    windows = window_check(run, tolerance_ms=watermark_tolerance_ms)
    checks = {"telemetry": telemetry, "pacing": pacing, "delivery": delivery,
              "identity_clock": identity_clock, "windows": windows}

    committed = _prices(run.get("committed") or [])
    historical_age = [record["wall_ms"] - record["event_ms"] for record in committed]
    latency = _boundary_latencies(run)
    transport = latency["enqueue_to_websocket_ms"] or latency["enqueue_to_committed_ms"]
    pacing_summary = run.get("pacing") or {}
    measurements = {
        "historical_event_age_ms": quantiles(historical_age),
        "watermark_wait_ms": quantiles(windows.pop("watermark_waits_ms")),
        "transport_ms": quantiles(transport),
        "committed_observation_ms": quantiles(latency["enqueue_to_committed_ms"]),
        "committed_to_websocket_ms": quantiles(latency["committed_to_websocket_ms"]),
        "window_finalization_ms": quantiles(windows.pop("latencies_ms")),
        "enqueue_to_ack_ms": quantiles([
            sample["ack_epoch_ms"] - sample["enqueued_epoch_ms"]
            for sample in pacing_summary.get("samples") or []
            if sample.get("ack_epoch_ms") is not None and sample.get("enqueued_epoch_ms") is not None]),
        "tied_identities": delivery["tied_timestamps"],
        "duplicate_identities": delivery["duplicate_identities"],
        "duplicate_rows": delivery["duplicate_rows"],
        "pacing": {segment: {"rows": stats.get("rows"), "achieved_ratio": stats.get("achieved_ratio"),
                             "max_lag_ms": stats.get("max_lag_ms")}
                   for segment, stats in (pacing_summary.get("segments") or {}).items()},
        "backlog": backlog_measurement(run.get("progress") or []),
        "sample_counts": {"input": len(run.get("input") or []),
                          "committed": len(run.get("committed") or []),
                          "websocket": len(run.get("websocket") or []),
                          "watermark": len(run.get("watermark_samples") or [])},
    }
    failures = [{"check": name, "reason": check.get("reason") or ", ".join(check.get("missing") or [])}
                for name, check in checks.items() if not check["passed"]]
    return {
        "overall_passed": all(check["passed"] for check in checks.values()),
        "checks": checks,
        "measurements": measurements,
        "settings": run.get("settings") or {},
        "limits": run.get("limits"),
        "clock": run.get("clock"),
        "failures": failures,
    }


# ---------------------------------------------------------------------------
# Live collection

def _request(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def wait_for_pipeline(gateway_url, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            running = any(job["state"] == "RUNNING" for job in _request("http://jobmanager:8081/jobs/overview")["jobs"])
            gateway = gateway_url is None or _request("http://gateway:8080/health")["source"] == {
                "type": "kafka", "status": "running"}
            if running and gateway:
                return
        except (OSError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError("Flink job and gateway did not reach a running state")


def collect(args):
    from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
    from websockets.sync.client import connect

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "price-events")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "events-processed")
    tickers = [ticker.strip() for ticker in (args.tickers or "").split(",") if ticker.strip()]
    if not tickers:
        raise SystemExit("--tickers is required so the subscriber watches the measured symbols before publication")

    wait_for_pipeline(args.gateway_url)
    socket = None
    if args.gateway_url:
        socket = connect(args.gateway_url, open_timeout=30, close_timeout=1)
        socket.send(json.dumps({"type": "subscribe", "tickers": tickers}))
        pending = set(tickers)
        deadline = time.monotonic() + 30
        while pending:
            frame = json.loads(socket.recv(timeout=max(0, deadline - time.monotonic())))
            if frame.get("type") != "history" or frame.get("ticker") not in pending:
                raise RuntimeError(f"unexpected subscription frame: {frame!r}")
            pending.remove(frame["ticker"])
    print(json.dumps({"pilot": "subscribed", "tickers": tickers}), flush=True)

    consumer = Consumer({"bootstrap.servers": brokers, "group.id": "capture-pilot-" + args.run_id,
                         "enable.auto.commit": False, "isolation.level": "read_committed",
                         "auto.offset.reset": "earliest"})
    metadata = consumer.list_topics(timeout=15)
    consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)
                     for topic in (input_topic, output_topic)
                     for partition in metadata.topics[topic].partitions])

    def integer(name):
        value = os.getenv(name)
        return int(value) if value not in (None, "") else None

    speed = args.speed_multiplier if args.speed_multiplier is not None else os.getenv("SPEED_MULTIPLIER")
    run = {
        "settings": {"window_ms": integer("WINDOW_SIZE_MS"),
                     "max_out_of_order_ms": integer("MAX_OUT_OF_ORDER_MS"),
                     "allowed_lateness_ms": integer("ALLOWED_LATENESS_MS"),
                     "checkpoint_interval_ms": integer("CHECKPOINT_INTERVAL_MS"),
                     "flink_parallelism": integer("FLINK_PARALLELISM"),
                     "dedup": os.getenv("DEDUP_STRATEGY"),
                     "late": os.getenv("LATE_EVENT_STRATEGY"),
                     "speed_multiplier": float(speed) if speed not in (None, "") else None},
        "input": [], "committed": [], "websocket": [], "ws_errors": [],
        "watermark_samples": [], "progress": [], "job_running": False,
        "limits": json.loads(Path(args.limits).read_text()) if args.limits else None,
        "clock": {"basis": "unverified" if args.unverified_clock else "same_host_wall",
                  "max_clock_error_ms": args.max_clock_error_ms},
    }

    def read_message(timeout):
        message = consumer.poll(timeout)
        if message is None or message.error():
            return
        value = json.loads(message.value())
        now = time.time() * 1000
        if message.topic() == input_topic:
            if value.get("id") in tickers:
                run["input"].append({"ticker": value["id"], "event_ms": value["eventTimestampMs"],
                                     "value": value.get("last"), "emit_ms": value.get("emitTimestampMs"),
                                     "wall_ms": now})
        elif set(value) == {"timestamp_emit", "ticker", "type", "value"} and value["ticker"] in tickers:
            run["committed"].append({"ticker": value["ticker"], "type": value["type"],
                                     "event_ms": value["timestamp_emit"], "value": value["value"], "wall_ms": now})

    def read_websocket():
        try:
            raw = socket.recv(timeout=1)
        except TimeoutError:
            return
        value = json.loads(raw)
        if value.get("ticker") in tickers and value.get("type") not in {"history", "error"}:
            run["websocket"].append({"ticker": value["ticker"], "type": value["type"],
                                     "event_ms": value["timestamp_emit"], "value": value["value"],
                                     "wall_ms": time.time() * 1000})
        elif value.get("type") == "error":
            run["ws_errors"].append(value)

    def sample_watermark():
        observed_at = time.time() * 1000
        try:
            for job in _request("http://jobmanager:8081/jobs/overview")["jobs"]:
                run["job_running"] = run["job_running"] or job["state"] == "RUNNING"
                if job["state"] != "RUNNING":
                    continue
                for vertex in _request("http://jobmanager:8081/jobs/" + job["jid"])["vertices"]:
                    values = _request(f"http://jobmanager:8081/jobs/{job['jid']}/vertices/{vertex['id']}/watermarks")
                    for value in values:
                        observed = int(value["value"])
                        if 0 < observed < 9_000_000_000_000_000_000:
                            run["watermark_samples"].append({"wall_ms": observed_at, "watermark": observed})
        except (OSError, ValueError):
            pass

    import threading

    stop = threading.Event()

    def consume_loop():
        while not stop.is_set():
            try:
                read_message(.1)
            except Exception as error:  # keep the run visible instead of dying mid-pilot
                run["ws_errors"].append(f"consumer: {error!r}")
                return

    def websocket_loop():
        while not stop.is_set():
            try:
                read_websocket()
            except Exception as error:
                run["ws_errors"].append(f"websocket: {error!r}")
                return

    threads = [threading.Thread(target=consume_loop, daemon=True)]
    if socket:
        threads.append(threading.Thread(target=websocket_loop, daemon=True))
    for thread in threads:
        thread.start()

    deadline = time.monotonic() + args.timeout
    last_data = time.monotonic()
    next_progress = next_watermark = time.monotonic()
    counts = [len(run["input"]), len(run["committed"]), len(run["websocket"])]
    timed_out = True
    while time.monotonic() < deadline:
        now = time.monotonic()
        current = [len(run["input"]), len(run["committed"]), len(run["websocket"])]
        if current != counts:
            counts = current
            last_data = now
        if now >= next_progress:
            run["progress"].append({"wall_ms": time.time() * 1000,
                                    **dict(zip(("input", "output", "websocket"), counts))})
            next_progress = now + 1
        if now >= next_watermark:
            sample_watermark()
            next_watermark = now + 1
        report_ready = args.pacing_report and Path(args.pacing_report).exists()
        if report_ready and now - last_data >= args.settle:
            timed_out = False
            break
        time.sleep(.05)
    stop.set()
    for thread in threads:
        thread.join(2)
    consumer.close()
    if socket:
        socket.close()
    if timed_out:
        raise RuntimeError("pilot timed out before the replay settled")
    if args.pacing_report:
        run["pacing"] = json.loads(Path(args.pacing_report).read_text())
    return run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--tickers", required=True, help="comma-separated measured symbols to subscribe before replay")
    parser.add_argument("--pacing-report", help="emitter PACING_REPORT_PATH shared into this container")
    parser.add_argument("--limits", help="JSON produced by docker inspect for the measured containers")
    parser.add_argument("--gateway-url", default="ws://gateway:8080/")
    parser.add_argument("--output-dir", default="/results")
    parser.add_argument("--speed-multiplier", type=float)
    parser.add_argument("--max-clock-error-ms", type=int, default=250)
    parser.add_argument("--unverified-clock", action="store_true",
                        help="mark the clock basis as unverified instead of the shared Docker host clock")
    parser.add_argument("--min-pacing-ratio", type=float, default=0.98)
    parser.add_argument("--watermark-tolerance-ms", type=int, default=3000,
                        help="allowed window receipt before the input-derived eligibility lower bound")
    parser.add_argument("--observation-tolerance-ms", type=int, default=5000,
                        help="allowed websocket-before-committed-observation skew within the pilot clock")
    parser.add_argument("--settle", type=float, default=10)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--allow-missing-limits", action="store_true")
    args = parser.parse_args()
    started = time.time()
    run = collect(args)
    result = evaluate(run, min_pacing_ratio=args.min_pacing_ratio, require_limits=not args.allow_missing_limits,
                      watermark_tolerance_ms=args.watermark_tolerance_ms,
                      observation_tolerance_ms=args.observation_tolerance_ms)
    result.update({"run_id": args.run_id, "collected_at_epoch_seconds": started,
                   "finished_at_epoch_seconds": time.time(),
                   "pacing_report": run.get("pacing"),
                   "watermark_samples": run["watermark_samples"][-500:],
                   "progress": run["progress"][-500:]})
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / (args.run_id + "-capture-pilot.json")
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"run_id": args.run_id, "overall_passed": result["overall_passed"],
                      "failures": result["failures"],
                      "measurements": result["measurements"]}, indent=2))
    if not result["overall_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

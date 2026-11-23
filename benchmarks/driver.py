"""Synthetic test driver only; this is not the application's emitter or gateway."""
import argparse
import collections
import hashlib
from itertools import accumulate
import json
import math
import os
from pathlib import Path
import random
import re
import threading
import time
import urllib.parse
import urllib.request
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING


def signature(event):
    return event["ticker"], event["timestamp_emit"], event.get("price", event.get("value"))


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    def q(p):
        i = p * (len(values) - 1)
        lo, hi = math.floor(i), math.ceil(i)
        return values[lo] + (i - lo) * (values[hi] - values[lo])
    return {"count": len(values), "p50": q(.5), "p95": q(.95), "p99": q(.99), "max": values[-1]}


def request_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def wait_for_job(timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            jobs = request_json("http://jobmanager:8081/jobs/overview")["jobs"]
            if any(job["state"] == "RUNNING" for job in jobs):
                return
        except (OSError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError("Flink job did not reach RUNNING")


def subscribe_gateway(symbols, timeout=60):
    from websockets.sync.client import connect

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if request_json("http://gateway:8080/health")["source"] == {"type": "kafka", "status": "running"}:
                break
        except (OSError, ValueError):
            pass
        time.sleep(.5)
    else:
        raise RuntimeError("Gateway Kafka source did not reach running")
    socket = connect("ws://gateway:8080/", proxy=None, open_timeout=5, close_timeout=1)
    try:
        for offset in range(0, len(symbols), 500):
            batch = symbols[offset:offset + 500]
            socket.send(json.dumps({"type": "subscribe", "tickers": batch}))
            pending = set(batch)
            deadline = time.monotonic() + 10
            while pending:
                frame = json.loads(socket.recv(timeout=max(0, deadline - time.monotonic())))
                if frame.get("type") != "history" or frame.get("ticker") not in pending or frame.get("events") != []:
                    raise RuntimeError(f"Expected empty history for a fresh subscription, got {frame!r}")
                pending.remove(frame["ticker"])
        return socket
    except Exception:
        socket.close()
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--rate", type=int, default=10000, help="original events/sec; duplicates add traffic")
    parser.add_argument("--symbols", type=int, default=1000)
    parser.add_argument("--skew", type=float, default=1.2, help="Zipf-like symbol weight exponent; zero is uniform")
    parser.add_argument("--duplicate-rate", type=float, default=.1)
    parser.add_argument("--late-rate", type=float, default=.05)
    parser.add_argument("--late-delay-ms", type=int, default=2000)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--drain-timeout", type=float, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--run-id", default=uuid.uuid4().hex[:12])
    parser.add_argument("--output-dir", default="/results")
    parser.add_argument("--gateway", action="store_true", help="measure and validate one live WebSocket subscriber")
    args = parser.parse_args()
    if args.duration <= 0 or args.rate <= 0 or args.symbols <= 0 or args.warmup < 0:
        parser.error("duration/rate/symbols must be positive; warmup must be nonnegative")
    if not 0 <= args.duplicate_rate <= 1 or not 0 <= args.late_rate <= 1:
        parser.error("duplicate and late rates must be in [0, 1]")
    wait_for_job()
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", "price-events")
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "events-processed")
    dedup = os.getenv("DEDUP_STRATEGY", "NONE").upper()
    prefix = "B" + args.run_id.replace("-", "") + "_"
    rng = random.Random(args.seed)
    cumulative_weights = list(accumulate(1 / (i + 1) ** args.skew for i in range(args.symbols)))
    symbols = [f"{prefix}{i}.{['FR', 'NL', 'ETR'][i % 3]}" for i in range(args.symbols)]
    subscriptions = symbols + (["W" + ticker for ticker in symbols] if args.warmup else [])
    if args.gateway and not all(re.fullmatch(r"[A-Za-z0-9._-]{1,32}", ticker) for ticker in subscriptions):
        parser.error("--gateway requires generated tickers to fit the gateway protocol; shorten --run-id")
    socket = subscribe_gateway(subscriptions) if args.gateway else None
    expected = collections.Counter()
    actual = collections.Counter()
    kafka_emas = collections.Counter()
    ws_prices = collections.Counter()
    ws_emas = collections.Counter()
    seen = set()
    sent_at = {}
    last_arrival = {}
    latency_samples = {"kafka": [], "websocket": []}
    event_age_sample = []
    sample_rng = {source: random.Random(args.seed) for source in latency_samples}
    observations = collections.Counter()
    counters = collections.Counter()
    errors = []
    ws_errors = []
    stop = threading.Event()
    lock = threading.Lock()
    producer = Producer({"bootstrap.servers": brokers, "enable.idempotence": True,
                         "acks": "all", "linger.ms": 5, "compression.type": "lz4",
                         "queue.buffering.max.messages": 1000000})
    consumer = Consumer({"bootstrap.servers": brokers, "group.id": "benchmark-" + args.run_id,
                         "enable.auto.commit": False, "isolation.level": "read_committed",
                         "auto.offset.reset": "earliest"})
    metadata = consumer.list_topics(output_topic, timeout=15)
    consumer.assign([TopicPartition(output_topic, p, OFFSET_BEGINNING)
                     for p in metadata.topics[output_topic].partitions])

    def observe(event, source, now):
        websocket = source == "websocket"
        if not event.get("ticker", "").startswith(prefix):
            return
        kind = event.get("type")
        # The live-advisory work added BUY/SELL records to the query output, so the
        # contract must accept them. They are counted and delivery-checked separately
        # and are never treated as prices or EMAs.
        if set(event) != {"timestamp_emit", "ticker", "type", "value"} or kind not in {"price", "EMA38", "EMA100", "BUY", "SELL"} or not math.isfinite(event["value"]):
            (ws_errors if websocket else errors).append("invalid output contract: " + repr(event))
            return
        with lock:
            counters[("websocket_" if websocket else "") + kind] += 1
            key = signature(event)
            if kind in ("BUY", "SELL"):
                return
            if kind != "price":
                if socket:
                    (ws_emas if websocket else kafka_emas)[(kind, *key)] += 1
                return
            (ws_prices if websocket else actual)[key] += 1
            last_arrival[source] = now
            if key in sent_at:
                observations[source] += 1
                n = observations[source]
                index = n - 1 if n <= 100000 else sample_rng[source].randrange(n)
                if index < 100000:
                    latency = (now - sent_at[key]) * 1000
                    latency_sample = latency_samples[source]
                    if index == len(latency_sample):
                        latency_sample.append(latency)
                        if not websocket:
                            event_age_sample.append(time.time() * 1000 - event["timestamp_emit"])
                    else:
                        latency_sample[index] = latency
                        if not websocket:
                            event_age_sample[index] = time.time() * 1000 - event["timestamp_emit"]

    def consume():
        try:
            while not stop.is_set():
                message = consumer.poll(.1)
                if message is None:
                    continue
                if message.error():
                    errors.append(str(message.error()))
                    continue
                now = time.monotonic()
                observe(json.loads(message.value()), "kafka", now)
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            consumer.close()
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()

    def consume_websocket():
        try:
            while not stop.is_set():
                try:
                    raw = socket.recv(timeout=.1)
                except TimeoutError:
                    continue
                now = time.monotonic()
                event = json.loads(raw)
                if event.get("type") in {"error", "history"}:
                    raise RuntimeError(f"Unexpected WebSocket frame: {event!r}")
                observe(event, "websocket", now)
        except Exception as exc:
            ws_errors.append(repr(exc))

    if socket:
        ws_thread = threading.Thread(target=consume_websocket, daemon=True)
        ws_thread.start()
    stats_samples = []
    sample_stop = threading.Event()
    def telemetry():
        while not sample_stop.is_set():
            sample = {"timestamp": time.time()}
            endpoints = {"jobs": "http://jobmanager:8081/jobs/overview", "workers": "http://jobmanager:8081/taskmanagers"}
            if socket:
                endpoints["gateway"] = "http://gateway:8080/health"
            for name, url in endpoints.items():
                try:
                    sample[name] = request_json(url)
                except Exception as exc:
                    sample[name] = {"error": str(exc)}
            stats_samples.append(sample)
            sample_stop.wait(2)
    telemetry_thread = threading.Thread(target=telemetry, daemon=True)
    telemetry_thread.start()
    delivery_errors = []
    progress = []

    def delivered(error, message):
        if error:
            delivery_errors.append(str(error))
        else:
            counters["acknowledged_input"] += 1
    def publish(event, measured):
        key = signature(event)
        if measured:
            with lock:
                sent_at.setdefault(key, time.monotonic())
                identity = key if dedup == "EXACT" else key[:2]
                if dedup == "NONE" or identity not in seen:
                    expected[key] += 1
                    counters["expected_prices_so_far"] += 1
                    seen.add(identity)
        raw = json.dumps(event, separators=(",", ":")).encode()
        while True:
            try:
                producer.produce(input_topic, key=event["ticker"], value=raw, on_delivery=delivered)
                break
            except BufferError:
                counters["producer_queue_full"] += 1
                producer.poll(.01)
        counters["input_records" if measured else "warmup_records"] += 1
    started = time.monotonic()
    measured_start = started + args.warmup
    deadline = measured_start + args.duration
    next_sample = started
    sequence = 0
    next_progress = measured_start

    def sample_progress(now):
        with lock:
            progress.append({"epoch_seconds": time.time(), "elapsed_seconds": now - measured_start,
                             "originals": counters["measured_originals"],
                             "input_enqueued": counters["input_records"] + counters["warmup_records"],
                             "input_acknowledged": counters["acknowledged_input"],
                             "producer_queue_full": counters["producer_queue_full"],
                             "expected_measured_prices": counters["expected_prices_so_far"],
                             "committed_measured_prices": counters["price"],
                             **({"websocket_measured_prices": counters["websocket_price"]} if socket else {})})

    while time.monotonic() < deadline:
        now = time.monotonic()
        if now >= next_progress:
            sample_progress(now)
            next_progress = now + 1
        if now < next_sample:
            time.sleep(min(.001, next_sample - now))
            producer.poll(0)
            continue
        measured = now >= measured_start
        ticker = rng.choices(symbols, cum_weights=cumulative_weights, k=1)[0]
        timestamp = int(time.time() * 1000)
        if rng.random() < args.late_rate:
            timestamp -= args.late_delay_ms
            if measured:
                counters["injected_delayed_originals"] += 1
        sequence += 1
        event = {"timestamp_emit": timestamp, "ticker": ticker, "sec_type": "E", "price": 100 + sequence / 100000}
        # Warmup symbols are disjoint so they do not enter measured result counts/state.
        if not measured:
            event["ticker"] = "W" + event["ticker"]
        else:
            counters["measured_originals"] += 1
        publish(event, measured)
        if rng.random() < args.duplicate_rate:
            publish(event, measured)
            if measured:
                counters["injected_duplicates"] += 1
        next_sample += 1 / args.rate
        producer.poll(0)
    load_finished = time.monotonic()
    sample_progress(load_finished)
    if producer.flush(30):
        delivery_errors.append("producer flush timed out")
    produced_at = time.monotonic()
    drain_deadline = produced_at + args.drain_timeout
    expected_total = sum(expected.values())
    while time.monotonic() < drain_deadline:
        with lock:
            complete = counters["price"] >= expected_total
            if socket and not ws_errors:
                complete &= counters["websocket_price"] >= expected_total
        if complete:
            break
        time.sleep(.1)
    # Wait one more checkpoint so unexpected extra output is not hidden by the first matching count.
    time.sleep(min(args.drain_timeout, int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000")) / 1000 + 1))
    while socket and not ws_errors and time.monotonic() < drain_deadline:
        with lock:
            if all(counters["websocket_" + kind] >= counters[kind] for kind in ("price", "EMA38", "EMA100", "BUY", "SELL")):
                break
        time.sleep(.1)
    stop.set()
    thread.join(5)
    if socket:
        ws_thread.join(5)
        socket.close()
    sample_stop.set()
    telemetry_thread.join(6)
    sample_progress(time.monotonic())
    missing = expected - actual
    extra = actual - expected
    final_arrival = last_arrival.get("kafka", produced_at)
    settings = {name: os.getenv(name) for name in ["DEDUP_STRATEGY", "LATE_EVENT_STRATEGY", "WINDOW_SIZE_MS",
                "MAX_OUT_OF_ORDER_MS", "ALLOWED_LATENESS_MS", "FLINK_PARALLELISM", "CHECKPOINT_INTERVAL_MS"]}
    result = {"run_id": args.run_id, "arguments": vars(args), "settings": settings,
              "measurement": {"input_records": counters["input_records"], "observed_prices": sum(actual.values()),
                  "expected_prices": sum(expected.values()), "missing_prices": sum(missing.values()),
                  "unexpected_prices": sum(extra.values()), "producer_seconds": produced_at - measured_start,
                   "drain_seconds": max(0, final_arrival - produced_at),
                   "achieved_originals_per_second": counters["measured_originals"] / max(.001, load_finished - measured_start),
                  "input_records_per_second": counters["input_records"] / max(.001, produced_at - measured_start),
                  "committed_prices_per_second": sum(actual.values()) / max(.001, final_arrival - measured_start),
                   "visible_roundtrip_ms": quantiles(latency_samples["kafka"]), "event_age_at_visibility_ms": quantiles(event_age_sample)},
              "counters": dict(counters), "errors": errors + delivery_errors,
              "price_correctness_passed": not missing and not extra and not errors and not delivery_errors,
              "telemetry": stats_samples,
              "progress": progress,
              "missing_examples": [[list(k), v] for k, v in list(missing.items())[:10]],
              "extra_examples": [[list(k), v] for k, v in list(extra.items())[:10]]}
    if socket:
        ws_missing_prices = sum((actual - ws_prices).values())
        ws_extra_prices = sum((ws_prices - actual).values())
        ws_missing_emas = sum((kafka_emas - ws_emas).values())
        ws_extra_emas = sum((ws_emas - kafka_emas).values())
        # Advisory delivery is checked at count level: the gateway must fan out at least
        # as many BUY/SELL records as the query committed. Signature-level comparison of
        # advisories is not implemented, and the evidence says so.
        ws_missing_advisories = sum(max(0, counters[kind] - counters["websocket_" + kind])
                                    for kind in ("BUY", "SELL"))
        result["gateway_correctness_passed"] = not any((ws_errors, ws_missing_prices, ws_extra_prices,
                                                        ws_missing_emas, ws_extra_emas, ws_missing_advisories))
        result["errors"].extend(ws_errors)
        result["measurement"].update({
            "websocket_prices": counters["websocket_price"],
            "websocket_ema38": counters["websocket_EMA38"], "websocket_ema100": counters["websocket_EMA100"],
            "websocket_buy": counters["websocket_BUY"], "websocket_sell": counters["websocket_SELL"],
            "advisory_buy": counters["BUY"], "advisory_sell": counters["SELL"],
            "websocket_missing_advisories": ws_missing_advisories,
            "websocket_missing_prices": ws_missing_prices, "websocket_unexpected_prices": ws_extra_prices,
            "websocket_missing_emas": ws_missing_emas, "websocket_unexpected_emas": ws_extra_emas,
            "websocket_prices_per_second": counters["websocket_price"] /
                max(.001, last_arrival.get("websocket", produced_at) - measured_start),
            "websocket_roundtrip_ms": quantiles(latency_samples["websocket"]),
        })
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / (args.run_id + ".json")).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in {"telemetry", "progress", "missing_examples", "extra_examples"}}, indent=2))
    if not result["price_correctness_passed"] or not result.get("gateway_correctness_passed", True):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

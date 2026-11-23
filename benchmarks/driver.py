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
    expected = collections.Counter()
    actual = collections.Counter()
    seen = set()
    sent_at = {}
    arrivals = []
    latency_sample = []
    event_age_sample = []
    sample_rng = random.Random(args.seed)
    counters = collections.Counter()
    errors = []
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
    def consume():
        observations = 0
        try:
            while not stop.is_set():
                message = consumer.poll(.1)
                if message is None:
                    continue
                if message.error():
                    errors.append(str(message.error()))
                    continue
                event = json.loads(message.value())
                if not event.get("ticker", "").startswith(prefix):
                    continue
                if set(event) != {"timestamp_emit", "ticker", "type", "value"} or event["type"] not in {"price", "EMA38", "EMA100"} or not math.isfinite(event["value"]):
                    errors.append("invalid output contract: " + repr(event))
                    continue
                with lock:
                    counters[event["type"]] += 1
                    if event["type"] != "price":
                        continue
                    key = signature(event)
                    actual[key] += 1
                    now = time.monotonic()
                    if key in sent_at:
                        latency = (now - sent_at[key]) * 1000
                        age = time.time() * 1000 - event["timestamp_emit"]
                        observations += 1
                        # Uniform reservoir avoids unbounded latency storage and head-only samples.
                        index = observations - 1 if observations <= 100000 else sample_rng.randrange(observations)
                        if index < 100000:
                            if index == len(latency_sample):
                                latency_sample.append(latency)
                                event_age_sample.append(age)
                            else:
                                latency_sample[index] = latency
                                event_age_sample[index] = age
                        arrivals.append(now)
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            consumer.close()
    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
    stats_samples = []
    sample_stop = threading.Event()
    def telemetry():
        while not sample_stop.is_set():
            sample = {"timestamp": time.time()}
            for name, url in {"jobs": "http://jobmanager:8081/jobs/overview",
                              "workers": "http://jobmanager:8081/taskmanagers"}.items():
                try:
                    sample[name] = request_json(url)
                except Exception as exc:
                    sample[name] = {"error": str(exc)}
            stats_samples.append(sample)
            sample_stop.wait(2)
    telemetry_thread = threading.Thread(target=telemetry, daemon=True)
    telemetry_thread.start()
    delivery_errors = []
    def delivered(error, message):
        if error:
            delivery_errors.append(str(error))
    def publish(event, measured):
        key = signature(event)
        if measured:
            with lock:
                sent_at.setdefault(key, time.monotonic())
                identity = key if dedup == "EXACT" else key[:2]
                if dedup == "NONE" or identity not in seen:
                    expected[key] += 1
                    seen.add(identity)
        raw = json.dumps(event, separators=(",", ":")).encode()
        while True:
            try:
                producer.produce(input_topic, key=event["ticker"], value=raw, on_delivery=delivered)
                break
            except BufferError:
                producer.poll(.01)
        counters["input_records" if measured else "warmup_records"] += 1
    started = time.monotonic()
    measured_start = started + args.warmup
    deadline = measured_start + args.duration
    next_sample = started
    sequence = 0
    while time.monotonic() < deadline:
        now = time.monotonic()
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
        publish(event, measured)
        if rng.random() < args.duplicate_rate:
            publish(event, measured)
            if measured:
                counters["injected_duplicates"] += 1
        next_sample += 1 / args.rate
        producer.poll(0)
    if producer.flush(30):
        delivery_errors.append("producer flush timed out")
    produced_at = time.monotonic()
    drain_deadline = produced_at + args.drain_timeout
    while time.monotonic() < drain_deadline:
        with lock:
            complete = sum(actual.values()) >= sum(expected.values())
        if complete:
            break
        time.sleep(.1)
    # Wait one more checkpoint so unexpected extra output is not hidden by the first matching count.
    time.sleep(min(args.drain_timeout, int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000")) / 1000 + 1))
    stop.set()
    thread.join(5)
    sample_stop.set()
    telemetry_thread.join(6)
    missing = expected - actual
    extra = actual - expected
    final_arrival = max(arrivals, default=produced_at)
    settings = {name: os.getenv(name) for name in ["DEDUP_STRATEGY", "LATE_EVENT_STRATEGY", "WINDOW_SIZE_MS",
                "MAX_OUT_OF_ORDER_MS", "ALLOWED_LATENESS_MS", "FLINK_PARALLELISM", "CHECKPOINT_INTERVAL_MS"]}
    result = {"run_id": args.run_id, "arguments": vars(args), "settings": settings,
              "measurement": {"input_records": counters["input_records"], "observed_prices": sum(actual.values()),
                  "expected_prices": sum(expected.values()), "missing_prices": sum(missing.values()),
                  "unexpected_prices": sum(extra.values()), "producer_seconds": produced_at - measured_start,
                  "drain_seconds": max(0, final_arrival - produced_at),
                  "input_records_per_second": counters["input_records"] / max(.001, produced_at - measured_start),
                  "committed_prices_per_second": sum(actual.values()) / max(.001, final_arrival - measured_start),
                  "visible_roundtrip_ms": quantiles(latency_sample), "event_age_at_visibility_ms": quantiles(event_age_sample)},
              "counters": dict(counters), "errors": errors + delivery_errors,
              "price_correctness_passed": not missing and not extra and not errors and not delivery_errors,
              "telemetry": stats_samples,
              "missing_examples": [[list(k), v] for k, v in list(missing.items())[:10]],
              "extra_examples": [[list(k), v] for k, v in list(extra.items())[:10]]}
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / (args.run_id + ".json")).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in {"telemetry", "missing_examples", "extra_examples"}}, indent=2))
    if not result["price_correctness_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

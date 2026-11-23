"""Configurable synthetic PriceEvent emitter with bounded queues and JSON/Prometheus telemetry."""
import argparse
import bisect
import collections
import copy
import heapq
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import random
import signal
import sys
import threading
import time


DEFAULTS = {
    "seed": 42,
    "rate": {"events_per_second": 1000, "duration_seconds": 60, "max_events": 0,
             "schedule": [], "burst_every_seconds": 0, "burst_duration_seconds": 1,
             "burst_multiplier": 5, "pause_every_seconds": 0, "pause_duration_seconds": 1},
    "symbols": {"count": 100, "prefix": "DUMMY", "tickers": [], "exchanges": {"FR": 55, "NL": 28, "ETR": 17},
                "security_types": {"E": 83, "I": 17}, "distribution": "zipf", "zipf_exponent": 1.2},
    "price": {"model": "random_walk", "initial_min": 20, "initial_max": 200, "floor": .01,
              "volatility": .001, "drift_per_second": .0001, "sine_amplitude": .1,
              "sine_period_seconds": 30, "regime_seconds": 20, "round_digits": 6},
    "event_time": {"mode": "wallclock", "start_epoch_ms": None, "speed": 1},
    "faults": {"duplicate_probability": .05, "duplicate_copies": 1, "duplicate_delay_ms": 0,
               "correction_probability": 0, "delay_probability": .05, "delay_min_ms": 100,
               "delay_max_ms": 5000, "backdate_probability": 0, "backdate_ms": 10000,
               "invalid_probability": 0, "invalid_modes": ["malformed_json", "missing_price", "bad_price", "bad_ticker", "negative_timestamp"]},
    "kafka": {"bootstrap_servers": "kafka:9092", "topic": "price-events", "key_mode": "ticker",
              "idempotence": True, "acks": "all", "compression": "lz4", "linger_ms": 5,
              "delivery_timeout_ms": 30000, "queue_max_messages": 100000,
              "transactional_id": None, "transaction_interval_ms": 1000},
    "runtime": {"max_pending_events": 100000, "drain_timeout_seconds": 15,
                "log_interval_seconds": 5, "summary_path": None},
    "metrics": {"enabled": True, "host": "0.0.0.0", "port": 8000},
    "profiling": {"enabled": True, "sample_every": 100},
}


def merge(base, overrides, path=""):
    result = copy.deepcopy(base)
    for key, value in overrides.items():
        if key not in base:
            raise ValueError("unknown setting: " + path + key)
        if isinstance(base[key], dict) and key not in {"exchanges", "security_types"}:
            if not isinstance(value, dict):
                raise ValueError(path + key + " must be an object")
            result[key] = merge(base[key], value, path + key + ".")
        else:
            result[key] = value
    return result


def validate(config):
    r, s, p, t, f, k, runtime = [config[name] for name in ["rate", "symbols", "price", "event_time", "faults", "kafka", "runtime"]]
    def number(value, name, minimum=0, strict=False):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < minimum or (strict and value == minimum):
            raise ValueError(name + f" must be finite and {'greater than' if strict else 'at least'} {minimum}")
    for name in ["events_per_second", "duration_seconds", "max_events", "burst_every_seconds", "burst_duration_seconds", "pause_every_seconds", "pause_duration_seconds"]:
        number(r[name], "rate." + name)
    if not isinstance(config["seed"], int) or not isinstance(r["max_events"], int):
        raise ValueError("seed and rate.max_events must be integers")
    number(r["burst_multiplier"], "burst_multiplier", 0, True)
    for stage in r["schedule"]:
        if set(stage) != {"duration_seconds", "events_per_second"}:
            raise ValueError("each rate schedule stage needs duration_seconds and events_per_second")
        number(stage["duration_seconds"], "stage duration", 0, True)
        number(stage["events_per_second"], "stage rate")
    for group in [s["exchanges"], s["security_types"]]:
        if not isinstance(group, dict) or not group:
            raise ValueError("exchange and security type weights must be nonempty objects")
        for label, weight in group.items():
            if not isinstance(label, str) or not label or any(c.isspace() for c in label) or "." in label:
                raise ValueError("invalid exchange/security type label")
            number(weight, "weight", 0, True)
    if not isinstance(s["tickers"], list):
        raise ValueError("symbols.tickers must be an array")
    for ticker in s["tickers"]:
        if not isinstance(ticker, str) or len(ticker.split(".")) < 2 or any(not part or any(c.isspace() for c in part) for part in ticker.split(".")):
            raise ValueError("explicit ticker must have form STOCK.EXCHANGE")
    if len(set(s["tickers"])) != len(s["tickers"]):
        raise ValueError("explicit tickers must be unique")
    if not isinstance(s["count"], int) or s["count"] <= 0 or not isinstance(s["prefix"], str) or not s["prefix"] or any(c.isspace() or c == "." for c in s["prefix"]):
        raise ValueError("symbols.count must be positive and prefix must be a nonempty stock identifier")
    if s["distribution"] not in {"uniform", "zipf"}:
        raise ValueError("distribution must be uniform or zipf")
    number(s["zipf_exponent"], "zipf_exponent")
    if p["model"] not in {"constant", "random_walk", "sine", "trend", "regime"}:
        raise ValueError("unknown price model")
    for key in ["initial_min", "initial_max", "floor", "sine_period_seconds", "regime_seconds"]:
        number(p[key], "price." + key, 0, True)
    if p["initial_min"] > p["initial_max"] or p["floor"] > p["initial_min"]:
        raise ValueError("price floor <= initial_min <= initial_max required")
    for key in ["volatility", "sine_amplitude"]:
        number(p[key], "price." + key)
    number(abs(p["drift_per_second"]), "price.drift_per_second")
    if not isinstance(p["round_digits"], int) or not 0 <= p["round_digits"] <= 12:
        raise ValueError("round_digits must be an integer in [0,12]")
    if t["mode"] not in {"wallclock", "accelerated"}:
        raise ValueError("event_time.mode must be wallclock or accelerated")
    number(t["speed"], "event_time.speed", 0, True)
    if t["start_epoch_ms"] is not None and (not isinstance(t["start_epoch_ms"], int) or t["start_epoch_ms"] < 0):
        raise ValueError("start_epoch_ms must be a nonnegative integer")
    for key, value in f.items():
        if key.endswith("_probability"):
            number(value, "faults." + key)
            if value > 1:
                raise ValueError(key + " must be <= 1")
    for key in ["duplicate_copies", "duplicate_delay_ms", "delay_min_ms", "delay_max_ms", "backdate_ms"]:
        if not isinstance(f[key], int) or f[key] < 0:
            raise ValueError(key + " must be a nonnegative integer")
    modes = {"malformed_json", "missing_price", "bad_price", "bad_ticker", "negative_timestamp", "tombstone"}
    if not isinstance(f["invalid_modes"], list) or not f["invalid_modes"] or not set(f["invalid_modes"]) <= modes:
        raise ValueError("invalid_modes must be a nonempty list of supported fault names")
    if f["delay_min_ms"] > f["delay_max_ms"]:
        raise ValueError("delay_min_ms must be <= delay_max_ms")
    if k["key_mode"] not in {"ticker", "random", "none"}:
        raise ValueError("key_mode must be ticker, random or none")
    if not isinstance(k["idempotence"], bool) or k["acks"] not in {"all", "1", "0"}:
        raise ValueError("invalid idempotence or acks setting")
    if k["idempotence"] and k["acks"] != "all":
        raise ValueError("idempotence requires acks=all")
    if k["transactional_id"] and not k["idempotence"]:
        raise ValueError("transactions require idempotence")
    if k["compression"] not in {"none", "gzip", "snappy", "lz4", "zstd"}:
        raise ValueError("unsupported compression")
    for key in ["queue_max_messages", "delivery_timeout_ms", "transaction_interval_ms"]:
        number(k[key], "kafka." + key, 0, True)
    number(k["linger_ms"], "linger_ms")
    for key in ["max_pending_events", "drain_timeout_seconds", "log_interval_seconds"]:
        number(runtime[key], "runtime." + key, 0, True)
    if not isinstance(runtime["max_pending_events"], int):
        raise ValueError("max_pending_events must be an integer")
    required = 1 + (f["duplicate_copies"] if f["duplicate_probability"] else 0) + (1 if f["correction_probability"] else 0)
    if runtime["max_pending_events"] < required:
        raise ValueError("max_pending_events must fit one original plus its configured duplicate/correction copies")
    if not isinstance(config["metrics"]["enabled"], bool) or not isinstance(config["metrics"]["port"], int) or not 0 < config["metrics"]["port"] < 65536:
        raise ValueError("metrics enabled/port invalid")
    if not isinstance(config["profiling"]["enabled"], bool) or not isinstance(config["profiling"]["sample_every"], int) or config["profiling"]["sample_every"] < 1:
        raise ValueError("profiling.enabled must be boolean and sample_every must be positive")
    return config


class Generator:
    def __init__(self, config, epoch_ms):
        self.config = config
        self.rng = random.Random(config["seed"])
        s, p = config["symbols"], config["price"]
        self.tickers = s["tickers"] or [f"{s['prefix']}{i:05d}." + self.choice(s["exchanges"]) for i in range(s["count"])]
        self.sec_types = [self.choice(s["security_types"]) for _ in self.tickers]
        self.initial = [self.rng.uniform(p["initial_min"], p["initial_max"]) for _ in self.tickers]
        self.prices = self.initial.copy()
        self.last_time = [0.] * len(self.tickers)
        weights = [1 if s["distribution"] == "uniform" else 1 / (i + 1) ** s["zipf_exponent"] for i in range(len(self.tickers))]
        total = 0
        self.cumulative = []
        for weight in weights:
            total += weight
            self.cumulative.append(total)
        self.epoch_ms = config["event_time"]["start_epoch_ms"] if config["event_time"]["start_epoch_ms"] is not None else epoch_ms

    def choice(self, weights):
        return self.rng.choices(list(weights), weights=list(weights.values()), k=1)[0]

    def event(self, elapsed, wall_ms):
        i = min(len(self.tickers) - 1, bisect.bisect_left(self.cumulative, self.rng.random() * self.cumulative[-1]))
        p, t = self.config["price"], self.config["event_time"]
        dt = max(0, elapsed - self.last_time[i])
        self.last_time[i] = elapsed
        base = self.initial[i]
        model = p["model"]
        if model == "constant":
            value = base
        elif model == "sine":
            value = base * (1 + p["sine_amplitude"] * math.sin(2 * math.pi * elapsed / p["sine_period_seconds"]))
        else:
            drift = 0 if model == "random_walk" else p["drift_per_second"]
            if model == "regime":
                drift *= 1 if int(elapsed / p["regime_seconds"]) % 2 == 0 else -1
            noise = self.rng.gauss(0, p["volatility"] * math.sqrt(dt)) if model != "trend" else 0
            value = self.prices[i] * math.exp(max(-50, min(50, drift * dt + noise)))
        value = max(p["floor"], min(value, 1e12))
        self.prices[i] = value
        timestamp = wall_ms if t["mode"] == "wallclock" else self.epoch_ms + int(elapsed * t["speed"] * 1000)
        f = self.config["faults"]
        self.last_backdated = self.rng.random() < f["backdate_probability"]
        if self.last_backdated:
            timestamp = max(0, timestamp - f["backdate_ms"])
        return {"timestamp_emit": timestamp, "ticker": self.tickers[i], "sec_type": self.sec_types[i],
                "price": max(p["floor"], round(value, p["round_digits"]))}

    def records(self, event):
        f = self.config["faults"]
        delay = self.rng.randint(f["delay_min_ms"], f["delay_max_ms"]) if self.rng.random() < f["delay_probability"] else 0
        value = event.copy()
        kind = "original"
        raw = None
        if self.rng.random() < f["invalid_probability"]:
            kind = self.rng.choice(f["invalid_modes"])
            if kind == "malformed_json":
                raw = b'{"broken":'
            elif kind == "missing_price":
                del value["price"]
            elif kind == "bad_price":
                value["price"] = "not-a-number"
            elif kind == "bad_ticker":
                value["ticker"] = "INVALID"
            elif kind == "negative_timestamp":
                value["timestamp_emit"] = -1
        if kind != "tombstone" and raw is None:
            raw = json.dumps(value, separators=(",", ":"), allow_nan=False).encode()
        key_mode = self.config["kafka"]["key_mode"]
        key = event["ticker"].encode() if key_mode == "ticker" else (str(self.rng.getrandbits(64)).encode() if key_mode == "random" else None)
        yield delay, key, raw, kind
        if self.rng.random() < f["duplicate_probability"]:
            for i in range(f["duplicate_copies"]):
                yield delay + (i + 1) * f["duplicate_delay_ms"], key, raw, "duplicate"
        if kind == "original" and self.rng.random() < f["correction_probability"]:
            correction = event | {"price": round(event["price"] * 1.01, self.config["price"]["round_digits"])}
            yield delay, key, json.dumps(correction, separators=(",", ":")).encode(), "correction"


def rate_at(config, elapsed):
    r = config["rate"]
    rate = r["events_per_second"]
    remaining = elapsed
    for stage in r["schedule"]:
        rate = stage["events_per_second"]
        if remaining < stage["duration_seconds"]:
            break
        remaining -= stage["duration_seconds"]
    if r["pause_every_seconds"] and elapsed % r["pause_every_seconds"] < r["pause_duration_seconds"]:
        return 0
    if r["burst_every_seconds"] and elapsed % r["burst_every_seconds"] < r["burst_duration_seconds"]:
        rate *= r["burst_multiplier"]
    return rate


class ComponentProfiler:
    def __init__(self, telemetry, component, enabled=True, every=100):
        self.telemetry = telemetry
        self.component = component
        self.enabled = enabled
        self.every = every
        self.values = {"calls": 0, "samples": 0, "wall_ns": 0, "cpu_ns": 0}
        telemetry.profiles[component] = self

    def begin(self):
        if not self.enabled:
            return None
        with self.telemetry.lock:
            self.values["calls"] += 1
            sampled = (self.values["calls"] - 1) % self.every == 0
        return (time.perf_counter_ns(), time.thread_time_ns()) if sampled else None

    def end(self, started):
        if started is None:
            return
        wall, cpu = time.perf_counter_ns() - started[0], time.thread_time_ns() - started[1]
        with self.telemetry.lock:
            self.values["samples"] += 1
            self.values["wall_ns"] += max(0, wall)
            self.values["cpu_ns"] += max(0, cpu)


class Telemetry:
    BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]

    def __init__(self):
        self.lock = threading.RLock()
        self.counts = collections.Counter()
        self.latency_buckets = [0] * (len(self.BUCKETS) + 1)
        self.latency_sum = 0
        self.pending = 0
        self.producer_queue = 0
        self.profiles = {}

    def increment(self, name, amount=1):
        with self.lock:
            self.counts[name] += amount

    def delivery(self, error, elapsed_ms, size):
        with self.lock:
            if error:
                self.counts["delivery_errors"] += 1
            else:
                self.counts["acknowledged"] += 1
                self.counts["acknowledged_bytes"] += size
                self.latency_sum += elapsed_ms
                self.latency_buckets[bisect.bisect_left(self.BUCKETS, elapsed_ms)] += 1

    def snapshot(self):
        with self.lock:
            return dict(self.counts) | {"pending_delayed": self.pending, "producer_queue": self.producer_queue,
                "component_profiles": {name: dict(profile.values) for name, profile in self.profiles.items()}}

    def prometheus(self):
        with self.lock:
            lines = []
            for name, value in sorted(self.counts.items()):
                metric = "dummy_emitter_" + name + "_total"
                lines += [f"# TYPE {metric} counter", f"{metric} {value}"]
            lines += ["# TYPE dummy_emitter_pending_delayed gauge", f"dummy_emitter_pending_delayed {self.pending}",
                      "# TYPE dummy_emitter_producer_queue gauge", f"dummy_emitter_producer_queue {self.producer_queue}",
                      "# TYPE dummy_emitter_delivery_ms histogram"]
            cumulative = 0
            for bound, value in zip(self.BUCKETS + ["+Inf"], self.latency_buckets):
                cumulative += value
                lines.append(f'dummy_emitter_delivery_ms_bucket{{le="{bound}"}} {cumulative}')
            lines += [f"dummy_emitter_delivery_ms_count {cumulative}", f"dummy_emitter_delivery_ms_sum {self.latency_sum}"]
            for component, profile in self.profiles.items():
                for name, source, divisor in [("calls_total", "calls", 1), ("samples_total", "samples", 1),
                                              ("wall_seconds_total", "wall_ns", 1e9), ("cpu_seconds_total", "cpu_ns", 1e9)]:
                    lines.append(f'dummy_emitter_profile_{name}{{component="{component}"}} {profile.values[source] / divisor}')
            return "\n".join(lines) + "\n"


def log(event, **fields):
    print(json.dumps({"timestamp": time.time(), "level": "INFO", "event": event, **fields}), file=sys.stderr, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--print-defaults", action="store_true")
    parser.add_argument("--validate", action="store_true", help="validate configuration without producing")
    parser.add_argument("--dry-run", action="store_true", help="write Kafka values as JSONL without contacting Kafka")
    parser.add_argument("--output-jsonl", type=Path, help="capture every emitted value, including injected faults")
    parser.add_argument("--trace-jsonl", type=Path, help="capture labeled injection metadata alongside unmodified Kafka values")
    parser.add_argument("--run-id", default=str(time.time_ns()))
    parser.add_argument("--rate", type=float)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--topic")
    args = parser.parse_args()
    if args.print_defaults:
        print(json.dumps(DEFAULTS, indent=2))
        return
    config = merge(DEFAULTS, json.loads(args.config.read_text()) if args.config else {})
    for value, section, key in [(args.rate, "rate", "events_per_second"), (args.duration, "rate", "duration_seconds"),
                                (args.max_events, "rate", "max_events"), (args.bootstrap_servers, "kafka", "bootstrap_servers"),
                                (args.topic, "kafka", "topic")]:
        if value is not None:
            config[section][key] = value
    if args.seed is not None:
        config["seed"] = args.seed
    if os.getenv("KAFKA_BOOTSTRAP_SERVERS") and args.bootstrap_servers is None:
        config["kafka"]["bootstrap_servers"] = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if os.getenv("KAFKA_INPUT_TOPIC") and args.topic is None:
        config["kafka"]["topic"] = os.environ["KAFKA_INPUT_TOPIC"]
    validate(config)
    if args.validate:
        print(json.dumps(config, indent=2))
        return
    metrics = Telemetry()
    profiles = {name: ComponentProfiler(metrics, name, config["profiling"]["enabled"], config["profiling"]["sample_every"])
                for name in ["generation", "quality_injection", "kafka_enqueue", "producer_poll"]}
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    server = None
    if config["metrics"]["enabled"] and not args.dry_run:
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path not in {"/metrics", "/health"}:
                    self.send_error(404)
                    return
                body = metrics.prometheus().encode() if self.path == "/metrics" else b"ok\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer((config["metrics"]["host"], config["metrics"]["port"]), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
    producer = None
    k = config["kafka"]
    if not args.dry_run:
        from confluent_kafka import Producer
        properties = {"bootstrap.servers": k["bootstrap_servers"], "enable.idempotence": k["idempotence"],
                      "acks": k["acks"], "compression.type": k["compression"], "linger.ms": k["linger_ms"],
                      "delivery.timeout.ms": k["delivery_timeout_ms"], "queue.buffering.max.messages": k["queue_max_messages"]}
        if k["transactional_id"]:
            properties["transactional.id"] = k["transactional_id"]
        producer = Producer(properties)
        if k["transactional_id"]:
            producer.init_transactions(30)
            producer.begin_transaction()
    output = args.output_jsonl.open("wb") if args.output_jsonl else None
    trace = args.trace_jsonl.open("w") if args.trace_jsonl else None
    started = time.monotonic()
    generator = Generator(config, int(time.time() * 1000))
    pending = []
    serial = 0
    next_generation = started
    next_log = started + config["runtime"]["log_interval_seconds"]
    next_commit = started + k["transaction_interval_ms"] / 1000
    generating = True
    drain_deadline = None
    log("emitter_started", run_id=args.run_id, config=config, dry_run=args.dry_run)
    try:
        while generating or pending:
            now = time.monotonic()
            elapsed = now - started
            r = config["rate"]
            if generating and (stop.is_set() or (r["duration_seconds"] > 0 and elapsed >= r["duration_seconds"]) or
                               (r["max_events"] > 0 and metrics.counts["generated"] >= r["max_events"])):
                generating = False
                drain_deadline = now + config["runtime"]["drain_timeout_seconds"]
            if not generating and now >= drain_deadline:
                metrics.increment("undrained", len(pending))
                pending.clear()
                break
            rate = rate_at(config, elapsed)
            required_room = 1 + (config["faults"]["duplicate_copies"] if config["faults"]["duplicate_probability"] else 0) + (1 if config["faults"]["correction_probability"] else 0)
            if generating and now >= next_generation and rate > 0 and len(pending) + required_room <= config["runtime"]["max_pending_events"]:
                # Batch scheduler handles high rates without a sleep per event.
                batch = min(500, int(min(499, (now - next_generation) * rate)) + 1,
                            (config["runtime"]["max_pending_events"] - len(pending)) // required_room)
                if r["max_events"]:
                    batch = min(batch, r["max_events"] - metrics.counts["generated"])
                for _ in range(batch):
                    profile_started = profiles["generation"].begin()
                    try:
                        event = generator.event(elapsed, int(time.time() * 1000))
                    finally:
                        profiles["generation"].end(profile_started)
                    metrics.increment("generated")
                    if generator.last_backdated:
                        metrics.increment("backdated_originals")
                    profile_started = profiles["quality_injection"].begin()
                    try:
                        records = list(generator.records(event))
                    finally:
                        profiles["quality_injection"].end(profile_started)
                    for delay, key, value, kind in records:
                        serial += 1
                        heapq.heappush(pending, (now + delay / 1000, serial, key, value, kind,
                                                metrics.counts["generated"], delay, event["timestamp_emit"]))
                        metrics.increment("scheduled_" + kind)
                        if delay:
                            metrics.increment("delayed_records")
                next_generation += batch / rate
                # Bound catch-up after a pause or producer backpressure.
                if now - next_generation > .1:
                    next_generation = now
            elif rate == 0:
                next_generation = now
            sent = 0
            while pending and pending[0][0] <= now and sent < 1000:
                _, record_id, key, value, kind, origin_id, delay_ms, event_timestamp = pending[0]
                if producer:
                    enqueued = time.monotonic()
                    size = len(value) if value else 0
                    try:
                        profile_started = profiles["kafka_enqueue"].begin()
                        try:
                            producer.produce(k["topic"], key=key, value=value,
                                on_delivery=lambda error, message, at=enqueued, size=size: metrics.delivery(error, (time.monotonic() - at) * 1000, size))
                        finally:
                            profiles["kafka_enqueue"].end(profile_started)
                    except BufferError:
                        metrics.increment("queue_full")
                        break
                else:
                    metrics.delivery(None, 0, len(value) if value else 0)
                heapq.heappop(pending)
                metrics.increment("enqueued")
                metrics.increment("enqueued_bytes", len(value) if value else 0)
                if output:
                    output.write((value if value is not None else b"null") + b"\n")
                if args.dry_run and output is None:
                    sys.stdout.buffer.write((value if value is not None else b"null") + b"\n")
                if trace:
                    trace.write(json.dumps({"run_id": args.run_id, "record_id": record_id, "origin_id": origin_id,
                        "kind": kind, "scheduled_delay_ms": delay_ms, "event_timestamp_ms": event_timestamp,
                        "enqueue_epoch_ms": int(time.time() * 1000), "key": key.decode() if key else None,
                        "value": value.decode() if value is not None else None,
                        "payload_sha256": hashlib.sha256(value).hexdigest() if value is not None else None}, separators=(",", ":")) + "\n")
                sent += 1
            if producer:
                profile_started = profiles["producer_poll"].begin()
                try:
                    producer.poll(0)
                finally:
                    profiles["producer_poll"].end(profile_started)
                metrics.producer_queue = len(producer)
                if k["transactional_id"] and now >= next_commit:
                    producer.commit_transaction(30)
                    producer.begin_transaction()
                    metrics.increment("committed_transactions")
                    next_commit = now + k["transaction_interval_ms"] / 1000
            metrics.pending = len(pending)
            if now >= next_log:
                log("emitter_stats", elapsed_seconds=elapsed, **metrics.snapshot())
                next_log = now + config["runtime"]["log_interval_seconds"]
            if not sent and (not generating or now < next_generation or rate == 0 or len(pending) >= config["runtime"]["max_pending_events"] - required_room):
                wait = min(.001, max(.00001, next_generation - now)) if generating and rate > 0 else .001
                if pending and pending[0][0] > now:
                    wait = min(wait, pending[0][0] - now)
                time.sleep(wait)
        if producer:
            if k["transactional_id"]:
                producer.commit_transaction(30)
                metrics.increment("committed_transactions")
            remaining = producer.flush(config["runtime"]["drain_timeout_seconds"])
            metrics.increment("unflushed", remaining)
            metrics.producer_queue = remaining
        metrics.pending = len(pending)
    except Exception as error:
        metrics.increment("fatal_errors")
        if producer and k["transactional_id"]:
            try:
                producer.abort_transaction(10)
            except Exception:
                pass
        log("emitter_error", error=repr(error), **metrics.snapshot())
        raise
    finally:
        if output:
            output.close()
        if trace:
            trace.close()
        if server:
            server.shutdown()
        summary = {"run_id": args.run_id, "elapsed_seconds": time.monotonic() - started, "seed": config["seed"], **metrics.snapshot()}
        log("emitter_finished", **summary)
        if config["runtime"]["summary_path"]:
            Path(config["runtime"]["summary_path"]).write_text(json.dumps(summary, indent=2) + "\n")
    if any(metrics.counts[name] for name in ["delivery_errors", "undrained", "unflushed", "fatal_errors"]):
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        log("configuration_or_io_error", error=str(error))
        raise SystemExit(2)

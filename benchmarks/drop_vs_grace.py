#!/usr/bin/env python3
"""Matched prompt/drop versus grace lateness comparison on scripted disorder.

This tool answers ticket 11: it runs the production query twice with the same
scripted disorder, seed, window, out-of-order budget, grace budget, dedup
settings, resources and checkpoint cadence, and compares the committed EMA and
advisory output of each arm against

* the complete-input oracle (every eligible trade contributes), and
* an independent policy-aware expectation whose admission decision is
  prescribed by the documented late-event rule, not taken from Flink.

The comparison counts missing, extra, wrong and delayed advisories, separates
the expected policy difference from processing or delivery defects, and reports
finalization/publication delay, managed state, sample sizes and a recommendation.

The deterministic core (fixture, expectations, classification, comparison) is
standard-library only and is exercised by ``benchmarks/tests/test_drop_vs_grace.py``.
The ``run-arm`` subcommand drives a live stack; ``compare`` combines two arm
artifacts without a stack.

Run ``python3 benchmarks/drop_vs_grace.py --help`` for commands.
"""
import argparse
import hashlib
import json
import math
import os
import sys
import time

DEFAULT_SEED = 11
DEFAULT_WINDOW_MS = 300_000
DEFAULT_MAX_OUT_OF_ORDER_MS = 5_000
DEFAULT_GRACE_MS = 30_000
DEFAULT_LATE_PRICE = 1000.0
SYMBOL = "DG11.FR"
# Windows 0..RAMP_WINDOWS hold one trade each; the next window holds the early
# trade plus the scripted late trade.
RAMP_WINDOWS = 30
LATE_WINDOW = RAMP_WINDOWS + 1
POLICIES = ("DROP", "GRACE")
EMA_ALPHA = {38: 2.0 / 39.0, 100: 2.0 / 101.0}


def scripted_disorder(seed=DEFAULT_SEED, window_ms=DEFAULT_WINDOW_MS,
                      max_out_of_order_ms=DEFAULT_MAX_OUT_OF_ORDER_MS, grace_ms=DEFAULT_GRACE_MS,
                      late_price=DEFAULT_LATE_PRICE):
    """Return a deterministic matched-disorder fixture shared by both arms.

    The schedule holds one trade per window for windows 0..RAMP_WINDOWS, then a
    window with an early trade and a late trade. The late trade is delivered
    after the watermark passes the window's nominal finalization, so ``DROP``
    rejects it and ``GRACE`` admits it, changing the closing price.
    """
    if window_ms <= 0:
        raise ValueError("window_ms must be positive")
    if max_out_of_order_ms < 0:
        raise ValueError("max_out_of_order_ms must be nonnegative")
    if grace_ms < 0:
        raise ValueError("grace_ms must be nonnegative")
    base_ms = 1_700_000_400_000 + (seed % 1000) * window_ms
    steps = []
    for window in range(LATE_WINDOW):
        price = 39.0 if window == 0 else 0.01
        steps.append({"kind": "trade", "ticker": SYMBOL,
                      "event_ms": base_ms + window * window_ms + 1000, "price": price})
    window_end = base_ms + (LATE_WINDOW + 1) * window_ms
    late = {"kind": "trade", "ticker": SYMBOL, "event_ms": base_ms + LATE_WINDOW * window_ms + 2000,
            "price": late_price, "window": base_ms + LATE_WINDOW * window_ms, "window_index": LATE_WINDOW}
    early_event_ms = base_ms + LATE_WINDOW * window_ms + 1000
    steps.append({"kind": "trade", "ticker": SYMBOL, "event_ms": early_event_ms, "price": 0.01})
    # Wait until the job has processed the ramp and the early trade before any advance
    # marker is published, so the ramp trades are never seen with a raised watermark.
    steps.append({"kind": "sync", "watermark_ms": early_event_ms - max_out_of_order_ms - 1})
    # Advance the watermark past the nominal finalization but inside the grace
    # budget: DROP rejects the late trade, GRACE admits it.
    steps.append({"kind": "advance", "watermark_ms": window_end - 1 + grace_ms // 2})
    # The runner waits for the watermark to reach this target before delivering the
    # late trade, so admission is prescribed rather than left to scheduler timing.
    steps.append({"kind": "sync", "watermark_ms": window_end - 1 + grace_ms // 2})
    steps.append(late)
    steps.append({"kind": "advance", "watermark_ms": window_end - 1 + grace_ms})
    return {"seed": seed, "window_ms": window_ms, "max_out_of_order_ms": max_out_of_order_ms,
            "grace_ms": grace_ms, "symbol": SYMBOL, "base_ms": base_ms, "steps": steps,
            "late_trade": late}


def fixture_digest(fixture):
    """Stable digest of the matched schedule and settings baked into the fixture."""
    payload = json.dumps({key: fixture[key] for key in
                          ("seed", "window_ms", "max_out_of_order_ms", "grace_ms", "symbol", "base_ms", "steps")},
                         sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _quantiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    def quantile(percentile):
        index = percentile * (len(values) - 1)
        low, high = math.floor(index), math.ceil(index)
        return values[low] + (index - low) * (values[high] - values[low])
    return {"count": len(values), "p50": quantile(.5), "p95": quantile(.95),
            "p99": quantile(.99), "max": values[-1]}


def _fired_through(registered, watermark):
    if watermark is None:
        return None
    fired = [timer for timer in registered if timer <= watermark]
    return max(fired) if fired else None


def _simulate_closes(fixture, policy):
    """Prescribed admission: which trade becomes each window's closing price."""
    if policy not in POLICIES:
        raise ValueError(f"unknown policy {policy!r}; expected DROP or GRACE")
    grace = fixture["grace_ms"] if policy == "GRACE" else 0
    symbol = fixture["symbol"]
    window_ms = fixture["window_ms"]
    disorder = fixture["max_out_of_order_ms"]
    watermark = None
    max_ts = None
    registered = set()
    closes = {}
    late = 0
    ordinal = 0
    for step in fixture["steps"]:
        if step["kind"] == "advance":
            watermark = step["watermark_ms"] if watermark is None else max(watermark, step["watermark_ms"])
            marker_ts = step["watermark_ms"] + disorder + 1
            max_ts = marker_ts if max_ts is None else max(max_ts, marker_ts)
            continue
        if step["kind"] != "trade" or step["ticker"] != symbol:
            continue
        event_ms = step["event_ms"]
        effective = watermark
        finalized = _fired_through(registered, watermark)
        if finalized is not None and (effective is None or finalized > effective):
            effective = finalized
        max_ts = event_ms if max_ts is None else max(max_ts, event_ms)
        watermark = max_ts - disorder - 1 if watermark is None else max(watermark, max_ts - disorder - 1)
        window_start = event_ms // window_ms * window_ms
        nominal_timer = window_start + window_ms - 1
        timer = nominal_timer + grace
        if effective is not None and timer <= effective:
            late += 1
            continue
        registered.add(timer)
        previous = closes.get(timer)
        if previous is None or event_ms >= previous["event_ms"]:
            closes[timer] = {"window": window_start, "event_ms": event_ms,
                             "price": step["price"], "ordinal": ordinal}
        ordinal += 1
    return closes, late


def _ema_records(closes, symbol):
    """Sequential EMA and crossover records for one ticker, in window order."""
    records = []
    fast = slow = 0.0
    for key in sorted(closes):
        close = closes[key]
        previous = fast - slow
        fast = EMA_ALPHA[38] * close["price"] + (1 - EMA_ALPHA[38]) * fast
        slow = EMA_ALPHA[100] * close["price"] + (1 - EMA_ALPHA[100]) * slow
        records.append({"symbol": symbol, "window": close["window"], "type": "EMA",
                        "ema38": fast, "ema100": slow, "timestamp_ms": close["event_ms"]})
        difference = fast - slow
        if previous <= 0 and difference > 0:
            records.append({"symbol": symbol, "window": close["window"], "type": "BUY"})
        elif previous >= 0 and difference < 0:
            records.append({"symbol": symbol, "window": close["window"], "type": "SELL"})
    return records


def oracle_truth(fixture):
    """Complete-input truth: the greatest (event time, source ordinal) trade per window."""
    symbol = fixture["symbol"]
    window_ms = fixture["window_ms"]
    best = {}
    ordinal = 0
    for step in fixture["steps"]:
        if step["kind"] != "trade" or step["ticker"] != symbol:
            continue
        window = step["event_ms"] // window_ms * window_ms
        if window not in best or (step["event_ms"], ordinal) > (best[window]["event_ms"], best[window]["ordinal"]):
            best[window] = {"window": window, "event_ms": step["event_ms"],
                            "price": step["price"], "ordinal": ordinal}
        ordinal += 1
    return _ema_records(best, symbol)


def policy_expectation(fixture, policy):
    """Independent expectation from the documented admission rule, not from Flink."""
    closes, _ = _simulate_closes(fixture, policy)
    return _ema_records(closes, fixture["symbol"])


def _index_records(records):
    ema, advisories = {}, {}
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("record must be an object")
        symbol, window, kind = record.get("symbol"), record.get("window"), record.get("type")
        if not isinstance(symbol, str) or not symbol or type(window) is not int:
            raise ValueError("symbol must be a nonempty string and window an integer")
        if kind not in ("EMA", "BUY", "SELL"):
            raise ValueError("type must be EMA, BUY, or SELL")
        if kind == "EMA":
            for field in ("ema38", "ema100"):
                value = record.get(field)
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError(f"{field} must be a finite number")
            ema[(symbol, window)] = record
        else:
            advisories[(symbol, window)] = kind
    return ema, advisories


def classify(expected, actual):
    """Compare canonical EMA/advisory multisets, separating mismatch kinds."""
    expected_ema, expected_adv = _index_records(expected)
    actual_ema, actual_adv = _index_records(actual)
    expected_keys, actual_keys = set(expected_ema), set(actual_ema)
    missing_ema = [{"symbol": key[0], "window": key[1], "type": "EMA"}
                   for key in sorted(expected_keys - actual_keys)]
    extra_ema = [{"symbol": key[0], "window": key[1], "type": "EMA"}
                 for key in sorted(actual_keys - expected_keys)]
    incorrect = []
    for key in sorted(expected_keys & actual_keys):
        for field in ("ema38", "ema100"):
            left, right = expected_ema[key][field], actual_ema[key][field]
            if not math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9):
                incorrect.append({"symbol": key[0], "window": key[1], "field": field,
                                  "expected": left, "actual": right})
    missing, extra, wrong = [], [], []
    for key in sorted(set(expected_adv) | set(actual_adv)):
        expected_kind, actual_kind = expected_adv.get(key), actual_adv.get(key)
        if expected_kind == actual_kind:
            continue
        entry = {"symbol": key[0], "window": key[1]}
        if expected_kind is None:
            extra.append(entry | {"type": actual_kind})
        elif actual_kind is None:
            missing.append(entry | {"type": expected_kind})
        else:
            wrong.append(entry | {"expected": expected_kind, "actual": actual_kind})
    delayed, remaining_extra, still_missing = [], list(extra), []
    for item in missing:
        match = next((candidate for candidate in remaining_extra
                      if candidate["symbol"] == item["symbol"] and candidate["type"] == item["type"]
                      and candidate["window"] > item["window"]), None)
        if match:
            delayed.append({"symbol": item["symbol"], "window": item["window"],
                            "type": item["type"], "observed_window": match["window"]})
            remaining_extra.remove(match)
        else:
            still_missing.append(item)
    counts = {"missing": len(still_missing), "extra": len(remaining_extra), "wrong": len(wrong),
              "delayed": len(delayed), "ema_incorrect": len(incorrect)}
    return {"match": not any(counts.values()), "counts": counts,
            "advisories": {"missing": still_missing, "extra": remaining_extra,
                           "wrong": wrong, "delayed": delayed},
            "ema": {"missing": missing_ema, "extra": extra_ema, "incorrect": incorrect}}


def policy_differences(truth, expectation):
    """Advisories where complete-input truth and the prescribed policy disagree."""
    truth_keys = {(r["symbol"], r["window"], r["type"]) for r in truth if r["type"] in ("BUY", "SELL")}
    expectation_keys = {(r["symbol"], r["window"], r["type"]) for r in expectation if r["type"] in ("BUY", "SELL")}
    return [{"symbol": key[0], "window": key[1], "type": key[2],
             "in_truth": key in truth_keys, "in_expectation": key in expectation_keys}
            for key in sorted(truth_keys | expectation_keys) if (key in truth_keys) != (key in expectation_keys)]


def normalize_committed(records, window_ms):
    """Normalize committed Flink output (``{timestamp_emit,ticker,type,value}``) to canonical records."""
    if window_ms <= 0:
        raise ValueError("window_ms must be positive")
    ema, advisories = {}, []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("record must be an object")
        ticker, kind, timestamp = record.get("ticker"), record.get("type"), record.get("timestamp_emit")
        if not isinstance(ticker, str) or not ticker or type(timestamp) is not int:
            raise ValueError("ticker must be a nonempty string and timestamp_emit an integer")
        window = timestamp // window_ms * window_ms
        if kind == "price":
            continue
        if kind in ("BUY", "SELL"):
            advisories.append({"symbol": ticker, "window": window, "type": kind})
        elif kind in ("EMA38", "EMA100"):
            slot = ema.setdefault((ticker, window), {"symbol": ticker, "window": window, "type": "EMA",
                                                     "timestamp_ms": timestamp})
            slot["ema38" if kind == "EMA38" else "ema100"] = record.get("value")
        else:
            raise ValueError(f"unknown committed type {kind!r}")
    for key in sorted(ema):
        if "ema38" not in ema[key] or "ema100" not in ema[key]:
            raise ValueError(f"unpaired EMA record for {key}")
    normalized = [ema[key] for key in sorted(ema)]
    normalized += sorted(advisories, key=lambda item: (item["symbol"], item["window"], item["type"]))
    return normalized


def _delay_summary(measurements):
    measurements = measurements or {}
    return {"finalization_ms": _quantiles(measurements.get("finalization_delay_ms", [])),
            "publication_ms": _quantiles(measurements.get("publication_delay_ms", []))}


def evaluate_arm(fixture, policy, committed, measurements=None):
    """Classify one arm's committed output against truth and its policy expectation."""
    truth = oracle_truth(fixture)
    expectation = policy_expectation(fixture, policy)
    vs_truth = classify(truth, committed)
    vs_policy = classify(expectation, committed)
    measurements = measurements or {}
    return {"policy": policy,
            "sample": {"committed": len(committed), "truth": len(truth), "expectation": len(expectation),
                       "measurements": measurements.get("samples")},
            "vs_truth": vs_truth, "vs_policy": vs_policy,
            "defects": {"counts": dict(vs_policy["counts"]), "advisories": vs_policy["advisories"],
                        "ema": vs_policy["ema"]},
            "expected_policy_differences": policy_differences(truth, expectation),
            "finalization_lag_ms": fixture["grace_ms"] if policy == "GRACE" else 0,
            "delay": _delay_summary(measurements), "state": dict(measurements.get("state", {})),
            "resources": dict(measurements.get("resources", {})), "passed": vs_policy["match"]}


def _advisory_loss(arm):
    counts = arm["vs_truth"]["counts"]
    return sum(counts[key] for key in ("missing", "extra", "wrong", "delayed", "ema_incorrect"))


def _defect_count(arm):
    counts = arm["defects"]["counts"]
    return sum(counts[key] for key in ("missing", "extra", "wrong", "delayed"))


def recommend(evaluated):
    """Recommend the default policy from the measured correctness/delay/state trade-off."""
    drop, grace = evaluated["DROP"], evaluated["GRACE"]
    drop_loss, grace_loss = _advisory_loss(drop), _advisory_loss(grace)
    drop_defects, grace_defects = _defect_count(drop), _defect_count(grace)
    drop_delay = drop["delay"]["finalization_ms"]["p95"] or 0
    grace_delay = grace["delay"]["finalization_ms"]["p95"] or 0
    grace_lag = grace["finalization_lag_ms"]
    state_delta = (grace["state"].get("checkpoint_bytes") or 0) - (drop["state"].get("checkpoint_bytes") or 0)
    if drop_defects or grace_defects:
        if grace_defects < drop_defects:
            default = "GRACE"
        elif drop_defects < grace_defects:
            default = "DROP"
        else:
            default = "DROP" if drop_delay <= grace_delay else "GRACE"
        reason = (f"processing/delivery defects: drop={drop_defects}, grace={grace_defects}; "
                  f"prefer the arm with fewer defects")
    elif grace_loss < drop_loss:
        default = "GRACE"
        reason = (f"grace preserves {drop_loss - grace_loss} late-trade difference(s) that "
                  f"drop loses against complete-input truth (truth loss drop={drop_loss}, grace={grace_loss}); "
                  f"cost: +{grace_lag:.0f} ms event-time finalization delay and "
                  f"{state_delta:+d} bytes checkpoint state")
    elif drop_loss < grace_loss:
        default = "DROP"
        reason = (f"drop matches truth better than grace (truth loss drop={drop_loss}, grace={grace_loss})")
    else:
        default = "DROP"
        reason = (f"both policies match complete-input truth; drop has lower event-time finalization "
                  f"delay (0 ms vs {grace_lag:.0f} ms) and less managed state")
    return {"default": default, "reason": reason,
            "evidence": {"drop": {"truth_loss": drop_loss, "defects": drop_defects,
                                  "finalization_lag_ms": drop["finalization_lag_ms"],
                                  "measured_finalization_p95_ms": drop_delay},
                         "grace": {"truth_loss": grace_loss, "defects": grace_defects,
                                   "finalization_lag_ms": grace["finalization_lag_ms"],
                                   "measured_finalization_p95_ms": grace_delay}}}


def compare_arms(arms):
    """Combine the two matched arms into one report with a recommendation."""
    if set(arms) != set(POLICIES):
        raise ValueError("compare_arms requires exactly the DROP and GRACE arms")
    digests = {name: fixture_digest(arm["fixture"]) for name, arm in arms.items()}
    if len(set(digests.values())) != 1:
        raise ValueError("arms must share one matched schedule; got different fixture digests")
    settings = [json.dumps(arm.get("settings", {}), sort_keys=True) for arm in arms.values()]
    if len(set(settings)) != 1:
        raise ValueError("arms must share matched settings (dedup, resources, checkpoint cadence)")
    for name, arm in arms.items():
        if arm["policy"] != name:
            raise ValueError(f"arm {name!r} declares policy {arm['policy']!r}")
    fixture = arms["DROP"]["fixture"]
    evaluated = {name: evaluate_arm(arm["fixture"], arm["policy"], arm["committed"], arm.get("measurements"))
                 for name, arm in arms.items()}
    return {"matched": {"fixture_digest": digests["DROP"], "seed": fixture["seed"],
                        "window_ms": fixture["window_ms"],
                        "max_out_of_order_ms": fixture["max_out_of_order_ms"],
                        "grace_ms": fixture["grace_ms"], "settings": arms["DROP"].get("settings", {})},
            "arms": evaluated, "recommendation": recommend(evaluated)}


def load_arm(path):
    with open(path, encoding="utf-8") as stream:
        arm = json.load(stream)
    for field in ("fixture", "policy", "committed"):
        if field not in arm:
            raise ValueError(f"{path}: arm is missing {field!r}")
    return arm


def _wait_for_watermark(jobmanager_url, target, timeout):
    """Poll the Flink REST API until every running job's watermark reaches ``target``."""
    import urllib.request
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        best = -(2 ** 63)
        try:
            with urllib.request.urlopen(jobmanager_url + "/jobs/overview", timeout=5) as response:
                jobs = json.load(response)["jobs"]
            for job in jobs:
                if job["state"] != "RUNNING":
                    continue
                with urllib.request.urlopen(f"{jobmanager_url}/jobs/{job['jid']}", timeout=5) as response:
                    vertices = json.load(response)["vertices"]
                for vertex in vertices:
                    with urllib.request.urlopen(
                            f"{jobmanager_url}/jobs/{job['jid']}/vertices/{vertex['id']}/watermarks",
                            timeout=5) as response:
                        for value in json.load(response):
                            observed = int(value["value"])
                            if 0 < observed < 9_000_000_000_000_000_000:
                                best = max(best, observed)
            if best >= target:
                return best
        except (OSError, ValueError, KeyError, TypeError):
            pass
        time.sleep(0.5)
    raise TimeoutError(f"watermark did not reach {target} within {timeout}s")


def _collect_state(jobmanager_url):
    """Best-effort latest completed checkpoint state, for the measured state cost."""
    import urllib.request
    try:
        with urllib.request.urlopen(jobmanager_url + "/jobs/overview", timeout=5) as response:
            jobs = json.load(response)["jobs"]
        for job in jobs:
            if job["state"] != "RUNNING":
                continue
            with urllib.request.urlopen(f"{jobmanager_url}/jobs/{job['jid']}/checkpoints", timeout=5) as response:
                checkpoints = json.load(response)
            completed = checkpoints.get("latest", {}).get("completed")
            if not completed:
                continue
            return {"checkpoint_id": completed.get("id"),
                    "checkpoint_bytes": completed.get("state_size"),
                    "checkpointed_bytes": completed.get("checkpointed_size"),
                    "checkpoint_duration_ms": completed.get("end_to_end_duration"),
                    "completed_checkpoints": checkpoints.get("counts", {}).get("completed")}
    except (OSError, ValueError, KeyError, TypeError):
        return {}
    return {}


def _publish_steps(producer, fixture, topic, partitions, wait_for_watermark=None):
    """Publish the scripted steps; advance markers reach every partition so all subtasks progress.

    Returns wall-clock publish times (epoch milliseconds) keyed by step index.
    """
    marker_ticker = fixture["symbol"] + ".MARKER"
    published = {}
    def delivered(error, _message):
        if error:
            raise RuntimeError(f"publish failed: {error}")
    for index, step in enumerate(fixture["steps"]):
        if step["kind"] == "sync":
            if producer.flush(10):
                raise RuntimeError("producer flush timed out before a watermark sync step")
            if wait_for_watermark is not None:
                wait_for_watermark(step["watermark_ms"])
            continue
        if step["kind"] == "advance":
            event_ms = step["watermark_ms"] + fixture["max_out_of_order_ms"] + 1
            for partition in partitions:
                payload = json.dumps({"timestamp_emit": event_ms, "ticker": marker_ticker,
                                      "sec_type": "E", "price": 1.0}).encode()
                producer.produce(topic, partition=partition, key=marker_ticker, value=payload,
                                 on_delivery=delivered)
        else:
            payload = json.dumps({"timestamp_emit": step["event_ms"], "ticker": step["ticker"],
                                  "sec_type": "E", "price": step["price"]}).encode()
            producer.produce(topic, partition=partitions[0], key=step["ticker"], value=payload,
                             on_delivery=delivered)
        published[index] = time.time() * 1000
        producer.poll(0)
    if producer.flush(30):
        raise RuntimeError("producer flush timed out")
    return published


def _topic_partitions(consumer, topic, timeout=60):
    """Discover a topic's partitions, retrying until metadata is populated."""
    deadline = time.monotonic() + timeout
    while True:
        metadata = consumer.list_topics(topic, timeout=10)
        entry = metadata.topics.get(topic)
        if entry is not None and entry.partitions:
            return sorted(entry.partitions)
        if time.monotonic() >= deadline:
            raise RuntimeError(f"no partitions discovered for topic {topic!r}")
        time.sleep(1)


def _consume_committed(consumer, output_topic, rejected_topic, symbol, deadline_seconds, expected=None):
    from confluent_kafka import TopicPartition, OFFSET_BEGINNING
    assignments = [TopicPartition(output_topic, p, OFFSET_BEGINNING)
                   for p in _topic_partitions(consumer, output_topic)]
    assignments += [TopicPartition(rejected_topic, p, OFFSET_BEGINNING)
                    for p in _topic_partitions(consumer, rejected_topic)]
    consumer.assign(assignments)
    committed, audit, observed, quiet, seen = [], [], {}, 0, 0
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None:
            quiet += 1
            if expected is not None and seen >= expected and quiet > 6:
                break
            if expected is None and quiet > 40 and committed:
                break
            continue
        if message.error():
            raise RuntimeError(str(message.error()))
        quiet = 0
        now_ms = time.time() * 1000
        value = json.loads(message.value())
        if message.topic() == rejected_topic:
            audit.append(value)
        elif value.get("ticker") == symbol:
            committed.append(value)
            observed[(value["ticker"], value["type"], value["timestamp_emit"])] = now_ms
            # Count one canonical record per window EMA pair plus each advisory.
            if value.get("type") in ("EMA38", "BUY", "SELL"):
                seen += 1
    return committed, audit, observed


def _delay_measurements(fixture, policy, published, observed):
    """Wall-clock delay for the late window from the deciding trade and the finalizing advance.

    Scope: one scripted symbol and one late window, so the sample size is one. The event
    times are synthetic, so this measures publication-to-visibility, not historical event age.
    """
    steps = fixture["steps"]
    late = fixture["late_trade"]
    late_index = next(index for index, step in enumerate(steps) if step is late)
    early_index = max(index for index, step in enumerate(steps)
                      if step["kind"] == "trade" and step is not late)
    advances = [index for index, step in enumerate(steps) if step["kind"] == "advance"]
    trigger = advances[0] if policy == "DROP" else advances[-1]
    deciding_index = early_index if policy == "DROP" else late_index
    deciding_event_ms = steps[deciding_index]["event_ms"]
    ema_observed = observed.get((fixture["symbol"], "EMA38", deciding_event_ms))
    if ema_observed is None:
        return {"finalization_delay_ms": [], "publication_delay_ms": [], "samples": 0}
    return {"finalization_delay_ms": [ema_observed - published[trigger]],
            "publication_delay_ms": [ema_observed - published[deciding_index]], "samples": 1}


def run_arm(fixture, policy, brokers, input_topic, output_topic, rejected_topic,
            timeout=180, settings=None, measurements=None, jobmanager_url=None, sync_timeout=120):
    """Publish the scripted steps once against a running stack configured for ``policy``."""
    from confluent_kafka import Consumer, Producer
    producer = Producer({"bootstrap.servers": brokers, "enable.idempotence": True})
    consumer = Consumer({"bootstrap.servers": brokers, "group.id": f"drop-vs-grace-{policy}-{os.getpid()}",
                         "enable.auto.commit": False, "isolation.level": "read_committed"})
    try:
        partitions = _topic_partitions(consumer, input_topic)
        waiter = ((lambda target: _wait_for_watermark(jobmanager_url, target, sync_timeout))
                  if jobmanager_url else None)
        published = _publish_steps(producer, fixture, input_topic, partitions, waiter)
        expected = len(policy_expectation(fixture, policy))
        committed, audit, observed = _consume_committed(consumer, output_topic, rejected_topic,
                                                        fixture["symbol"], timeout, expected)
    finally:
        consumer.close()
    normalized = normalize_committed(committed, fixture["window_ms"])
    measurements = dict(measurements or {})
    measurements.update(_delay_measurements(fixture, policy, published, observed))
    if jobmanager_url:
        measurements.setdefault("state", {}).update(_collect_state(jobmanager_url))
    return {"fixture": fixture, "policy": policy, "committed": normalized,
            "measurements": measurements, "settings": settings or {},
            "audit": {"count": len(audit), "sample": audit[:5]}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    compare = subparsers.add_parser("compare", help="combine two arm artifacts into one report")
    compare.add_argument("--drop", required=True, help="DROP arm JSON artifact")
    compare.add_argument("--grace", required=True, help="GRACE arm JSON artifact")
    compare.add_argument("--output", help="write the comparison report to this path")
    fixture_parser = subparsers.add_parser("fixture", help="print the scripted fixture or its digest")
    fixture_parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    fixture_parser.add_argument("--window-ms", type=int, default=DEFAULT_WINDOW_MS)
    fixture_parser.add_argument("--max-out-of-order-ms", type=int, default=DEFAULT_MAX_OUT_OF_ORDER_MS)
    fixture_parser.add_argument("--grace-ms", type=int, default=DEFAULT_GRACE_MS)
    fixture_parser.add_argument("--late-price", type=float, default=DEFAULT_LATE_PRICE)
    fixture_parser.add_argument("--digest", action="store_true", help="print only the fixture digest")
    fixture_parser.add_argument("--truth", action="store_true", help="print complete-input truth as JSONL")
    run = subparsers.add_parser("run-arm", help="publish the scripted steps against a running stack")
    run.add_argument("--policy", required=True, choices=POLICIES)
    run.add_argument("--output", required=True, help="write the arm artifact to this path")
    run.add_argument("--seed", type=int, default=DEFAULT_SEED)
    run.add_argument("--window-ms", type=int, default=DEFAULT_WINDOW_MS)
    run.add_argument("--max-out-of-order-ms", type=int, default=DEFAULT_MAX_OUT_OF_ORDER_MS)
    run.add_argument("--grace-ms", type=int, default=DEFAULT_GRACE_MS)
    run.add_argument("--late-price", type=float, default=DEFAULT_LATE_PRICE)
    run.add_argument("--timeout", type=float, default=180)
    run.add_argument("--sync-timeout", type=float, default=120)
    run.add_argument("--jobmanager-url", default=os.getenv("JOBMANAGER_URL", "http://jobmanager:8081"))
    args = parser.parse_args(argv)
    if args.command == "fixture":
        fixture = scripted_disorder(args.seed, args.window_ms, args.max_out_of_order_ms,
                                    args.grace_ms, args.late_price)
        if args.digest:
            print(fixture_digest(fixture))
        elif args.truth:
            for record in oracle_truth(fixture):
                print(json.dumps(record, allow_nan=False))
        else:
            print(json.dumps(fixture, indent=2, allow_nan=False))
        return 0
    if args.command == "compare":
        try:
            report = compare_arms({"DROP": load_arm(args.drop), "GRACE": load_arm(args.grace)})
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        text = json.dumps(report, indent=2, allow_nan=False)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as stream:
                stream.write(text + "\n")
        print(text)
        return 0
    fixture = scripted_disorder(args.seed, args.window_ms, args.max_out_of_order_ms,
                                args.grace_ms, args.late_price)
    settings = {name: os.getenv(name) for name in ("DEDUP_STRATEGY", "WINDOW_SIZE_MS",
                "MAX_OUT_OF_ORDER_MS", "ALLOWED_LATENESS_MS", "FLINK_PARALLELISM",
                "CHECKPOINT_INTERVAL_MS", "SOURCE_IDLE_TIMEOUT_MS")}
    arm = run_arm(fixture, args.policy, os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                  os.getenv("KAFKA_INPUT_TOPIC", "price-events"),
                  os.getenv("KAFKA_OUTPUT_TOPIC", "events-processed"),
                  os.getenv("KAFKA_REJECTED_TOPIC", "price-events-rejected"),
                  timeout=args.timeout, settings=settings, jobmanager_url=args.jobmanager_url,
                  sync_timeout=args.sync_timeout)
    with open(args.output, "w", encoding="utf-8") as stream:
        json.dump(arm, stream, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps({"policy": arm["policy"], "committed": len(arm["committed"]),
                      "audit": arm["audit"]["count"], "output": args.output}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

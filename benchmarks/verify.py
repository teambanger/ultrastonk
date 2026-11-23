"""Deterministic Kafka-to-Flink-to-Kafka contract and EMA integration check."""
import collections
import json
import os
import re
import time
import uuid


def isolated_topics(env):
    project = re.fullmatch(r"ultrastonk-verify-([a-z0-9][a-z0-9-]{0,63})", env.get("COMPOSE_PROJECT_NAME", ""))
    if not project:
        raise ValueError("use a fresh COMPOSE_PROJECT_NAME=ultrastonk-verify-<id>")
    prefix = "verify-" + project[1]
    topics = tuple(f"{prefix}-{suffix}" for suffix in ("in", "out", "audit"))
    keys = ("KAFKA_INPUT_TOPIC", "KAFKA_OUTPUT_TOPIC", "KAFKA_REJECTED_TOPIC")
    if tuple(env.get(key) for key in keys) != topics:
        raise ValueError("use verifier-only topics: " + " ".join(f"{key}={topic}" for key, topic in zip(keys, topics)))
    return topics


def flush_timestamp(window_end, max_out_of_order, grace):
    # Flink emits watermark = max_timestamp - max_out_of_order - 1.
    # The timer for a window fires at window_end - 1 + grace.
    return window_end + max_out_of_order + grace


def main():
    try:
        topic_in, topic_out, rejected = isolated_topics(os.environ)
    except ValueError as error:
        raise SystemExit(str(error)) from None
    from confluent_kafka import Consumer
    from driver import wait_for_job, request_json

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    window = int(os.getenv("WINDOW_SIZE_MS", "300000"))
    disorder = int(os.getenv("MAX_OUT_OF_ORDER_MS", "5000"))
    grace = int(os.getenv("ALLOWED_LATENESS_MS", "30000"))
    dedup = os.getenv("DEDUP_STRATEGY", "NONE").upper()
    policy = os.getenv("LATE_EVENT_STRATEGY", "DROP").upper()
    checkpoint = int(os.getenv("CHECKPOINT_INTERVAL_MS", "10000")) / 1000
    if window < 1000:
        raise RuntimeError("integration verifier requires WINDOW_SIZE_MS >= 1000")
    if disorder < 0 or grace < 0:
        raise RuntimeError("MAX_OUT_OF_ORDER_MS and ALLOWED_LATENESS_MS must be nonnegative")
    if dedup not in {"NONE", "EXACT", "TIMESTAMP"} or policy not in {"DROP", "SIDE_OUTPUT", "GRACE"}:
        raise RuntimeError(f"unknown policy: dedup={dedup}, late={policy}")
    grace = grace if policy == "GRACE" else 0
    wait_for_job()
    run = "V" + uuid.uuid4().hex[:10]
    a, b = run + "A.FR", run + "B.FR"
    # A repeatable verifier starts above existing watermarks in this test deployment.
    # It advances synthetic event time and must be used on isolated test topics.
    watermark = int(time.time() * 1000)
    for job in request_json("http://jobmanager:8081/jobs/overview")["jobs"]:
        if job["state"] != "RUNNING":
            continue
        for vertex in request_json("http://jobmanager:8081/jobs/" + job["jid"])["vertices"]:
            values = request_json(f"http://jobmanager:8081/jobs/{job['jid']}/vertices/{vertex['id']}/watermarks")
            for value in values:
                observed = int(value["value"])
                if 0 < observed < 9_000_000_000_000_000_000:
                    watermark = max(watermark, observed)
    base = (watermark // window + 1) * window
    advance = flush_timestamp(base + 2 * window, disorder, grace)
    finish = flush_timestamp((advance // window + 1) * window, disorder, grace)
    if finish > (1 << 63) - 1 - max(172_800_000, window + grace):
        raise RuntimeError("verifier timestamps would exceed the supported event-time range")
    consumer = Consumer({"bootstrap.servers": brokers, "group.id": run, "enable.auto.commit": False,
                         "isolation.level": "read_committed"})
    try:
        verify(consumer, brokers, topic_in, topic_out, rejected, dedup, policy, checkpoint,
               run, a, b, base, window, advance, finish)
    finally:
        consumer.close()


def verify(consumer, brokers, topic_in, topic_out, rejected, dedup, policy, checkpoint,
           run, a, b, base, window, advance, finish):
    from confluent_kafka import Producer, TopicPartition, OFFSET_BEGINNING

    producer = Producer({"bootstrap.servers": brokers, "enable.idempotence": True})
    metadata = consumer.list_topics(timeout=10)
    partitions = sorted(metadata.topics[topic_in].partitions)
    for topic in (topic_in, topic_out, rejected):
        for partition in metadata.topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if high != 0:
                raise RuntimeError(f"topic {topic!r} is not fresh; create a new verifier project and topics")
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for topic in [topic_out, rejected]
                     for p in metadata.topics[topic].partitions])
    actual = collections.Counter()
    quarantined = collections.Counter()
    expected_audit = collections.Counter()
    expected = collections.Counter()
    delivery_errors = []

    def signature(event):
        return (event["ticker"], event["timestamp_emit"], event["type"], round(event["value"], 9))

    def event(ticker, offset, price):
        return {"timestamp_emit": base + offset, "ticker": ticker, "sec_type": "E", "price": price}

    def delivered(error, message):
        if error:
            delivery_errors.append(str(error))

    def publish(key, value, partition=partitions[0]):
        producer.produce(topic_in, partition=partition, key=key, value=value, on_delivery=delivered)

    def drain_producer():
        pending = producer.flush(10)
        if pending or delivery_errors:
            raise RuntimeError(f"verifier delivery failed: {pending} pending, {delivery_errors}")

    def expect_ema(ticker, timestamp, close, previous=0):
        for period in (38, 100):
            alpha = 2 / (period + 1)
            value = close * alpha + (1 - alpha) * (previous * alpha)
            expected[(ticker, timestamp, "EMA" + str(period), round(value, 9))] += 1

    def read_output(timeout):
        message = consumer.poll(timeout)
        if message is None:
            return
        if message.error():
            raise RuntimeError(str(message.error()))
        value = json.loads(message.value())
        if message.topic() == rejected:
            assert set(value) == {"raw", "reason"} and isinstance(value["reason"], str) and value["reason"], value
            raw = value["raw"]
            if value["reason"] == "window already finalized":
                audit_tick = json.loads(raw)
                assert audit_tick == late, value
                quarantined["late"] += 1
            else:
                quarantined[raw] += 1
        else:
            assert set(value) == {"timestamp_emit", "ticker", "type", "value"}, value
            actual[signature(value)] += 1

    def check_no_extras():
        assert not (actual - expected), {"extra output": actual - expected}
        assert not (quarantined - expected_audit), {"extra audit": quarantined - expected_audit}

    def poll_expected():
        deadline = time.monotonic() + max(90, 3 * checkpoint)
        while time.monotonic() < deadline:
            read_output(.2)
            check_no_extras()
            if actual == expected and quarantined == expected_audit:
                return
        raise AssertionError({"missing output": expected - actual, "missing audit": expected_audit - quarantined})

    def publish_markers(timestamp, batch):
        # Keep every source subtask able to advance its watermark, regardless of partition assignment.
        for partition in partitions:
            ticker = f"{run}{batch}{partition}.FR"
            publish(ticker, json.dumps(event(ticker, timestamp - base, 1)), partition)
            expected[(ticker, timestamp, "price", 1)] += 1
        drain_producer()

    inputs = [event(a, 100, 100), event(a, 100, 100), event(a, 800, 200), event(a, 200, 150),
              event(a, 800, 250), event(b, 200, 50), event(a, window + 200, 300)]
    late = event(a, 900, 999)
    seen = set()
    close = 200 if dedup == "TIMESTAMP" else 250
    for tick in inputs:
        identity = (tick["ticker"], tick["timestamp_emit"])
        if dedup == "EXACT":
            identity += (tick["price"],)
        if dedup == "NONE" or identity not in seen:
            expected[(tick["ticker"], tick["timestamp_emit"], "price", round(tick["price"], 9))] += 1
            seen.add(identity)
        publish(tick["ticker"], json.dumps(tick))
    # The second-window fixture can itself advance the first window's timer.
    expect_ema(a, base + 800, close)
    expect_ema(b, base + 200, 50)
    expect_ema(a, base + window + 200, 300, close)
    marker = "invalid-" + run
    publish(run, marker)
    publish(run, None)
    expected_audit.update([marker, "null"])
    publish_markers(advance, "FLUSH")
    poll_expected()

    publish(a, json.dumps(late))
    expected[(a, base + 900, "price", 999)] += 1
    if policy != "DROP":
        expected_audit["late"] += 1
    drain_producer()
    poll_expected()
    for partition in partitions:
        expect_ema(f"{run}FLUSH{partition}.FR", advance, 1)
    publish_markers(finish, "FINISH")
    poll_expected()
    # Keep reading across another checkpoint so a matching prefix is not enough to pass.
    deadline = time.monotonic() + checkpoint + 1
    while time.monotonic() < deadline:
        read_output(.2)
        check_no_extras()
    print(json.dumps({"integration_passed": True, "dedup": dedup, "late": policy,
                      "validated_records": sum(actual.values()), "quarantined": sum(quarantined.values())}, indent=2))


if __name__ == "__main__":
    main()

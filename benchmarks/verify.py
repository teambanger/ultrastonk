"""Deterministic Kafka-to-Flink-to-Kafka contract and EMA integration check."""
import collections
import json
import math
import os
import time
import uuid
from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
from driver import wait_for_job, request_json


def main():
    wait_for_job()
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic_in = os.getenv("KAFKA_INPUT_TOPIC", "price-events")
    topic_out = os.getenv("KAFKA_OUTPUT_TOPIC", "events-processed")
    rejected = os.getenv("KAFKA_REJECTED_TOPIC", "price-events-rejected")
    window = int(os.getenv("WINDOW_SIZE_MS", "300000"))
    if window < 1000:
        raise RuntimeError("integration verifier requires WINDOW_SIZE_MS >= 1000")
    run = "V" + uuid.uuid4().hex[:10]
    a, b, flush = [run + name + ".FR" for name in ["A", "B", "FLUSH"]]
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
    producer = Producer({"bootstrap.servers": brokers, "enable.idempotence": True})
    consumer = Consumer({"bootstrap.servers": brokers, "group.id": run, "enable.auto.commit": False,
                         "isolation.level": "read_committed"})
    metadata = consumer.list_topics(timeout=10)
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for topic in [topic_out, rejected]
                     for p in metadata.topics[topic].partitions])
    actual = collections.Counter()
    quarantined = []
    def signature(event):
        return (event["ticker"], event["timestamp_emit"], event["type"], round(event["value"], 9))
    def event(ticker, offset, price):
        return {"timestamp_emit": base + offset, "ticker": ticker, "sec_type": "E", "price": price}
    inputs = [event(a, 100, 100), event(a, 100, 100), event(a, 800, 200), event(a, 200, 150),
              event(a, 800, 250), event(b, 200, 50), event(a, window + 200, 300)]
    expected = collections.Counter()
    seen = set()
    dedup = os.getenv("DEDUP_STRATEGY", "NONE")
    for tick in inputs:
        identity = (tick["ticker"], tick["timestamp_emit"])
        if dedup == "EXACT":
            identity += (tick["price"],)
        if dedup == "NONE" or identity not in seen:
            expected[(tick["ticker"], tick["timestamp_emit"], "price", round(tick["price"], 9))] += 1
            seen.add(identity)
        producer.produce(topic_in, key=tick["ticker"], value=json.dumps(tick))
    close = 200 if dedup == "TIMESTAMP" else 250
    for period in [38, 100]:
        alpha = 2 / (period + 1)
        expected[(a, base + 800, "EMA" + str(period), round(close * alpha, 9))] += 1
        expected[(b, base + 200, "EMA" + str(period), round(50 * alpha, 9))] += 1
        expected[(a, base + window + 200, "EMA" + str(period), round(300 * alpha + close * alpha * (1 - alpha), 9))] += 1
    marker = "invalid-" + run
    producer.produce(topic_in, key=run, value=marker)
    producer.produce(topic_in, key=run, value=None)
    producer.flush(10)
    # Allow the source to consume earlier windows before advancing event time.
    time.sleep(1)
    advance = event(flush, 10 * window, 1)
    producer.produce(topic_in, key=flush, value=json.dumps(advance))
    producer.flush(10)
    def poll_until(predicate, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = consumer.poll(.2)
            if message:
                if message.error():
                    raise RuntimeError(str(message.error()))
                value = json.loads(message.value())
                if message.topic() == rejected:
                    if run in value.get("raw", ""):
                        quarantined.append(value)
                elif value["ticker"] in [a, b]:
                    assert set(value) == {"timestamp_emit", "ticker", "type", "value"}, value
                    actual[signature(value)] += 1
            if predicate():
                return
        raise AssertionError({"missing": list((expected - actual).items()), "extra": list((actual - expected).items()),
                              "quarantined": quarantined})
    poll_until(lambda: not (expected - actual) and any(q["raw"] == marker for q in quarantined))
    assert actual == expected, (actual, expected)
    late = event(a, 900, 999)
    producer.produce(topic_in, key=a, value=json.dumps(late))
    producer.flush(10)
    expected[(a, base + 900, "price", 999)] += 1
    policy = os.getenv("LATE_EVENT_STRATEGY", "DROP")
    poll_until(lambda: not (expected - actual) and (policy == "DROP" or len(quarantined) >= 2))
    assert actual == expected, (actual, expected)
    consumer.close()
    print(json.dumps({"integration_passed": True, "dedup": dedup, "late": policy,
                      "validated_records": sum(actual.values()), "quarantined": len(quarantined)}, indent=2))


if __name__ == "__main__":
    main()

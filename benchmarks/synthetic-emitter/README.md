# Configurable dummy PriceEvent emitter

The emitter generates the requested four-field JSON contract and publishes to Kafka with ticker keys. It is a test input tool; it does not read the real course dataset. The benchmark driver remains the tool for output correctness and consumer-visible latency comparisons.

## Run

```bash
docker compose --profile synthetic build dummy-emitter
docker compose run --rm --no-deps dummy-emitter --config /config/default.json
docker compose run --rm --no-deps dummy-emitter --config /config/clean.json --rate 10000 --duration 30
docker compose run --rm --no-deps dummy-emitter --config /config/chaos.json
```

Start Kafka/Flink first. One-off `compose run` does not publish the metrics port by default. To keep metrics accessible, start the optional service with `compose --profile synthetic up -d dummy-emitter`, or use `compose run --service-ports ...`. The default service runs for sixty seconds. For continuous emission, set duration to zero and max events to zero. Host metrics are available at [localhost:8001/metrics](http://localhost:8001/metrics). The optional monitoring stack discovers the service automatically while it is running.

For a local Python installation, install `requirements.txt` and set `--bootstrap-servers localhost:9092`. Kafka is unnecessary for configuration validation and dry runs:

```bash
cd benchmarks/synthetic-emitter
python3 dummy_emitter.py --print-defaults
python3 dummy_emitter.py --config configs/chaos.json --validate
python3 dummy_emitter.py --config configs/clean.json --dry-run --max-events 20
python3 dummy_emitter.py --config configs/clean.json --dry-run --max-events 100 --output-jsonl /tmp/prices.jsonl
python3 dummy_emitter.py --config configs/duplicates.json --dry-run --max-events 100 --trace-jsonl /tmp/injected.jsonl
python3 -m unittest discover -s tests -v
```

Logs and final summaries are JSON on stderr. Dry-run values are JSONL on stdout unless `--output-jsonl` captures them. Injected malformed payloads remain malformed in captures; tombstones are represented by a `null` line. A capture preserves publish order and values but not Kafka keys or exact timing.

## Configuration

JSON files override defaults recursively; unknown keys fail validation. Use `--print-defaults` for the complete configuration. CLI overrides: `--rate`, `--duration`, `--max-events`, `--seed`, `--topic`, `--bootstrap-servers`. Explicit CLI values take precedence over `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_INPUT_TOPIC`, which take precedence over the JSON configuration for these connection fields.

| Group | Options |
|---|---|
| `seed` | Seed for symbol selection, initial prices, price noise, and fault choices |
| `rate` | `events_per_second`, `duration_seconds` (0 means unlimited), `max_events` (0 means unlimited), `schedule`, burst cadence/duration/multiplier, pause cadence/duration |
| `symbols` | Generated `count` / `prefix`, or explicit `tickers`; exchange/security-type weights; `uniform` / `zipf` distribution and `zipf_exponent` |
| `price` | `constant`, `random_walk`, `sine`, `trend`, or `regime`; initial price range, floor, volatility, drift, sine amplitude/period, regime duration, decimal precision |
| `event_time` | `wallclock` or `accelerated`; optional `start_epoch_ms` and `speed` |
| `faults` | Exact duplicate probability/copies/delay, same-time corrections, real delayed publication, timestamp backdating, invalid records and selected fault modes |
| `kafka` | Bootstrap/topic, ticker/random/no keys, idempotence/acks, compression/linger, delivery timeout, queue capacity, optional transactional ID/commit cadence |
| `runtime` | Delayed-event queue capacity, drain timeout, statistics log interval, optional summary output path |
| `metrics` | HTTP enable flag, bind host, and port |

Duration and event limits count **generated original events**, including originals that become invalid. Duplicates and corrections add records beyond that count. Rates also count originals. Realized rates can be lower due to generator CPU, broker backpressure, or queue capacity. The scheduler avoids an unbounded catch-up burst after pauses/backpressure.

`schedule` contains ordered objects with `duration_seconds` and `events_per_second`. After the last stage, its rate continues until the global duration/event limit ends. A zero rate pauses generation while queued records continue publishing. Periodic pauses override bursts; both start at elapsed time zero. All scheduling uses monotonic time, so wall-clock adjustments do not distort rates.

Exchange and security-type weights assign each generated ticker to one exchange/type for its lifetime. Symbol selection is O(log N) over precomputed cumulative weights. Ticker keys preserve Kafka partition affinity; `random` and `none` are explicit partition-order stress modes and can weaken consistent per-ticker arrival ordering.

Price noise scales with the square root of the elapsed time since that ticker's last event. Trend applies configured continuous drift without noise; regime alternates drift direction at the configured interval and includes noise. Sine uses elapsed **wall time** even when event time is accelerated. Prices remain finite, floored, capped at 1e12, and rounded.

## Scenarios

| Preset | Purpose |
|---|---|
| `clean.json` | Uniform traffic, no injected faults; establish a processing baseline |
| `default.json` | Long-tail traffic with modest duplicates and delays |
| `duplicates.json` | Exact duplicate copies, with delayed retries |
| `late.json` | Reordered publication and backdated timestamps, without duplicates |
| `corrections.json` | Different prices at the same ticker/timestamp |
| `invalid.json` | Contract violations and Kafka tombstones, without duplicate/late noise |
| `chaos.json` | Bursts, pauses, hot symbols, duplicate copies, corrections, real delays, backdating, malformed values, and tombstones |
| `trends.json` | Three exchanges, alternating trends, accelerated windows for EMA inspection |
| `steps.json` | Three load stages: 1000, 10000, and 30000 originals/sec |

Real delay retains the original generation timestamp and schedules publication later, producing reordered arrivals. Backdating changes the timestamp without waiting. They can be combined. Duplicate copies retain the original payload and timestamp; a configured duplicate delay can test expiry beyond the dedup TTL. Corrections change price at the same ticker/timestamp and expose the difference between payload and timestamp deduplication.

Invalid modes are `malformed_json`, `missing_price`, `bad_price`, `bad_ticker`, `negative_timestamp`, and `tombstone`. Fault probabilities are independent except corrections only apply to valid originals. Duplicates can copy an invalid original, allowing quarantine-volume tests.

`--trace-jsonl` records the injection ground truth: run/record/origin IDs, fault kind, scheduled delay, event timestamp, enqueue time, Kafka key, original value, and SHA-256 payload hash. Duplicate copies share an origin ID and payload hash. Corrections share the origin ID but change the hash. These labels are only in the trace; Kafka values preserve the requested contract. A trace records enqueue attempts accepted by the producer, while delivery counters describe Kafka acknowledgement. It is not an output correctness oracle by itself.

Compare each quality preset against `clean.json` while holding rate, symbols, price model, window/grace, dedup TTL, and worker count fixed. With five-minute windows, a short delay only makes ticks finalized-window late near a boundary. For a quick isolated test, use one-second windows, a 100 ms watermark delay, and two-second grace; inspect `duplicateEvents`, `graceAcceptedEvents`, `latePriceEvents`, and audit records. Do not equate injected delay count with the number of ticks excluded by Flink.

For automated output-count and latency comparisons, use `python3 benchmarks/matrix.py --qualities baseline,duplicates,late,mixed` from the repository root. That grid varies duplicate/backdated-input quality separately from dedup/late strategies. The dedicated emitter additionally exercises real delayed publication, malformed values, and same-time corrections.

Accelerated timestamps can run ahead of wall-clock time. They are intended for isolated test jobs because advancing watermarks can make subsequent real-time events late. They are not suitable for interpreting event-age latency. A fixed seed reproduces choices for a fixed timeline; operating-system scheduling and wall-clock timestamps mean two live runs are not byte-for-byte replays. Unit tests use an explicit timeline to verify reproducibility.

## Telemetry and shutdown

JSON statistics include generated/enqueued/acknowledged records and bytes, injected fault types, delayed queue depth, producer queue length, queue-full observations, errors, and undrained/unflushed counts. `/metrics` exports these counters plus a delivery-acknowledgement latency histogram with cumulative buckets. Queue metrics use no ticker labels. `/health` indicates the HTTP process is alive; it does not guarantee Kafka connectivity or Flink health.

SIGINT/SIGTERM stops generation, drains delayed records up to the configured timeout, commits any final input transaction, and flushes Kafka. It exits unsuccessfully if delivery failed or records remained undrained/unflushed. The delayed queue and producer queue are bounded; they are held in memory and are not crash-durable. Each transactional emitter instance needs a unique `transactional_id`; sharing it deliberately fences earlier producers. Retries are handled by the Kafka client, while final delivery failures remain visible in counters and exit status.

The container runs without root privileges. An optional `runtime.summary_path` or capture path must point to a writable mounted directory. Configure container memory/CPU limits separately when measuring the generator; the emitter cannot prove pipeline capacity by itself.

# Reproducible evaluation

## Workload and outputs

`benchmarks/matrix.py` uses a separate Compose project and unique topics, source groups, transaction prefixes, and ticker prefixes per run. It does not delete application data. The default grid covers three dedup policies, three late policies, parallelism one/two, offered rates 1000/10000 originals per second, and three repetitions. Experiment order is randomized by seed. Each run starts a fresh job and performs five seconds of warmup on disjoint symbols before measuring for thirty seconds.

The synthetic workload weights symbols by `1 / rank**skew`, spans FR/NL/ETR, sends a configurable fraction of exact duplicates, and backdates a configurable fraction of timestamps. The target rate counts original records; injected duplicates add traffic. Producer backpressure may lower the achieved rate, so always compare the recorded achieved rate and lag. Delayed-event injection does not guarantee a particular number of finalized-window exclusions: actual watermark progress and scheduling determine those counts.

Set `--skew 0` for uniform traffic and increase skew to expose hot-key limits. Increase `--symbols`, `--rates`, and `--parallelism` independently. Keep other parameters fixed when attributing a difference to one factor.

Use `--qualities baseline,duplicates,late,mixed` to cross the same strategy/resource grid with four input-quality conditions. Baseline disables both injections, duplicates enables only the configured duplicate rate, late enables only timestamp backdating, and mixed enables both. Each condition is labeled in the CSV and grouped separately in the report. Malformed values, real delayed publication, corrections, and tombstones have dedicated dummy-emitter presets and deterministic integration tests.

The driver consumes transactional output concurrently and checks the complete measured raw-price multiset against `NONE`, `EXACT`, or first-wins `TIMESTAMP` semantics. Price is varied per original record so exact duplicates can be distinguished from new events. Expected dedup identities remain valid for the full experiment: the matrix sets TTL longer than warmup, measurement, and drain. When running the driver directly, keep `DEDUP_TTL_MS` longer than the run.

Latency uses a uniform reservoir of at most 100000 observations. Counts and correctness retain all measured identities, so the load generator's memory grows with experiment size. Measure generator CPU/memory and use a separate host for serious capacity measurements. A saturated single producer/consumer or shared-host resources can become the bottleneck before Flink does. Clock synchronization is required for event-age comparisons across hosts; the driver's monotonic roundtrip timing does not need synchronized remote clocks.

Per-run files:

| Artifact | Contents |
|---|---|
| `RUN.json` | Settings, workload arguments, counts, correctness, latency, drain, and sampled Flink REST state |
| `RUN.driver.log` | Driver stdout/stderr, including failures |
| `RUN.containers.log` | Flink logs for diagnosis |
| `RUN.resources.jsonl` | Docker resource snapshot after the run |
| `RUN.prometheus.json` | Full Flink/Kafka time series over the run, when `--monitoring` is set |
| `SESSION-matrix.csv` | Comparison rows, written after every run |
| `SESSION-matrix.md` | Optional grouped report produced by `report.py` |

`committed_prices_per_second` divides observed raw prices by measurement start through final observed price, including drain. `input_records_per_second` divides enqueued measured records by measurement plus producer flush time. Neither includes EMA events in its numerator. The report shows medians across repetitions and throughput standard deviation; inspect raw runs before interpreting small differences.

## Correctness

The Java build tests schema validation, EMA initialization/recurrence, out-of-order closing prices, per-symbol isolation, empty-window behavior, late policies, dedup TTL, and checkpoint restoration of pending timers/EMA state. The deterministic integration verifier exercises real Kafka records, both EMA periods, duplicates, equal-time corrections, malformed input, and a post-finalization late tick. Run it under each policy combination when changing semantics.

The load benchmark validates price counts and output shape. It does not claim exact EMA oracle correctness for a concurrent, watermark-dependent arrival schedule. Use operator tests and `verify.py` for that; use the runtime counters and logs to characterize late decisions under load.

## Failure and recovery experiments

1. TaskManager failure: run a sustained workload, kill one worker with `docker compose kill -s SIGKILL taskmanager`, restart it with `docker compose up -d taskmanager`, and compare restart count, recovery duration, lag/drain, checkpoint restoration, and the driver's price multiset. Keep downtime under the Kafka transaction timeout.
2. Broker failure: start a fresh project with `compose.cluster.yaml`, stop one broker, and run the verifier/driver. With three replicas, two in-sync replicas can continue acknowledged writes. Record unavailable partitions, producer failures, and consumer lag. Two failed brokers cannot satisfy this policy.
3. Rescale: stop with a savepoint, restore at higher worker count, and verify the continuation against uninterrupted EMA results. Never resume only from group offsets.
4. JobManager loss: stop the manager, restore a completed checkpoint/savepoint explicitly, and distinguish this manual procedure from HA failover. An old savepoint taken before later committed output can cause replay of already published records; prefer coordinated stop-with-savepoint for planned changes.

The supplied local runner automates workload/policy/resource comparisons, not destructive chaos experiments. Keep failure injection isolated from application deployments. Benchmark claims must state image versions, CPU/memory limits, storage, broker topology, partition count, checkpoint interval, distribution/skew, warmup, repetitions, and generator placement. Five-second smoke runs validate the harness; they do not establish sustained capacity or full-week dataset performance.

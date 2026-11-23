# Reproducible evaluation

## Workload and outputs

`benchmarks/matrix.py` creates a fresh `ultrastonk-benchmark-<session>` Compose project per invocation and uses unique topics, source groups, transaction prefixes, and ticker prefixes per run. `--project` accepts an unused project name; the runner refuses pre-existing containers (including stopped ones), networks, volumes, or benchmark image tags for that project. Unless `--keep-running` is set, it removes only the benchmark project's containers, networks, volumes, and locally built images. For manual cleanup, run `docker compose -p PROJECT -f compose.yaml --profile benchmark down --volumes --rmi local` (also pass `-f deploy/compose.monitoring.yaml` if used), then `docker image rm ultrastonk-matrix-flink:PROJECT`. Results on the host are retained in `benchmarks/results/`.

The runner requires Docker Compose v2.24.4 or newer. Kafka is internal-only by default; use `--kafka-port` to publish it on an available host port. Docker assigns free localhost ports for Flink, Prometheus, and Grafana unless overridden with `--flink-port`, `--prometheus-port`, or `--grafana-port`. Use `docker compose -p PROJECT port jobmanager 8081` to find the assigned Flink port while the project is running.

The default grid covers three dedup policies, three late policies, parallelism one/two, offered rates 1000/10000 originals per second, and three repetitions. Experiment order is randomized by seed. Each run starts a fresh job and performs five seconds of warmup on disjoint symbols before measuring for thirty seconds.

The synthetic workload weights symbols by `1 / rank**skew`, spans FR/NL/ETR, sends a configurable fraction of exact duplicates, and backdates a configurable fraction of timestamps. The target rate counts original records; injected duplicates add traffic. Producer backpressure may lower the achieved rate, so always compare the recorded achieved rate and lag. Delayed-event injection does not guarantee a particular number of finalized-window exclusions: actual watermark progress and scheduling determine those counts.

Set `--skew 0` for uniform traffic and increase skew to expose hot-key limits. Increase `--symbols`, `--rates`, and `--parallelism` independently. Keep other parameters fixed when attributing a difference to one factor.

Repeat `--resource-profile NAME=SERVICE:CPU:MEMORY,...` to cross the matrix with container limits. Supported services are `taskmanager`, `jobmanager`, `kafka`, `gateway` (with `--gateway`), and the `benchmark` driver. Use `-` for an unset CPU or memory limit and `NAME=` for an uncapped comparison. A TaskManager limit applies to **each** worker; adding workers increases the total budget. To compare fixed total budgets, compare runs where worker count times per-worker quota is constant. Keep Flink's configured process size below the container cap for capacity tests; smaller caps can turn the run into a startup/OOM test instead.

```bash
python3 benchmarks/matrix.py --gateway --monitoring --duration 60 --parallelism 1,2 \
  --rates 1000,3000,6000 --dedup NONE --late DROP \
  --resource-profile 'half=taskmanager:0.5:2g,kafka:2:2g,gateway:1:512m' \
  --resource-profile 'one=taskmanager:1:2g,kafka:2:2g,gateway:1:512m'
```

Compare profiles and worker counts at the same offered rate and policy. Use the progress samples to distinguish producer shortfall from growth in expected-minus-committed prices or committed-minus-WebSocket prices. Check the resource time series for CPU throttling and memory pressure. Checkpoint batching can cause temporary backlog spikes, so repeat longer runs around the highest passing and first failing loads; a delivery ratio at one offered load is not a capacity estimate. Treat missing telemetry as inconclusive for bottleneck attribution.

`--monitoring` starts Prometheus, Kafka Exporter, and the resource exporter. Add `--dashboards` only when the live dashboards or centralized log search are useful; compare runs with the same instrumentation enabled.

Use `--qualities baseline,duplicates,late,mixed` to cross the same strategy/resource grid with four input-quality conditions. Baseline disables both injections, duplicates enables only the configured duplicate rate, late enables only timestamp backdating, and mixed enables both. Each condition is labeled in the CSV and grouped separately in the report. Malformed values, real delayed publication, corrections, and tombstones have dedicated dummy-emitter presets and deterministic integration tests.

The driver consumes transactional output concurrently and checks the complete measured raw-price multiset against `NONE`, `EXACT`, or first-wins `TIMESTAMP` semantics. Price is varied per original record so exact duplicates can be distinguished from new events. Expected dedup identities remain valid for the full experiment: the matrix sets TTL longer than warmup, measurement, and drain. When running the driver directly, keep `DEDUP_TTL_MS` longer than the run.

Latency uses a uniform reservoir of at most 100000 observations. Counts and correctness retain all measured identities, so the load generator's memory grows with experiment size. Measure generator CPU/memory and use a separate host for serious capacity measurements. A saturated single producer/consumer or shared-host resources can become the bottleneck before Flink does. Clock synchronization is required for event-age comparisons across hosts; the driver's monotonic roundtrip timing does not need synchronized remote clocks.

Add `--gateway` to measure the full synthetic producer → Kafka → Flink → Kafka → gateway → WebSocket path. Each run starts one gateway with a fresh consumer group. One in-network WebSocket client subscribes to every workload ticker before warmup, including the separate warmup symbols. Without `--gateway` the matrix still stops at Kafka.

The gateway mode reports raw-price delivery rate and latency from the driver's first enqueue attempt for that price identity to WebSocket receipt, using the same monotonic clock as Kafka visibility latency. Exact copies share the first attempt timestamp. Both boundaries include Kafka transaction visibility. The driver compares WebSocket price and EMA multisets against committed Kafka output observed during the run and fails on missing, extra, or invalid deliveries. It does not force the final windows to close or establish a browser-rendering or multi-client capacity result. For synthetic fan-out load, use the separate [gateway benchmark](../gateway/README.md#performance-results).

Per-run files:

| Artifact | Contents |
|---|---|
| `RUN.json` | Settings, workload arguments, counts, correctness, latency, drain, and time-aligned producer/Kafka/WebSocket progress and REST samples |
| `RUN.driver.log` | Driver stdout/stderr, including failures |
| `RUN.containers.log` | Flink logs and, in gateway mode, gateway logs |
| `RUN.resources.jsonl` | Docker resource snapshot after the run |
| `RUN.limits.json` | Requested profile and Docker-inspected CPU quotas, memory limits, and OOM state |
| `RUN.prometheus.json` | Flink, Kafka, and resource-exporter time series over the run, when `--monitoring` is set |
| `SESSION-manifest.json` | Project, arguments, runtime versions, and source/configuration hashes |
| `SESSION-matrix.csv` | Comparison rows, written after every run |
| `SESSION-matrix.md` | Optional grouped report produced by `report.py` |

`committed_prices_per_second` and `websocket_prices_per_second` divide observed raw prices by elapsed time from measurement start through the final observed price at each boundary, including drain. `input_records_per_second` divides enqueued measured records by measurement plus producer flush time. None includes EMA events in its numerator. The report only summarizes runs with a zero exit code and passing correctness checks. Failed runs still count in `Passes/runs`, and their measurements stay in the raw artifacts. A group with no passing measurements shows `n/a`.

## Correctness

The Java build tests schema validation, EMA initialization/recurrence, out-of-order closing prices, per-symbol isolation, empty-window behavior, late policies, dedup TTL, and checkpoint restoration of pending timers/EMA state. The deterministic integration verifier exercises real Kafka records, both EMA periods, duplicates, equal-time corrections, malformed input, a tombstone, and a post-finalization late tick. Run it under each policy combination when changing semantics, using a new project and empty topics each time.

The verifier advances event time. Before connecting, it requires `COMPOSE_PROJECT_NAME=ultrastonk-verify-<id>` with `KAFKA_INPUT_TOPIC=verify-<id>-in`, `KAFKA_OUTPUT_TOPIC=verify-<id>-out`, and `KAFKA_REJECTED_TOPIC=verify-<id>-audit` for the same unique ID. Follow the isolated verifier commands in the [README](../README.md#validate-and-benchmark). Do not run replay or a synthetic emitter in that project. Marker timestamps account for `WINDOW_SIZE_MS` (minimum 1000), `MAX_OUT_OF_ORDER_MS`, and `ALLOWED_LATENESS_MS` under `GRACE`; use the same values for the verifier and Flink.

The load benchmark validates price counts and output shape. It does not claim exact EMA oracle correctness for a concurrent, watermark-dependent arrival schedule. Use operator tests and `verify.py` for that; use the runtime counters and logs to characterize late decisions under load.

## Failure and recovery experiments

1. TaskManager failure: run a sustained workload, kill one worker with `docker compose kill -s SIGKILL taskmanager`, restart it with `docker compose up -d taskmanager`, and compare restart count, recovery duration, lag/drain, checkpoint restoration, and the driver's price multiset. Keep downtime under the Kafka transaction timeout.
2. Broker failure: start a fresh project with `-f compose.yaml -f deploy/compose.cluster.yaml`, stop one broker, and run the verifier/driver. With three replicas, two in-sync replicas can continue acknowledged writes. Record unavailable partitions, producer failures, and consumer lag. Two failed brokers cannot satisfy this policy.
3. Rescale: stop with a savepoint, restore at higher worker count, and verify the continuation against uninterrupted EMA results. Never resume only from group offsets.
4. JobManager loss: stop the manager, restore a completed checkpoint/savepoint explicitly, and distinguish this manual procedure from HA failover. An old savepoint taken before later committed output can cause replay of already published records; prefer coordinated stop-with-savepoint for planned changes.

The supplied local runner automates workload/policy/resource comparisons, not destructive chaos experiments. Keep failure injection isolated from application deployments. Benchmark claims must state image versions, CPU/memory limits, storage, broker topology, partition count, checkpoint interval, distribution/skew, warmup, repetitions, and generator placement. Five-second smoke runs validate the harness; they do not establish sustained capacity or full-week dataset performance.

## Capture-paced pilot

The opt-in `PACING=capture` mode paces Historical replay by the recorded system date/time while the payload keeps exchange event time. `benchmarks/capture_pilot.py` evaluates one isolated run at three boundaries in the same experiment: committed input, committed output, and one live WebSocket subscriber. It is a pilot gate, not a capacity run.

The pilot fails rather than passing when any required telemetry is absent (pacing report, capture mode, all three segment labels, clock-correlation samples, enqueue/ack samples, run settings, clock basis, effective limits, running job, watermark samples, progress, input/output/WebSocket records), when the measured segment's achieved pacing ratio is below `--min-pacing-ratio` (default 0.98), or when delivery is missing, extra or duplicated at ingest, committed Kafka output, or WebSocket. It reports those outcomes alongside the actual run settings, so an inconclusive run is visible instead of being summarized as a pass.

Identity is `(ticker, event timestamp, price)`. Identities repeated at the same timestamp are counted, not collapsed: a lost copy of a tied identity appears as a missing count, not as a duplicate. The report distinguishes `duplicate_identities` (the same identity offered more than once) from `tied_timestamps` (distinct identities sharing an event timestamp); window-finalization checks skip windows whose closing timestamp is ambiguous and count them as `tied_windows`.

Stages are reported separately: `historical_event_age_ms` is committed-observation wall time minus exchange event time; `watermark_wait_ms` is the deterministic eligibility lower bound (the enqueue of the first input record that can advance the watermark past the window timer) minus the last enqueue of that window; `transport_ms` is enqueue attempt to committed observation; window finalization is that lower bound to EMA pair receipt, and it requires the REST watermark samples to show the crossing; `pre_observation_count` reports receipts that arrived before the poll that observed the crossing, which is expected from polling resolution and is not treated as transport latency; `committed_to_websocket_ms` is committed observation to receipt. Container clocks on one Docker host share the kernel clock, so `clock.basis=same_host_wall` is the default; `--max-clock-error-ms` bounds accepted negative samples. Use `--unverified-clock` when clocks are not shared and treat that run as inconclusive for sub-millisecond claims.

Pre-roll, measured and trailing input are labeled by capture offset in the pacing report. Pre-roll warms EMA state, measured input supplies the timed sample, and trailing input is published only to advance watermarks and finalize windows; trailing rows are not part of the pacing sample. Accelerating with `SPEED_MULTIPLIER` shortens wall time but not exchange event time, so window finalization still requires enough event-time input. A pilot with no eligible finalized window fails the window check; do not convert that into a passing result by relaxing the window duration after the fact.

The pilot must subscribe before the emitter publishes (`--tickers`), because a subscription started later cannot receive earlier WebSocket frames. Effective limits are read from the JSON path passed with `--limits`; produce it from `docker inspect` as in the README. Record the pilot command, slice, seed-free CSV order, pacing report, limits and `PILOT.json` together. A passing pilot does not prove peak capacity or full-week capture-order correctness.

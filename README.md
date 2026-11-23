# Ultrastonk event processing

Containerized Kafka messaging, a Java 17 / Apache Flink 1.20.3 DataStream job, and a configurable dummy event emitter for testing. The production data emitter, gateway, frontend, crossover advisories, and report are outside this implementation. Synthetic producers/consumers under `benchmarks/` are test tools.

```text
emitter -> price-events -> Flink validation -> keyed price/EMA processing -> events-processed -> gateway
                                  |                     |
                                  +---- rejected/late --+--> price-events-rejected

Flink / Kafka metrics -> Prometheus -> Grafana
Container stdout -> Alloy -> Loki -> Grafana
```

## Start

Requires Docker with Compose v2+ and roughly 6 GB of available memory for the base stack. Monitoring and three Kafka brokers require additional resources.

```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f jobmanager taskmanager
```

The image build runs the Java correctness tests. Two TaskManagers, each with one slot, run a job with parallelism two. Kafka uses KRaft, six partitions per topic, durable volumes, and explicit topic initialization. Flink starts automatically after topic initialization. [Flink dashboard](http://localhost:8081).

With metrics, dashboards, alerts, and searchable logs:

```bash
docker compose -f compose.yaml -f compose.monitoring.yaml up -d --build
```

[Grafana dashboard](http://localhost:3000/d/ultrastonk) (anonymous viewing; local administrator `admin` / `ultrastonk-local`), [Prometheus](http://localhost:9090). All published ports bind to loopback. Set `GRAFANA_ADMIN_PASSWORD` before the first start to change the administrator password. Alloy reads the Docker socket to collect this Compose project's logs.

```bash
docker compose -f compose.yaml -f compose.monitoring.yaml down
```

Volumes remain after `down`. Use the same overlays when operating an existing deployment.

## Emitter and gateway contracts

| Setting | Container network | Host |
|---|---|---|
| Kafka bootstrap server | `kafka:9092` | `localhost:9092` |
| Emitter topic | `price-events` | same |
| Gateway topic | `events-processed` | same |
| Audit topic | `price-events-rejected` | same |

Emitter: one UTF-8 JSON object per Kafka value; use the ticker as the UTF-8 record key. Set producer `enable.idempotence=true` and `acks=all`. A ticker must consistently map to one input partition to preserve its source arrival order. Flink also keys its managed state by ticker.

```json
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","sec_type":"E","price":28.5}
```

`timestamp_emit` is a nonnegative integer of Unix epoch **milliseconds**. `sec_type` is a nonempty string; the pipeline processes all provided security types. Price must be finite and nonnegative. Malformed records are quarantined with `{"raw":"...","reason":"..."}` and processing continues.

The gateway must use `isolation.level=read_committed` and consume values such as:

```json
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","type":"price","value":28.5}
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","type":"EMA38","value":1.4615384615384615}
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","type":"EMA100","value":0.5643564356435644}
```

The EMA100 name and period follow your clarification. There are exactly four fields in each output value. Raw prices retain their input timestamp; EMA records retain the closing tick's timestamp. `sec_type` is not included in processed events. Internal instrumentation fields are excluded from JSON.

Each accepted price produces a `price` event. Each completed, nonempty window produces an EMA38/EMA100 pair. Both EMA states start at zero:

```text
alpha = 2 / (period + 1)
EMA = alpha * window_closing_price + (1 - alpha) * previous_EMA
```

The closing price is the tick with the greatest event timestamp within the window; equal timestamps use the last accepted arrival. Empty windows do not advance the EMA. Five-minute windows align to epoch boundaries, which also align with midnight CEST at this duration. Changing the duration changes the window alignment rules accordingly.

Event time comes from `timestamp_emit`. If that field denotes replay wall-clock emission time, windows describe replay time; calculating windows over the original trading timeline requires the emitter to supply that timeline in this field or an explicit future schema extension. Historical event timestamps also make event-age latency include historical age.

Watermarks allow five seconds of out-of-order data by default. Timestamp assignment occurs after validation, so idleness is tracked per source task, not per Kafka partition. An idle source task stops holding back other tasks after 30 seconds. Windows finalize when the watermark crosses their timer; a completely idle/unbounded input does **not** flush its final window. A later event must advance event time. No updates are generated merely because wall-clock time passes.

## Policies

See [policy design and comparison](docs/policies.md). Configure with `.env` and recreate/restore the job as described below.

| Setting | Default | Options / meaning |
|---|---|---|
| `DEDUP_STRATEGY` | `NONE` | `NONE`, `EXACT`, `TIMESTAMP` |
| `DEDUP_TTL_MS` | `3600000` | Processing-time retention for dedup identities |
| `LATE_EVENT_STRATEGY` | `DROP` | `DROP`, `SIDE_OUTPUT`, `GRACE` |
| `ALLOWED_LATENESS_MS` | `30000` | Additional finalization delay under `GRACE` |
| `WINDOW_SIZE_MS` | `300000` | Window duration, maximum one day |
| `MAX_OUT_OF_ORDER_MS` | `5000` | Watermark delay |
| `SOURCE_IDLE_TIMEOUT_MS` | `30000` | Source task idleness threshold |
| `CHECKPOINT_INTERVAL_MS` | `10000` | Transaction commit cadence |
| `LOG_INTERVAL_MS` | `10000` | Per-task statistics log interval |

All late policies preserve accepted raw prices. `DROP` excludes finalized-window ticks from EMA computation. `SIDE_OUTPUT` also audits those ticks. `GRACE` delays finalization for every window, accepts delayed ticks during that interval, and audits ticks arriving beyond it. These strategies emit each window once, avoiding revisions that the requested output schema cannot identify.

## Scale and distribute

More Flink workers increase compute parallelism across tickers. A single hot ticker remains sequential. Maximum parallelism is fixed at 128; source parallelism is additionally limited by Kafka partitions. The default six partitions allow source scaling up to six tasks. Provision more partitions for larger experiments; changing existing partition counts can change ticker routing, so coordinate producers and drain/restore the job.

For a **fresh** deployment with three Kafka brokers/controllers, replication factor three and minimum in-sync replicas two:

```bash
docker compose -p ultrastonk-cluster -f compose.yaml -f compose.cluster.yaml -f compose.monitoring.yaml up -d --build
```

Use a separate project instead of switching a running single-broker data directory to the cluster overlay. Stop an existing default stack first if using the same published ports. Host clients bootstrap `localhost:9092,localhost:9094,localhost:9096`; containers use `kafka:9092,kafka-2:9092,kafka-3:9092`. Existing topics are not automatically repartitioned or assigned a higher replication factor.

The containers can also run on a distributed Flink deployment. Set reachable Kafka addresses and replace local checkpoint/savepoint paths with a shared filesystem or supported object-store URI. The Compose named state volume only shares storage on **one Docker host**; it is not a distributed filesystem. S3 deployment additionally needs Flink's S3 filesystem plugin enabled on all workers and the JobManager. For multiple hosts, configure reachable advertised broker/controller addresses and durable storage for each broker. The local overlays do not provide orchestration across hosts or JobManager high availability.

## Recovery and rescaling

Managed RocksDB state includes window closes, pending timers, EMA values, and optional dedup identities. Flink checkpoints include Kafka source offsets and transactional sinks. TaskManager failure restarts tasks from the latest completed checkpoint. Transaction timeout is 15 minutes; restart time must remain within it.

JobManager restart has no HA metadata service in this setup. Restore a retained checkpoint/savepoint explicitly; do not treat a fresh job with committed Kafka group offsets as a complete recovery. Without a restore path, the job intentionally replays retained input from the earliest offset to rebuild indicators, and may republish previously committed output. If input retention has deleted earlier data, a cold replay cannot recreate the full EMA history.

To change parallelism while preserving state:

```bash
# Find the running job ID.
docker compose exec jobmanager /opt/flink/bin/flink list -r
# Stop consistently and record the returned savepoint path.
docker compose exec jobmanager /opt/flink/bin/flink stop --savepointPath file:///opt/flink/state/savepoints JOB_ID
docker compose stop jobmanager taskmanager
# Set FLINK_RESTORE_PATH to that path and FLINK_PARALLELISM=4 in .env.
docker compose up -d --scale taskmanager=4
```

Keep stable operator UIDs, window duration, dedup policy, and late policy when restoring production state. Policy comparisons use fresh topics and state. Every concurrently running job on the same Kafka cluster needs its own `KAFKA_TRANSACTION_PREFIX`; otherwise transactional producers can fence each other.

Exactly-once applies to transactional Kafka output when recovering the coordinated job state. It does not identify separately produced business duplicates; the supplied input schema has no unique event ID. Consumer visibility waits for checkpoints, even though prices are forwarded through Flink immediately.

## Validate and benchmark

```bash
# Build includes Java operator/state/contract tests.
docker compose build jobmanager
docker compose --profile benchmark build benchmark
# Run deterministic integration checks against an isolated TEST stack.
docker compose run --rm --no-deps --entrypoint python benchmark verify.py
# Run a workload against the active stack; results are saved under benchmarks/results/.
docker compose run --rm --no-deps benchmark --duration 30 --rate 10000
```

The default integration check can wait up to 90 seconds for watermark idleness and checkpoint visibility. For faster checks, use `SOURCE_IDLE_TIMEOUT_MS=1000` and `CHECKPOINT_INTERVAL_MS=1000` in an isolated deployment.

The verifier advances synthetic event time above the job's current watermarks. Run it on isolated test topics/jobs; it is not a health probe for a live real-time feed.

The matrix runner creates an isolated project, fresh topics, consumer groups, transaction prefixes, and a new job for every run. It randomizes experiment order, performs warmup, injects duplicates/delays, varies worker counts and offered load, checks the raw-price multiset against the selected dedup policy, and exports JSON, CSV, container logs, resource snapshots, and optional raw Prometheus time series.

```bash
# Nine policy combinations, one small run each.
python3 benchmarks/matrix.py --duration 5 --warmup 1 --rates 1000 --parallelism 2 --repetitions 1 --monitoring
# Full repeated comparison over the default load/resource grid.
python3 benchmarks/matrix.py --monitoring
python3 benchmarks/report.py benchmarks/results/SESSION-matrix.csv
```

See [benchmark methodology](docs/benchmarking.md) and [telemetry reference](docs/telemetry.md) for interpretation, limitations, and failure experiments.

## Dummy emitter

```bash
docker compose --profile emitter build dummy-emitter
docker compose run --rm --no-deps dummy-emitter --config /config/clean.json --duration 30 --rate 5000
docker compose run --rm --no-deps dummy-emitter --config /config/chaos.json
# Keep the emitter service running and scrape its /metrics endpoint.
docker compose --profile emitter -f compose.yaml -f compose.monitoring.yaml up -d dummy-emitter
# Dry run requires only Python's standard library.
python3 emitter/dummy_emitter.py --config emitter/configs/trends.json --dry-run --max-events 20
```

See [emitter configuration and scenarios](emitter/README.md) for all options, rate stages, distributions, price models, fault injection, reproducibility, and telemetry. Use a fresh test deployment for accelerated event time or malformed-data scenarios.

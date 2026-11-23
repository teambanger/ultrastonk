# Ultrastonk event processing

Containerized Kafka messaging, a Java CSV replay emitter, a Java 17 / Apache Flink 1.20.3 DataStream job, and a WebSocket gateway with a browser test page. The dummy emitter and the synthetic producers/consumers under `benchmarks/` are test tools.

```text
emitter -> price-events -> Flink validation -> keyed price/EMA processing -> events-processed -> gateway -> browsers
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

The Flink image build runs the Java correctness tests. Two TaskManagers, each with one slot, run a job with parallelism two. Kafka uses KRaft, six partitions per topic, durable volumes, and explicit topic initialization. Flink starts automatically after topic initialization. [Flink dashboard](http://localhost:8081).

Open the [gateway test page](http://localhost:8080) and subscribe to a ticker such as `RDSA.NL`. The gateway reads committed records from `KAFKA_OUTPUT_TOPIC` and sends recent in-memory history followed by live events for the selected ticker. `/health` reports the gateway and its Kafka source status. Set `GATEWAY_PORT` to change the loopback port. See the [gateway README](gateway/README.md) for WebSocket messages and tests.

**Replay real data.** Put the DEBS CSV under `data/src/` (or run `bash scripts/download_src_data.sh` for the default trading day), then start the stack with the real emitter:

```bash
docker compose --profile emitter up -d --build
docker compose logs -f event-emitter
```

The replay exits at the end of the input. Set `CSV_PATH` to a path inside `/data` (or a comma-separated list in replay order), `SPEED_MULTIPLIER` to accelerate replay, or `MAX_SLEEP_MS` to cap long gaps. `REPLAY_DATA_DIR` selects the read-only host directory mounted at `/data`. The base stack without an emitter profile is suitable for isolated synthetic verification and benchmarks. Do not mix replay and synthetic workloads on the same input topic: synthetic timestamps can advance the watermark past historical events.

**Finite replay with final EMAs.** Use a fresh Compose project and include the replay overlay. Flink waits for the emitter to finish, then reads Kafka to the captured end offsets and finalizes the remaining windows. Results are not streamed while the CSV is being published. Use a fresh project/topics for each run, and leave Kafka running long enough to consume the results.

```bash
replay_project="ultrastonk-replay-$(date +%s)"
KAFKA_PORT=19092 FLINK_UI_PORT=18081 GATEWAY_PORT=18080 \
  docker compose -p "$replay_project" -f compose.yaml -f deploy/compose.replay.yaml \
  --profile emitter up -d --build
```

With metrics, dashboards, and searchable logs:

```bash
docker compose -f compose.yaml -f deploy/compose.monitoring.yaml up -d --build
```

[Grafana dashboard](http://localhost:3000/d/ultrastonk) (anonymous viewing; local administrator `admin` / `ultrastonk-local`), [Prometheus](http://localhost:9090). All published ports bind to loopback. Set `GRAFANA_ADMIN_PASSWORD` before the first start to change the administrator password. Alloy reads the Docker socket to collect this Compose project's logs.

```bash
docker compose -f compose.yaml -f deploy/compose.monitoring.yaml down
```

Volumes remain after `down`. Use the same overlays when operating an existing deployment.

**Repository layout**
- `event-emitter/`: Java CSV replay producer.
- `flink/`: processing job and Java tests.
- `gateway/`: Kafka consumer, WebSocket subscriptions, and browser test page.
- `benchmarks/`: verification, load tests, and the optional `synthetic-emitter/`.
- `deploy/`: optional Compose overlays; `monitoring/`: dashboards and observability configuration.
- `scripts/`: dataset download; `docs/`: benchmark, validation, and policy guides.

For branch provenance, module entry points, and integration checks, see the [module integration reference](docs/module-integration.md).

## Kafka contracts

| Setting | Container network | Host |
|---|---|---|
| Kafka bootstrap server | `kafka:9092` | `localhost:9092` |
| Emitter topic | `price-events` | same |
| Processed output topic | `events-processed` | same |
| Audit topic | `price-events-rejected` | same |

The CSV emitter publishes one raw JSON object per Kafka value, keyed by instrument ID, with `enable.idempotence=true` and `acks=all`. Flink keys its managed state by the same ticker. Example replay value:

```json
{"eventTimestampMs":1636351200021,"emitTimestampMs":1792152000100,"id":"RDSA.NL","secType":"E","last":28.5,"bid":28.49}
```

Flink uses the original `eventTimestampMs` and the `last` trade price. Quote and housekeeping rows without a `last` price, and rows with `last: 0`, are skipped without being rejected. Replay keeps the historical timestamp in the payload and uses a current Kafka record timestamp so normal topic retention does not immediately expire historical data.

Synthetic producers can still send the normalized test format:

```json
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","sec_type":"E","price":28.5}
```

`timestamp_emit` is a nonnegative integer of Unix epoch **milliseconds**. `sec_type` is a nonempty string; the pipeline processes all provided security types. Price must be finite and nonnegative. Malformed records are quarantined with `{"raw":"...","reason":"..."}` and processing continues. Oversized raw values are shortened and tagged with `"truncated":true`.

Output consumers must use `isolation.level=read_committed` and accept values such as:

```json
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","type":"price","value":28.5}
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","type":"EMA38","value":1.4615384615384615}
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","type":"EMA100","value":0.5643564356435644}
```

There are exactly four fields in each output value. Raw prices retain their input timestamp; EMA records retain the closing tick's timestamp. `sec_type` is not included in processed events. Internal instrumentation fields are excluded from JSON.

Each accepted price produces a `price` event. Each completed, nonempty window produces an EMA38/EMA100 pair. Both EMA states start at zero:

```text
alpha = 2 / (period + 1)
EMA = alpha * window_closing_price + (1 - alpha) * previous_EMA
```

The closing price is the tick with the greatest event timestamp within the window; equal timestamps use the last accepted arrival. Empty windows do not advance the EMA. Five-minute windows align to epoch boundaries, which also align with midnight CEST at this duration. Changing the duration changes the window alignment rules accordingly.

Event time comes from `eventTimestampMs` for replay records and `timestamp_emit` for normalized inputs. The output field `timestamp_emit` retains that event time. For historical replay, event-age metrics include the historical age and do not measure replay transport latency.

Watermarks allow five seconds of out-of-order data by default and track progress per Kafka partition. Valid quote and housekeeping timestamps advance that progress without producing prices. In continuous mode, a partition with no records stops holding back the watermark after 30 seconds; an idle feed still does not flush its final window. Finite replay disables idle exclusion and flushes remaining windows when the source reaches its captured end offsets.

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
| `SOURCE_IDLE_TIMEOUT_MS` | `30000` | Kafka partition idleness threshold in continuous mode |
| `CHECKPOINT_INTERVAL_MS` | `10000` | Transaction commit cadence |
| `LOG_INTERVAL_MS` | `10000` | Per-task statistics log interval |

All late policies preserve accepted raw prices. `DROP` excludes finalized-window ticks from EMA computation. `SIDE_OUTPUT` also audits those ticks. `GRACE` delays finalization for every window, accepts delayed ticks during that interval, and audits ticks arriving beyond it. These strategies emit each window once, avoiding revisions that the requested output schema cannot identify.

## Scale and distribute

More Flink workers increase compute parallelism across tickers. A single hot ticker remains sequential. Maximum parallelism is fixed at 128; source parallelism is additionally limited by Kafka partitions. The default six partitions allow source scaling up to six tasks. Provision more partitions for larger experiments; changing existing partition counts can change ticker routing, so coordinate producers and drain/restore the job.

For a **fresh** deployment with three Kafka brokers/controllers, replication factor three and minimum in-sync replicas two:

```bash
docker compose -p ultrastonk-cluster -f compose.yaml -f deploy/compose.cluster.yaml -f deploy/compose.monitoring.yaml up -d --build
```

Use a separate project instead of switching a running single-broker data directory to the cluster overlay. Stop an existing default stack first if using the same published ports. Host clients bootstrap `localhost:9092,localhost:9094,localhost:9096`; containers use `kafka:9092,kafka-2:9092,kafka-3:9092`. Existing topics are not automatically repartitioned or assigned a higher replication factor.

The containers can also run on a distributed Flink deployment. Set reachable Kafka addresses and replace local checkpoint/savepoint paths with a shared filesystem or supported object-store URI. The Compose named state volume only shares storage on **one Docker host**; it is not a distributed filesystem. S3 deployment additionally needs Flink's S3 filesystem plugin enabled on all workers and the JobManager. For multiple hosts, configure reachable advertised broker/controller addresses and durable storage for each broker. The local overlays do not provide orchestration across hosts or JobManager high availability.

## Recovery and rescaling

Managed RocksDB state includes window closes, pending timers, EMA values, and optional dedup identities. Flink checkpoints include Kafka source offsets and transactional sinks. TaskManager failure restarts tasks from the latest completed checkpoint. Transaction timeout is 15 minutes; restart time must remain within it.

JobManager restart has no HA metadata service in this setup, so it is not restarted automatically. Restore a retained checkpoint/savepoint explicitly; do not treat a fresh job with committed Kafka group offsets as a complete recovery. Without a restore path, the job intentionally replays retained input from the earliest offset to rebuild indicators, and may republish previously committed output. If input retention has deleted earlier data, a cold replay cannot recreate the full EMA history.

To change parallelism while preserving state:

```bash
# Find the running job ID.
docker compose exec jobmanager /opt/flink/bin/flink list -r
# Stop consistently and record the returned savepoint path.
docker compose exec jobmanager /opt/flink/bin/flink stop --savepointPath file:///opt/flink/state/savepoints JOB_ID
docker compose stop jobmanager taskmanager
# Set FLINK_RESTORE_PATH to that path and FLINK_PARALLELISM=4 in .env.
# If using monitoring, also set EXPECTED_TASKMANAGERS=4 in .env and recreate resource-exporter.
docker compose up -d --scale taskmanager=4
```

Keep stable operator UIDs, window duration, dedup policy, and late policy when restoring production state. Policy comparisons use fresh topics and state. Every concurrently running job on the same Kafka cluster needs its own `KAFKA_TRANSACTION_PREFIX`; otherwise transactional producers can fence each other.

The switch to source-level watermarks changed the job topology. Restore from a checkpoint made by the older topology was not validated; test that upgrade against an isolated deployment before relying on it.

Exactly-once applies to transactional Kafka output when recovering the coordinated job state. It does not identify separately produced business duplicates; the supplied input schema has no unique event ID. Consumer visibility waits for checkpoints, even though prices are forwarded through Flink immediately.

## Validate and benchmark

```bash
# Build includes Java operator/state/contract tests.
docker compose build jobmanager
docker compose --profile benchmark build benchmark
```

The verifier advances synthetic event time and must not run on a live or replay feed. Run it from the repository root in a fresh project with its own Kafka topics:

```bash
(
  set -e
  id=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')
  export COMPOSE_PROJECT_NAME="ultrastonk-verify-$id"
  export KAFKA_INPUT_TOPIC="verify-$id-in" KAFKA_OUTPUT_TOPIC="verify-$id-out" KAFKA_REJECTED_TOPIC="verify-$id-audit"
  export KAFKA_PORT=29092 FLINK_UI_PORT=28081 SOURCE_IDLE_TIMEOUT_MS=1000 CHECKPOINT_INTERVAL_MS=1000
  export FLINK_RESTORE_PATH= FLINK_BOUNDED_REPLAY=false KAFKA_BOOTSTRAP_SERVERS=kafka:9092
  trap 'docker compose down --volumes' EXIT
  docker compose up -d --build kafka kafka-init jobmanager taskmanager
  docker compose --profile benchmark build benchmark
  docker compose run --rm --no-deps --entrypoint python benchmark verify.py
)
```

The matrix runner creates an isolated project and uses fresh topics, consumer groups, transaction prefixes, and a new job for each run. It randomizes experiment order, performs warmup, injects duplicates/delays, varies worker counts and offered load, checks the raw-price multiset against the selected dedup policy, and exports JSON, CSV, container logs, resource snapshots, and optional raw Prometheus time series. Run the load driver directly only against a disposable synthetic stack, not against a live or replay feed.

```bash
# Nine policy combinations, one small run each.
python3 benchmarks/matrix.py --duration 5 --warmup 1 --rates 1000 --parallelism 2 --repetitions 1 --monitoring
# Include one gateway and measure delivery to a WebSocket subscriber.
python3 benchmarks/matrix.py --gateway --duration 5 --warmup 1 --rates 1000 --parallelism 2 --dedup NONE --late DROP --repetitions 1
# Full repeated comparison over the default load/resource grid.
python3 benchmarks/matrix.py --monitoring
python3 benchmarks/report.py benchmarks/results/SESSION-matrix.csv
```

See [benchmark methodology](docs/benchmarking.md) and [telemetry reference](docs/telemetry.md) for interpretation, limitations, and failure experiments.

## Dummy emitter

```bash
docker compose --profile synthetic build dummy-emitter
docker compose run --rm --no-deps dummy-emitter --config /config/clean.json --duration 30 --rate 5000
docker compose run --rm --no-deps dummy-emitter --config /config/chaos.json
# Keep the emitter service running and scrape its /metrics endpoint.
docker compose --profile synthetic -f compose.yaml -f deploy/compose.monitoring.yaml up -d dummy-emitter
# Dry run requires only Python's standard library.
python3 benchmarks/synthetic-emitter/dummy_emitter.py --config benchmarks/synthetic-emitter/configs/trends.json --dry-run --max-events 20
```

See [synthetic emitter configuration and scenarios](benchmarks/synthetic-emitter/README.md) for all options, rate stages, distributions, price models, fault injection, reproducibility, and telemetry. Use a fresh test deployment for accelerated event time or malformed-data scenarios.

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

The Flink image build runs the Java correctness tests; the event-emitter image build runs its pacing and CSV tests. Two TaskManagers, each with one slot, run a job with parallelism two. Kafka uses KRaft, six partitions per topic, durable volumes, and explicit topic initialization. Flink starts automatically after topic initialization. [Flink dashboard](http://localhost:8081).

Open the [gateway test page](http://localhost:8080) and subscribe to a ticker such as `RDSA.NL`. The gateway reads committed records from `KAFKA_OUTPUT_TOPIC` and sends recent in-memory history followed by live events for the selected ticker. **The base stack does not start an emitter.** On a fresh stack the page stays empty until a producer sends trades and Flink commits its output. Set `GATEWAY_PORT` to change the loopback port. See the [gateway README](gateway/README.md) for WebSocket messages and tests.

**Replay real data.** Put the DEBS CSV under `data/src/` (or run `bash scripts/download_src_data.sh` for the default trading day), then start the stack with the real emitter:

```bash
docker compose --profile emitter up -d --build
docker compose logs -f event-emitter
```

The replay exits at the end of the input. Set `CSV_PATH` to a path inside `/data` (or a comma-separated list in replay order), `SPEED_MULTIPLIER` to accelerate replay, or `MAX_SLEEP_MS` to cap long gaps. `REPLAY_DATA_DIR` selects the read-only host directory mounted at `/data`. The base stack without an emitter profile is suitable for isolated synthetic verification and benchmarks. Do not mix replay and synthetic workloads on the same input topic: synthetic timestamps can advance the watermark past historical events.

**See replay output sooner.** The default CSV starts with non-trade updates around 01:01 and does not contain a trade until around 07:00. At the default 1x speed with no gap cap, that is nearly six hours before the first price. For a local demo with the default CSV, start replay like this instead (this deliberately changes pacing; do not use it for a 1x measurement):

```bash
SPEED_MULTIPLIER=100 MAX_SLEEP_MS=20 docker compose --profile emitter up -d --build
curl -fsS http://localhost:8080/health
```

Check `totals.in` again after the first trade and a successful Flink checkpoint (10-second cadence by default). `source.status: running` only means the gateway consumer is running, not that it has received output. If `totals.in` is increasing but the page is empty, subscribe to a ticker in the replay, such as `INCAS.FR` for the default CSV. Prices do not wait for an event-time window to close; EMAs and BUY/SELL advisories do. To stop the replay while leaving the stack up, run `docker compose --profile emitter stop event-emitter`.

**Capture-paced replay (opt-in).** By default the emitter paces by exchange event time and the payload, Kafka record and query all share that clock. Set `PACING=capture` to pace by the recorded system "Date"/"Time" columns (2/3) instead, so a replay reproduces the publication order and gaps of a live capture. The payload still carries the exchange `eventTimestampMs`; Flink keeps processing event time, and the four-field output contract is unchanged.

| Setting | Default | Meaning |
|---|---|---|
| `PACING` | `event` | `event` (unchanged) or `capture` |
| `MISSING_CAPTURE_TIME` | `event` | Row without readable system date/time: pace by `event` time, `no-wait` (hold the previous capture position), or `fail` (abort the measured run) |
| `REPLAY_PREROLL_MS` | `0` | Capture offset before the measured interval; rows here warm state and are excluded from the measured segment |
| `REPLAY_MEASURE_MS` | `0` | Measured capture interval after pre-roll; `0` measures the remainder |
| `PACING_REPORT_PATH` | unset | Write the pacing report JSON (segment counts, scheduled versus actual pacing, clock correlation, enqueue/ack samples) |
| `PACING_SAMPLE_LIMIT` | `1000` | Bound on retained per-row report samples |

In capture mode the payload gains `captureTimestampMs` and `sourceOrdinal` so the pilot can correlate the same event across boundaries; query event time is unchanged. Rows keep recorded order. Out-of-order capture times are emitted immediately, and `MAX_SLEEP_MS` caps and re-bases long gaps exactly as the event clock does. Segment labels are assigned by capture offset: pre-roll, measured, and trailing input. Trailing input is still published (and advances watermarks) but is not part of the measured interval.

```bash
REPLAY_DATA_DIR=./benchmarks/fixtures CSV_PATH=/data/capture-pilot.csv \
  PACING=capture MISSING_CAPTURE_TIME=fail REPLAY_PREROLL_MS=10000 REPLAY_MEASURE_MS=60000 \
  PACING_REPORT_PATH=/results/pilot-pacing.json SPEED_MULTIPLIER=20 \
  docker compose --profile emitter run --rm --user "$(id -u):$(id -g)" event-emitter
```

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
{"timestamp_emit":1792152000100,"ticker":"RDSA.NL","type":"BUY","value":28.5}
```

There are exactly four fields in each output value. Raw prices retain their input timestamp; EMA and advisory records retain the closing tick's timestamp. Advisory values are the closing price. `sec_type` is not included in processed events. Internal instrumentation fields are excluded from JSON.

Each accepted price produces a `price` event. Each completed, nonempty window produces an EMA38/EMA100 pair. Both EMA states start at zero:

```text
alpha = 2 / (period + 1)
EMA = alpha * window_closing_price + (1 - alpha) * previous_EMA
```

A window also emits at most one advisory: BUY when the previous EMA38-minus-EMA100 difference was nonpositive and the new difference is positive, or SELL when the previous difference was nonnegative and the new difference is negative. Remaining on the same side or ending at equality emits no advisory.

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

For a live advisory and subscription-isolation demo, use the same isolated
command above with these changes: also export `WINDOW_SIZE_MS=1000`,
`MAX_OUT_OF_ORDER_MS=0`, and `GATEWAY_PORT=28080`; include `gateway` in the
`docker compose up` services; and run `verify.py --gateway-url ws://gateway:8080`.
The verifier checks the hand-checked 33-window sequence: BUY at window 0,
SELL at window 30, and BUY at window 32, with no intervening advisories. It
compares committed Kafka records, three live WebSocket subscriptions, and
history separately. The shortened windows are a deterministic test fixture,
not a historical latency measurement. The browser at `http://localhost:28080`
must use a window size of `1000` for this demo.
To retain the expected and read-committed actual records, append
`--evidence /results/live-advisory-comparison.json` to the verifier command;
the file appears in `benchmarks/results/`. Its empty `missing` and `extra`
arrays apply to this fixture, not to arbitrary historical input.
For a browser that subscribes before publication, `--run-id evidence01` fixes
the crossover ticker to `Vevidence01C.FR`; `--wait-for-file` holds publication
until the specified file appears (up to 120 seconds). Use a fresh shared mount
for this gate and remove any old gate file before starting.

**Capture-paced pilot.** `benchmarks/capture_pilot.py` proves that a small live Historical replay measures the publication-to-subscriber path, not historical event age. It consumes committed input, committed output, and one live WebSocket subscriber from the same run, correlates the same event identity at each boundary, checks the emitter's scheduled-versus-achieved pacing, and fails on missing telemetry, insufficient pacing, or missing/duplicate delivery. It reports enqueue-to-ack, enqueue-to-committed, committed-to-WebSocket, window-finalization, historical event age, and watermark waiting separately, and records actual run settings and effective limits.

Use a fresh project and disposable topics. Start the pilot before the emitter so the subscriber is registered before publication (for example with the isolated command above plus `--profile emitter`, `PACING=capture`, and a small `CSV_PATH` slice):

```bash
pilot_project="ultrastonk-pilot-$(date +%s)"
id=${pilot_project#ultrastonk-pilot-}
export COMPOSE_PROJECT_NAME=$pilot_project
export KAFKA_INPUT_TOPIC=pilot-$id-in KAFKA_OUTPUT_TOPIC=pilot-$id-out KAFKA_REJECTED_TOPIC=pilot-$id-audit
export KAFKA_PORT=39092 FLINK_UI_PORT=38081 GATEWAY_PORT=38080 PROMETHEUS_PORT=0
export FLINK_BOUNDED_REPLAY=false WINDOW_SIZE_MS=60000 MAX_OUT_OF_ORDER_MS=5000 CHECKPOINT_INTERVAL_MS=1000
export SOURCE_IDLE_TIMEOUT_MS=1000
docker compose up -d --build kafka kafka-init jobmanager taskmanager gateway
docker compose ps -q | xargs docker inspect > benchmarks/results/pilot-limits.json
docker compose --profile benchmark build benchmark
docker compose --profile benchmark run -d --name pilot-client --no-deps --entrypoint python benchmark \
  capture_pilot.py --run-id pilot --tickers RDSA.NL,A2ASZ7.ETR --pacing-report /results/pilot-pacing.json \
  --limits /results/pilot-limits.json
REPLAY_DATA_DIR=./benchmarks/fixtures CSV_PATH=/data/capture-pilot.csv \
  PACING=capture MISSING_CAPTURE_TIME=fail REPLAY_PREROLL_MS=10000 REPLAY_MEASURE_MS=60000 \
  PACING_REPORT_PATH=/results/pilot-pacing.json SPEED_MULTIPLIER=20 \
  docker compose --profile emitter run --rm --user "$(id -u):$(id -g)" event-emitter
pilot_exit=$(docker wait pilot-client)
docker logs pilot-client
docker rm pilot-client
```

The emitter runs as an unprivileged user, so `--user` lets it write the report into the host-owned results directory. The pilot writes `benchmarks/results/pilot-capture-pilot.json` and exits nonzero (`pilot_exit`) unless every check passes. `--tickers` must list the measured symbols before publication; a replay that starts first cannot be subscribed retroactively. Add `--allow-missing-limits` only for a rehearsal run, and only claim a passing pilot when limits are present. Keep the measured slice short, use a fresh project, and do not run it against live or shared topics.

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

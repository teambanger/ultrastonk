# Telemetry and operational interpretation

Prometheus scrapes every five seconds. Docker DNS discovery finds all TaskManager replicas, including new IPs after restart. Grafana provisions the dashboard and both data sources automatically. Loki retains fourteen days of container logs; Prometheus retains fourteen days of metrics.

## Application instruments

Custom operator metrics use Flink's `pipeline` metric group. Prometheus names start with `flink_taskmanager_job_task_operator_pipeline_`; sink serializer latency metrics use `flink_taskmanager_job_task_operator_user_pipeline_` because Flink adds a user serialization scope.

| Metric | Meaning |
|---|---|
| `receivedEvents` | Values read by the JSON validator, including invalid records |
| `invalidEvents` | Values quarantined by contract validation |
| `acceptedEvents` | Valid ticks after duplicate suppression |
| `duplicateEvents` | Records suppressed by the configured identity policy |
| `latePriceEvents` | Accepted price ticks excluded from already finalized EMA windows |
| `graceAcceptedEvents` | Ticks admitted after nominal closure but before grace expiry |
| `completedWindows` | Nonempty ticker windows finalized |
| `emittedEvents` | Price plus EMA values emitted by the indicator operator |
| `operatorMicros` | Time spent in a tick's keyed processing call, including synchronous state access and collection |
| `lateByMs` | Watermark minus nominal window timer for nominally late arrivals |
| `watermark` | Last watermark observed while processing a keyed tick |
| `ingressToSinkMs` | Wall time from JSON validation to Kafka sink serialization, for raw-price records |
| `emitToSinkMs` | Raw-price event age at sink serialization, before Kafka transaction commit |

Histograms retain a bounded rolling sample of 2048 values per task. Quantiles are local summaries: do not average task p99 values and call the result a global p99. Counters reset on task restart; use Prometheus `rate`/`increase` across time. The watermark gauge updates on ticks and can remain stale while idle.

Sink histograms do not include Kafka acknowledgement, checkpoint commit, or gateway/client delivery. The benchmark's `visible_roundtrip_ms` measures producer enqueue to consumption with `read_committed`, including transaction commit delay. `event_age_at_visibility_ms` additionally includes deliberately backdated timestamps. Neither measures a real gateway or client.

## Runtime and Kafka instruments

The dashboard includes task busy/backpressure time, JVM heap, JVM CPU, GC, checkpoint duration/size/failures, restarts, source consumer lag, and scrape health. Flink also exposes native connector metrics and latency markers. Kafka Exporter reports partition offsets, broker/topic metadata, and consumer lag; it does not expose all Kafka JVM/JMX internals. Benchmark runs preserve Docker resource snapshots, while the optional Prometheus export captures Flink resource time series throughout the run.

The benchmark captures metrics with `--monitoring`; `--dashboards` additionally starts Grafana, Alloy, and Loki. Deployment alerts are not part of the experiment stack.

Loki's WAL is persisted under `/loki/wal`. It stops accepting writes when its storage filesystem exceeds `LOKI_WAL_DISK_FULL_THRESHOLD` (default 0.9). Provision adequate free disk space; log readiness and data availability must be checked separately from metric scrape health. `monitoring/check.py` verifies Grafana, required scrape targets, and the presence of stored Flink logs.

## Structured logs

Flink emits one JSON object per stdout line with timestamp, level, logger, thread, escaped message, and escaped exception stack. Policy configuration and periodic per-task counters are logged without per-tick payload logging. Periodic statistics are emitted on activity, at most once per `LOG_INTERVAL_MS` per indicator task. This bounds logging overhead under load and produces no idle heartbeat spam.

Alloy discovers only containers with the current Compose project label, attaches service/container/project labels, extracts level/logger from Flink JSON, and forwards to Loki. Kafka and other services retain their native log messages. Avoid ticker labels in Prometheus/Loki: the long-tail symbol population would create high cardinality.

Useful Loki queries in Grafana Explore:

```logql
{service="taskmanager"} | json | message =~ "event=pipeline_stats.*"
{service=~"jobmanager|taskmanager", level="ERROR"} | json
```

Compare runs with monitoring consistently enabled or consistently disabled. Metrics, RocksDB instrumentation, and logging have measurable overhead; do a separate controlled run if quantifying that cost.

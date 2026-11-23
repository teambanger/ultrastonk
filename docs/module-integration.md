# Module Integration

## Branch Boundaries

`merge` is the integrated stack. It contains the Flink processing job and tests from `origin/feat/processing`, the Java CSV emitter from `origin/event-emitter`, and the WebSocket gateway from `origin/gateway`. The emitter and gateway were imported by path; branch ancestry alone does not tell you whether a module's code is present.

| Ref or path | Current state |
|---|---|
| `origin/feat/processing` | Source branch for the Flink job, synthetic tools, and monitoring. The integrated versions live on `merge`; the old root `emitter/` and root Compose overlay paths are obsolete. |
| `origin/event-emitter` | Source branch for the Java producer. Its standalone Compose file is not the stack configuration on `merge`. |
| `origin/data-downloader` | Contains standalone `download-data.sh`. The downloader used on `merge` is [`scripts/download_src_data.sh`](../scripts/download_src_data.sh). |
| `origin/gateway` | Source branch for [`gateway/`](../gateway/). The integrated service uses the root Compose configuration instead of the branch's standalone Compose file. |

Use `git status --short --branch`, `git diff --name-status HEAD...origin/<branch>`, and `git ls-tree -r --name-only origin/<branch> -- <module-path>` to separate branch changes from files already integrated. Use `git show origin/<branch>:<path>` to read branch-only build or deployment files without switching branches.

## Integration Interfaces

| Area | Where to look first |
|---|---|
| Deployment | [`compose.yaml`](../compose.yaml) defines Kafka, `kafka-init`, Flink, and workload profiles. [`.env.example`](../.env.example) lists operator settings; [`deploy/`](../deploy/) contains overlays for clustered Kafka, monitoring, resource limits, and bounded replay. Pass the root Compose file first so overlay-relative paths resolve from the repository root. |
| CSV replay | [`event-emitter/`](../event-emitter/) is the Java 21 producer. `EventEmitter.java` owns pacing and Kafka configuration; `DataLoader.java` owns CSV extraction; `RawPriceEvent.java` owns the payload. Compose maps the shared bootstrap and input topic settings to this module's `KAFKA_SERVER` and `TOPIC_NAME`. |
| Processing | [`PricePipeline.java`](../flink/src/main/java/fi/ultrastonk/PricePipeline.java) owns source watermarks and Kafka sinks. [`EventJson.java`](../flink/src/main/java/fi/ultrastonk/EventJson.java) validates the wire formats; [`PriceIndicators.java`](../flink/src/main/java/fi/ultrastonk/PriceIndicators.java) owns keyed state, window closes, and EMA updates. See [policies](policies.md) for state and late-data behavior. |
| Gateway | [`gateway/`](../gateway/) consumes committed `KAFKA_OUTPUT_TOPIC` records in its own group and forwards them to WebSocket subscribers. [`gateway/README.md`](../gateway/README.md) covers the browser test page, client messages, and tests. |
| Synthetic inputs | [`benchmarks/synthetic-emitter/`](../benchmarks/synthetic-emitter/) is the Python dummy emitter (`synthetic` profile), not the Java replay service (`emitter` profile). [`benchmarks/`](../benchmarks/) also contains a separate verifier, load driver, matrix runner, and report generator. |
| Monitoring | [`deploy/compose.monitoring.yaml`](../deploy/compose.monitoring.yaml) wires [`monitoring/`](../monitoring/) configs, alerts, dashboards, and the resource exporter. See [telemetry](telemetry.md) for metric meaning and checks. |

**Kafka boundary.** Kafka separates the Flink job from the gateway. Use `kafka:9092` on the base Compose network and `localhost:9092` from the host by default. The cluster overlay defines the peer broker addresses. Topic auto-creation is disabled: `kafka-init` creates the configured topics but does not reconcile existing topic settings.

| Setting | Default | Contract |
|---|---|---|
| `KAFKA_INPUT_TOPIC` | `price-events` | UTF-8 JSON, keyed by instrument ID/ticker. Replay uses `id`, `secType`, `eventTimestampMs`, and `last`; normalized test input uses `ticker`, `sec_type`, `timestamp_emit`, and `price`. |
| `KAFKA_OUTPUT_TOPIC` | `events-processed` | UTF-8 ticker key; JSON value with `timestamp_emit`, `ticker`, `type`, and `value`. Types are `price`, `EMA38`, and `EMA100`. Consumers use `isolation.level=read_committed`; records become visible when the Flink sink transaction commits. |
| `KAFKA_REJECTED_TOPIC` | `price-events-rejected` | Rejected inputs and policy-dependent late audits. JSON contains `raw` and `reason`, plus `truncated: true` for oversized raw payloads. |

`timestamp_emit` in output is **event time in epoch milliseconds**, not transport latency. Replay uses the historical exchange timestamp; EMA records use the closing trade's timestamp. Quote-only rows advance replay event time without emitting a price. In continuous mode, input idleness does not finalize the last window; the bounded replay overlay waits for CSV publication, reads to captured Kafka end offsets, and finalizes pending windows. See the [README wire examples](../README.md#kafka-contracts).

## Integration Checks

1. **Reconcile deployment.** Compare the source module's build, environment, network and startup assumptions against the root Compose model. Retain the root stack's topic ownership and configured endpoints; include any new topic in provisioning when the service requires one. Resolve the base model and each affected overlay/profile with `docker compose ... config --quiet`.
2. **Verify the boundary.** Run the module's own tests and the [root integration verifier](../README.md#validate-and-benchmark) when Kafka or Flink is affected. Use a fresh project and topics; synthetic test events can advance the watermark past historical replay input. For output consumers, verify a committed `price` and EMA pair against the actual Flink output, not just a module-local fixture. Use bounded replay to check the final EMA window.
3. **Account for state.** If touching Flink topology, keyed state, or transactional sinks, identify the effect on existing checkpoints/savepoints and transaction prefixes. JobManager has no automatic HA recovery; startup without an explicit restore replays retained input from the earliest offset. Restore from checkpoints made before source-level watermarking has not been validated. See [recovery and rescaling](../README.md#recovery-and-rescaling).

The [validation notes](validation.md) are dated historical results. Run the current README checks on the revision being integrated rather than treating the old counts as current evidence.

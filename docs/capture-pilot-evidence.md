# Capture-paced pilot evidence

This note records one opt-in `PACING=capture` pilot run that proves the measurement path. It is a synthetic, accelerated fixture, not a capacity, latency or historical-correctness claim.

## Revision and command

The run used the revision in this worktree that adds `ReplayPacer`, the opt-in emitter settings, `benchmarks/capture_pilot.py`, and the fixture `benchmarks/fixtures/capture-pilot.csv`. The exact procedure is the capture-paced pilot block in the root [README](../README.md#validate-and-benchmark). The effective settings were:

| Setting | Value |
|---|---|
| Project | a fresh `ultrastonk-pilot-*` Compose project with pilot-only topics |
| Fixture | `benchmarks/fixtures/capture-pilot.csv`, 158 rows, two symbols, 130 s capture span |
| Capture vs event clock | system capture time runs 250 ms behind the trading event time |
| `PACING` / missing policy | `capture` / `fail` |
| Pre-roll / measured / trailing | 10 s / 60 s / 60 s of capture time |
| `SPEED_MULTIPLIER` | 20 |
| Window / disorder / grace / checkpoint | 60 s / 5 s / 0 / 1 s |
| Workers / partitions | 1 TaskManager / 6 partitions, default uncapped container limits |
| Subscribers | one pilot WebSocket client subscribed to both measured symbols before publication |

## Expected versus actual

The pilot observed 158 committed input records, 158 committed price outputs plus 10 EMA outputs, and the same 168 records over the WebSocket. Delivery was an exact multiset match at ingest, committed Kafka and WebSocket: no missing, extra or duplicate records. The two tied-identity counters were both zero in this fixture; tied/duplicate handling is covered by controlled tests instead.

| Measurement | Result |
|---|---|
| Measured-segment achieved pacing | 1.00034 (required >= 0.98), max lag 0 ms |
| Pre-roll / trailing achieved pacing | 1.000 / 1.000 |
| Enqueue to acknowledgement (p50 / p99) | 8 ms / 32 ms, 158 samples |
| Enqueue to committed observation (p50 / p99) | 616 ms / 1680 ms |
| Enqueue to WebSocket receipt (p50 / p99) | 618 ms / 1681 ms |
| Committed observation to WebSocket (p50 / p99) | 2.2 ms / 12.4 ms |
| Window finalization from the eligibility lower bound (p50) | 268 ms, 4 eligible windows |
| Watermark waiting (p50) | 400 ms |
| Historical event age (p50) | about 1.59e11 ms (separate from transport) |
| Capture minus event clock offset | 250 ms for all 158 samples |

All five checks passed: telemetry present, capture pacing achieved, exact delivery, boundary identity/clock correlation with documented clock error, and window finalization for every eligible window.

## Limitations

- The fixture is synthetic and accelerated 20x. The slice, ticker mix, advisory crossings and prices are not a Historical measurement.
- Windows were shortened to 60 s and the source idle timeout to 1 s so two windows finalize quickly. Do not compare these latencies with production settings.
- Containers ran with default (uncapped) limits and one worker. Effective limits were recorded but only prove the run's settings.
- Watermark eligibility uses the enqueue of the first input that can advance the watermark, while an observed REST watermark crossing is required. `pre_observation_count = 4` shows the EMA receipt arrived before the poll that saw the crossing; this is polling resolution, not a transport claim.
- Container clocks share the Docker host kernel clock. A cross-host run must document clock error and cannot treat this evidence as a common-time-basis proof.

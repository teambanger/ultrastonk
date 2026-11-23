# Drop versus grace on scripted disorder

Ticket 11 asks for a matched comparison of prompt (`DROP`) and grace (`GRACE`)
late-event finalization on identical scripted disorder, against complete-input
oracle truth, with independent policy-aware expectations, delay and state cost,
and a recommendation.

`benchmarks/drop_vs_grace.py` builds the fixture, the complete-input oracle and
the policy expectations, runs each arm against the production Flink query, and
combines the two arms into one report. `benchmarks/tests/test_drop_vs_grace.py`
tests the fixture, the expectations and the comparison at their public seams.

## Matched schedule

Both arms use one fixture (digest
`f481616bfa972639eeed22c6dcac466112ace8945d9c9c4b0e39db323e237a4b`) with the
same seed `11`, window `300000` ms, out-of-order budget `5000` ms, grace
`30000` ms, dedup `NONE`, parallelism `2`, six input partitions, checkpoint
interval `1000` ms and one TaskManager pair. The query is the production
`PricePipeline`; only `LATE_EVENT_STRATEGY` differs between arms.

The schedule holds one trade per window for windows `0..30` (window 0 price
`39`, the rest `0.01`), then a window `31` with an early trade (`0.01`) and a
late trade (`1000`). The late trade is delivered after the watermark passes the
window's nominal finalization (`window_end - 1`) but inside the grace budget
(`window_end - 1 + 15000`), so `DROP` rejects it and `GRACE` admits it, changing
window 31's closing price. A runner `sync` step waits for the watermark to reach
each target, so admission is prescribed rather than left to scheduler timing.

## Results

Complete-input truth for this fixture equals the `GRACE` expectation: window 0
`BUY`, window 30 `SELL`, window 31 `BUY`. Both arms implement their own policy
with no processing or delivery defect; the only difference is the expected
policy difference, which is the late trade.

| Arm | committed records | defects (vs policy) | truth loss (vs complete input) |
|---|---|---|---|
| `DROP` | 34 (32 EMA, BUY@0, SELL@30) | 0 | 1 missing advisory (BUY@31), 2 incorrect EMA values (window 31) |
| `GRACE` | 35 (32 EMA, BUY@0, SELL@30, BUY@31) | 0 | 0 |

The two replicates (`arm-R1-*`, `arm-R2-*`) agree exactly on record counts,
advisories and truth loss.

## Delay and state

`finalization_lag_ms` is the policy-intrinsic event-time delay between the
window end and the timer that finalizes it: `0` for `DROP`, `grace_ms`
(`30000`) for `GRACE`. This is the deliberate trade-off and is exact.

`state.checkpoint_bytes` is the latest completed checkpoint's state size for the
measured job, scope one matched run, sample size one checkpoint per arm:

| Replicate | DROP bytes | GRACE bytes | delta |
|---|---|---|---|
| R1 | 78026 | 79126 | +1100 |
| R2 | 79054 | 83398 | +4344 |

The measured wall-clock `delay.finalization_ms` is also recorded (R1 DROP
`10173` ms / GRACE `804` ms; R2 DROP `10244` ms / GRACE `85` ms). It is not a
live-latency measurement: the synthetic event times are not wall-clock, the
figure is a single late window per arm, and it is dominated by job startup and
checkpoint visibility rather than by the grace wait. Treat it as a relative
artifact of the fixture, not a latency claim.

## Recommendation

**Default to `GRACE`** for this workload. It preserves the late-trade advisory
and both EMA values that `DROP` loses against complete-input truth, at the cost
of `30000` ms of event-time finalization delay and a small managed-state
increase (about `1` to `4` KB per checkpoint here). `DROP` is the better default
only when no eligible late trade can change a closing price, where both arms
match truth and `DROP` finalizes sooner with less state.

## Reproduce

The full run needs Docker and a disposable Compose project. Build the benchmark
image, then for each arm start a fresh project, topics and Flink state so no
checkpoint is restored across arms:

```bash
export COMPOSE_PROJECT_NAME=ultrastonk-dvg-live
export KAFKA_PORT=39592 FLINK_UI_PORT=38581 GATEWAY_PORT=0
export WINDOW_SIZE_MS=300000 MAX_OUT_OF_ORDER_MS=5000 ALLOWED_LATENESS_MS=30000
export SOURCE_IDLE_TIMEOUT_MS=1000 CHECKPOINT_INTERVAL_MS=1000 FLINK_PARALLELISM=2
export KAFKA_PARTITIONS=6 FLINK_BOUNDED_REPLAY=false FLINK_RESTORE_PATH= DEDUP_STRATEGY=NONE
docker compose --profile benchmark build benchmark
# for arm in DROP GRACE:
export LATE_EVENT_STRATEGY=$arm
export KAFKA_INPUT_TOPIC=dvg-$arm-in KAFKA_OUTPUT_TOPIC=dvg-$arm-out KAFKA_REJECTED_TOPIC=dvg-$arm-audit
export KAFKA_GROUP_ID=dvg-$arm KAFKA_TRANSACTION_PREFIX=dvg-$arm-
docker compose down --volumes --remove-orphans
docker compose up -d kafka kafka-init jobmanager taskmanager
docker compose run --rm --no-deps --entrypoint python benchmark drop_vs_grace.py run-arm \
  --policy $arm --output /results/dvg-$arm.json --jobmanager-url http://jobmanager:8081
# then
python3 benchmarks/drop_vs_grace.py compare \
  --drop benchmarks/results/dvg-DROP.json --grace benchmarks/results/dvg-GRACE.json
```

Without Docker, the deterministic parts run anywhere:

```bash
python3 -m pytest benchmarks/tests/test_drop_vs_grace.py
python3 benchmarks/drop_vs_grace.py fixture --truth | head
```

## Scope and limits

- The disorder is scripted and synthetic, not the historical week; it isolates
  the late-event policy, not real market lateness.
- One late window per arm, so delay and state are small-sample; the state size
  is a single checkpoint, and the measured wall-clock delay is not comparable
  across arms (see above).
- Another heavy reference run may be active concurrently, so timing is relative
  between the two arms; no exclusive capacity or live latency is claimed.
- The fixture uses the production query unchanged; it does not exercise
  recovery, side outputs, or `TIMESTAMP`/`EXACT` dedup.

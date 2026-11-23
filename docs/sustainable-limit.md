# Sustainable limit: capacity bracket for one and two TaskManagers

Ticket 08 preparation. This document freezes the acceptance contract, the scored
commands, the resource profiles and the search schedule **before** any scored run.
It contains no measurement: the bracket is written to
`benchmarks/evidence/sustainable-limit/` only by the scored phase.

The runner is `benchmarks/capacity_bracket.py`. It reuses the synthetic workload
through `benchmarks/matrix.py` with the live query revision (the current `flink/`
source, pinned by `docs/benchmarking.md` and the per-session `*-manifest.json`
`sources_sha256` and `git_commit`), fixes partitions, policies, workload shape,
per-worker caps and instrumentation across both worker counts, steps offered load
coarse-to-fine, and exports the highest-passing and first-failing bounds — or an
explicit `unbracketed` outcome.

## Held fixed across both worker counts

| Factor | Value | Where fixed |
|---|---|---|
| Kafka partitions | 6 | `matrix.py` (`KAFKA_PARTITIONS=6`) |
| Policies | `DEDUP_STRATEGY=NONE`, `LATE_EVENT_STRATEGY=DROP` | runner flags `--dedup NONE --late DROP` |
| Workload shape | 1000 symbols, Zipf `skew=1.2`, no duplicates, no backdating, 1000 ms window, 2000 ms grace | runner defaults, overridable |
| Per-worker caps | one resource profile applied to every run | runner `--profile-spec` |
| Instrumentation | Prometheus + Kafka exporter + resource exporter; one WebSocket subscriber path | runner `--monitoring --gateway` (default on) |
| Warmup / seed / measurement | fixed warmup, `--seed 42`, fixed coarse/fine durations | runner flags |

Partitions, policies, shape, caps and instrumentation are identical for the one- and
two-TaskManager runs; only `--parallelism` and the offered rate change. Because the
per-worker cap is fixed, total resources grow with worker count; every point reports
the aggregate worker CPU and memory quota alongside delivery, per `docs/benchmarking.md`.

## Pre-registered acceptance rules

Implemented in `default_rules()` and pinned by
`benchmarks/evidence/sustainable-limit/pre-registered-rules.json` (a unit test fails
if the code and the file drift). A run's verdict is decided in this order:

1. **INCONCLUSIVE — missing telemetry.** The run is not counted as pass or fail when
   any required field is absent: driver measurement, achieved input rate, offered
   load, price correctness, gateway correctness when `--gateway` is on, progress
   samples (`>= 5`), window size, checkpoint interval, effective `taskmanager` caps,
   host hardware, or aggregate worker CPU.
2. **INCONCLUSIVE — generator shortfall.** Achieved originals per second is below
   `min_achieved_input_ratio = 0.98` of the offered rate. The offered load was never
   actually applied, so the run cannot attribute a limit. This outranks a delivery
   failure: a shortfall that also drops output is still inconclusive, not a fail.
3. **FAIL — incorrect delivery.** Committed Kafka or WebSocket delivery is missing,
   extra, or the output contract is broken. At the boundary this is the decisive
   failure.
4. **FAIL — sustained stage-backlog growth.** Either the ingestion backlog
   (`expected_measured_prices - committed_measured_prices`) or the gateway backlog
   (`committed_measured_prices - websocket_measured_prices`) grows across the measured
   load window after checkpoint-batching spikes are removed.
5. **PASS** otherwise.

Concrete thresholds: `backlog_growth_slack = 0.0`, `backlog_terminal_floor = 0.0`,
`ignore_load_window_slack_s = 2.0`, `min_progress_samples = 5`,
`stretch_capacity_gain = 1.5`.

### Checkpoint-batching accounting

Committed output is transactional, so the ingestion backlog sawtooths: it climbs
between checkpoints and drops at commit. The rule therefore takes the **per-checkpoint-
interval minimum** (the trough) of the backlog series before testing growth, so a
one-sample batching spike cannot be mistaken for overload. A bucket width of
`checkpoint_interval_ms / sample_interval` is used; only the trough trend across the
load window — not the peak — decides PASS/FAIL. The raw series, trough series, bucket
width and suppressed-spike count are all retained in the evidence.

Samples after the producer stops (elapsed greater than the run duration plus
`ignore_load_window_slack_s`) are excluded, because they measure drain rather than
keeping up under load.

### Point and bracket aggregation

A repeated point is **INCONCLUSIVE if any repeat is inconclusive**, otherwise **FAIL
if any repeat fails**, otherwise **PASS** — one shortfall or telemetry gap makes the
point inconclusive rather than letting a lucky repeat win. A worker count is
*separated* when it has a highest-passing load strictly below a first-failing load;
otherwise the outcome is `unbracketed` with an explicit reason (`no passing offered
load`, `no failing offered load`, or `first failing is below the highest passing
load`). A non-monotonic ladder is flagged.

## Search schedule

Coarse-to-fine, single host, exclusive:

1. **Coarse** — one `matrix.py` batch over the geometric ladder
   `1000, 2000, 4000, 8000, 16000, 32000` originals/s for **both** worker counts at
   once (`--parallelism 1,2`), `--duration-coarse 60`, `--warmup 10`,
   `--repetitions-coarse 1`. Matrix randomizes run order within the batch.
2. **Fine** — for **each** separated worker count, longer, repeated runs at
   `--fine-points 3` rates evenly spaced between the coarse highest-passing and
   first-failing bounds (extended by `--fine-fraction 0.25` on each side), with
   `--duration-fine 120` and `--repetitions-fine 3`. A worker count without
   separation produces no fine batch and is reported unbracketed.
3. **Export** — highest-passing / first-failing per worker count, per-point medians
   and repeat spread of steady throughput, tails (p50/p95/p99/max at both boundaries),
   drain, stage lag, producer backpressure, checkpoint interval and detected batching,
   worker throttling and memory, and worker-capacity gain with total resources.

`--coarse-stop` should sit above the expected limit. If the ladder never fails, the
runner reports `unbracketed: no failing offered load`; that is a valid outcome, not an
error. `--evaluate-session SESSION` re-evaluates existing matrix artifacts without
running anything.

## Scoped scored plan (two-hour programme; ticket 07 runs first)

A later scope decision downscales ticket 08 to fit a roughly two-hour measurement
programme and runs it **after** ticket 07's peak run, on an otherwise idle host. Only
one configuration set is used, held fixed across both worker counts; the verdict rules
are unchanged. This reduces the ladder and the repeat count, so the report must state
the reduced confidence.

**Scored command (exact):**

```bash
SPEC='taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g'

python3 benchmarks/capacity_bracket.py --profile-spec "$SPEC" \
  --coarse-rates 500,1000,2000,4000 \
  --duration-coarse 30 --warmup 5 --repetitions-coarse 1 \
  --coarse-extra-rate 8000 \
  --fine-single --fine-points 1 --duration-fine 60 --repetitions-fine 1 \
  --wall-budget-seconds 2700
```

- **Coarse batch:** rates `500,1000,2000,4000` originals/s, `--parallelism 1,2` in one
  batch, 30 s, 5 s warmup, 1 repetition, the fixed per-worker profile, monitoring and
  gateway on, seed 42, the same shape and policies. `--coarse-extra-rate 8000` adds an
  `8000` batch automatically **only if the `4000` point passed for both workers**.
- **Fine confirmation:** at most one run (`--fine-single --fine-points 1`), for the most
  informative separated worker, 60 s, 1 repetition, at the midpoint rate between the
  coarse highest-passing and first-failing bounds. It is skipped if under ~15 minutes
  remain (`--wall-budget-seconds 2700` enforces the fit automatically).
- **Hard cap:** 45 minutes from go-ahead to summary, including the image build. Cut
  order if the build plus the batch cannot fit: (1) drop `--coarse-extra-rate 8000`;
  (2) drop the fine run; (3) drop to three rates with
  `--coarse-rates 1000,4000,8000`.

Because every configured point is now a **single repetition**, per-point repeat spread
is a single run and has no meaningful stdev; that is stated wherever spread is reported.

**Reduced-confidence fallbacks (exact):**

```bash
# Cut 1: no 8000 rate.
python3 benchmarks/capacity_bracket.py --profile-spec "$SPEC" \
  --coarse-rates 500,1000,2000,4000 --duration-coarse 30 --warmup 5 \
  --fine-single --fine-points 1 --duration-fine 60 --repetitions-fine 1

# Cut 2: no fine run (coarse batch only).
python3 benchmarks/capacity_bracket.py --profile-spec "$SPEC" \
  --coarse-rates 500,1000,2000,4000 --duration-coarse 30 --warmup 5

# Cut 3: three rates, coarse batch only.
python3 benchmarks/capacity_bracket.py --profile-spec "$SPEC" \
  --coarse-rates 1000,4000,8000 --duration-coarse 30 --warmup 5
```

## Full commands (non-scoped reference)

All commands run from the repository root with the host otherwise idle (no full-week
replay, no drop-vs-grace comparison, no peak run concurrently). `--profile-spec` is the
primary fixed per-worker profile:

```bash
SPEC='taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g'

# 1. Print exactly what will run; starts nothing and generates no load.
python3 benchmarks/capacity_bracket.py --dry-run --profile-spec "$SPEC"

# 2. Coarse ladder for both worker counts, then fine repeats near each boundary,
#    writing the ledger and bracket.json.
python3 benchmarks/capacity_bracket.py --profile-spec "$SPEC"

# 3. Re-evaluate the recorded sessions without re-running (after any interruption).
python3 benchmarks/capacity_bracket.py --profile-spec "$SPEC" \
  --evaluate-session <SESSION> [--evaluate-session <SESSION> ...]
```

The coarse command the runner issues (also printed by `--dry-run`, shown here for the
scoped `500,1000,2000,4000` ladder):

```bash
python3 benchmarks/matrix.py \
  --project ultrastonk-capacity-coarse \
  --duration 30 --warmup 5 \
  --rates 500,1000,2000,4000 --parallelism 1,2 \
  --dedup NONE --late DROP --qualities baseline --repetitions 1 \
  --duplicate-rate 0 --late-rate 0 \
  --resource-profile "perworker=$SPEC" --seed 42 \
  --symbols 1000 --skew 1.2 --window-ms 1000 --grace-ms 2000 \
  --gateway --monitoring
```

The optional extra batch uses the same command with `--project ultrastonk-capacity-coarse-extra`
and `--rates 8000`. Fine commands are identical except for `--parallelism <one worker>`,
the fine rates, `--duration 60`, and `--repetitions 1`, with project names
`ultrastonk-capacity-fine0`, … Each `matrix.py` batch is a fresh `ultrastonk-benchmark-*`
project. The full non-scoped reference schedule is the geometric `1000,2000,4000,8000,16000,32000`
ladder at 60 s / 3 fine repetitions described under *Search schedule*.

### Resource profiles

| Profile | Services | Purpose |
|---|---|---|
| `perworker=taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g` | fixed per worker | Primary one-vs-two comparison. Each TaskManager gets 1 CPU / 2 GiB, so two workers have twice the worker budget. |
| `tight=taskmanager:0.5:2g,…` | – | Optional first drop of the per-worker cap if the coarse ladder is unbracketed high. |
| uncapped (`perworker=`) | – | Only for a one-budget comparator, never for the bracket itself. |

Keep each TaskManager's configured process size below its container cap so the run
measures capacity, not startup or OOM.

## Isolation contract

The scored phase uses fresh `COMPOSE_PROJECT_NAME` values
(`ultrastonk-capacity-coarse`, `-fine0`, …) through `matrix.py`, which sets unique
`KAFKA_*_TOPIC` values with an identical per-run suffix, publishes Kafka internally
only, and assigns Docker-assigned ports. The verifier-only namespace
(`ultrastonk-verify-*`, `verify-<id>-{in,out,audit}`) is untouched. `matrix.py` refuses
pre-existing containers, networks, volumes or image tags for a project. TaskManager
count is `EXPECTED_TASKMANAGERS=parallelism`, and `FLINK_BOUNDED_REPLAY=false` for the
synthetic workload.

## Evidence layout

| Artifact | Contents |
|---|---|
| `benchmarks/evidence/sustainable-limit/pre-registered-rules.json` | Frozen acceptance thresholds. |
| `benchmarks/evidence/sustainable-limit/run-ledger.jsonl` | Append-only ledger of every evaluated run, including failed and inconclusive attempts. |
| `benchmarks/evidence/sustainable-limit/bracket.json` | Bracket bounds, per-point medians and repeat spread, worker-capacity gain, and every evaluated run. |
| `benchmarks/results/SESSION-matrix.csv`, `RUN.json`, `RUN.limits.json`, `RUN.resources.jsonl`, `RUN.prometheus.json`, `SESSION-manifest.json` | Raw per-run artifacts named by `bracket.json` `source_sessions`. |

## Bottleneck claims

A capacity claim is only bracketed from PASS/FAIL runs whose offered load was actually
achieved and whose telemetry is complete. Where a bottleneck is claimed:

- support it with **stage evidence** (`capacity_bracket.stage_evidence`: ingestion and
  gateway backlog plus worker CPU utilization and throttling), and
- exactly **one one-budget change** validated by
  `capacity_bracket.one_budget_change` (the pair shares offered load, workload shape and
  policy and differs only in `taskmanager_cpu`, `taskmanager_memory`, or `parallelism`),
  or a targeted Module experiment.
- Report **worker-capacity gain** (`capacity_bracket.worker_capacity_gain`) alongside
  total resources (the `total_resource_multiplier`). The **1.5x stretch check counts
  only when Flink is demonstrably the limiter**
  (`capacity_bracket.flink_limiter_evidence`: worker CPU saturation or throttling, with
  the generator keeping up); correlated CPU throttling alone is not sufficient.

## Not measured yet

No bracket, throughput, tail, drain, lag, throttling or scaling number exists until the
scored phase runs. The capped 5,000 originals/s smoke runs on the `integration/report-evidence`
branch show eventual delivery after drain, not a Sustainable limit, and are retained as
separately scoped historical evidence (`docs/benchmarking.md`).

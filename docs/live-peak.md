# Live peak-load measurement (ticket 07)

This is the preparation record, runbook and measured result for the official-week
peak load. The scored phase **ran on 2026-11-23** on branch
`integration/report-evidence`, under a 40-minute cap and a deliberate downscale
(1x only, warm only, pre-roll 120 s / trailing 90 s, one repetition). The measured
numbers and their limits are in
[Scored results](#scored-results-2026-11-23--measured) below. An earlier attempt
that same day was blocked by a foreign Compose project; that record is retained
unchanged in `benchmarks/evidence/live-peak/blocked.json`. The pre-registered plan
below is unchanged; the scored section states exactly what was cut.

Peak load is offered by replaying a slice of the official DEBS CSV at recorded
capture pace (`PACING=capture`, ticket 05) on an isolated Compose project. The
slice length, pacing levels, repeats, decision rules and ledger format below are
**pre-registered**: they are fixed before the scored runs so the result cannot be
tuned after the fact.

## What is ready

| Artifact | Purpose |
|---|---|
| `benchmarks/peak_slice.py` | Carves the peak slice from `week-fixture.json`, preserving burst order, with pre-roll and trailing segments and the emitter offsets to label them. |
| `benchmarks/live_peak.py` | Plans the run matrix, builds isolation-compliant Compose env, collects a run inside the benchmark container, evaluates, aggregates, compares to the oracle, writes the ledger, and records checkpoint cadence. |
| `benchmarks/tests/test_peak_slice.py`, `benchmarks/tests/test_live_peak.py` | Unit tests for every pure function above (run with the rest of `benchmarks/tests`). |
| `benchmarks/evidence/live-peak/` | Where the plan, per-run artifacts, oracle comparison, summary and ledger are written. |

`benchmarks/matrix.py` and `benchmarks/report.py` are untouched; the new code
lives in ticket-specific files.

## The peak and the slice

Ticket 02 verified the highest rolling five-minute raw-row capture interval:

- capture window `2021-11-09T15:58:01` → `16:03:01` (inclusive → exclusive),
- `1,223,873` rows, exchange mix FR `651,418` / NL `411,885` / ETR `160,570`,
- burst order preserved (capture times are non-monotonic; the emitter emits
  out-of-order rows immediately and keeps recorded order).

The slice adds two support segments around that measured interval:

- **pre-roll** (default `300 s`, one full window) so at least one complete
  preceding five-minute window finalises before the peak and the EMA state and
  watermark entering the peak are non-zero. `--pre-roll-ms` deepens it; the
  forced-cold state drops it entirely.
- **trailing** (default `180 s`) so input continues past the close of the last
  window the peak touches (`measured end` rounds up to `+5:00`, plus the 5 s
  disorder budget, plus margin). This advances the watermark so that window
  finalises naturally instead of only at end of input.

Rows are selected with the same rule as `scripts/profile_fixture.py` (40 columns,
last-dot suffix FR/NL/ETR, parseable system capture time in the half-open
interval), copied in original physical order. The measured segment therefore
reproduces the fixture's `1,223,873` rows and exchange mix exactly, which the
tool asserts by default (`--skip-verify` only for rehearsal).

### Extract the slices

```bash
python3 benchmarks/peak_slice.py \
  --fixture /mnt/shared/data/official-week-Ij8jlfeA/results/week-fixture.json \
  --data-dir /mnt/shared/data/official-week-Ij8jlfeA \
  --state warm --output benchmarks/evidence/live-peak/slices/peak-warm.csv \
  --manifest-out benchmarks/evidence/live-peak/slices/peak-warm-manifest.json
python3 benchmarks/peak_slice.py \
  --fixture /mnt/shared/data/official-week-Ij8jlfeA/results/week-fixture.json \
  --data-dir /mnt/shared/data/official-week-Ij8jlfeA \
  --state cold --output benchmarks/evidence/live-peak/slices/peak-cold.csv \
  --manifest-out benchmarks/evidence/live-peak/slices/peak-cold-manifest.json
```

Each manifest carries `segments`, `measured_by_exchange`, and the
`emitter.REPLAY_PREROLL_MS` / `emitter.REPLAY_MEASURE_MS` the runner passes
through. Extraction reads the whole day-09 CSV once; it is a scored-phase step
because that read is host I/O.

## Isolation contract

Every run uses a fresh Compose project and follows the shared verifier isolation
contract, enforced by `compose_env` + `contract_violations`:

- `COMPOSE_PROJECT_NAME` fresh (never the default project, never the
  `ultrastonk-verify-*` namespace);
- `KAFKA_INPUT_TOPIC` / `KAFKA_OUTPUT_TOPIC` / `KAFKA_REJECTED_TOPIC` distinct and
  all carrying the project's unique suffix;
- `FLINK_UI_PORT=0` (Docker assigns a collision-free port);
- `EXPECTED_TASKMANAGERS` equal to `FLINK_PARALLELISM`;
- `FLINK_BOUNDED_REPLAY=false` — this measures a **live** paced replay, so the
  finite preload-and-process overlay is deliberately *not* used.
- fresh consumer group and transaction prefix per run.

## Pre-registered run plan

- **Headline conditions:** capture pace at `SPEED_MULTIPLIER` `1x` and `1.25x`,
  **three valid repeats each**, warm state by default.
- **Forced cold-start** (`--states cold`) is an optional labelled condition; it
  drops the pre-roll so the peak is offered with EMA state starting at zero.
- **Rollover** is a separate controlled fixture, reported on its own line and
  never mixed into the headline numbers.
- **Window / disorder / lateness:** 300 s / 5 s / `DROP` (no extra lateness).
- **Parallelism / partitions:** 2 TaskManagers of one slot (2), 6 input
  partitions — the project default, so the number describes *this* configuration.
- **Checkpoint cadence:** frozen at `CHECKPOINT_INTERVAL_MS=10000` (recorded per
  run). Ticket 05 deferred any latency-driven cadence tuning to this ticket; **no
  tuning is performed by default**. If the operator tunes it, the matched
  visibility/throughput/checkpoint-cost comparison must be recorded with
  `live_peak.cadence_tuning_comparison` before freezing, and the run ledger must
  say the cadence changed.
- **Subscriptions:** the client subscribes to every ticker present in the slice
  (batched at the gateway's request limit; the per-client cap is raised through a
  generated Compose override). `--subscribe topN` restricts to the N most active
  tickers and must be recorded.

Write the plan (before running anything):

```bash
python3 benchmarks/live_peak.py plan \
  --fixture /mnt/shared/data/official-week-Ij8jlfeA/results/week-fixture.json \
  --slice-manifest warm=benchmarks/evidence/live-peak/slices/peak-warm-manifest.json \
  --slice-manifest cold=benchmarks/evidence/live-peak/slices/peak-cold-manifest.json \
  --repeats 3 --states warm \
  --output benchmarks/evidence/live-peak/run-plan.json
```

## Run the scored phase

Inspect the exact host commands without touching Compose, then execute:

```bash
python3 benchmarks/live_peak.py run \
  --fixture /mnt/shared/data/official-week-Ij8jlfeA/results/week-fixture.json \
  --slice warm=benchmarks/evidence/live-peak/slices/peak-warm.csv \
  --slice-manifest warm=benchmarks/evidence/live-peak/slices/peak-warm-manifest.json \
  --repeats 3 --states warm --parallelism 2 \
  --results-dir benchmarks/evidence/live-peak/runs \
  --ledger benchmarks/evidence/live-peak/ledger.json --dry-run

# then drop --dry-run to run it
```

Per run the orchestrator: starts the isolated pipeline, builds the collector
image, snapshots effective container limits, subscribes the client to the whole
slice **before** publication, runs the emitter to completion at capture pace,
waits for the collector to settle, then tears the project down. Failed or invalid
attempts are kept in the ledger and are not silently retried.

## Metrics collected

- **Achieved pacing** per segment (scheduled vs actual, max lag) from the
  emitter's pacing report, required `>= 0.98` on the measured segment.
- **Expected vs delivered counts by subscription**, on the committed input,
  committed output and WebSocket, overall and per ticker.
- **Stage backlog:** broker consumer-group lag (Flink ingest and gateway), the
  Flink input-minus-output gap, and the gateway output-minus-WebSocket gap, with
  growth direction.
- **Latency** p50/p95/p99 **with sample sizes**, for the trade path and the
  advisory path separately.
- **Restarts / OOMs** from container inspection and task restart telemetry, and
  **drain** seconds from the last committed input to the last delivery.
- **Checkpoint cadence** actually in effect, and any tuning comparison.
- **Keeping up** verdict: correct delivery (committed input → committed output →
  WebSocket) **and** no sustained Flink/gateway backlog growth, with measured
  pacing and OOM kills as gates. This is explicitly *not* judged by eventual
  parity — a run that only catches up after the peak is reported `did_not_keep_up`.
- **Missing evidence** surfaced as an explicit list, not silently dropped.

## Decision rules (pre-registered)

1. **Two independent latency distributions.** Trade enqueue-to-WebSocket and
   estimated window eligibility-to-advisory-receipt are reported separately;
   one never stands in for the other. Each needs its own minimum sample count
   (`30` and `20`); below it the result is reported `inconclusive`.
2. **Live advisory p99 is reported `inconclusive` when signals are too sparse**,
   never fudged from a handful of crossovers. A controlled rollover fixture is
   reported on its own line, never merged into the live advisory number.
3. **Correctness vs the oracle.** The live output is compared against
   `benchmarks/query_truth.py` over the same slice. Deviations split into
   **transport loss** (offered eligible trades missing from the committed input)
   and **policy-related** differences (the inputs reached the query, so the live
   policy — lateness, dedup, grace — made the call). "Unexplained" deviations are
   kept visible, and there is no extrapolation from the slice to full-week
   capture order.

## Correctness comparison

```bash
# Independent complete-input truth over the exact slice.
python3 benchmarks/query_truth.py --timezone Europe/Berlin \
  benchmarks/evidence/live-peak/slices/peak-warm.csv \
  > benchmarks/evidence/live-peak/oracle-warm.jsonl

# Normalize committed four-field Kafka output to the oracle schema, then compare.
python3 benchmarks/live_peak.py compare \
  --run benchmarks/evidence/live-peak/runs/peak-warm-1x-r1-run.json \
  --slice benchmarks/evidence/live-peak/slices/peak-warm.csv \
  --oracle benchmarks/evidence/live-peak/oracle-warm.jsonl \
  --actual benchmarks/evidence/live-peak/committed-warm-1x-r1-normalized.jsonl \
  --db /tmp/live-peak-compare.sqlite \
  --output benchmarks/evidence/live-peak/compare-warm-1x-r1.json
```

`--db` uses the disk-backed comparator for slice-scale files. Query event time is
`Europe/Berlin` on both sides (matching the emitter's default `EVENT_TIME_ZONE`).

## Summarize and ledger

```bash
python3 benchmarks/live_peak.py summarize \
  --run benchmarks/evidence/live-peak/runs/peak-warm-1x-r1-run.json \
  --run benchmarks/evidence/live-peak/runs/peak-warm-1x-r2-run.json \
  --run benchmarks/evidence/live-peak/runs/peak-warm-1x-r3-run.json \
  --ledger benchmarks/evidence/live-peak/ledger.json \
  --output benchmarks/evidence/live-peak/summary.json
```

The summary carries each run's metric groups and the per-condition median and
spread, and every attempt is appended to the ledger with status `valid`,
`invalid` or `failed`. A run that does not meet preconditions is `invalid`; a run
whose steps errored is `failed`. Neither is dropped or silently retried.

## Expected resource needs

Scores must run on an otherwise idle host (the contended phase is why this is
split into two phases).

- **Slice & disk:** the warm slice is roughly `2–3 M` rows (~`200–300 MB` CSV);
  add ~`600 MB` of Kafka log per run (torn down with the project). Allow
  `≥ 15 GB` free disk for images, topics, Oracle SQLite scratch and artifacts.
- **CPU/RAM:** 2 TaskManagers (one slot each) + JobManager + Kafka + gateway +
  collector. Budget roughly `4` sustained cores and `8 GB` RAM; the Flink
  TaskManagers dominate memory.
- **Wall time:** at `1x` a warm run lasts the slice's ~`13 min` of capture time
  plus a few minutes of startup and drain; `1.25x` is proportionally shorter. Six
  headline runs plus one image build is roughly `1.5–2 h`. A cold-state sweep adds
  a similar amount.
- **Oracle:** `query_truth.py` over a ~3 M-row slice is a few minutes and a few
  hundred MB of scratch.

## Interpreting the result later

Numbers from the scored phase describe *this* slice, ticker mix, pacing levels and
resource configuration. They are not a capacity claim, not a full-week claim, and
not a live-policy correctness expectation; they are the peak-window behaviour of
the measured configuration, with deviations from complete-input truth attributed
as above.

## Scored results (2026-11-23) — measured

**Status: measured, valid.** One scored repetition ran on a clear host. All five
`capture_pilot` gates passed (`telemetry`, `pacing`, `delivery`, `identity_clock`,
`windows`); the run is `valid` in the ledger. This is a peak-window result for the
measured configuration, not a capacity or full-week claim.

### Downscale actually applied (pre-registered plan above kept intact)

- capture pace `SPEED_MULTIPLIER=1`, `PACING=capture`, **warm state only** — no
  1.25x and no forced-cold variant;
- pre-roll **120 s**, trailing **90 s** (overriding the 300 s / 180 s defaults);
- `FLINK_BOUNDED_REPLAY=false`; window/disorder/lateness 300 s / 5 s / `DROP`;
  parallelism 2, 6 input partitions, checkpoint 10 s, subscribe all tickers;
- **repeats actually run: 1.** The rule was "two if one repetition costs ≤ 5 min".
  The emitter phase alone is 510 s of capture (120 + 300 + 90) plus startup and
  build, so the first repetition cost well over 5 min and no second repeat was run.
  Fewer repeats lower confidence; there is no spread estimate.
- measured peak segment unchanged and asserted: **1,223,873 rows**, FR 651,418 /
  NL 411,885 / ETR 160,570, first/last measured capture times matching the fixture.

Host preconditions were verified immediately before starting: `docker ps -a`
empty and `ps -ef | grep -E 'sort_week|full_week'` empty. A unique project
(`ultrastonk-peak-80d41f7b99`) was created, used, and torn down; `docker ps -a` was
empty again afterwards.

### Attempts (all kept)

1. **`peak-warm-1x-r1-attempt1` — `failed`.** The first started stack crash-looped
   the gateway: `compose_env` set `GATEWAY_HISTORY_LIMIT=0`, but the gateway
   requires an integer `>= 1`, so it never started and the collector exited 1
   before collecting anything. Project `ultrastonk-peak-07a5b8b183` was torn down;
   no run artifact exists. Fixed to `GATEWAY_HISTORY_LIMIT=1`.
2. **`peak-warm-1x-r1` — `valid`.** Full run, all gates pass.

### Run plan actually used

`run-plan.json` records pacing level `[1.0]`, one repeat, one run
(`peak-warm-1x-r1`, condition `warm@1x`). This is the downscale of the
pre-registered 1x/1.25x three-repeat campaign described above.

### Achieved pacing (scheduled vs actual, emitter report)

| Segment | Rows | Achieved ratio | Max lag |
|---|---|---|---|
| PRE_ROLL | 373,210 | 0.99983 | 739 ms |
| MEASURED | 1,223,873 | 0.99991 | 35 ms |
| TRAILING | 317,895 | 0.99980 | 47 ms |

Measured-segment pacing is effectively 1.00x. (The slice manifest counts
373,213 / 1,223,873 / 318,120 rows by strict capture-stamp membership; the 225-row
trailing difference is the emitter's capture-offset boundary labeling.)

### Expected vs delivered by subscription

Price records: **expected 195,061 = committed 195,061 = WebSocket 195,061**, with
zero missing, zero extra, and no EMA-pair gaps. The 1,914,978 committed-input rows
include tied/duplicate identities inherent to the recorded data (8,257 duplicate
identities / 16,529 duplicate rows; 1,395 tied timestamps), which the delivery
check treats correctly.

### Stage backlog and drain

- **Kafka broker lag / gateway consumer-group lag: not collected.** The collector
  wrote `consumer_lag: {}`; this is recorded as missing evidence, not silently
  dropped.
- **Flink input-minus-output gap:** max 1,713,373, final 1,710,455, nominally
  "growing". This gap is structural — the output stream is per-window EMA/advisory
  records, not one row per trade — so it is not a throughput backlog.
- **Gateway output-minus-WebSocket gap:** max 1,654, final 0, not growing — the
  gateway caught up.
- **Drain:** 7.08 s from the first committed-input peak to the first delivery peak.

### Restarts and OOMs

Post-run `docker inspect` of every container in the project: **restart count 0,
`OOMKilled=false`** for all (jobmanager, both taskmanagers, kafka, gateway,
client, kafka-init; client exit 0). Task-restart JSON was not supplied to the
collector, so `summary.restarts.available` is false; the container inspection is
the source (`runs/post-run-containers.txt`).

### Latency (p50/p95/p99 with sample sizes), two paths reported independently

**Trade: enqueue → WebSocket** — n = 195,061
| p50 | p95 | p99 | max |
|---|---|---|---|
| 5,060 ms | 9,063 ms | 9,225 ms | 10,171 ms |

These are percentiles of per-record elapsed time: p50 is the median, while p95
and p99 describe the upper tail. Most of the observed delay is already present
at the committed Kafka reader. Enqueue to committed observation is p50 5,043 /
p95 9,047 / p99 9,206 ms. Emitter enqueue to broker acknowledgement is p50 34 /
p95 55 / p99 58 ms (n = 5,000). The separate committed-reader-to-WebSocket
observation gap is p50 15.5 / p95 104 / p99 134 ms (max 422 ms); it includes
consumer polling and scheduling, not just network transit. An independent
Kafka reader can observe a record after the subscriber does. Do not subtract
percentiles to estimate stage costs.

The trade price is emitted before the operator updates window state. The 5 s
out-of-order allowance holds back event-time windows, not individual price
records. The output sink is transactional, subscribers read committed data,
and the run used 10 s checkpoints. Waiting for a checkpoint commit is the
leading explanation for the visibility delay. The historical run did not
instrument per-record checkpoint wait or change its cadence.

**Estimated window eligibility → advisory receipt** — n = 3,154
| p50 | p95 | p99 | max |
|---|---|---|---|
| 240,940 ms | 241,114 ms | 241,126 ms | 241,138 ms |

The starting point is the earliest recorded enqueue among inputs with event
times high enough to make that window eligible. It is a lower bound, not a
timestamp from the Flink timer. It can run ahead of actual partition watermark
progress when event time leads the replay schedule. The separate EMA eligibility
check has 3,111 samples and a p99 of 241,192 ms; it is not the advisory series.
Historical event age (about 5.04 years at p50) is yet another metric, not a
component to add to replay-clock latency. These measurements do not isolate
timer firing to advisory receipt; that would need a timer-side wall-clock probe.

### Oracle comparison (`benchmarks/query_truth.py` over the same slice)

Expected 9,649 oracle records vs 6,308 live records; **missing 3,341, extra 0,
incorrect 0, duplicate 0** (so every live record it emitted matched the oracle).
Exhaustive split of the 3,341 missing keys:

- **policy-related 3,309** — all in a single five-minute window group; this is the
  live pipeline withholding one whole window's EMA/advisory set, consistent with
  that window not closing inside the capture/trailing window (the final window's
  close is past the end of offered capture). It is a measurement-window boundary
  effect, not a per-trade policy decision; the classifier labels it policy-related
  because the inputs arrived.
- **transport loss 18** — window keys whose committed inputs were short of the
  eligible inputs (≈0.5% of expected records). Transport loss is negligible.
- **unexplained 14** — a far-future event-time window (a data clock anomaly), kept
  visible.

The disk-backed comparator samples the first 20 mismatches; the numbers above are
from an exhaustive in-memory pass over the same JSONL files (they are small).

### Wall time and cap

First extraction command 18:07:25Z. Direct capture-order extraction (5 GB source)
18:21–18:26; failed attempt 1 18:27–18:31; valid run 18:31–~18:40 (build/start +
8.5 min emitter); oracle ~5 s; summarize/compare/ledger ~30 s. The written summary
landed at ~18:48Z, roughly **41–42 minutes**: ~1–2 minutes over the 40-minute hard
cap. Nothing was cut from the condition set (one repeat and 90 s trailing were both
kept); the overrun came from two harness defects that had to be fixed to produce
valid evidence, below.

### Harness changes required (uncommitted, working tree)

- `benchmarks/peak_slice.py`: added `--reorder capture`. The DEBS physical order
  makes the first in-range row 16:02:54.650, later than the measured start
  (15:58:01), so the physically-ordered slice fails closed exactly as designed
  (`emitter offsets do not place the measured start in the measured segment`). The
  fixture's own `timestamp_order` is capture-time order with source-ordinal ties,
  so `--reorder capture` (stable, preserving same-timestamp burst order) reproduces
  the measured membership; fail-closed fixture verification is unchanged.
- `benchmarks/live_peak.py`: added `--pacings`/`--pacings` to `plan`/`run` so the
  mandated 1x-only condition can be selected (the CLI previously always planned
  1x and 1.25x); set `GATEWAY_HISTORY_LIMIT=1` (was `0`, invalid).
- `benchmarks/capture_pilot.py`: replaced the per-window full rescan in
  `window_check` with a suffix-min index over event-time-sorted advancers
  (identical semantics; the former path was O(windows × rows) and infeasible at
  1.9 M input rows).
- `speed_multiplier` in the client record is `None` because `docker compose run`
  did not propagate `SPEED_MULTIPLIER` to the collector service; the summary takes
  it from the emitter's authoritative pacing report (`1.0`). This is noted in
  `summary.json`.

`benchmarks/matrix.py`, `benchmarks/report.py`, the `.scratch/` tickets,
`.kilo/plans/`, `docs/report-evidence.md`, `docs/evidence-audit.md`, and
`blocked.json` were not modified. Nothing was staged or committed.

### Evidence

Under `benchmarks/evidence/live-peak/`: `slices/peak-warm.csv` +
`slices/peak-warm-manifest.json` (verified), `run-plan.json`, `runs/peak-warm-1x-r1-{run,limits,pacing}.json`
and `runs/post-run-containers.txt`, `oracle-warm.jsonl`,
`committed-warm-1x-r1.jsonl`, `committed-warm-1x-r1-normalized.jsonl`,
`comparison-warm-1x-r1.json`, `comparison-warm-1x-r1-classification.json`,
`summary.json`, `ledger.json`, and the retained `blocked.json` from the earlier
host-contention attempt.

### What this result does and does not support

- It **is** a measured peak-window result: pacing ~1.00x, exact expected-vs-delivered
  delivery, no restarts/OOMs, and negligible transport loss against complete-input
  truth.
- It does **not** support a capacity or **full-week** claim. The slice is five
  minutes of the recorded peak; a peak high-water mark is not a whole-week result.
- It does **not** support a 1.25x or cold-start result (not run), a spread/confidence
  interval (one repetition), or broker/gateway consumer-lag numbers (not collected).
- Trade visibility and advisory window eligibility are separate measurements.
  The latter does not measure delivery time after the Flink timer fires.
- Confidence is **lower than a multi-repeat campaign** would give; treat the numbers
  as one valid peak-window observation of this configuration.

## Synthetic price latency comparison

The follow-up experiment changes one setting at a time: shorten the checkpoint
interval from 10 s to 1 s while retaining the 5 s watermark allowance, or shorten
the watermark allowance to 100 ms while retaining 10 s checkpoints. If commits
drive the price tail, only the first change should substantially reduce it.

The three trials run sequentially in fresh Compose projects, with 12 s warmup
and 40 s measured at 1,000 synthetic originals/s, seed 42, 200 fixed synthetic
ticker keys, six input partitions, two TaskManagers, five-minute windows,
`NONE` deduplication, `DROP` lateness, no injected duplicates or late events,
and one WebSocket subscriber. The driver measures both boundaries on the same
monotonic clock: enqueue to read-committed Kafka observation and enqueue to
WebSocket receipt. This is a price-path experiment, not an advisory-timing,
recovery, resource-cost, or Sustainable-limit experiment.

| Checkpoint interval | Watermark allowance | Enqueue to committed Kafka p50 / p95 / p99 (s) | Enqueue to WebSocket p50 / p95 / p99 (s) |
|---:|---:|---:|---:|
| 10 s | 5 s | 5.102 / 9.593 / 9.970 | 5.134 / 9.619 / 10.004 |
| 1 s | 5 s | 0.532 / 0.980 / 1.020 | 0.542 / 0.987 / 1.028 |
| 10 s | 100 ms | 5.105 / 9.589 / 9.986 | 5.127 / 9.600 / 9.991 |

All three trials achieved approximately 1,000 originals/s and matched all 40,000
expected prices at Kafka and WebSocket. Shortening checkpoints cut WebSocket p99
by about **90%**, and the improvement was already visible at Kafka. Shortening
the watermark allowance alone left the price tail near 10 s. This isolates the
checkpoint interval as the main driver **in this synthetic price experiment**
and supports the explanation for the historical peak. It does not establish the
historical peak's latency at 1 s checkpoints, checkpoint resource overhead, or
the effect of the watermark on advisory timing. Production defaults are unchanged.

[Machine-readable results and run IDs](../benchmarks/evidence/live-peak/price-latency-comparison.json).

To reproduce on an idle Docker host from the repository root:

```sh
run_latency_arm() {
  python3 benchmarks/matrix.py --project "ultrastonk-benchmark-latency-$1" \
    --duration 40 --warmup 12 --rates 1000 --parallelism 2 --dedup NONE \
    --late DROP --qualities baseline --repetitions 1 --symbols 200 \
    --ticker-prefix B --seed 42 --window-ms 300000 --gateway \
    --checkpoint-ms "$2" --max-out-of-order-ms "$3"
}
run_latency_arm baseline 10000 5000 &&
run_latency_arm short-checkpoint 1000 5000 &&
run_latency_arm short-watermark 10000 100
```

## Checkpoint interval sweep

The follow-up compares 500, 1,000, 2,500, and 10,000 ms checkpoints. It keeps the
synthetic load and pipeline settings above fixed, including the 5 s watermark
allowance and exactly-once, read-committed output. This is one fresh Compose
project per trial, two trials per interval, in ascending then descending order
to reduce run-order drift. The 12 s warmup uses separate ticker keys; each
measured 40 s interval contains 40,000 price records.

Checkpoint cost is measured as the number of successful Flink checkpoints and
the sum of the byte counts in their coordinator completion logs, divided by the
40 s measured interval. Only completions during that interval count; startup,
warmup and drain do not. The resulting bytes/s are a proxy for checkpoint work,
not physical disk writes, retained disk occupancy, CPU, or recovery time.

| Checkpoint interval | WebSocket p50 (s) | WebSocket p95 (s) | WebSocket p99 (s) | Completed/40 s | Reported checkpoint bytes/s (kB/s) |
|---:|---:|---:|---:|---:|---:|
| 0.5 s | 0.289–0.301 | 0.513–0.525 | 0.536–0.556 | 80 | 261.2–270.0 |
| 1 s | 0.540–0.543 | 0.987–0.989 | 1.027–1.030 | 40 | 116.2–118.3 |
| 2.5 s | 1.310–1.314 | 2.430–2.432 | 2.521–2.523 | 16 | 44.8–45.7 |
| 10 s | 5.117–5.131 | 9.598–9.605 | 9.989–9.998 | 4 | 11.0 |

Ranges are the two observed trial values, not confidence intervals; kB is 1,000
bytes. All eight trials delivered 40,000/40,000 prices to both committed Kafka
and WebSocket. Kafka observation p99 tracked WebSocket p99 at every cadence.

**Recommendation:** Start with **1 s** checkpoints when tuning interactive
price delivery. Compared with 10 s, it reduced WebSocket p99 by about 90%,
with roughly 11 times the reported checkpoint bytes/s. Halving the interval
again to 500 ms saved another 0.47–0.49 s at p99 but more than doubled that
byte rate (about 2.3 times). Use 2.5 s when p99 around 2.52 s is acceptable
and reducing checkpoint work matters: it used about 60% fewer reported
bytes/s than 1 s. Validate at the recorded peak and measure CPU, storage,
failed checkpoints, and recovery before changing the production default.
These short trials do not establish a Sustainable limit or advisory timing.

[Machine-readable results, run IDs and raw artifact hashes](../benchmarks/evidence/live-peak/checkpoint-interval-sweep.json).

To reproduce on an otherwise idle host, reuse the `run_latency_arm` function above:

```sh
for ms in 500 1000 2500 10000; do run_latency_arm "cadence-$ms-r1" "$ms" 5000 || break; done
for ms in 10000 2500 1000 500; do run_latency_arm "cadence-$ms-r2" "$ms" 5000 || break; done
python3 -m benchmarks.checkpoint_sweep \
  b98a673c75b1-000 37eba85c283d-000 48ce3f278e4a-000 f8d55fccf9b8-000 \
  b172bfb3ef54-000 d9223ffbd7a7-000 8cee89826676-000 aa53ce65e3dd-000
```

The command above regenerates the original summary with the original local raw
artifacts. For fresh trials, substitute the run IDs from the generated
`benchmarks/results/*-matrix.csv` files.

# Live peak-load measurement (ticket 07)

This is the preparation record and runbook for measuring the system under the
official-week peak load. **The scored phase was attempted on 2026-11-23 but could
not run: the host was not idle.** No scored measurement has been taken, and
nothing here is a capacity, latency or correctness result. See
[Scored results](#scored-results-2026-11-23--not-executed) below. The scored runs
are a second phase, triggered only when the host has no other scored workload
running.

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

1. **Two independent project targets.** Trade enqueue-to-WebSocket and
   window-finalization-to-advisory-receipt are measured and reported separately;
   one never stands in for the other. Each needs its own minimum sample count
   (`30` and `20`); below it the result is reported `inconclusive`.
2. **Live advisory p99 is reported `inconclusive` when signals are too sparse**,
   never fudged from a handful of crossovers. A controlled rollover fixture is
   reported on its own line, never merged into the live advisory number.
3. **Project targets** (`1 s` p99) are project goals, **not rubric
   requirements**; they are recorded, not asserted as pass/fail of the project.
4. **Correctness vs the oracle.** The live output is compared against
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

## Scored results (2026-11-23) — not executed

The scored phase was attempted on 2026-11-23 on branch `integration/report-evidence`
under a hard 40-minute wall-clock cap, but **no measurement was taken: the host was
not idle.** The runbook requires the scored phase to run on an otherwise idle host,
and the pre-start check failed: `docker ps` showed a foreign, actively-running
Compose project, `ultrastonk-fullweek-3` (ticket 06's full-week reference replay, in
the `full-week-queries` worktree), present for the whole window. Per the idle-host
rule the run was held and re-checked rather than started on top of the other
workload, and that project was never touched. `ps -ef | grep sort_week` was empty.

The foreign project's measured work finished early (`run3/evidence/compare.json`
written 17:50Z), but its TaskManagers entered a crash/restart loop (no JobManager
reachable) and stayed Up with Kafka and the gateway for the rest of the window, so
the host never became idle. With a single warm 1x run needing ~8.5 min of capture
plus startup/drain and the slice extraction, no valid scored run fitted inside the
remaining cap once the host freed.

### Intended downscale (pre-registered conditions unchanged; campaign size reduced)

- capture pace `SPEED_MULTIPLIER=1`, `PACING=capture`, **warm state only** — no 1.25x
  and no forced-cold variant;
- pre-roll **120 s** and trailing **90 s**, overriding the 300 s / 180 s defaults;
- `FLINK_BOUNDED_REPLAY=false` — a live paced replay, not preload-and-process;
- repeats: **two if one repetition costs ≤ 5 min wall clock, otherwise one.** Not
  decided, because no repetition ran;
- the measured peak segment is unchanged: the `1,223,873`-row five-minute slice with
  the fixture's FR/NL/ETR mix, which the extractor asserts and fails closed on.

### What was and was not produced

No slice was extracted and no run was executed (extraction is itself a scored-phase
host-I/O step and was deliberately not run on a contended host), so none of the
scored evidence exists: no slice CSVs or manifests, no `run-plan.json` (which
depends on the manifests), no `runs/` artifacts, no `ledger.json` entries, no oracle
output, no comparison JSON and no `summary.json`. The only artifact is
`benchmarks/evidence/live-peak/blocked.json`, which records the host-contention
evidence and the re-check timeline.

### What this result does and does not support

- It does **not** support any pacing, expected-vs-delivered, backlog, drain,
  restart/OOM, latency (trade or advisory) or oracle-correctness claim: there are no
  numbers. The live advisory p99 is not merely sparse but absent, so it is
  **inconclusive** for lack of any run, not for lack of samples.
- It does **not** support a capacity claim, and a peak high-water-mark on this slice
  would not have been a full-week claim even had it run; the slice is five minutes of
  capture order, not the full week.
- It **does** record that the peak-window configuration could not be scored inside
  the imposed 40-minute cap on this host because the host was occupied by another
  scored workload, which is exactly the contended phase this ticket is split out to
  avoid.

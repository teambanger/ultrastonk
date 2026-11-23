# Compiled evidence for the report

This is the single document the report is written from. Every claim below carries its artifact path and
a confidence label, so a writer can copy a number into the report and a reader can trace it back.

Confidence labels used throughout:

| Label | Meaning |
|---|---|
| **MEASURED** | Produced by a run whose raw artifacts are retained and were re-checked against the summary numbers. |
| **MEASURED (prose-only)** | Reported by a run whose raw artifact was not retained; the number is in a committed evidence note only. |
| **DERIVED** | Computed from MEASURED artifacts by a script in this repository (for example the capacity model). |
| **PREDICTED** | Output of the capacity model; a bound or interpolation between measured points, not a validated law. |
| **PENDING** | Ticket not yet finished at the time of writing; the section states what will fill it. |
| **REDUCED / DEFERRED** | Cut or narrowed by an explicit team decision, recorded in `.kilo/plans/ultrastonk-report-evidence-spec.md` under "Scope Reduction" and "Goal Reframing". |

An independent audit of these claims, including the checks that did *not* pass, is in
`docs/evidence-audit.md`. The machine-readable index is `benchmarks/evidence/README.md`.

---

## 1. What the system is

Ultrastonk is a streaming pipeline over the official DEBS 2022 Grand Challenge trading capture:

```
CSV capture ──► event emitter (capture-paced replay) ──► Kafka (recorded order, exchange-timed payloads)
                                                   │
                                                   ├─► Flink query (5-minute windows per symbol)
                                                   │      • window closing price
                                                   │      • EMA(38) and EMA(100)
                                                   │      • BUY/SELL advisory on EMA crossover
                                                   │      • late-event policy: SIDE_OUTPUT (audit) / DROP / GRACE
                                                   │
                                                   └─► WebSocket gateway (read-committed fan-out)
                                                          • live subscriptions + history on demand
```

Components and the artifacts that prove each part works:

| Component | Evidence | Confidence |
|---|---|---|
| Event emitter with capture-paced replay mode | `docs/capture-pilot-evidence.md`, `benchmarks/capture_pilot.py`, `benchmarks/fixtures/capture-pilot.csv` | MEASURED (prose-only numbers) |
| Flink query: window price, EMA pair, crossover advisory | `flink/src/main/java/fi/ultrastonk/PriceIndicators.java` + `PriceIndicatorsTest.java`; live proof `benchmarks/evidence/live-advisory/` | MEASURED |
| Independent oracle for the same query | `benchmarks/query_truth.py`, `benchmarks/query-truth.md` | MEASURED |
| Late-event policy comparison (DROP vs GRACE) | `benchmarks/evidence/drop-vs-grace/comparison.json` + four arm files | MEASURED |
| Gateway fan-out, live vs history separation | `benchmarks/evidence/live-advisory/browser-live.png`, `comparison-live.json` | MEASURED |
| Whole-week processing, once with fresh topics | `run` artifacts under `/mnt/shared/data/ultrastonk-full-week/run2/` | MEASURED |
| Capacity and scaling model | `benchmarks/capacity_model.py`, `docs/capacity-model.md` | DERIVED / PREDICTED |

---

## 2. The dataset (workload definition)

Source: Zenodo record `10.5281/zenodo.6382482`, seven daily CSVs for 2021-11-08…2021-11-14,
24,874,180,296 bytes total, CC BY-NC-SA 4.0. All seven files verified against the official MD5s;
`scripts/official-week-manifest.json` (SHA-256 `b2cf2fb7…`) records them.

| Property | Value | Artifact |
|---|---|---|
| Rows read from the capture | 289,116,808 | `emitter.log` (`Total:` line) |
| Rows emitted to Kafka | 289,049,752 (67,049 rows have no valid event time, 7 unparsable) | `emitter.log` |
| Provisional trade rows (strictly positive `Last`) | 20.55% of rows | `results/week-profile.json` (SHA-256 `c6c38203…`) |
| Instrument split | securities 239,521,477 / indices 49,595,331 | `results/week-profile.json` |
| Exchange split | FR 158,331,078 / NL 79,991,190 / ETR 50,794,540 | `results/week-profile.json` |
| Concentration | top-1 symbol 2.39%, top-10 15.71%, top-100 58.53% | `results/week-profile.json` |
| Highest rolling five-minute raw-row rate | 1,223,873 rows in `2021-11-09T15:58:01→16:03:01`, ≈4,079 rows/s | `results/week-profile.json`, `results/week-fixture.json` |
| Highest trade-window activity | 130,524 rows in `2021-11-10T17:34:37→17:39:37`; 23,016 rows in the single second `17:40:00` | `results/week-rates.jsonl` |
| Rows without a parseable system timestamp | 67,056 (excluded, and counted as such) | `results/week-profile.json` |

**What the report should say:** the workload is not uniform. It is a snapshot-style feed with a long
symbol tail, a bursty five-minute peak, and a 20% trade minority; the peak is about 4,079 raw rows/s,
which is the number every latency and capacity claim is measured against, and any synthetic rate above
that is an extrapolation, not the recorded workload.

**Caveat to state:** "capture time" means the CSV's recorded system timestamp, not a proven network
capture instant, and the paper's "CEST" wording conflicts with this post-DST week, so no absolute
timezone conversion is claimed (`docs/dataset-profile.md`).

---

## 3. Functionality — what is proven to work

### 3.1 The query is exactly reproducible from the raw capture (independent oracle)

`benchmarks/query_truth.py` re-derives window closes, EMA(38)/EMA(100) and crossovers from the CSV with
no Flink involvement; `benchmarks/query_truth_compare.py` compares them field by field. The oracle was
validated against hand-calculated windows before being used as truth (`benchmarks/query-truth.md`).

### 3.2 Live advisories reach real subscribers

| Claim | Number | Artifact |
|---|---|---|
| Read-committed output records checked | 148 | `benchmarks/evidence/live-advisory/comparison.json` |
| Missing / extra records | 0 / 0 | same |
| Live WebSocket subscribers | 3 | `browser-live.png`, `frames.json` |
| Advisory sequence observed | BUY, SELL, BUY with both EMA pairs and trigger windows shown | `browser-live.png` |
| Window-close and advisory boundary behaviour | operator tests at the equality boundary, mutation-checked | `flink/src/test/java/fi/ultrastonk/PriceIndicatorsTest.java` |

**Limits to state:** that live proof is a synthetic accelerated-time run (1 s windows, `NONE`/`DROP`)
with uncapped containers; checkpoint restore was exercised only at the operator seam; and one 1,000 ms
source-idle timeout needed a successful 30,000 ms retry (`benchmarks/evidence/live-advisory/`).

### 3.3 Full-week correctness: exact on complete input in the labeled reference mode

This is the headline correctness result and it must be quoted with its mode label.

| Result | Number | Artifact |
|---|---|---|
| Ordered reference replay, one full day (2021-11-08) | expected = actual = **377,966 records**, 0 missing, 0 extra, 0 numeric, 0 duplicate, 0 wrong-direction | `<run3>/evidence/compare.json` |
| Final window agreement | identical (`1636407300000`) on both sides | same |
| Per-exchange agreement | ETR 114,763 / FR 211,427, all exact | same |
| Capture-order whole-week run | input accounting exact: topic LEO **289,049,752** = emitter submitted | `<run2>/evidence/offsets.json` |
| Capture-order deviations | **10,669,014** eligible late exclusions; 376,584 missing closes; 1,482,724 numeric; 6,228 extra; 0 duplicate; 0 wrong-direction; expected 1,868,812 vs 1,498,456; final window equal | `<run2>/evidence/compare.json` |

**What the report should say:** the query logic is exact on complete input when the day is replayed in
event-time order (one full day, 377,966 records, zero mismatches). Replaying the raw capture in recorded
order loses ~3.7% of eligible trades to the 5 s out-of-order budget and that single lost close
propagates through the EMA chain, which is why 1.48 M derived values also differ. This is a property of
the source ordering, not a defect in the query, and the pipeline reports those records as late
exclusions rather than silently dropping them.

**REDUCED:** the ordered reference covers one day, not the week (team decision to fit the time budget).
Correctness on complete input therefore rests on the day-08 reference plus ticket 04's verified slice
(1,605,199 rows, 250,716 eligible trades, oracle exact including the final window).

### 3.4 The lateness-policy trade-off is measured, not asserted

Matched DROP-versus-GRACE comparison on one scripted fixture (digest `f481616b…`, seed 11, 300 s windows,
5 s disorder budget, 30 s grace, `DEDUP=NONE`, parallelism 2), two replicates per arm, only
`LATE_EVENT_STRATEGY` differing:

| Arm | Committed records | Policy defects | Loss against complete-input truth | Event-time finalization delay | Checkpoint state |
|---|---|---|---|---|---|
| DROP | 34 | 0 | 1 missing advisory (BUY@31) + 2 wrong EMA values | 0 ms | 78,026 → 79,054 bytes |
| GRACE | 35 | 0 | 0 | 30,000 ms | 79,126 → 83,398 bytes |

Artifact: `benchmarks/evidence/drop-vs-grace/comparison.json` plus `arm-R1/R2-*.json`.
Recommendation recorded there: default to GRACE, reserving DROP for workloads where no eligible late
trade can change a closing price.

**Limits:** scripted synthetic disorder, one late window per arm, no per-service caps applied
(`resources` empty), and the measured wall-clock finalization figures (10,173/804 ms and 10,244/85 ms)
are artifacts of the fixture, not latency claims.

---

## 4. Performance — how the system behaves under load

### 4.1 Whole-week end-to-end (direct measurement)

| Quantity | Value | Artifact |
|---|---|---|
| Input rows processed | 289,049,752 | `<run2>/evidence/offsets.json`, `emitter.log` |
| Emitter throughput | 545,563 rows/s, 0 failed, one pass | `<run2>/logs/emitter.log` |
| Flink job window | 428.031 s (`RUNNING 16:45:44.953 → FINISHED 16:52:52.984`) | `<run2>/logs/jobmanager.log` |
| Implied aggregate processing rate | 675,301 rows/s | DERIVED (`docs/capacity-model.md`) |
| Output | 62,381,131 committed records (dumps reclaimed after audit; counts retained in `offsets.json` and `compare.json`) | `<run2>/evidence/offsets.json` |
| Broker state on disk | 16,507,840,934 bytes | `<run2>/logs/kafka_data_bytes.txt` |
| Storage per unit | 57.11 MB per million rows ingested; 18.28 bytes output per row; 0.216 output records per row | DERIVED |

**What the report should say:** the official week processes end-to-end in about **7 minutes of
processing** (428 s) after the capture has been loaded, at a rate that is ~165× the recorded peak rate —
so the recorded workload is far inside capacity in batch terms.

**Caveat:** that run overlapped nothing else on the measured host, but it is a single repetition with
uncapped containers, and 165× is a batch-drain bound, not a live-latency headroom claim.

### 4.2 Behaviour at the recorded peak (MEASURED, one repetition)

Mode: 1× capture pace (`SPEED_MULTIPLIER=1`, `PACING=capture`), warm state, one repetition,
`FLINK_BOUNDED_REPLAY=false`. The offered slice is the 5-minute peak plus support: 1,914,978 rows =
300 s pre-roll + the 1,223,873-row measured segment + 180 s trailing, offered in the fixture's
documented capture-time order (ties broken by source ordinal) because the raw physical order starts
after the measured start — a harness property that must be stated, since it is not the physical file
order. Artifacts: `benchmarks/evidence/live-peak/` (`summary.json`,
`comparison-warm-1x-r1.json`, `runs/peak-warm-1x-r1-pacing.json`, `slices/peak-warm-manifest.json`,
`ledger.json`) and `docs/live-peak.md`.

| Quantity | Result |
|---|---|
| Achieved pacing, measured segment | **0.99991** (max lag 35 ms); pre-roll 0.99983, trailing 0.99980 |
| Measured segment reproduced | 1,223,873 rows, FR 651,418 / NL 411,885 / ETR 160,570 — verification passed |
| Price records: expected = committed = WebSocket | **195,061 = 195,061 = 195,061**, zero missing, zero extra, no EMA-pair gaps |
| Trade enqueue→WebSocket (n=195,061) | p50 **5,060 ms**, p95 9,063 ms, p99 **9,225 ms**, max 10,171 ms |
| Committed→WebSocket (transport only) | p50 **15.5 ms**, p99 134 ms |
| Enqueue→broker acknowledgement (n=5,000) | p50 34 ms, p99 58 ms |
| Window finalization→advisory receipt (n=3,154) | p50 240,962 ms, p95 241,187 ms, p99 241,192 ms |
| Drain | 7.08 s |
| Restarts / OOM kills | 0 / 0 |

**Interpretation the report must carry:** the enqueue→WebSocket figure is dominated by the deliberate
5 s out-of-order budget held before windows are finalized, not by transport — the transport-only leg is
p50 15.5 ms / p99 134 ms and broker acknowledgement is p50 34 ms. The advisory figure is dominated by
the historical event time versus the replay's capture clock (event age p50 ≈ 158,988,927,518 ms, about
5.04 years), with watermark waiting reported separately (p50 −218,592 ms, max 4,853 ms). Consequently the
**1 s p99 project target is not met for the trade path, and as stated it is not achievable** while a 5 s
disorder budget is in force: the target and the budget conflict, and the honest reading is that transport
and acknowledgement are sub-100 ms while end-to-end visibility is bounded by the chosen watermark policy.

**Oracle comparison on the slice:** expected 9,649 window keys versus 6,308 live records — missing 3,341,
extra 0, incorrect 0, duplicate 0; every live record matched. The exhaustive split is policy 3,309 /
transport 18 / unexplained 14, and all 3,341 missing keys sit in the single final window, which had not
closed inside the offered capture — a measurement-window boundary effect, not per-trade loss. True
transport loss is therefore about 0.5% of window keys (`comparison-warm-1x-r1-classification.json`).

**Gaps to state:** Kafka broker lag and gateway consumer-group lag were **not collected** (missing
telemetry, recorded in the summary); the Flink input−output gap is structural because output is per-window
rather than per-trade; index, slice CSV, per-run raw JSON and the committed-record dump stay outside Git by
design (`.gitignore`), with their manifests and checksums committed instead.
**REDUCED:** one repetition (no spread), 1× only (no 1.25×), warm only (no cold), one five-minute peak
window, and the run finished ~1–2 minutes over its 40-minute cap. Harness fixes this required
(`peak_slice.py --reorder capture`, `live_peak.py --pacings` and a gateway history-limit correction, and a
quadratic `capture_pilot.window_check` replaced by a suffix-min index) are committed with the evidence,
because the measured numbers depend on them.

### 4.3 Capture-paced end-to-end measurement path

Ticket 05 proved the measurement path itself on a 158-row fixture whose capture clock trails event time
by 250 ms: achieved pacing **1.00034** against a 0.98 threshold, and exact multiset agreement at ingest,
committed Kafka and WebSocket (158 prices + 10 EMA records, no missing, extra or duplicate). Historical
event age (~1.59e11 ms) and watermark waiting (p50 400 ms) are reported separately from transport
latency, and the pilot fails closed on missing telemetry, insufficient pacing or delivery mismatch.

**Confidence: MEASURED (prose-only)** — the audit found no retained raw run artifact for the pacing and
latency figures, only the committed evidence note. Either regenerate the pilot's raw JSON before the
report is finalised or label these numbers as note-only.

---

## 5. Scalability — volume and worker scaling

**MEASURED (trimmed, one repetition).** One ladder of offered rates at one and two TaskManagers with
pre-registered, test-pinned verdict rules (`benchmarks/evidence/sustainable-limit/pre-registered-rules.json`),
per-point delivery correctness, backlog troughs, tails, drain, throttling, memory and aggregate worker CPU,
and an explicit `unbracketed` outcome. Headline: **the bracket did not separate** — the limit is
unbracketed, 8,000/s is only a floor, and a second worker added no throughput at these rates.

**REDUCED:** a three-point ladder (1,000/4,000/8,000) instead of four-to-five, one repetition per point
(no repeat spread), no fine confirmation run, and no 1.25×/cold conditions. Per-worker caps are fixed, so
the two-worker point uses twice the aggregate quota; that is reported per point rather than hidden. A
planned 500/s point was dropped from the trimmed set.

**One integration defect this ticket found and fixed:** the benchmark driver's output contract rejected the
`BUY` advisory records that ticket 03 added to the query, which made every point FAIL delivery on a
contract violation rather than on lost data. The contract now accepts `BUY`/`SELL`, counts them separately
from prices and EMAs, includes them in the WebSocket catch-up wait, and requires the WebSocket side to
cover the committed advisory counts (count-level, not signature-level). The pre-fix run is preserved as
`bracket-attempt2-contract-mismatch.json`, and an even earlier attempt that accidentally ran with **no**
resource caps (`perworker=` passed literally) is preserved as `bracket-attempt1-uncapped.json` rather than
deleted.

### 5.1 Measured volume and worker ladder (MEASURED, one repetition, trimmed)

Trimmed bracket: rates 1000/4000/8000 originals/s at one and two TaskManagers, fixed per-worker cap
`taskmanager:1:2g, kafka:2:2g, gateway:1:512m, benchmark:2:1g`, 20 s runs, 5 s warmup, one repetition,
monitoring on, pinned pre-registered verdict rules, six points in two batches, 5 m 41 s end to end.
Artifacts: `benchmarks/evidence/sustainable-limit/{bracket.json,run-ledger.jsonl,run-console-attempt3.log}`
and raw matrix sessions `benchmarks/results/6092fc46e099-*`, `eb9c21b179e8-*`.

| Workers | Offered/s | Verdict | Achieved input | Committed/s = WS/s | visible p99 / max (ms) | Drain (s) | Worker CPU / quota |
|---|---|---|---|---|---|---|---|
| 1 | 1000 | FAIL (backlog rule) | 999.996/s (99.9996%) | 952.9 | 1,116 / 1,298 | 0.13 | 0.999 / 1 core, throttled |
| 1 | 4000 | PASS | 3,999.99 (99.9997%) | 3,937.1 | 1,129 / 1,310 | 0.72 | 0.997 / 1 core, throttled |
| 1 | 8000 | PASS | 7,999.97 (99.9997%) | 7,666.9 | 3,361 / 3,910 | 0.38 | 0.999 / 1 core, throttled |
| 2 | 1000 | FAIL (backlog rule) | 999.997 (99.9997%) | 968.6 | 1,070 / 1,231 | 0.42 | 1.997 / 2 cores, throttled |
| 2 | 4000 | PASS | 3,999.99 (99.9997%) | 3,888.5 | 1,793 / 2,136 | 0.51 | 1.998 / 2 cores, throttled |
| 2 | 8000 | PASS | 7,999.98 (99.9997%) | 7,793.2 | 3,530 / 4,447 | 0.58 | 2.000 / 2 cores, throttled |

Delivery was exact at every point: zero missing or unexpected prices on Kafka and on WebSocket, zero
missing EMAs, and advisory counts matching the committed side (`websocket_missing_advisories = 0`);
committed→WebSocket backlog was zero throughout and no producer backpressure occurred.

**Findings the report must carry, stated exactly:**

1. **The sustainable limit is not bracketed.** Both worker counts are `unbracketed` and
   non-monotonic: the highest passing load is 8,000/s while 1,000/s FAILs, because the pinned backlog
   rule judges growth from the first third of the series and the `elapsed = 0` startup sample drags that
   mean down. The raw per-checkpoint troughs are flat at 1,000/s (≈942–950 at one worker, ≈627–630 at
   two), so the FAIL is a **startup-transient artifact of the pinned rule, not lost or duplicated data
   and not missing telemetry**. Consequence: within the tested range 8,000/s is a *floor*, not a ceiling,
   and no sustainable-limit number can be quoted from this ladder.
2. **Doubling the workers bought no throughput: 0.988× at 4,000/s and 1.016× at 8,000/s** (i.e. per-worker
   scaling efficiency ≈ 0.50). Both counts saturate their worker CPU quota (throttled ratio 1.0) yet
   deliver the same ~7.7–7.9 k/s, which points at a shared component — the single 2-core Kafka broker,
   the single benchmark driver/consumer, or the gateway — rather than worker CPU. The 1.5× stretch check
   is therefore **not met**, and the honest claim is "more TaskManagers did not raise measured throughput
   in this configuration", not "the system does not scale".
3. **Caveats that limit what this supports:** per-worker caps are fixed, so the two-worker point uses twice
   the aggregate quota (2 cores / 4 GiB versus 1 core / 2 GiB) — the comparison is directional and not
   iso-resource; one repetition per point means no spread; no rates between 1,000–4,000 or above 8,000
   were probed; checkpoint failures of 4–6 per run occurred with zero job restarts and are **unexplained**;
   and the workload is synthetic (1,000 symbols, Zipf 1.2, 1 s windows) rather than the real week's long
   tail. "Sustainable limit" in the glossary sense is therefore **not established** by this ladder.

### 5.2 What the model predicts (DERIVED / PREDICTED)

`benchmarks/capacity_model.py` now ingests both the measured week run and the six ladder points, and
reports a per-worker service rate of 337,650 rows/s with the week at 675,301 rows/s on two workers.

| Workload | Workers | Processing wall (prediction) | Range | Resource envelope |
|---|---|---|---|---|
| Official week (289.05 M rows) | 2 | 428 s (the measured point) | 342–514 s | 2 cores / 4 GiB, 15.37 GiB broker |
| Official week | 1 | 856 s | 428–856 s | 1 core / 2 GiB, 15.37 GiB broker |
| 3× the week (867.15 M rows) | 2 | 1,284 s | 1,027–1,541 s | 2 cores / 4 GiB, 46.12 GiB broker |
| 3× the week | 1 | 2,568 s | 1,284–2,568 s | 1 core / 2 GiB, 46.12 GiB broker |

Read together with §5.1, the prediction carries a warning the report must repeat: because the ladder
shows no throughput gain from a second worker at these rates, the "one worker takes about twice as long"
half of this table is a monotone bound, not a measurement, and the two-worker figures are the ones
actually measured. A model field named `scaling_efficiency` currently reports ≈0.99, which is the
throughput *ratio* between two workers and one, **not** an efficiency fraction (the per-worker efficiency
is ≈0.50); it must not be quoted as an efficiency without that clarification.

---

## 6. Evaluation and performance analysis (the argument, not the numbers)

1. **Measured workload, not assumed.** Peak, trade share, symbol concentration and per-day volumes come
   from the official capture itself, so every later claim is anchored to a real number rather than a
   round one.
2. **Correctness separated from ordering.** The pipeline is exact on complete input in ordered replay
   (377,966/377,966 on a full day) while the recorded order costs ~3.7% of eligible trades; both facts
   are reported, and the harness *rejects* rather than passes when accounting does not close.
3. **The gate that caught a false pass.** The first whole-week attempt was invalidated because the input
   topic held a 53,635,606-record stale prefix (a bind mount survived `docker compose down -v`); the
   stale prefix was identified from the broker's per-partition startup snapshots and the run was
   discarded and preserved as gate-failure evidence. The harness now asserts an empty topic before the
   emitter and exact input accounting after it, so a polluted run cannot reach the comparison stage.
4. **Capacity answer.** The week processes in ~7 minutes at ~675 k rows/s — roughly 165× the recorded
   peak rate — with a predicted envelope for larger volumes; the live headroom question is what ticket 07
   answers.
5. **Limits that must be stated:** single repetitions on part of the ladder, uncapped containers in the
   week run, one-day ordered reference, no latency prediction until 07 lands, and synthetic workloads for
   the policy and growth experiments.

---

## 7. Creative explorations

Each item is an extension beyond the minimal brief, with its artifact:

- Independent oracle and comparator, validated before use as truth (`benchmarks/query_truth*.py`).
- Capture-order versus event-time-ordered replay as a first-class distinction, with the deviation
  quantified rather than hidden (ticket 06).
- Policy-aware expectations in the DROP/GRACE comparison, so expected policy differences are separated
  from processing defects (`benchmarks/evidence/drop-vs-grace/comparison.json`).
- A capacity model that turns two measured points into wall-time and resource predictions with ranges
  (`docs/capacity-model.md`).
- A live terminal dashboard that reads container, replay, Kafka and Flink state in one view
  (`scripts/stack_tui.py`) — used during development to catch the Kafka ownership crash-loop.
- An independent evidence audit that checks summary numbers against raw artifacts and reports the gaps
  (`docs/evidence-audit.md`).

---

## 8. Reduced scope and what is not claimed

Recorded in full in `.kilo/plans/ultrastonk-report-evidence-spec.md`:

- The event-time-ordered replay of the whole week was dropped; the bounded one-day ordered reference
  replaces it (ticket 06).
- Peak measurement reduced to 1× and warm only with fewer repeats (ticket 07).
- Capacity ladder reduced to a single repetition and four-to-five points (ticket 08).
- Symbol-distribution and subscriber-growth experiments are **deferred** as outside the reframed goal
  (tickets 09, 10); they do not change week-processing time or the resource envelope.
- Latency predictions are absent until ticket 07 reports; the model carries a PENDING latency path.

Never claimed anywhere in this pack: a validated capacity law, a live Sustainable limit before ticket 08,
a latency figure before ticket 07, absolute wall times without a range, or strategy profitability.

---

## 9. Figures and tables for the 4–5 page ACM report

| # | Figure / table | Source artifact | Status |
|---|---|---|---|
| F1 | Pipeline architecture | `docs/module-integration.md` | ready |
| F2 | Dataset shape: rows/day, exchange mix, symbol concentration | `results/week-profile.json` | ready |
| F3 | Peak-rate profile with the 1,223,873-row five-minute peak | `results/week-rates.jsonl` | ready |
| T1 | Full-week coverage and error table (capture order vs ordered day) | `<run2>`, `<run3>` `compare.json` | ready |
| T2 | DROP vs GRACE trade-off (correctness, delay, state) | `benchmarks/evidence/drop-vs-grace/comparison.json` | ready |
| T3 | Volume/worker ladder and Sustainable-limit bounds | `benchmarks/evidence/sustainable-limit/` | PENDING (08) |
| T4 | Latency tails at the recorded peak with sample sizes | `benchmarks/evidence/live-peak/` | PENDING (07) |
| F4 | Predicted wall time and resource envelope versus volume | `docs/capacity-model.md` | ready |

---

## 10. Contribution and teamwork mapping

Ownership as recorded in the tickets (each member fills their own three rubric reflections):

| Ticket | Scope | Owner |
|---|---|---|
| 01 | Oracle and comparator | repo history, `benchmarks/query-truth.md` |
| 02 | Dataset profiling and peak fixture | `docs/dataset-profile.md` |
| 03 | Live advisories to subscribers | `benchmarks/evidence/live-advisory/` |
| 04 | Reference-slice reconciliation | `benchmarks/reference-slice.md` |
| 05 | Capture-paced measurement path | `docs/capture-pilot-evidence.md` |
| 06 | Whole-week processing and correctness | `/mnt/shared/data/ultrastonk-full-week/`, `INVALID-RUN.md` |
| 07 | Peak behaviour | PENDING |
| 08 | Volume and scaling | PENDING |
| 09, 10 | Growth experiments | DEFERRED |
| 11 | Lateness policy comparison | `benchmarks/evidence/drop-vs-grace/` |
| 12 | Audit and packaging | `docs/evidence-audit.md`, `benchmarks/evidence/README.md` |

Cross-review evidence to collect before submission: a teammate reproduces one table or figure from this
pack alone, and the run ledgers retain failed and inconclusive attempts.

---

## 11. How to regenerate

```sh
# whole suite
python3 -m unittest discover -s benchmarks/tests

# dataset profile and peak fixture (ticket 02)
python3 scripts/profile_week.py --data-dir /mnt/shared/data/official-week-Ij8jlfeA --output-dir <out>

# capacity and volume predictions (DERIVED)
python3 benchmarks/capacity_model.py

# audit of this pack
sed -n '1,80p' docs/evidence-audit.md
```

Raw capture data, broker contents and scratch databases stay outside Git by design; the manifests and
checksums that identify them are committed, and `docs/evidence-audit.md` lists every number in this
document that currently relies on a note rather than a retained raw artifact.

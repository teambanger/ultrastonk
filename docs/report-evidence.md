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
| Output | 62,381,131 committed records; 5,282,388,854 bytes dumped | `<run2>/evidence/offsets.json`, `output.jsonl` |
| Broker state on disk | 16,507,840,934 bytes | `<run2>/logs/kafka_data_bytes.txt` |
| Storage per unit | 57.11 MB per million rows ingested; 18.28 bytes output per row; 0.216 output records per row | DERIVED |

**What the report should say:** the official week processes end-to-end in about **7 minutes of
processing** (428 s) after the capture has been loaded, at a rate that is ~165× the recorded peak rate —
so the recorded workload is far inside capacity in batch terms.

**Caveat:** that run overlapped nothing else on the measured host, but it is a single repetition with
uncapped containers, and 165× is a batch-drain bound, not a live-latency headroom claim.

### 4.2 Behaviour at the recorded peak

**PENDING —** ticket 07 (peak-load measurement) is running at the time of writing. It will fill: achieved
capture pacing versus schedule, expected-versus-delivered counts by subscription, p50/p95/p99 with sample
sizes for trade-enqueue→WebSocket and window-finalization→advisory receipt independently, backlog and
drain, restarts/OOMs, and the oracle comparison with policy-related deviations split from transport loss.
Artifacts will be `benchmarks/evidence/live-peak/` and `docs/live-peak.md`.

**REDUCED:** 1× capture pace and warm state only; 1.25× and the cold variant were dropped; repeats
reduced by measured run cost; pre-roll 120 s, trailing 90 s.

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

**PENDING —** ticket 08 is queued behind ticket 07 and will fill this section with one ladder of offered
rates (500/1000/2000/4000 originals/s, adding 8000 only if all lower points pass) at one and two
TaskManagers with a single repetition each, per-point delivery correctness, backlog troughs, tails, drain,
throttling, memory and aggregate worker CPU, and either highest-passing/first-failing bounds or an
explicit `unbracketed` reason. The verdict rules were pre-registered and pinned by a test:
`benchmarks/evidence/sustainable-limit/pre-registered-rules.json`.

**REDUCED:** single repetition per point and a four-to-five point ladder, so the outcome is a directional
bound, not the three-repeat bracket originally planned. Per-worker caps are fixed, so total resources
grow with worker count and that is reported per point rather than hidden.

### 5.1 What scaling can be predicted today (DERIVED / PREDICTED)

`benchmarks/capacity_model.py` fits the measured week run and interpolates or bounds the rest:

| Workload | Workers | Prediction (processing wall time) | Range |
|---|---|---|---|
| Official week (289.05 M rows) | 2 | 428 s (the measured point) | 342–514 s |
| Official week | 1 | 856 s | 428–856 s (monotone bound) |
| 3× the week (867.15 M rows) | 2 | 1,284 s | 1,027–1,541 s |
| 3× the week | 1 | 1,284–2,568 s | bound |
| Half the week | 2 | 214 s | 171–257 s |

Per-worker service rate implied by the measured point: 337,650 rows/s. Resource envelope: worker CPU/mem
quota per point, broker disk `N × 57.11 MB per million rows`, output `N × 18.28 bytes`.

**What the report must say:** these are bounds or interpolations between measured points with a single
repetition, so they are directional. Scaling efficiency between one and two workers stays PENDING until
ticket 08 reports its second worker count; the 1.5× worker-capacity stretch check applies only where
worker CPU saturation shows Flink is the limiter.

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

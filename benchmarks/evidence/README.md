# Evidence pack index

This is the index for the Ultrastonk report-evidence pack (tracker
`.scratch/ultrastonk-report-evidence/issues/`, spec
`.kilo/plans/ultrastonk-report-evidence-spec.md`). It lists, per ticket, the
question it answers, the exact artifacts, the reproduce command, the
mode/configuration used, the measured headline numbers and the stated limits.

Status legend: **done** = artifact exists and was checked; **partial** = the run
exists but a required acceptance step is missing; **pending** = no measurement
yet, recorded as a plan only.

Paths in this file are relative to the repository root unless marked absolute.
Raw CSVs, Kafka data and scratch databases deliberately live **outside Git** and
are not published.

---

## Ticket 01 — Verify query truth on a hand-checked CSV (done)

- **Question:** Does an independent CSV oracle reproduce the exact EMA38/EMA100
  and BUY/SELL results the query should produce?
- **Artifacts:** `benchmarks/query_truth.py`, `benchmarks/query_truth_compare.py`,
  `benchmarks/query-truth.md`, `benchmarks/fixtures/query-truth.csv`,
  `benchmarks/tests/test_query_truth.py`, `benchmarks/tests/test_query_truth_compare.py`.
- **Reproduce:**
  ```sh
  python3 benchmarks/query_truth.py --timezone UTC benchmarks/fixtures/query-truth.csv
  python3 -m unittest discover -s benchmarks/tests -p 'test_query_truth*.py' -v
  ```
- **Mode/configuration:** complete-input batch interpretation; 5-minute
  midnight-aligned windows; zero-initialized EMA pairs; `abs_tol=1e-9`,
  `rel_tol=1e-9`.
- **Headline numbers:** oracle reads raw CSV without production parsers; worked
  example `ABC.X` closes at 39 → EMA38 `2`, EMA100 `0.7722772277227723`, BUY.
  Comparator `(symbol, window, type)` with exact direction and `isclose` EMA.
- **Limits:** complete-input truth only — not a live-policy expectation. The
  default comparator holds inputs in memory; use `query_truth_compare.py --db`
  for week scale.
- **Artifact-check status:** files present. The "12 tests passed" claim in the
  ticket was not independently re-run in this audit.

## Ticket 02 — Profile the official week and the real peak (done)

- **Question:** What is the verified seven-day dataset profile and the peak
  fixture, and is the peak selection reproducible?
- **Artifacts:** `scripts/profile_week.py`, `scripts/profile_fixture.py`,
  `scripts/official-week-manifest.json`, `docs/dataset-profile.md`.
  Generated (outside Git): `/mnt/shared/data/official-week-Ij8jlfeA/results/week-profile.json`
  and `.../week-fixture.json`.
- **Reproduce:**
  ```sh
  python3 scripts/profile_week.py --manifest scripts/official-week-manifest.json \
    --data-dir /path/to/private-data --scratch-dir /path/to/scratch \
    --output /path/to/profile.json --series /path/to/rates.jsonl
  python3 scripts/profile_fixture.py --profile /path/to/profile.json \
    --manifest scripts/official-week-manifest.json --data-dir /path/to/private-data \
    --scratch-dir /path/to/scratch --output /path/to/fixture.json
  ```
- **Mode/configuration:** bounded-memory scan of all seven Nov 8–14 2021 CSVs;
  manifest SHA-256 `b2cf2fb7a1f5d665deb92755ebafb32c73808ed06dbe1b497b070bfb6767a034`;
  profile SHA-256 `c6c38203f027dcbf6b842b60add002fa7c269366a0e59b97b90a7111aa34b358`.
- **Headline numbers (verified against the profile):** `289,116,808` rows
  (FR `158,331,078` / NL `79,991,190` / ETR `50,794,540`); security types E
  `239,521,477`, I `49,595,331`; `67,056` rows lack a valid system timestamp
  (`289,049,752` usable). Raw capture peak `1,223,873` rows in
  `2021-11-09T15:58:01` → `16:03:01` (~4,080 rows/s); separate trade peak
  `130,524` rows `2021-11-10T17:34:37` → `17:39:37`; one-second burst `23,016`
  at `2021-11-10T17:40:00`; top symbol RDSA.NL `6,903,592` (2.39%). Fixture:
  FR `651,418` / NL `411,885` / ETR `160,570`.
- **Limits:** "capture time" is the CSV system wall clock, not a network capture
  instant; the trade definition is a provisional strictly-positive finite `Last`
  proxy; no absolute timezone conversion; peak holds only for the verified
  manifest revision.

## Ticket 03 — Deliver live BUY/SELL to subscribers (done)

- **Question:** Do subscribers receive genuine BUY/SELL from finalized Flink
  windows with the triggering window and EMA pair, without unrelated symbols?
- **Artifacts:** `benchmarks/evidence/live-advisory/{comparison.json,comparison-live.json,browser.png,browser-live.png,browser-frames.json,capture.cjs,capture-live.cjs,README.md}`.
- **Reproduce:** see the two command blocks in
  `benchmarks/evidence/live-advisory/README.md` (fresh Compose project, fresh
  topics, `verify.py` + the browser capture).
- **Mode/configuration:** synthetic accelerated run, one-second windows,
  `DEDUP_STRATEGY=NONE`, `LATE_EVENT_STRATEGY=DROP`, `WINDOW_SIZE_MS=1000`,
  `MAX_OUT_OF_ORDER_MS=0`.
- **Headline numbers (verified):** expected = actual = 148 read-committed
  records, `missing`/`extra` empty, `quarantined=2`, `websocket_clients=3`
  (history checked separately). SHA-256 of all five retained artifacts matches
  the README.
- **Limits:** synthetic and accelerated (one-second windows), not latency,
  recovery or historical evidence; end-to-end Kafka recovery not demonstrated
  (checkpoint restore covered at the operator seam); the README records one
  unexplained timeout with a 1,000 ms source-idle timeout; the successful retry
  used 30,000 ms.

## Ticket 04 — Reconcile a reference Historical replay slice (done)

- **Question:** Does a bounded Historical replay of a representative slice
  reproduce independent oracle truth exactly, including the final window?
- **Artifacts:** `benchmarks/reconcile_replay.py`, `benchmarks/reference-slice.md`,
  `benchmarks/evidence/reference-slice/manifest.json`.
- **Reproduce:** see the `extract`/`scan`/`normalize`/compare command block in
  `benchmarks/reference-slice.md`.
- **Mode/configuration:** capture-order bounded replay (chosen); day-12 slice
  rows `652282..2257481`; `DEDUP_STRATEGY=NONE`, `LATE_EVENT_STRATEGY=SIDE_OUTPUT`,
  `WINDOW_SIZE_MS=300000`, `MAX_OUT_OF_ORDER_MS=5000`,
  `CHECKPOINT_INTERVAL_MS=10000`, `FLINK_PARALLELISM=2`, `SPEED_MULTIPLIER=1000`.
- **Headline numbers (verified from the manifest):** `1,605,199` raw rows,
  `250,716` eligible trades, `270,261` committed output records, `7,937` EMA
  pairs, `3,671` advisories, `0` audit records; comparison
  missing/extra/incorrect/duplicate = `0/0/0/0` over `11,608` oracle records;
  final window `1636704300000`; gate `passed=true`. A labeled event-time-ordered
  reference cross-check also passed identically.
- **Limits:** one weekday slice, not the full week; storage figures are
  byte-scaled preflight estimates; replay accelerated 1000×, so correctness and
  storage only, not throughput/latency; no failure/restart schedule; a week
  section with larger lateness may still require the event-time reference.

## Ticket 05 — Prove capture-paced End-to-end measurement (done, partial evidence)

- **Question:** Does capture-time pacing measure the publication-to-subscriber
  path rather than historical event age, and can the pilot fail closed?
- **Artifacts:** `docs/capture-pilot-evidence.md`, `benchmarks/capture_pilot.py`,
  `benchmarks/fixtures/capture-pilot.csv` (158 data rows after 13 header lines).
- **Reproduce:** the capture-paced pilot block in the root `README.md`
  ("Validate and benchmark").
- **Mode/configuration:** `PACING=capture`, missing-policy `fail`, 60 s windows,
  pre-roll/measured/trailing `10 s / 60 s / 60 s`, `SPEED_MULTIPLIER=20`,
  one TaskManager, uncapped containers.
- **Headline numbers (prose only):** measured-segment achieved pacing `1.00034`
  (threshold 0.98), `158` prices + `10` EMAs = `168` records matched exactly at
  ingest, committed Kafka and WebSocket; enqueue→WebSocket p50 `618 ms` /
  p99 `1681 ms`; watermark waiting p50 `400 ms`.
- **Limits:** synthetic, accelerated 20×; shortened windows; uncapped, one
  worker; container clocks share the host kernel clock after checkpoint
  correlation. **Caveat: no raw run JSON is retained — the pilot numbers are
  prose in the doc, not re-derivable from a committed artifact.**

## Ticket 06 — Verify both queries over the entire week (partial, in progress)

- **Question:** Which rows were processed over the whole week, and did the query
  match independent truth?
- **Artifacts (outside Git, absolute paths):**
  `/mnt/shared/data/ultrastonk-full-week/run2/evidence/{compare.json,normalize.json,offsets.json,expected.jsonl,actual.jsonl,audit.jsonl,output.jsonl}`,
  `/mnt/shared/data/ultrastonk-full-week/run2/logs/{emitter.log,input_leo_check.txt,leo-*.json}`,
  `/mnt/shared/data/ultrastonk-full-week/run1/INVALID-RUN.md` (preserved gate
  failure), `/mnt/shared/data/ultrastonk-full-week/run3/` (bounded ordered
  reference replay, **not finished**).
  Harness (on branch `full-week-queries`, **not merged into
  `integration/report-evidence`**):
  `.kilo/worktrees/full-week-queries/benchmarks/full_week.py`,
  `.../benchmarks/sort_week.py`, launcher `/mnt/shared/data/ultrastonk-full-week/run_fullweek.sh`,
  analysis `/mnt/shared/data/ultrastonk-full-week/analyze_run.sh`.
- **Reproduce:** `run_fullweek.sh` (fresh bind-mount wipe, LEO==0 pre-emit
  assertion, post-emit LEO==submitted assertion) then `analyze_run.sh`; both are
  run scratch, not committed.
- **Mode/configuration (run 2):** capture-order bounded Historical replay of all
  seven files; `DEDUP_STRATEGY=NONE`, `LATE_EVENT_STRATEGY=SIDE_OUTPUT`,
  `WINDOW_SIZE_MS=300000`, `MAX_OUT_OF_ORDER_MS=5000`,
  `CHECKPOINT_INTERVAL_MS=10000`, `FLINK_PARALLELISM=2`, `PACING=event`,
  `SPEED_MULTIPLIER=1000`.
- **Headline numbers (verified from the raw `compare.json` + offsets):**
  input `289,049,752` submitted, `0` failed, `545,563` ev/s, read `289,116,808`
  minus `67,049` no-event-time and `7` unparsable = `289,049,752`. Oracle
  `1,868,812` records vs `1,498,456` actual. Comparison `match=false`:
  missing `376,584`, extra `6,228`, numeric `1,482,724`, duplicate `0`,
  wrong-direction `0`; expected and actual final window both
  `1636752900000`. Audit topic `10,669,014` eligible late exclusions. Dump
  deficits are exactly-once control records (out `62,380,615` vs LEO
  `62,381,131`; audit `10,668,887` vs LEO `10,669,014`).
- **Limits:** the capture-order run is **not** exact full-input correctness
  (the dataset's ~3.7% eligible-trade capture-order lateness makes it fail the
  gate). The correctness run is the labeled event-time-ordered reference replay,
  which **is not yet complete** (run 3 covers only day 08, LEO still 0, no
  comparison). No repo-side `benchmarks/evidence/full-week/` pack or summary doc
  exists yet.
- **Run 1 (invalid):** replayed the week but the bind-mounted broker retained a
  stale `53,635,606`-record prefix; `input.records (342,685,358) !=
  emitter.acknowledged (289,049,752)`, rejected by the gate.

## Ticket 07 — Measure live peak load and headroom (pending)

- **Question:** Did the running pipeline keep up with the recorded peak, and
  was there headroom?
- **Artifacts:** `benchmarks/peak_slice.py`, `benchmarks/live_peak.py`,
  `benchmarks/tests/test_peak_slice.py`, `benchmarks/tests/test_live_peak.py`,
  `docs/live-peak.md`, `benchmarks/evidence/live-peak/` (currently only
  `README.md`, `ledger.template.json`, empty `runs/`, empty `slices/`).
- **Status:** **no measurement taken.** `docs/live-peak.md` line 4 states this
  explicitly. Planned outputs: `benchmarks/evidence/live-peak/` per `README.md`.
- **Planned mode/configuration:** capture-paced (`PACING=capture`), warm slice,
  2 TaskManagers, 6 partitions, `WINDOW_SIZE_MS=300000`,
  `MAX_OUT_OF_ORDER_MS=5000`, `LATE_EVENT_STRATEGY=DROP`,
  `CHECKPOINT_INTERVAL_MS=10000`. See the reduced scope below for the cut-down
  conditions.

## Ticket 08 — Bracket the Sustainable limit (pending)

- **Question:** What is the measured Sustainable limit for one and two
  TaskManagers for the same synthetic workload?
- **Artifacts:** `benchmarks/capacity_bracket.py` (drives `benchmarks/matrix.py`),
  `benchmarks/tests/test_capacity_bracket.py`, `docs/sustainable-limit.md`,
  `benchmarks/evidence/sustainable-limit/{README.md,pre-registered-rules.json}`.
  Planned: `run-ledger.jsonl`, `bracket.json`, and raw
  `benchmarks/results/SESSION-*` artifacts named by `bracket.json source_sessions`.
- **Status:** **no measurement taken** — only the frozen acceptance contract is
  committed (`pre-registered-rules.json`).
- **Acceptance order:** INCONCLUSIVE (missing telemetry) → INCONCLUSIVE
  (achieved input < 0.98 of offered) → FAIL (incorrect delivery) → FAIL
  (sustained stage-backlog growth) → PASS.

## Tickets 09 and 10 — Growth effects (pending / reduced)

- **09:** long-tail vs uniform traffic and 2× symbol count at a documented
  passing baseline. **10:** baseline vs 2× subscribers on one gateway at a
  verified baseline. Both artifacts: none yet;
  issues `09-measure-long-tail-and-symbol-growth.md`,
  `10-measure-subscriber-growth.md`. In the reduced scope these are conditional.

## Ticket 11 — Quantify the drop-versus-grace trade-off (done)

- **Question:** What is gained or lost in advisory correctness, publication
  delay and managed state between `DROP` and `GRACE` late-event handling?
- **Artifacts:** `benchmarks/drop_vs_grace.py`,
  `benchmarks/evidence/drop-vs-grace/{comparison.json,arm-R1-DROP.json,arm-R1-GRACE.json,arm-R2-DROP.json,arm-R2-GRACE.json,README.md}`,
  `benchmarks/tests/test_drop_vs_grace.py`.
- **Reproduce:** see the two command blocks in
  `benchmarks/evidence/drop-vs-grace/README.md` (build the benchmark image,
  one disposable Compose project per arm, then `drop_vs_grace.py compare`).
- **Mode/configuration:** one scripted fixture (digest
  `f481616bfa972639eeed22c6dcac466112ace8945d9c9c4b0e39db323e237a4b`, verified),
  seed `11`, `WINDOW_SIZE_MS=300000`, `MAX_OUT_OF_ORDER_MS=5000`,
  `ALLOWED_LATENESS_MS=30000`, `DEDUP_STRATEGY=NONE`, `FLINK_PARALLELISM=2`,
  `CHECKPOINT_INTERVAL_MS=1000`; only `LATE_EVENT_STRATEGY` differs.
- **Headline numbers (verified from the arm files):** `DROP` commits `34`
  records (32 EMA + BUY@0 + SELL@30) with 0 defects and truth loss = 1 missing
  advisory (BUY@31) + 2 incorrect EMA values; `GRACE` commits `35` (adds
  BUY@31) with 0 defects and 0 truth loss; both replicates agree. Policy-intrinsic
  finalization lag `0` ms (`DROP`) vs `30000` ms (`GRACE`); checkpoint bytes
  R1 `78,026 → 79,126` (+1,100), R2 `79,054 → 83,398` (+4,344); measured
  wall-clock finalization R1 `10,173`/`804` ms, R2 `10,244`/`85` ms.
  Recommendation: default to `GRACE`.
- **Limits:** scripted synthetic disorder, not historical; one late window per
  arm; no per-service CPU/memory caps and no resource samples (`resources`
  empty) — "identical resources" holds by construction; timing relative, not a
  latency claim; recovery, side outputs and `TIMESTAMP`/`EXACT` dedup not
  exercised.

## Ticket 12 — Audit and package (this document)

- **Artifacts:** `benchmarks/evidence/README.md` (this index),
  `docs/evidence-audit.md` (the audit report).

---

## How to reproduce the whole pack

The pack is layered; each layer needs the one below it.

1. **Data (ticket 02):** acquire all seven Zenodo files (DOI
   `10.5281/zenodo.6382482`), verify size and MD5 against
   `scripts/official-week-manifest.json`, then run `scripts/profile_week.py` and
   `scripts/profile_fixture.py`.
2. **Oracle (ticket 01):** `python3 -m unittest discover -s benchmarks/tests -p 'test_query_truth*.py'`
   then generate truth for any input with `benchmarks/query_truth.py`.
3. **Query correctness (ticket 04 slice, ticket 06 week):** bounded Historical
   replay through `benchmarks/reconcile_replay.py` (slice) and the
   `full-week-queries` launcher + `benchmarks/full_week.py` (week), compared with
   `benchmarks/query_truth_compare.py --db`.
4. **Live advisories (ticket 03):** the verifier + browser capture commands in
   `benchmarks/evidence/live-advisory/README.md`.
5. **Capture pacing (ticket 05):** the pilot block in the root `README.md`.
6. **Peak and capacity (tickets 07, 08):** `benchmarks/live_peak.py` and
   `benchmarks/capacity_bracket.py` on an **otherwise idle host** (see the
   command blocks in `docs/live-peak.md` and `docs/sustainable-limit.md`).
7. **Policy comparison (ticket 11):** `benchmarks/drop_vs_grace.py` (README
   commands).
8. **Audit (ticket 12):** re-read the raw artifacts against this index and
   `docs/evidence-audit.md`.

Raw CSVs, Kafka contents and scratch databases stay outside Git; only the
summaries and machine-readable manifests are committed, and generation commands
carry local paths that must be sanitised before publication.

---

## Reduced scope (2026-11-23 team decision)

The measurement programme was cut to fit roughly two machine-hours. Every
reduction is recorded here and in the spec's **Scope Reduction** section so the
report states confidence honestly rather than implying the full plan ran.

- **Whole-week correctness — ordered replay dropped.** The labeled
  event-time-ordered replay over all seven files is dropped; the capture-order
  whole-week run is the primary whole-week evidence, reported as **capture-order
  mode with quantified deviations**: `10,669,014` eligible late exclusions at the
  5 s out-of-order budget, `376,584` missing `(symbol, window)` closes,
  `1,482,724` numeric, `6,228` extra, `0` duplicate, `0` wrong-direction, final
  window present, all source rows accounted for, input accounting exact. A
  bounded ordered reference (≤ one day, hard-capped at 20 minutes) may be added
  as a labeled reference-mode check; if skipped, complete-input correctness rests
  on ticket 04's ordered reference slice (zero mismatches) plus this deviation
  report. **No step may present the capture-order run as exact full-input
  correctness.**
- **Ticket 07 (live peak):** `1x` capture pace and warm state only; the `1.25x`
  condition and the cold variant are dropped; pre-roll 120 s and trailing 90 s;
  one or two repeats decided by measured run cost; 40-minute wall-clock cap.
- **Ticket 08 (capacity bracket):** one coarse ladder (`500/1000/2000/4000`
  originals/s, adding `8000` only if all pass) with 30 s runs, a **single
  repetition**, both worker counts in one batch, and at most one fine
  confirmation; 45-minute cap. Bounds may therefore be loose or `unbracketed`,
  and that outcome is reported as such instead of being tightened away.
- **Tickets 09 and 10 (growth):** reduced to single-condition comparisons run
  only if the clock allows; otherwise reported as reduced or deferred with the
  reason.
- **Ticket 12 (packaging):** runs in parallel and must label every reduced item;
  reduced-repeat and reduced-ladder results are directional evidence, **not** the
  three-repeat bracket the original plan specified.

> Caution for report writers: `docs/live-peak.md` still shows the **pre-registered,
> un-reduced** plan (`1x`/`1.25x`, "three valid repeats each", `--repeats 3`). Only
> ticket 07's comment records the downscale, so the reduced conditions above, not
> the doc's plan, are what applies to anything presented as executed.
> `docs/sustainable-limit.md` **does** now carry the scoped plan
> ("Scoped scored plan": `500/1000/2000/4000`, single repetition, 45-minute cap)
> alongside the non-scoped reference schedule. See `docs/evidence-audit.md`.

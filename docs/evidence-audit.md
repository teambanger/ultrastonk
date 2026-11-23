# Evidence audit (ticket 12)

Read-only audit of the Ultrastonk report-evidence pack against each ticket's
acceptance criteria. Scope: `/mnt/shared/data/shool/BigD/SS26/ultrastonk` at
branch `integration/report-evidence`, plus the out-of-Git run artifacts under
`/mnt/shared/data/`. No benchmark, container or load command was run; every
number below was checked by reading a raw artifact. Where a number could not be
verified it is called out as such rather than accepted.

Compared index: `benchmarks/evidence/README.md`.

---

## 1. Support table

| Ticket | Verdict | Criterion checked | Artifact checked |
|---|---|---|---|
| 01 oracle + comparator | **supported** (files), test count unverified | independent CSV oracle + `1e-9` comparator + worked example | `benchmarks/query-truth.md`, `benchmarks/fixtures/query-truth.csv`, `benchmarks/query_truth*.py` |
| 02 week profile + fixture | **supported** | verified manifest, exact row/peak figures | `scripts/official-week-manifest.json` SHA-256, `week-profile.json` SHA-256, day/exchange sums |
| 03 live advisories | **supported** | 148-record exact multiset + 3 subscribers + artifact hashes | `benchmarks/evidence/live-advisory/comparison.json`, `sha256sum` of 5 files |
| 04 reference slice | **supported** | exact zero-mismatch gate incl. final window | `benchmarks/evidence/reference-slice/manifest.json` |
| 05 capture pacing | **partially supported** (prose-only numbers) | fixture size, fail-closed checks | `benchmarks/fixtures/capture-pilot.csv`, `docs/capture-pilot-evidence.md`; **no raw run JSON** |
| 06 full-week queries | **unsupported (incomplete)** | all seven files, exact reference correctness | `run2/evidence/compare.json`, `run2/logs/emitter.log`, `run2/evidence/offsets.json`, `run2` line counts; `full_week.py` unmerged; `run3` unfinished |
| 07 live peak | **pending** | no run | `docs/live-peak.md` (says "No measurement has been taken yet"), `benchmarks/evidence/live-peak/` empty |
| 08 capacity bracket | **pending** | no run | `benchmarks/evidence/sustainable-limit/pre-registered-rules.json` only |
| 09 symbol growth | **pending** | no run | issue file only |
| 10 subscriber growth | **pending** | no run | issue file only |
| 11 drop vs grace | **supported** | 34/35, defects 0, delay, state bytes, fixture digest | `comparison.json`, `arm-R{1,2}-{DROP,GRACE}.json` |

---

## 2. Detail of what was checked

### Ticket 01 — supported (artifact level)
`benchmarks/query-truth.md` documents the reference interpretation (40-column
rows, strictly-positive finite `last`, exchange-qualified key, zero-init EMAs,
`abs_tol=rel_tol=1e-9`). `query_truth.py`, `query_truth_compare.py` and both test
files exist. The ticket's "12 tests passed" and "comparator mutation tests fail"
claims were **not** re-run here; they are asserted, not verified.

### Ticket 02 — supported
- `sha256sum scripts/official-week-manifest.json` =
  `b2cf2fb7a1f5d665deb92755ebafb32c73808ed06dbe1b497b070bfb6767a034` — matches
  `docs/dataset-profile.md`.
- `sha256sum .../week-profile.json` =
  `c6c38203f027dcbf6b842b60add002fa7c269366a0e59b97b90a7111aa34b358` — matches.
- Row table sums reproduce exactly: days `289,116,808`; FR `158,331,078`;
  NL `79,991,190`; ETR `50,794,540`.
- `67,056` invalid-capture rows is internally consistent with ticket 06's emitter
  (`67,049` no-event-time + `7` unparsable = `67,056`; `289,116,808 − 67,056 =
  289,049,752`).
- Cross-source consistency: the profile's provisional `59,391,670` trade-like
  rows equal ticket 06's committed price count in `normalize.json`.

### Ticket 03 — supported
- `comparison.json` and `comparison-live.json`: `missing=[]`, `extra=[]`,
  expected == actual == 148 records (147 entries, `count` sums to 148),
  `quarantined=2`, `websocket_clients=3`.
- All five README SHA-256 values verify against the files.
- Limits (one unexplained 1,000 ms-timeout run; no end-to-end recovery) are
  disclosed in the README and ticket. **Gap:** `browser.png` "displays
  BUY/SELL/BUY" but the README admits "The browser does not display the earlier
  SELL's EMA pair in this screenshot" — a figure the report must caption
  carefully.

### Ticket 04 — supported
`manifest.json` is internally consistent: `250,716` prices + `15,874` EMA
components (`= 2 × 7,937` pairs) + `3,671` advisories = `270,261` committed
records; comparison missing/extra/incorrect/duplicate `0/0/0/0` over `11,608`
oracle records; final window `1636704300000`; `gate_result.passed=true`;
labeled event-time reference matches identically. Raw slice CSV and SQLite
databases are outside Git, so their SHA-256 (in the manifest) cannot be
re-checked from the committed pack. Storage figures are byte-scaled estimates.

### Ticket 05 — partially supported
- `benchmarks/fixtures/capture-pilot.csv` has 171 lines; minus the 13 skipped
  header lines = **158** rows, matching the doc.
- **Gap:** the measured numbers (achieved pacing `1.00034`, enqueue→WebSocket
  p50/p99 `618`/`1681` ms, watermark waiting `400` ms, `168` records) exist only
  as prose in `docs/capture-pilot-evidence.md`. There is **no retained raw run
  JSON** in `benchmarks/evidence/`, so none of these can be re-derived from a
  committed artifact. The `benchmarks/results/*` sessions are unrelated
  historical smoke runs.

### Ticket 06 — unsupported / incomplete
The capture-order run's numbers **do** match its raw artifact exactly:
- `compare.json`: missing `376,584`, extra `6,228`, numeric `1,482,724`,
  duplicate `0`, wrong_direction `0`; expected `1,868,812` vs actual
  `1,498,456`; final windows equal `1636752900000`. Line counts confirm
  `expected.jsonl` 1,868,812, `actual.jsonl` 1,498,456, `output.jsonl`
  62,380,615, `audit.jsonl` 10,668,887.
- `per_day` and `per_exchange` sub-totals sum to those totals exactly.
- `emitter.log`: `289,049,752 submitted, 0 failed, 545563 ev/s`; `read
  289,116,808 -> emitted 289,049,752 (67,049 no event time, 7 unparsable)`.
- `offsets.json` audit high−low sums to `10,669,014`; out sums to `62,381,131`.
- `emitter_inspect.json`: exit 0, not OOM-killed, restart count 0.

But the **acceptance criteria are not met**:
- "Exact reference correctness is a pass only when those error counts are zero."
  The capture-order comparison is `match=false`; it is evidence of the dataset's
  lateness, not a passing correctness run.
- The required labeled event-time-ordered reference replay is **not complete**.
  `run3/` contains only `evidence/expected.jsonl`, a `scan-input/` symlink to the
  sorted day-08 file, and `logs/leo-*.json` all zero — the emitter has not run
  and there is no `compare.json`.
- The harness `benchmarks/full_week.py` and `benchmarks/sort_week.py` are
  committed only on branch `full-week-queries` (commits `f53bd59`, `ad6ece7`,
  `afa05e3`) and are **not merged into `integration/report-evidence`**.
- There is **no** committed `benchmarks/evidence/full-week/` pack and no
  `docs/full-week*.md` summary; `run2/manifest.json`, `run2/source.json` and
  `run2/scan/` were never produced by `analyze_run.sh`.
- Run 1 is correctly preserved and rejected (`INVALID-RUN.md`).

### Tickets 07, 08 — pending (correctly labelled)
`docs/live-peak.md` line 4 and `benchmarks/evidence/live-peak/README.md` both
state no measurement exists; only a template ledger is committed.
`benchmarks/evidence/sustainable-limit/` contains only the frozen
`pre-registered-rules.json`. No result is claimed.

### Tickets 09, 10 — pending
No artifacts; issue files only. Consistent with "reduced / deferred".

### Ticket 11 — supported (fully verified)
- Fixture digest recomputed with `drop_vs_grace.fixture_digest` =
  `f481616bfa972639eeed22c6dcac466112ace8945d9c9c4b0e39db323e237a4b` — matches
  `comparison.json`.
- `arm-R1-DROP.json` committed length 34, advisories BUY@`1700003700000`
  (window 0) and SELL@`1700012700000` (window 30) → 32 EMA; `arm-R1-GRACE.json`
  35 with an added BUY@`1700013000000` (window 31). R2 identical.
- `measurements.state.checkpoint_bytes`: R1 `78026 → 79126` (+1100),
  R2 `79054 → 83398` (+4344) — matches the README/ticket.
- `measurements.finalization_delay_ms`: R1 `10172.8`/`804.0`, R2
  `10244.1`/`85.0` — matches `10173`/`804` and `10244`/`85`.
- `comparison.json` `finalization_lag_ms`: DROP `0`, GRACE `30000`; `vs_truth`
  DROP missing 1 + `ema_incorrect` 2; GRACE match; `defects` all zero.
- `settings` blocks byte-identical across all four arms; `resources` = `{}` in
  every arm, so the ticket's "`resources` is empty" is accurate.

---

## 3. Ranked gaps (most likely to cost report marks first)

1. **Ticket 06 correctness run is missing (highest).** The only full-week
   result is a failing capture-order comparison; the event-time-ordered
   reference replay that the spec calls "the correctness run" is unfinished, and
   its harness is unmerged. Any "full-week correctness" criterion is currently
   **unsupported**. Fix: finish run 3, produce `compare.json` with zero errors,
   merge `full_week.py`, and commit a `benchmarks/evidence/full-week/` summary.

2. **Reduced scope partly undocumented — `docs/live-peak.md` still un-reduced.**
   `docs/live-peak.md` (unchanged since `091c672`) still specifies `1x`/`1.25x`
   and "three valid repeats each" with `--repeats 3`; only ticket 07's comment
   records the downscale. A report writer citing "three repeats at 1.25x" from
   the doc as executed would be unsupported. `docs/sustainable-limit.md` **is**
   updated with a "Scoped scored plan" (`500/1000/2000/4000`, single repetition,
   45-minute cap, reduced-confidence fallbacks) and tickets 07/08 both carry a
   "Downscaled by team decision" comment, so only the peak doc lags. Fix: annotate
   `docs/live-peak.md` with the `1x`/warm-only, reduced-repeat conditions.

3. **Ticket 05 numbers are prose-only.** Pacing, latency and record totals cannot
   be re-derived from any committed artifact. Fix: retain the pilot run JSON
   under `benchmarks/evidence/` and cite it.

4. **Ticket 06 run 1's SHA-256 manifest names deleted files.** `run1-evidence-sha256.txt`
   lists `evidence/{audit,expected,output}.jsonl` but these were reclaimed; only
   `INVALID-RUN.md`, the offsets/LEO JSONs and logs remain. Not wrong (the bulk
   was intentionally freed), but the manifest overstates what is retained.

5. **Ticket 03 recovery and figure gaps.** No end-to-end Kafka recovery
   demonstration and the retained screenshot omits the earlier SELL EMA pair;
   the one unexplained timeout is disclosed but unexplained.

6. **Ticket 04 slice does not generalise to the week.** Capture-order was chosen
   because the slice had zero late exclusions; the full week shows ~3.7% eligible
   lateness. Storage numbers are estimates. Both are disclosed in the ticket.

7. **Ticket 11 resource controls are by construction.** No per-service caps and
   no samples (`resources` empty); "identical resources" is inferred from
   identical settings. Disclosed, low risk.

8. **Ticket 01 / 02 test counts unverified.** "12 tests passed" (01) and
   "14 profiling + 17 benchmark tests" (02) are prose claims, not re-run here.

---

## 4. Criteria that currently fail

- **Ticket 06 acceptance criterion 2** ("Exact reference correctness is a pass
  only when those error counts are zero") — the only full-week comparison is
  `match=false`, and the reference replay that could pass it is unfinished.
  **FAIL (unsupported).**
- **Ticket 06 acceptance criterion 1** (process all seven files and record
  dispositions) — the capture-order run did process all seven files with clean
  input accounting, but the "reference mode from the pilot" step required by the
  ticket is not yet executed. **PARTIAL.**
- **Ticket 07, 08, 09, 10 acceptance criteria** — no measurements. **NOT MET
  (pending).**

## 5. Contradictions between a claim and its own artifacts

- None found in tickets 02, 03, 04, 06 and 11 where a raw artifact exists: every
  headline number re-checked reproduces. Ticket 06's ticket-comment figures match
  `compare.json` and the offsets exactly.
- The only claim/artifact mismatch is **ticket 05**, where the headline numbers
  appear in prose but in no retained raw artifact (a missing-artifact gap rather
  than a wrong number).
- **Docs vs spec:** `docs/live-peak.md` still describes the un-reduced plan
  (`1x`/`1.25x`, three repeats) and conflicts with the spec's Scope Reduction
  section and ticket 07's "Downscaled by team decision" comment; a documentation
  lag, not a bad measurement. `docs/sustainable-limit.md` and tickets 07/08 now
  record the reduction, so only the peak doc remains stale. Recorded in the
  index's "Reduced scope" caution.

## Update 2026-11-23 (after tickets 07 and 08 landed)

- **Ticket 07 is MEASURED and resolved** (`benchmarks/evidence/live-peak/`, `docs/live-peak.md`): one valid
  repetition at 1x capture pace, warm, 1,914,978-row slice, achieved pacing 0.99991, delivery exact
  (195,061 = committed = WebSocket), latency and oracle numbers as recorded in the ticket. Recorded gaps:
  Kafka broker and gateway consumer-group lag were not collected; one repetition only; the run exceeded its
  40-minute cap by 1–2 minutes. Three harness defects were fixed and committed with the evidence
  (`peak_slice.py --reorder capture`, `live_peak.py --pacings` and gateway history limit,
  `capture_pilot.py` quadratic window check), so the measured numbers depend on committed code.
- **Ticket 08 is MEASURED and resolved** (`benchmarks/evidence/sustainable-limit/`): six points, delivery
  exact, achieved input ≈99.9997%, **unbracketed** at both worker counts, worker throughput gain ≈1.0×.
  The first complete ladder run is preserved as `bracket-attempt2-contract-mismatch.json`; its FAILs were a
  stale output contract (advisories), now fixed in `benchmarks/driver.py`. An earlier attempt that ran with
  no resource caps is preserved as `bracket-attempt1-uncapped.json` and is not capacity evidence.
- **Ambiguity found in the capacity model** (`benchmarks/capacity_model.py`, `docs/capacity-model.md`): the
  field `scaling_efficiency` currently reports ≈0.99 for the two-worker ladder, but that value is the
  throughput *ratio* between two workers and one worker, not an efficiency fraction — the per-worker
  efficiency is ≈0.50. Either the field should be divided by the worker ratio to match the documented
  `η(p) = (R(p)/R(m))/(p/m)` formula, or renamed. Until then it must not be quoted as an efficiency.
  Unresolved at the time of writing; flagged rather than silently patched.
- **Still open:** ticket 09 and 10 (deferred, outside the reframed goal), ticket 12's final pack (the
  evidence index still marks 07/08 as PENDING and a teammate reproduction is outstanding), and the
  checkpoint failures of 4–6 per capacity run with zero job restarts, which remain unexplained.

**Update 2026-11-23 (after the two-hour programme, extension runs).**

- **Tickets 09 and 10 are MEASURED and resolved**, and the index, dossier (§5.3) and audit no longer mark
  them pending: `benchmarks/growth.py` drives both pairs through `matrix.py` at the same documented
  4,000/s/one-worker baseline as ticket 08. Ticket 09: exact delivery in uniform/long-tail/long-tail-2×
  conditions; the quoted fixed-ticker-prefix triple commits 3,982.7 / 3,879.9 / 3,848.3 prices/s with
  partition input max/mean 1.123 / 1.271 / 1.270 — skew concentrates the busiest partition, a larger symbol
  population leaves concentration and throughput unchanged. Two earlier triples with per-run ticker prefixes
  agree on delivery and throughput (−2.1%, −2.0%) but their partition numbers move with the key hash
  (1.3–1.7 max/mean for the same distribution), which is recorded as a limitation. Ticket 10: fan-out
  8,475 → 17,022 frames/s (2.009×; 1.977× in an unmonitored repeat), every client exact on both runs, zero
  slow-client drops, gateway at 0.204 → 0.232 cores, upstream (TaskManager 0.997 core throttled, Kafka 0.63)
  the saturated components. Both tickets are **one repetition per condition** and labelled directional.
- **Harness changes** committed with the evidence: `driver.py` gained an independent per-client
  verification path (each subscriber checked against committed output, per-client latency and errors),
  a `--distribution-file` measured-weight input, measured per-partition input counts from the producer's
  delivery callbacks, and a client-count-aware drain; `matrix.py` passes `--distribution-file`/`--clients`
  through; `benchmarks/growth.py` and 15 tests are new. `benchmarks/tests` totals 278 tests.
- **Still open:** the checkpoint failures of 4–6 per capacity run (zero restarts) remain unexplained; the
  capacity-model `scaling_efficiency` label is still a throughput ratio, not an efficiency; ticket 12's
  teammate reproduction is outstanding.

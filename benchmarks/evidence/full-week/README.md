# Ticket 06 — full-week query verification evidence

One labeled reconciliation of the production Flink query over the official DEBS week, with
the exact reference correctness carried by a bounded ordered replay (reduced scope, recorded
below). Machine-readable artifacts sit next to this file in `capture-order/` and
`reference-day08/`.

## Verdict

- **Whole-week processing (capture order) — deviations quantified, not a pass.** Run 2
  replayed all seven files in manifest order through the production operators with fresh
  topics and state, and accounted for every source row. It loses eligible trades because the
  recorded order is not event-time order: the audit late channel held 10,668,887 records and
  the comparison shows 376,584 missing, 1,482,724 numeric, 6,228 extra, 0 duplicate and 0
  wrong-direction results. The fail-closed gate fails exactly the `zero_late_exclusions` and
  `oracle_exact` checks; every accounting and coverage check passes.
- **Complete-input correctness (bounded ordered reference) — PASS.** Run 3 replayed one full
  day (2021-11-08) from event-time-ordered input through the same operators: 0 missing, 0
  extra, 0 numeric, 0 duplicate, 0 wrong-direction, expected = actual = 377,966 records, the
  final window identical on both sides, and 0 audit records. All twelve gate checks pass.
- **Mode decision.** Ordinary capture-order replay drops eligible trades, so per the spec the
  separately labeled event-time-ordered reference is the correctness mode. The full ordered
  week was dropped by team decision to release the shared host; correctness therefore rests on
  the bounded day-08 ordered reference here **plus ticket 04's verified ordered slice**.

## Scope and label

Label: **reference-mode correctness run**, not a live-capacity or live-latency measurement.
A preload-and-process bounded replay with `SPEED_MULTIPLIER=1000` does not measure live
latency or live capture-order correctness.

Data provenance: `/mnt/shared/data/official-week-Ij8jlfeA`, the seven verified official CSVs
(289,116,808 rows), in ascending-day manifest order (`scripts/official-week-manifest.json`,
`doi 10.5281/zenodo.6382482`, `cc-by-nc-sa-4.0`). Raw CSVs, Kafka data and scratch databases
are outside Git.

Configuration: five-minute aligned event-time windows, `MAX_OUT_OF_ORDER_MS=5000`,
`DEDUP_STRATEGY=NONE`, `LATE_EVENT_STRATEGY=SIDE_OUTPUT`, `EVENT_TIME_ZONE=Europe/Berlin`,
`FLINK_PARALLELISM=2`, 6 input partitions, fresh topics and state, retention 604800000 ms,
broker log and Flink checkpoints bind-mounted to disk-backed `/mnt/shared`.

## Run 2 — whole-week capture order (fresh topics and state)

Topic offsets (records): input 289,049,752; output per-event 62,381,131; audit 10,669,014.
Every topic was asserted empty before the emitter and input LEO was asserted equal to the
emitter's submitted count before any dump.

Source dispositions (ticket 02 profile + `emitter.log`; the source side was not re-scanned):
289,116,808 rows read, 289,049,752 emitted, 67,049 without a valid event time, 7 unparsable
timestamps, 0 bad width / 0 no symbol / 0 malformed-price; 59,391,670 eligible trades.

Comparison vs the independent oracle (`comparison.json`): the committed output holds
62,380,615 per-event price/EMA/advisory records, which `reconcile_replay.normalize`
aggregates into 1,498,456 comparable records — **one EMA per (symbol, window)** (1,490,489)
plus advisories (7,967). The oracle `expected.jsonl` holds 1,868,812 records. These are
different units (per-event vs per-window close), not a mismatch.

| error kind | count |
| --- | --- |
| missing (expected close/advisory absent) | 376,584 |
| extra (committed record not expected) | 6,228 |
| numeric (EMA out of `1e-9`) | 1,482,724 |
| duplicate | 0 |
| wrong-direction (BUY/SELL flipped) | 0 |
| eligible late exclusions (audit) | 10,668,887 |
| final window (both sides) | 1,636,752,900,000 |

Reconciliation: per-day source rows/emitted/eligible-trades and committed outputs are in
`source.json`; per-exchange committed outputs come from `comparison.json`. The EMA chain
propagates one early wrong close across a symbol, which is why almost every close is numeric
once any window is finalized before its closing tick arrives.

Timing split (seconds): ingestion 530.4 (emitter wall; pacer-bound at 1000x), processing
428.0 (Flink RUNNING→FINISHED), drain 616.3 (single-consumer output 521.9 + audit 88.6),
oracle 1154.6. All rows accounted; restart/OOM: none (all containers exit 0).

## Run 3 — bounded event-time-ordered reference (day 2021-11-08)

Input: day-08 sorted by (event millisecond, source ordinal) with `benchmarks/sort_week.py`;
the emitted multiset and source ordinals are preserved and no closes are precomputed.

Source dispositions: 53,982,555 rows, all emitted (the ordered file already holds the loader
multiset); 11,794,520 eligible trades.

Comparison vs oracle (`reference-day08/comparison.json`): match **true**; expected = actual =
377,966 records; final window 1,636,407,300,000 identical; error counts all 0. Per-exchange
exact: ETR 114,763, FR 211,427, NL 51,776. Audit topic LEO 0 — **the ordered replay produced
zero eligible late exclusions** (the dump ran: `dump-audit.log` records 0, complete true).

Timing split (seconds): source scan 443.3 (single file, analysis), ingestion 234.3, processing
110.0, drain 50.5 (per-partition parallel dump: 12.5M records in 48.4s vs 521.9s single
consumer for run 2). Gate: all twelve checks pass.

## Reduced scope and the invalidated first attempt

- The full ordered week was abandoned by team decision to release the shared host for ticket
  07; only day-08 was ordered. Complete-input correctness is therefore demonstrated on one
  full day plus ticket 04's ordered slice, not the whole week.
- Run 1 was invalidated: its bind-mounted broker directory kept a 53,635,606-record stale
  prefix from an earlier aborted pass (`docker compose down -v` clears named volumes, not
  bind mounts). The gate rejects it on `input.records != emitter.acknowledged`. Preserved at
  `/mnt/shared/data/ultrastonk-full-week/run1/INVALID-RUN.md` with offsets, LEO reads, logs
  and a `sha256` manifest; this is the gate working.

## Harness cost and limitations

- Peak disk: `sorted/` peaked ~33 GB with 4 sort workers (the per-day `.keyed` intermediate is
  deleted as each day completes); run 2 broker state ~15 GB; run 3 broker state 4.8 GB. The
  disk-backed mount was used throughout; the 16 GiB RAM-backed `/tmp` held no large state.
  Freed after the run: 33,696,526,336 bytes (sorted, run 3 broker state, bulky run 2 dumps)
  plus 5,049,256,347 bytes of run 1 bulk.
- Worker counts: sort 4; per-partition drain 6; day-08 scan 1.
- Peak RSS was not separately instrumented; the emitter and Flink run in JVM containers
  bounded by `MaxRAMPercentage=75`, and the oracle/scan are single Python processes. Recorded
  as a gap.
- Per-exchange source `emitted`/`eligible_trades` for the whole week are not in the ticket 02
  profile (raw rows only); day-08 per-exchange figures come from the run 3 scan.

## Artifacts

- `capture-order/{manifest,gate,comparison,source,timings,retention}.json`
- `reference-day08/{manifest,gate,comparison,source,timings,retention}.json`
- Harness: `benchmarks/full_week.py` (merge, comparison taxonomy, gate, export, parallel
  drain), `benchmarks/sort_week.py` (ordered reference input), `benchmarks/reconcile_replay.py`
  (scan/normalize/dump), `deploy/compose.diskstate.yaml` (disk-backed state).

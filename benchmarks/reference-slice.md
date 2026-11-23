# Historical reference-slice reconciliation

Ticket 04 checks that a bounded historical replay produces the same committed EMA and advisory
results as the independent CSV oracle. Raw CSVs, Kafka data and scratch databases stay outside
Git; only this summary and `benchmarks/evidence/reference-slice/manifest.json` are committed.

## Dataset and slice

The source is `debs2022-gc-trading-day-12-11-21.csv` from Zenodo record
[6382482](https://doi.org/10.5281/zenodo.6382482). Its size and MD5 match
`scripts/official-week-manifest.json`; the local SHA-256 is in the manifest.

The slice is the contiguous data row block `652282..2257481` (end exclusive) from that file. It
starts at the first event-time crossing of 08:55 Europe/Berlin after row 500000 and ends at the
first crossing of 09:10. It contains three nonempty windows (08:55, 09:00, 09:05), 1,605,199 raw
rows, 250,716 eligible trades and 3,671 active symbols. The final nonempty window is
`1636704300000` (09:05 Europe/Berlin).

The slice has 835,332 event-time reversals in capture order, 26,316 of them eligible trades; the
largest reversal is 996 ms (978 ms for eligible trades). No reversal exceeds the 5,000 ms
out-of-order budget on its own, but the reconciliation does not infer lateness from that number:
it reads the audit topic and Kafka offsets.

Reproduce the slice:

```sh
python3 benchmarks/reconcile_replay.py extract \
  --csv /path/to/debs2022-gc-trading-day-12-11-21.csv \
  --output /disk/scratch/slice-large.csv \
  --start-row 652282 --end-row 2257481

python3 benchmarks/reconcile_replay.py scan \
  --csv /disk/scratch/slice-large.csv --timezone Europe/Berlin \
  --output /disk/scratch/source-scan.json
```

## Mode decision

**Chosen mode: capture-order bounded replay.**

Ordinary replay in original source order had zero eligible late exclusions, an exact oracle
match and final-window coverage, so event-time reordering was not needed. This avoids the
disk-backed sort for the week-scale replay.

The event-time-ordered reference was still run as a labeled cross-check. It also had zero late
exclusions and an exact match. The oracle output from the original and sorted slices matched
exactly (11,608 records, same final window), which verifies that the stable `(event millisecond,
original ordinal)` sort preserved the replayable row multiset and the production last-arrival tie
rule. The reference run is not the chosen path because it requires the sort scratch and sorted
CSV shown below.

## Oracle and committed output

The oracle was generated with:

```sh
TMPDIR=/disk/scratch python3 benchmarks/query_truth.py \
  --timezone Europe/Berlin /disk/scratch/slice-large.csv > expected.jsonl
```

The replay used fresh topics, `DEDUP_STRATEGY=NONE`, `LATE_EVENT_STRATEGY=SIDE_OUTPUT`,
`WINDOW_SIZE_MS=300000`, `MAX_OUT_OF_ORDER_MS=5000`, `CHECKPOINT_INTERVAL_MS=10000`,
`FLINK_PARALLELISM=2`, `EVENT_TIME_ZONE=Europe/Berlin` and `SPEED_MULTIPLIER=1000`. The bounded
overlay is `deploy/compose.replay.yaml` with the `emitter` profile; the emitter completed before
the bounded Flink job started, and the job reached `FINISHED`.

Committed output was consumed with `isolation.level=read_committed`, normalized to the oracle
schema, and compared with the disk-backed comparator:

```sh
python3 benchmarks/reconcile_replay.py normalize \
  --input output.jsonl --output actual.jsonl --window-ms 300000

python3 benchmarks/query_truth_compare.py expected.jsonl actual.jsonl \
  --db /disk/scratch/compare.sqlite
```

| Check | Capture-order | Event-time reference |
|---|---:|---:|
| Emitter rows emitted / submitted / failed | 1,605,197 / 1,605,197 / 0 | 1,605,197 / 1,605,197 / 0 |
| Input topic records | 1,605,197 | 1,605,197 |
| Committed output records (prices + EMA components + advisories) | 270,261 | 270,261 |
| Price records | 250,716 | 250,716 |
| EMA pairs | 7,937 | 7,937 |
| BUY/SELL advisories | 3,671 | 3,671 |
| Audit records (late) | 0 | 0 |
| Missing / extra / incorrect / duplicate | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Final window present | yes (`1636704300000`) | yes (`1636704300000`) |
| Gate result | pass | pass |

The two raw rows dropped by the emitter (`dropped_no_time`) are accounted for in the source scan.
Every remaining row was emitted. The output topic's high offsets exceed delivered records by 24
because transaction control batches occupy offsets but are not delivered; the dump allows a
four-offset control tail after a quiet drain, and the gate independently requires the exact
normalized count and an exact oracle match. A truncated dump therefore cannot pass.

`SIDE_OUTPUT` is a late **audit** policy; it does not change EMA semantics relative to `DROP`.
The Flink late-policy tests confirm that the audit side output is produced for an excluded late
tick (`PoliciesTest`, `PriceIndicatorsTest`; 14 tests passed in this revision). Zero audit records
therefore mean zero eligible exclusions for this slice, not an untested audit path.

## Memory-bounded scale path

- `scan_source` and `extract_rows` stream rows. Scan memory scales with symbols and windows, not
  rows.
- `sort_event_time` uses a disk SQLite table ordered by `(event_ms, ordinal)` and batches 10,000
  rows. It never holds the file in memory.
- `OutputNormalizer` streams committed records and buffers only adjacent EMA38/EMA100 components;
  an incomplete pair fails the run.
- `query_truth_compare.py --db PATH` and `reconcile_replay.py compare` index both JSONL files in
  SQLite and compute missing, extra, duplicate and tolerance-checked EMA mismatches without
  loading the week into Python memory. Counts are exact; examples are capped by `--sample-limit`.
- `query_truth.py` already stores intermediate closes in a temporary SQLite database; set
  `TMPDIR` to disk scratch for the full week.

## Storage preflight

Observed for the 1,605,199-row slice:

| Artifact | Observed |
|---|---:|
| Slice CSV | 137,024,060 B |
| Input topic | 29,265,346 B |
| Output topic | 27,663,540 B |
| Audit topic | 0 B |
| Entire broker log directory (incl. internal topics) | 56,943,271 B |
| Raw committed-output JSONL | 22,697,848 B |
| Normalized JSONL | 1,444,919 B |
| Comparator SQLite | 2,084,864 B |
| Final checkpoint | 685,624 B (checkpoint 1: 314,639 B) |
| Sort SQLite scratch | 202,956,800 B |
| Sorted CSV | 135,418,682 B |
| Event-time reference input / output topics | 26,156,630 B / 27,519,044 B |

The full week manifest totals 24,874,180,296 bytes. Scaling linearly by source bytes
(scale factor 181.53) estimates 291,394,149 rows and:

| Full-week path | Estimate | With 15% headroom |
|---|---:|---:|
| Capture-order peak (source + topics + dumps + compare + checkpoint) | 40,094,168,170 B | 46,108,293,395 B |
| Extra for event-time reference (sort DB + sorted CSV) | 61,425,801,768 B | — |
| Event-time reference peak | 101,519,969,938 B | 116,747,965,428 B |

The host had 178 GiB free when this ran. The chosen capture-order path needs about 46 GB with
headroom; the event-time fallback needs about 117 GB including the source week, still below the
free space. If the source week is already present, the additional capture-order footprint is about
15 GB (or about 77 GB for the reference fallback). Kafka retention is 7 days
(`retention.ms=604800000`), so retaining full-week input and output needs about 10.4 GB of broker
space. The checkpoint estimate is deliberately conservative: it scales a 685,624-byte checkpoint
linearly by rows to 124 MB, although checkpoint state tracks symbols and open windows, not
throughput. The oracle's temporary SQLite scratch is disk-backed through `TMPDIR` but was not
separately measured; it is bounded by distinct symbol/window closes and is smaller than the sort
scratch.

## Limits

- Evidence covers one official weekday slice, not the full week. The full-week storage numbers are
  byte-scaled estimates, not a completed full-week replay; ticket 06 owns that run.
- Replay was accelerated 1000x. This is correctness and storage evidence, not a throughput or
  latency claim; capture-paced measurement belongs to ticket 05.
- No failure, restart or checkpoint-restore schedule was exercised. The bounded job reached
  `FINISHED`; checkpoints 1 and 2 are recorded only as storage observations.
- The bounded comparator reports exact mismatch counts but only a capped set of examples.
- Capture-order is the chosen mode for this slice. A different week section with larger lateness
  may still require the event-time reference path.

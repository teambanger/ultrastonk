# Long-tail traffic and symbol-population growth (ticket 09)

**Status: MEASURED, one repetition per condition — directional, not a limit or a scaling law.**

The question an evaluator can now answer: at a fixed offered load, does a *hot-symbol*
(long-tail) distribution behave differently from uniform traffic, and does doubling the
symbol population change that behaviour?

## Method

Baseline is the documented passing point from the capacity ladder (ticket 08): 4,000
offered originals/s, one TaskManager, `DEDUP_STRATEGY=NONE`, `LATE_EVENT_STRATEGY=DROP`,
1 s windows, 100 ms disorder budget, 2 s grace, fixed per-worker caps
`taskmanager:1:2g,kafka:2:2g,benchmark:2:1g`, 20 s measured after a 5 s warmup, seed 42,
no duplicates and no backdating. Only the symbol distribution and the symbol count move.

| Condition | Symbols | Distribution | How |
|---|---|---:|---|
| `uniform-1000` | 1,000 | uniform | `matrix.py --skew 0` |
| `longtail-1000` | 1,000 | measured week shares | `--distribution-file` from `week-profile.json` ranks 1–1,000 |
| `longtail-2000` | 2,000 | measured week shares | ranks 1–2,000; ranks 1–1,000 are **identical** to the previous row, the tail is appended |

The distribution inputs are published as `symbol-weights-1000.json` and
`symbol-weights-2000.json`. Normalized over the selected prefix they cover 2.70% /
17.75% / 66.11% of traffic in the top 1 / 10 / 100 symbols for 1,000 symbols (week-wide:
2.39% / 15.71% / 58.53%), and 2.56% / 16.87% / 62.83% for 2,000. Prefix renormalization
inflates the shares slightly because the dropped tail mass is redistributed over the
kept symbols; the *shape* is the measured one.

## Results (`benchmarks/evidence/long-tail/summary.json`)

| Condition | Achieved input | Committed/s | Delivered | p50 / p95 / p99 visible (ms) | Max partition / mean | Partition CV |
|---|---:|---:|---|---:|---:|---:|
| uniform-1000 | 4,000.0/s (100%) | 3,924.7 | 80,000 / 80,000, 0 missing, 0 extra | 676.8 / 1,856.8 / 2,654.3 | 1.122 | 0.094 |
| longtail-1000 | 4,000.0/s (100%) | 3,843.8 | exact | 664.9 / 1,966.0 / 2,699.3 | **1.692** | **0.320** |
| longtail-2000 | 4,000.0/s (100%) | 3,853.9 | exact | 639.2 / 1,413.8 / 2,199.3 | 1.103 | 0.108 |

Delivery was exact in all three conditions (zero missing, zero unexpected prices on
Kafka; drain 0.38–0.81 s), so the comparisons below are about load shape, not lost data.

1. **Skew costs throughput and concentrates partitions.** Measured long-tail at 1,000
   symbols committed 2.1% fewer prices per second than uniform traffic (3,843.8 vs
   3,924.7), with p95 5.9% higher; the visible tail (p99) moved only 1.7%. The sharper
   effect is on partition load: one of six Kafka partitions carried **28.2%** of the
   input (max/mean **1.69**) under the measured distribution versus 1.12 under uniform.
   With `parallelism=1` that is a broker/consumer-pressure effect, not a Flink-task
   imbalance.
2. **Doubling the symbol population at the same offered load smoothed partitions back
   toward uniform.** `longtail-2000` committed the same throughput as `longtail-1000`
   (1.003×) and ran the same delivered volume exactly, but max/mean partition load fell
   from 1.69 to **1.10** and the partition CV from 0.320 to 0.108. The hot symbols are
   unchanged; the extra 1,000 tail tickers spread keys across partitions. p95 fell 28%
   and p99 19% versus the 1,000-symbol long-tail run, which is within the range the
   checkpoint-visibility sampling can move at this length and should not be read as a
   latency improvement.

## Limits

- **One repetition per condition**, 20 s each; no spread and no repeat agreement check.
- The visible latency is **commit-visibility** latency under 1 s checkpoints
  (`visible_roundtrip_ms`), not the live path's deliberate 5 s disorder hold; it is
  comparable only within this ladder (and with ticket 08's ladder).
- Partition counts are *measured at the producer* (`msg.partition()` on delivery) for
  the generator's synthetic ticker keys, not DEBS symbol strings. The comparison between
  conditions is internally valid because only the weights/count moved, but the absolute
  1.69 figure is a property of this key space, not a prediction for real symbols.
- Exchange mix is the generator's fixed round-robin (FR/NL/ETR), not the week's
  FR-heavy mix; it is identical in all three conditions.
- No gateway in this pair: this measures the processing path, not delivery to clients.

## Reproduce

```sh
python3 benchmarks/growth.py --tickets 09
python3 -m unittest benchmarks.tests.test_growth
```

Artifacts: `benchmarks/evidence/long-tail/{summary.json,symbol-weights-1000.json,symbol-weights-2000.json,runs/}`;
raw matrix sessions are named in `summary.json.source_sessions` under
`benchmarks/results/`.
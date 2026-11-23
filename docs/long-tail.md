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

The quoted triple uses a **fixed ticker prefix** so all conditions share an identical key
set and the Kafka partition mapping is reproducible. (Two earlier triples used per-run
ticker prefixes; they agree on the throughput ordering but their partition numbers moved
with the hash — see the limits.)

| Condition | Achieved input | Committed/s | Delivered | p50 / p95 / p99 visible (ms) | Max partition / mean | Partition CV |
|---|---:|---:|---|---:|---:|---:|
| uniform-1000 | 4,000.0/s (100%) | 3,982.7 | 80,000 / 80,000, 0 missing, 0 extra | 662 / 1,887 / 2,593 | 1.123 | 0.087 |
| longtail-1000 | 4,000.0/s (100%) | 3,879.9 | exact | 668 / 1,586 / 2,316 | **1.271** | **0.157** |
| longtail-2000 | 4,000.0/s (100%) | 3,848.3 | exact | 699 / 1,864 / 2,317 | 1.270 | 0.153 |

Delivery was exact in all three conditions (zero missing, zero unexpected prices on
Kafka, zero missing EMAs), so the comparisons below are about load shape, not lost data.

1. **Skew costs throughput and concentrates partitions.** Measured long-tail at 1,000
   symbols committed **2.6% fewer** prices per second than uniform traffic (3,879.9 vs
   3,982.7); two earlier triples with different synthetic keys reproduced the direction
   at 2.1% and 2.0%. With the fixed key set the hottest partition carried 21.2% of the
   input versus 18.7% under uniform (max/mean **1.271 vs 1.123**, CV 0.157 vs 0.087):
   the measured weights add about two and a half percentage points to the busiest
   partition. With `parallelism=1` that is broker/consumer pressure, not a Flink-task
   imbalance.
2. **Doubling the symbol population did not change partition concentration and cost
   nothing measurable in throughput.** `longtail-2000` committed 3,848.3/s (0.8% below
   the 1,000-symbol long-tail run, +0.3% and +4.5% in the earlier triples — i.e. no
   consistent penalty) with exact delivery, and its partition shares are essentially
   identical to `longtail-1000` (max/mean 1.270, CV 0.153). Ranks 1–1,000 keep the same
   keys and weights; the appended tail tickers spread almost uniformly, so the hot
   partition stays hot. An earlier per-run-prefix triple appeared to *reduce* skew at
   2,000 symbols (1.69 → 1.10); that reversal was hash remapping, not the distribution.
3. **Latency was not separable at this length.** p99 was 2,316–2,593 ms in all three
   conditions with no consistent ordering (uniform had the highest p99 in this triple,
   the lowest in both earlier triples). This latency is commit visibility under 1 s
   checkpoints on 20 s runs; it should not be read as a skew effect.

## Limits

- **One repetition per condition**, 20 s each. Three independent triples were run; they
  agree on delivery exactness and on the throughput ordering, but only the fixed-prefix
  triple gives a reproducible partition mapping, and it is the one quoted. Sessions:
  fixed-prefix quoted triple `b6e4a0ece593` / `ddf66eb58ae5` / `ff5408db1ae2`; earlier
  per-run-prefix triples `5058cb4b9046` / `a2800a3eddae` / `c827d6b5db8e` and
  `ff1455612cf5` / `f9e634201e00` / `dd17b66d300a` (raw artifacts under
  `benchmarks/results/`).
- Partition counts are measured at the producer (`msg.partition()`) for the generator's
  **synthetic ticker keys**. With the fixed prefix the key sets are identical across
  conditions, so the comparison is exact for this key space; the absolute shares are not
  a prediction for DEBS symbols, whose strings hash differently.
- The visible latency is **commit-visibility** latency under 1 s checkpoints
  (`visible_roundtrip_ms`), not the live path's deliberate 5 s disorder hold; it is
  comparable only within this ladder (and with ticket 08's ladder).
- Exchange mix is the generator's fixed round-robin (FR/NL/ETR), not the week's
  FR-heavy mix; it is identical in all conditions.
- No gateway in this pair: this measures the processing path, not delivery to clients.

## Reproduce

```sh
python3 benchmarks/growth.py --tickets 09   # passes --ticker-prefix B for a stable mapping
python3 -m unittest benchmarks.tests.test_growth
```

Artifacts: `benchmarks/evidence/long-tail/{summary.json,symbol-weights-1000.json,symbol-weights-2000.json,runs/}`;
raw matrix sessions are named in `summary.json.source_sessions` under
`benchmarks/results/`.
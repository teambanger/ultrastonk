# 09: Measure long-tail and symbol growth effects

**What to build:** An evaluator can distinguish the effect of hot symbols from the effect of a larger symbol population.

**Blocked by:** 02 - Profile the official week and identify the real peak; 08 - Bracket the Sustainable limit with one and two workers.

**Status:** resolved

- [ ] At a documented passing baseline rate, compare uniform traffic with a measured long-tail distribution derived from the verified dataset. Use comparable achieved input, exchange mix, symbols, subscribers, resources, and query settings.
- [ ] Independently increase symbol count to 2x at the declared baseline; hold load, resource budget and subscription conditions fixed, and describe hot-key coverage and any intentional changes to rank weights.
- [ ] Publish distribution inputs, exact delivery and backlog evidence, latency and per-partition or per-task load imbalance. Distinguish changed skew from more symbols and preserve inconclusive or failing runs.

## Comments

- **Conditional and downscaled by team decision (2026-11-23).** Blocked behind ticket 08, and the programme must finish within roughly two hours of machine time, so both growth conditions (long-tail versus uniform, and 2x symbols) are reduced to a single short comparison at one documented baseline rate, run only if the clock allows. If they are not run, this ticket is reported as reduced/deferred with the reason rather than left ambiguous. Any result obtained under a single repetition is directional: it shows whether skew or symbol count moved delivery or imbalance at that baseline, and it does not establish a limit or a scaling law.

- **MEASURED and resolved (2026-11-23).** `benchmarks/growth.py` ran three conditions at the ticket-08
  baseline (4,000 offered/s, one TaskManager, `NONE`/`DROP`, 1 s window, 100 ms disorder, 20 s measured,
  seed 42, one repetition, no gateway): uniform 1,000 symbols, measured week weights for 1,000 symbols,
  and the same weights extended to 2,000 symbols with ranks 1–1,000 unchanged. Delivery was exact in all
  three (80,000/80,000, zero missing/extra). Three independent triples were run; the quoted one fixes the
  ticker prefix so all conditions share one key set and the Kafka partition mapping is reproducible:
  committed 3,982.7 / 3,879.9 / 3,848.3 prices/s; measured per-partition input max/mean **1.123 / 1.271 /
  1.270** (CV 0.087 / 0.157 / 0.153; busiest partition 21.2% vs 18.7% of input). Skew costs ~2–2.6% of
  throughput and concentrates the busiest partition; doubling the symbol population leaves concentration
  and throughput unchanged. The two earlier per-run-prefix triples agree on delivery and throughput
  ordering but their partition numbers move with the key hash (1.3–1.7 max/mean for the same
  distribution), so the fixed-prefix triple is the quoted one. Distribution inputs are published as
  `benchmarks/evidence/long-tail/symbol-weights-{1000,2000}.json`; argument in `docs/long-tail.md`.
  Limits: one repetition per condition; visible latency is 1 s-checkpoint commit visibility, not the live
  disorder path, and was not separable between conditions; partition counts are measured on the
  generator's synthetic keys (exact comparison only for that fixed key space, not a DEBS prediction);
  exchange mix is the generator's fixed round-robin; no gateway in this pair. Directional only.

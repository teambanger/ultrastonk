# 11: Quantify the drop-versus-grace trade-off

**What to build:** The team can defend its lateness policy with a paired comparison that shows what is gained or lost in advisory correctness, publication delay, and managed state.

**Blocked by:** 03 - Deliver live BUY/SELL to subscribers.

**Status:** resolved

- [x] Run matched prompt/drop and grace trials with identical scheduled disorder, seed, deduplication settings, resources, and checkpoint cadence. Include a scripted late trade that can change a closing price and a crossover.
- [x] Compare committed EMA/advisory results to complete-input oracle truth, with independent policy-aware fixture expectations where admission is prescribed. Count missing, extra, wrong, and delayed advisories; distinguish expected policy differences from incorrect processing or delivery.
- [x] Report finalization/publication delay and measured state, checkpoint or resource cost, with scope and sample sizes. Include a reproducible comparison and a recommendation for the default backed by the measured trade-off.

## Answer

Integrated as merge `49a8560` (branch `drop-vs-grace`). Both arms run the unmodified production query (the branch changes no `flink/` sources) on one scripted fixture (digest `f481616b…`) with identical seed `11`, 300,000 ms windows, 5,000 ms disorder budget, 30,000 ms grace, `DEDUP_STRATEGY=NONE`, parallelism 2, six input partitions and 1,000 ms checkpoints. Only `LATE_EVENT_STRATEGY` differs: both arms' recorded `settings` blocks are byte-identical and their `policy` blocks differ. Two replicates per arm.

The fixture schedules one trade per window for windows `0..30` (window 0 price `39`, the rest `0.01`) then window `31` with an early trade (`0.01`) plus a late trade (`1000`) delivered after nominal finalization but inside the grace budget, so `DROP` rejects it and `GRACE` admits it, changing window 31's closing price and producing a crossover. A runner sync step prescribes admission by waiting for the watermark at each target rather than leaving it to scheduler timing.

Against complete-input oracle truth (which equals the `GRACE` expectation): `DROP` commits 34 records (32 EMA, BUY@0, SELL@30) with 0 policy defects and truth loss of 1 missing advisory (BUY@31) plus 2 incorrect EMA values on window 31; `GRACE` commits 35 records with 0 defects and 0 truth loss. Replicates agree exactly. The one expected policy difference is isolated from defects, so neither arm shows a processing or delivery defect.

Trade-off: `GRACE` costs a policy-intrinsic 30,000 ms event-time finalization delay (`DROP` 0 ms) and a small managed-state increase — one completed checkpoint per arm: 78,026 → 79,126 bytes (R1, +1,100) and 79,054 → 83,398 bytes (R2, +4,344). Measured wall-clock finalization (R1 10,173 ms DROP / 804 ms GRACE; R2 10,244 / 85 ms) is recorded but is not a latency claim: synthetic event times, one late window per arm, dominated by startup and checkpoint visibility. Recommendation: default to `GRACE` for this workload, reserving `DROP` for workloads where no eligible late trade can change a closing price. Evidence: `benchmarks/evidence/drop-vs-grace/{comparison.json,arm-R1-DROP.json,arm-R1-GRACE.json,arm-R2-DROP.json,arm-R2-GRACE.json,README.md}`, with reproduction commands in the README; 28 unit tests pass and the full benchmark suite is green.

## Comments

Limits: no per-service CPU/memory caps were applied and no resource samples were collected in either arm (`resources` is empty), so "identical resources" holds by construction — same Compose file and identical settings, only the policy variable differs — rather than by measurement. Timing is relative because the full-week reference run was active on the same host. The disorder is scripted and synthetic, not historical market lateness; there is one late window per arm; the fixture does not exercise recovery, side outputs, or `TIMESTAMP`/`EXACT` dedup.

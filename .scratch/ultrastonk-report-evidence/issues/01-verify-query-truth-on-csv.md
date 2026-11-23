# 01: Verify query truth on a hand-checked CSV

**What to build:** A teammate can run a small raw CSV through an independent oracle and inspect the exact EMA38, EMA100, BUY, and SELL results expected from the pipeline.

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] Review the contract for trade eligibility, exchange-qualified symbol identity, event versus capture time, timezone and timestamp fallback, precision and tie ordering, empty windows, deduplication, lateness, and final flush. Record interpretations separately from observations about current Flink behavior.
- [x] The independent oracle reads raw CSV without using production parsers or observed Flink results. Hand-calculated expectations cover five-minute midnight-aligned windows, zero-initialized EMA pairs, the first divergence, both crossover directions, equality, non-crossing continuations, and symbol isolation.
- [x] The comparator checks `(symbol, window, type)`, exact advisory directions, and EMA values with `abs_tol=1e-9` and `rel_tol=1e-9`. Tests deliberately introduce missing, extra, duplicate, and incorrect results and verify that they fail.
- [x] Document a reproducible fixture invocation and a worked crossover; keep complete-input expectations separate from any prescribed live-policy expectations.

## Comments

Integrated oracle commit `e000147` via merge `d1557b2`. Benchmark suite: 12 tests passed. The comparator currently loads inputs into memory; ticket 04 must address bounded comparisons before full-week use. Complete-input truth does not establish live-policy correctness.

# 09: Measure long-tail and symbol growth effects

**What to build:** An evaluator can distinguish the effect of hot symbols from the effect of a larger symbol population.

**Blocked by:** 02 - Profile the official week and identify the real peak; 08 - Bracket the Sustainable limit with one and two workers.

**Status:** wontfix

- [ ] At a documented passing baseline rate, compare uniform traffic with a measured long-tail distribution derived from the verified dataset. Use comparable achieved input, exchange mix, symbols, subscribers, resources, and query settings.
- [ ] Independently increase symbol count to 2x at the declared baseline; hold load, resource budget and subscription conditions fixed, and describe hot-key coverage and any intentional changes to rank weights.
- [ ] Publish distribution inputs, exact delivery and backlog evidence, latency and per-partition or per-task load imbalance. Distinguish changed skew from more symbols and preserve inconclusive or failing runs.

## Comments

- **Conditional and downscaled by team decision (2026-11-23).** Blocked behind ticket 08, and the programme must finish within roughly two hours of machine time, so both growth conditions (long-tail versus uniform, and 2x symbols) are reduced to a single short comparison at one documented baseline rate, run only if the clock allows. If they are not run, this ticket is reported as reduced/deferred with the reason rather than left ambiguous. Any result obtained under a single repetition is directional: it shows whether skew or symbol count moved delivery or imbalance at that baseline, and it does not establish a limit or a scaling law.

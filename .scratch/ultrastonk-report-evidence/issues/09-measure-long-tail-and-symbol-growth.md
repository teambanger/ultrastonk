# 09: Measure long-tail and symbol growth effects

**What to build:** An evaluator can distinguish the effect of hot symbols from the effect of a larger symbol population.

**Blocked by:** 02 - Profile the official week and identify the real peak; 08 - Bracket the Sustainable limit with one and two workers.

**Status:** ready-for-agent

- [ ] At a documented passing baseline rate, compare uniform traffic with a measured long-tail distribution derived from the verified dataset. Use comparable achieved input, exchange mix, symbols, subscribers, resources, and query settings.
- [ ] Independently increase symbol count to 2x at the declared baseline; hold load, resource budget and subscription conditions fixed, and describe hot-key coverage and any intentional changes to rank weights.
- [ ] Publish distribution inputs, exact delivery and backlog evidence, latency and per-partition or per-task load imbalance. Distinguish changed skew from more symbols and preserve inconclusive or failing runs.

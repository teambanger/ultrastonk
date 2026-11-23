# 11: Quantify the drop-versus-grace trade-off

**What to build:** The team can defend its lateness policy with a paired comparison that shows what is gained or lost in advisory correctness, publication delay, and managed state.

**Blocked by:** 03 - Deliver live BUY/SELL to subscribers.

**Status:** ready-for-agent

- [ ] Run matched prompt/drop and grace trials with identical scheduled disorder, seed, deduplication settings, resources, and checkpoint cadence. Include a scripted late trade that can change a closing price and a crossover.
- [ ] Compare committed EMA/advisory results to complete-input oracle truth, with independent policy-aware fixture expectations where admission is prescribed. Count missing, extra, wrong, and delayed advisories; distinguish expected policy differences from incorrect processing or delivery.
- [ ] Report finalization/publication delay and measured state, checkpoint or resource cost, with scope and sample sizes. Include a reproducible comparison and a recommendation for the default backed by the measured trade-off.

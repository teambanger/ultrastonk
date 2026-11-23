# 07: Measure live peak load and headroom

**What to build:** A report writer can tell whether the running pipeline kept up with the recorded peak and had headroom, independently of the full-week reference result.

**Blocked by:** 02 - Profile the official week and identify the real peak; 05 - Prove capture-paced End-to-end measurement.

**Status:** ready-for-agent

- [ ] Run three valid repeats per headline condition at 1x and 1.25x capture-time pacing on the same peak slice, preserving exchange mix and burst order. Document warm state or label the run cold, and record every attempt including failures and invalid runs.
- [ ] Show achieved pacing, expected and delivered counts by subscription, stage backlog, p50/p95/p99 and sample sizes, restarts/OOMs, and drain. Judge keeping up by correct delivery and no sustained backlog growth, not by eventual parity.
- [ ] Evaluate the 1x project target of p99 <= 1 s for trade enqueue to WebSocket and window finalization to advisory receipt independently. If signals are too sparse, label live advisory p99 inconclusive and report any controlled rollover fixture separately. Do not present these project thresholds as rubric requirements.
- [ ] Compare live query results to independent complete-input truth with verified pre-roll where possible; report policy-related deviations separately from transport loss and identify missing evidence. Do not extrapolate to full-week capture order.

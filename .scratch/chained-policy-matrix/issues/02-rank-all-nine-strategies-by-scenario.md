# 02: Rank all nine strategies by scenario

**What to build:** Produce a scored comparison of the nine deduplication and lateness combinations on the verified chained trace, and publish recommendations that explain which combinations work best for each scenario and why.

**Blocked by:** 01: Verify and score a chained scenario replay

**Status:** ready-for-agent

- [ ] Before scoring, freeze the trace, workload and resource settings, metric normalization, and a scoring rule with equally weighted scenarios. Use 50% signal fidelity, 25% speed, and 25% resources as the starting preference. Gate out unexpected output or delivery errors and missing required audits before ranking; keep intentional policy differences visible as fidelity losses, not implementation defects.
- [ ] Run `NONE`, `EXACT`, and `TIMESTAMP` crossed with `DROP`, `SIDE_OUTPUT`, and `GRACE` under identical conditions on an otherwise idle host. Record the first pass for all nine before spending time on repeats of the top two or close results. Keep the scored campaign under one hour of machine time; record failed or incomplete arms and any repeats the budget prevented.
- [ ] Publish raw metrics and scores per scenario and overall, with run IDs and reproducible inputs. Keep price p99, advisory timing, burst recovery, and resource costs distinguishable. Show whether reasonable changes to scoring weights change the recommendation; report ties or trade-offs rather than forcing a winner.
- [ ] Link the result from the benchmark evidence index and Creative Explorations in the evaluation answer sheet. State which recommendations apply to the tested synthetic workload and do not claim a Historical replay result, a universal winner, or a Sustainable limit.

## Comments

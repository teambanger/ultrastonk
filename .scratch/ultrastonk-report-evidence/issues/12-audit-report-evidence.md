# 12: Audit and package the report evidence

**What to build:** A teammate can trace each proposed report claim to a reproducible artifact and see which rubric requirements lack sufficient evidence before writing the report.

**Blocked by:** 06 - Verify both queries over the entire week; 07 - Measure live peak load and headroom; 09 - Measure long-tail and symbol growth effects; 10 - Measure subscriber growth on one gateway; 11 - Quantify the drop-versus-grace trade-off.

**Status:** ready-for-agent

- [ ] Map all eight scored system/report criteria and both contribution criteria to claim and run ledgers. Each claim has a scope, unit/denominator, dataset/revision/configuration, source locator, limitations, and reviewer. Retain failed and inconclusive attempts and mark unsupported claims explicitly.
- [ ] Package a verified pipeline diagram, field/metadata mapping, worked crossover, full-week coverage/error table, real workload shape, and a compact capacity/latency/resource comparison. Link commands, seeds and stable sanitized artifacts with checksums; exclude raw market data, secrets, and raw broker content.
- [ ] Record ownership, decisions, cross-review, and each team member's answers about decisions, challenges, and contribution/team functioning. Have a teammate reproduce a small fixture and regenerate at least one table or figure using the pack.
- [ ] Provide a writer handoff with source citations, figure units/configuration, the evidence-to-section map, and checks for the eventual 4-5 page ACM report and public GitHub link. Do not write or submit the report as part of this ticket.

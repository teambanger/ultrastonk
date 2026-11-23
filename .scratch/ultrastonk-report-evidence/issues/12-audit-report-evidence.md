# 12: Audit and package the report evidence

**What to build:** A teammate can trace each proposed report claim to a reproducible artifact and see which rubric requirements lack sufficient evidence before writing the report.

**Blocked by:** 06 - Verify both queries over the entire week; 07 - Measure live peak load and headroom; 09 - Measure long-tail and symbol growth effects; 10 - Measure subscriber growth on one gateway; 11 - Quantify the drop-versus-grace trade-off.

**Status:** ready-for-agent

- [ ] Map all eight scored system/report criteria and both contribution criteria to claim and run ledgers. Each claim has a scope, unit/denominator, dataset/revision/configuration, source locator, limitations, and reviewer. Retain failed and inconclusive attempts and mark unsupported claims explicitly.
- [ ] Package a verified pipeline diagram, field/metadata mapping, worked crossover, full-week coverage/error table, real workload shape, and a compact capacity/latency/resource comparison. Link commands, seeds and stable sanitized artifacts with checksums; exclude raw market data, secrets, and raw broker content.
- [ ] Record ownership, decisions, cross-review, and each team member's answers about decisions, challenges, and contribution/team functioning. Have a teammate reproduce a small fixture and regenerate at least one table or figure using the pack.
- [ ] Provide a writer handoff with source citations, figure units/configuration, the evidence-to-section map, and checks for the eventual 4-5 page ACM report and public GitHub link. Do not write or submit the report as part of this ticket.

## Comments

- **Started early and in parallel by team decision (2026-11-23).** The programme must finish within roughly two hours of machine time, so the index (`benchmarks/evidence/README.md`) and the audit (`docs/evidence-audit.md`) are being built now against the artifacts that exist (tickets 01-06 and 11, plus the 07/08 harnesses), with tickets 07, 08, 09 and 10 marked PENDING rather than guessed. The pack must label every reduced item — the dropped ordered whole-week replay, 07's reduced repeats, 08's single-repetition ladder, and 09/10's conditional status — and must not present reduced-repeat results as equal-confidence evidence. Ticket 12 stays open until the pending runs land and a teammate reproduces one table or figure from the pack alone.

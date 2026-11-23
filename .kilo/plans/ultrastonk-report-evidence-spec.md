# Build reproducible report evidence for Ultrastonk

## Problem Statement

The Ultrastonk team needs defensible evidence for an Exceptional course evaluation. Today, the production Flink path emits prices and EMAs but not BUY/SELL advisories. Existing tests and short benchmark runs show some correct delivery and exercise the harness, but they do not prove both queries over the official week, timely delivery during recorded peak intervals, or a Sustainable limit. The current bounded Historical replay is a preload-and-process run, not a live latency measurement. The report writers need to know what was measured, how to reproduce it, and which claims the evidence does not support.

## Solution

Deliver live advisories and a reproducible evidence pack covering dataset provenance, independent query correctness, complete-week Historical replay, capture-paced End-to-end experiments, controlled scaling and policy comparisons, and team contributions. Use one traceable claim ledger to connect every scored criterion to the exact data, revision, run, and artifact that supports it. Keep exact correctness on a separately labeled reference replay distinct from the behavior of the live capture-order policy.

## User Stories

1. As an experimenter, I want a verified manifest for all seven official CSV files, so that I can prove which dataset revision and dates I processed.
2. As a report writer, I want independently counted rows by day, exchange, security type, and row disposition, so that I can distinguish the dataset from the subset that contributes to the queries.
3. As a report writer, I want raw-row and trade-rate time series and symbol-rank distributions, so that I can justify the peak fixture and explain long-tail load.
4. As an implementer, I want a reviewed contract covering capture time, event time, timestamp fallback and precision, trade eligibility, tie order, empty windows, deduplication and lateness, so that ambiguities are resolved before measuring.
5. As a subscriber, I want EMA38 and EMA100 based on the last eligible trade in each five-minute symbol window, so that the displayed indicators follow the project specification.
6. As a subscriber, I want BUY and SELL only on the specified EMA sign transitions, so that I see meaningful crossovers rather than repeated signals.
7. As a subscriber, I want the gateway to deliver only the symbols I requested, so that I can follow my watchlist without unrelated updates.
8. As an evaluator, I want evidence that unsubscribed symbols are still computed, so that subscription filtering cannot conceal incomplete query execution.
9. As an evaluator, I want an independent CSV-based oracle checked against hand-calculated fixtures, so that the system is not graded against its own logic or output.
10. As an evaluator, I want a full-week comparison listing missing, extra, duplicate, numeric and advisory-direction mismatches, so that correctness is verifiable rather than inferred from aggregate counts.
11. As an evaluator, I want the final nonempty window included in the comparison, so that reaching end of input cannot hide incomplete results.
12. As an experimenter, I want an agreed separately labeled reference replay when capture-order lateness prevents exact comparison, so that I can verify the query logic without claiming that live policy preserves every eligible trade.
13. As a report writer, I want row and output reconciliation and distinct ingestion, processing and drain timings, so that I can describe the full-week result without claiming it measures live throughput or latency.
14. As an experimenter, I want a repeatable peak fixture with preserved capture order, gaps and exchange mix, so that I can replay the historical load without changing event-time semantics.
15. As an experimenter, I want to verify pacing and warmup independently of query outputs, so that I can distinguish a real-time result from a late publisher or a cold EMA state.
16. As an evaluator, I want repeatable 1x and 1.25x peak End-to-end experiments, so that I can see timely delivery at the observed load and any measured headroom.
17. As a report writer, I want price enqueue-to-receipt and window-finalization-to-advisory latency measured separately from historical event age and watermark waiting, so that I can describe what a subscriber actually waits for.
18. As an experimenter, I want correct delivery and time-aligned progress through producer, committed Kafka and WebSocket, so that drain cannot be mistaken for keeping up under load.
19. As an experimenter, I want to bracket the Sustainable limit for one and two TaskManagers with verified caps, so that I can quantify scaling under comparable conditions.
20. As an evaluator, I want measured long-tail and uniform workloads compared at the same load, so that any hot-symbol effect is supported by evidence.
21. As an experimenter, I want to vary symbol count and subscriber count separately, so that I can measure their effects without confounding fan-out with processing load.
22. As an experimenter, I want targeted Module experiments or one-budget perturbations when needed, so that a claimed bottleneck is supported by more than correlated CPU throttling.
23. As an evaluator, I want a matched comparison of prompt finalization and grace under controlled disorder, so that I can see the correctness, latency and state-cost trade-off.
24. As a demo viewer, I want to see both EMA lines and real BUY/SELL markers with the triggering window, so that I can interpret the query result without using fake mode.
25. As a report writer, I want failures, missing telemetry, limits and sample sizes retained alongside successful runs, so that uncertainty is visible.
26. As a report writer, I want a revision-pinned architecture diagram, field mapping and worked crossover, so that I can explain design and implementation concisely.
27. As a teammate, I want linked commands, seeds, effective configurations and durable artifacts, so that I can reproduce a fixture and regenerate a reported figure.
28. As a team member, I want a record of design decisions, challenges, ownership, reviews and my contribution, so that the required teamwork and individual reflections are factual.
29. As a report writer, I want compact, labeled figures, citations and criterion-to-claim mapping, so that I can assemble a 4-5 page ACM report without inventing results.

## Implementation Decisions

- Acquire the seven files from the official DOI `10.5281/zenodo.6382482`; the current downloader covers only one day. Verify provider checksums, record local SHA-256 and exact row counts, and size broker retention and scratch storage on a pilot. Keep raw data read-only and out of the published pack.
- Use five-minute midnight-aligned event-time windows, exchange-qualified symbol keys and zero-initialized EMA pairs. At finalization, emit at most one advisory for a symbol window: BUY when the previous EMA38-minus-EMA100 difference was nonpositive and is now positive; SELL when it was nonnegative and is now negative. Other transitions produce no signal. Use the production keyed/checkpointed query and preserve the existing four-field output contract and closing-tick timestamp unless a fixture demonstrates a need to change them. The gateway forwards committed results and does not compute signals.
- Build a separate batch oracle from raw CSV and the reviewed semantic contract. Use disk-bounded aggregation and original source ordinals for deterministic ties. Compare by symbol, window and type. Require exact keys and directions with no missing, extra, duplicate or out-of-tolerance outputs in reference mode; compare EMA values at absolute and relative tolerance `1e-9`. Keep complete-input truth separate from policy-aware expectations; do not derive expected admission from Flink's decisions.
- Pilot bounded replay on a representative slice before the full week, including reversals and end-of-input finalization. If ordinary replay drops eligible trades, use the agreed separately labeled event-time-ordered reference replay through the same production operators. Preserve the input multiset and source ordinals on ties, do not precompute closes, verify zero late exclusions, and account for sorting cost separately. Report live capture-order deviations against the ideal independently. A live peak slice is not full-week capture-order validation.
- Add opt-in capture-time pacing to Historical replay while preserving exchange event time for query processing. Select the highest rolling five-minute raw-row capture interval across the three exchanges and record the trade peak separately if it differs. Preserve burst shape, pre-roll EMA state where feasible, use unscored trailing input to advance watermarks, and check scheduled versus achieved pacing.
- Run on fresh projects/topics and consume `read_committed`. Preserve separate timing for enqueue, broker acknowledgement, committed observation, window eligibility/finalization and WebSocket receipt where measured. Verify clock error or use a common time basis, preserve event identity when timestamps collide, and use bounded latency sampling and streamed comparisons. Tune checkpoint cadence only in a separately recorded comparison before freezing scored configurations.
- Run three valid replicates per headline peak condition. The 1x project goals are exact subscribed delivery, no sustained backlog growth, and p99 at most 1 s for trade enqueue to WebSocket receipt and window finalization to advisory receipt. Report sample counts, checkpoint and watermark waiting, and drain separately. If live signals are too sparse, use a labeled rollover fixture and do not substitute its p99 for a real-peak p99. These are project targets, not grading thresholds.
- Use stepped synthetic End-to-end experiments to bracket Sustainable limits and repeat boundary runs. Before scoring, fix a pacing tolerance (at least 98% achieved for synthetic steps), steady measurement windows, checkpoint-aware backlog criteria and output correctness rules. Generator shortfall or missing required telemetry makes that capacity attribution inconclusive. Report median and replicate spread; keep steady throughput separate from delivery including drain.
- Hold per-worker limits fixed for the primary one-versus-two TaskManager comparison and disclose aggregate CPU. Test measured long-tail versus uniform traffic, then vary symbol population and subscriber count independently, keeping one gateway. Record CPU utilization/throttling, memory, checkpoints, lag and drop/backpressure evidence. Treat a 1.5x worker-capacity gain as a stretch goal only where Flink is demonstrably limiting.
- Compare drop and grace on matched inputs and schedules against the independent oracle, alongside finalization delay and state/resource cost. Use live output for the annotated chart. Do not use gateway-only or fake results to support query or pipeline claims.
- Ship a compact evidence pack with dataset and run manifests, reference diffs, raw artifact links/checksums, reproducible summaries, architecture and implementation notes, and contribution records. Each claim names its criterion, scope, unit and denominator, configuration, source revision, run IDs, artifact location, limits and reviewer. Index failed and inconclusive runs. Keep public evidence sanitized and verify the eventual report's public repository link and page limit.

## Testing Decisions

- Use one primary seam: launch an isolated run through the existing verifier or experiment runner, supply recorded or deterministic inputs, and assert observable output at committed Kafka and subscribed WebSocket boundaries in the same run. Check report artifacts at that seam rather than asserting internal queue, timer or state implementation details.
- Extend the existing deterministic integration verifier and gateway parity conventions. Check BUY, SELL, equality boundaries, ties, empty and final windows, overlapping and disjoint subscriptions, and the absence of unrelated live events. Test history delivery separately from live subscriptions. Reuse existing operator tests for narrowly controlled watermark and checkpoint-restore scenarios where the full pipeline cannot force the required ordering reliably.
- Validate the independent oracle against hand-calculated windows before comparing it with Flink. Exercise the comparator with intentionally missing, duplicate and incorrect outputs. Treat the whole-week comparison as an evidence run, not a requirement for the fast test suite.
- Reuse benchmark manifest and reporting patterns. Run a pilot to prove capture pacing, full slice reconciliation, effective limits, output coverage and telemetry; inject missing telemetry to confirm it cannot yield a passing capacity claim. Require a passing pilot before allocating the full-week job.
- Require an independent teammate to reproduce a small fixture and regenerate at least one result table or figure using the evidence pack alone.

## Out of Scope

- Writing the final report, submitting coursework, or guaranteeing a particular grade.
- Replacing Kafka/Flink, deploying multiple gateways, building new HA infrastructure, adding GPU support or running a broad failure campaign.
- Committing raw market data, raw broker contents or credentials, or implying strategy profitability from crossover correctness.
- Implementing any change during specification publication.

## Further Notes

- Cover all eight 3-point rubric criteria: functionality, performance, scalability, creative explorations, architecture, implementation explanation, evaluation and performance, and academic writing. Also cover both 1-point criteria. Each member supplies the rubric's three individual reflections; capture ownership, cross-review and shared reproducibility evidence for teamwork.
- Start data acquisition/profiling and the hand-calculated fixture in parallel. Freeze semantics before developing the independent oracle and the production crossover. Pilot correctness and measurement before the week and peak trials. Avoid concurrent scored workloads on the measured host. Prioritize full-week correctness, real peak, a worker/resource comparison and the matched policy comparison over broad sweeps.
- Existing capped smoke results at 5,000 originals/s show eventual delivery after drain, not a Sustainable limit. Prior gateway-only runs bypass Flink. Preserve these as explicitly scoped historical evidence.

## Scope Reduction (2026-11-23, team decision)

The team directed that the measurement programme finish within roughly two hours of machine time, so the plan above was cut. Every reduction is recorded here and in the affected tickets so the report states confidence honestly rather than implying the full plan was executed.

- **Whole-week correctness.** The labeled event-time-ordered replay over all seven files is dropped: the recorded capture order is the ordering the pipeline actually receives from the source, so the capture-order whole-week run is the primary whole-week evidence. It is reported as the labeled capture-order mode with its deviations quantified — 10,669,014 eligible late exclusions at the 5 s out-of-order budget, 376,584 missing (symbol, window) closes, 1,482,724 numeric, 6,228 extra, 0 duplicate, 0 wrong-direction, final window present, all source rows accounted for, input accounting exact. A bounded ordered reference (at most one day, hard-capped at 20 minutes) may be added as the labeled reference-mode check; if it is skipped, complete-input correctness rests on ticket 04's ordered reference slice (zero mismatches) together with the capture-order deviation report. No step may present the capture-order run as exact full-input correctness.
- **Ticket 07 (live peak).** 1x capture pace and warm state only; the 1.25x condition and the cold variant are dropped; pre-roll 120 s and trailing 90 s; one or two repeats decided by measured run cost; 40-minute wall-clock cap.
- **Ticket 08 (capacity bracket).** One coarse ladder (500/1000/2000/4000 originals/s, adding 8000 only if all pass) with 30 s runs, a single repetition, both worker counts in one batch, and at most one fine confirmation; 45-minute cap. Bounds may therefore be loose or `unbracketed`, and that outcome is reported as such instead of being tightened away.
- **Tickets 09 and 10 (growth).** Reduced to single-condition comparisons run only if the clock allows; otherwise reported as reduced or deferred with the reason.
- **Ticket 12 (packaging).** Runs in parallel, and must label every reduced item and never present reduced-repeat results as equal-confidence evidence.

Reduced-repeat and reduced-ladder results are directional evidence about the Sustainable limit and the worker comparison, not the three-repeat bracket the original plan specified.

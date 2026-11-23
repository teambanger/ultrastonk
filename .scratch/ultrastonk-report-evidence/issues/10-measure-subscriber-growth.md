# 10: Measure subscriber growth on one gateway

**What to build:** An evaluator can see how increasing subscribers affects a single live gateway while the upstream processing workload remains unchanged.

**Blocked by:** 08 - Bracket the Sustainable limit with one and two workers.

**Status:** resolved

- [ ] Run a baseline and 2x client count on one gateway at a verified baseline. Hold offered event load, symbols, subscriptions per client, subscription assignment method, and resource configuration fixed. Subscribe clients before the measurement interval.
- [ ] Verify expected per-client live price and query delivery against read-committed output, with history separate. Record fan-out messages per second, sample counts, latency tails, gateway CPU/memory, send-queue or drop/disconnect evidence, and upstream backlog.
- [ ] Link the paired End-to-end experiment artifacts and note any gateway limit, upstream limit, or inconclusive evidence. Treat existing gateway-only measurements as Module experiments, not pipeline evidence.

## Comments

- **Conditional and downscaled by team decision (2026-11-23).** Blocked behind ticket 08 and constrained by the two-hour programme budget, so the baseline-versus-2x-subscriber comparison is reduced to one short run at a verified baseline, executed only if the clock allows; if it is not run the ticket is reported as reduced/deferred with the reason. Per-client delivery verification, fan-out rate, latency tails, gateway CPU/memory and queue or disconnect evidence remain required for whatever is run, and a single repetition makes the result directional rather than a measured gateway limit.

- **MEASURED and resolved (2026-11-23).** One gateway, same ticket-08 baseline (4,000/s, one TaskManager,
  200 measured symbols, `gateway:1:512m`), 2 → 4 subscribers each subscribed to the full ticker set before
  the interval, every client verified independently against committed output. Monitored pair: fan-out
  8,475 → 17,022 frames/s (**2.009×**), per-client 3,845.8 → 3,861.1 prices/s (1.004×), zero missing
  prices/EMAs/advisories, zero errors, zero slow-client drops, client p99 2,243–2,261 → 2,451–2,522 ms,
  gateway 0.204 → 0.232 cores of 1 (throttle 4% → 8%), RSS ≈202 → 186 MB. An unmonitored repeat agrees
  (1.977×, exact, p99 2.9 → 2.8 s). TaskManager (0.997 core, all periods throttled) and Kafka (0.63, ~35%
  throttled) were the saturated components, so **no gateway limit is established**; at this baseline the
  gateway is not the subscriber-growth bottleneck. Evidence:
  `benchmarks/evidence/subscribers/{summary.json,runs/}`, argument in `docs/subscriber-growth.md`.
  Limits: one repetition per condition within each pair; broadcast fan-out grows linearly by construction;
  2–4 subscribers are far below `MAX_CLIENTS=1000`. Directional only.

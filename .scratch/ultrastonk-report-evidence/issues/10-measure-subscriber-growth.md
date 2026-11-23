# 10: Measure subscriber growth on one gateway

**What to build:** An evaluator can see how increasing subscribers affects a single live gateway while the upstream processing workload remains unchanged.

**Blocked by:** 08 - Bracket the Sustainable limit with one and two workers.

**Status:** wontfix

- [ ] Run a baseline and 2x client count on one gateway at a verified baseline. Hold offered event load, symbols, subscriptions per client, subscription assignment method, and resource configuration fixed. Subscribe clients before the measurement interval.
- [ ] Verify expected per-client live price and query delivery against read-committed output, with history separate. Record fan-out messages per second, sample counts, latency tails, gateway CPU/memory, send-queue or drop/disconnect evidence, and upstream backlog.
- [ ] Link the paired End-to-end experiment artifacts and note any gateway limit, upstream limit, or inconclusive evidence. Treat existing gateway-only measurements as Module experiments, not pipeline evidence.

## Comments

- **Conditional and downscaled by team decision (2026-11-23).** Blocked behind ticket 08 and constrained by the two-hour programme budget, so the baseline-versus-2x-subscriber comparison is reduced to one short run at a verified baseline, executed only if the clock allows; if it is not run the ticket is reported as reduced/deferred with the reason. Per-client delivery verification, fan-out rate, latency tails, gateway CPU/memory and queue or disconnect evidence remain required for whatever is run, and a single repetition makes the result directional rather than a measured gateway limit.

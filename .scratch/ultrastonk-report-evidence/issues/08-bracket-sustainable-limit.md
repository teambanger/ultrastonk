# 08: Bracket the Sustainable limit with one and two workers

**What to build:** An evaluator can compare the measured Sustainable limit of one and two TaskManagers for the same synthetic workload, without mistaking drain or generator limits for pipeline capacity.

**Blocked by:** 03 - Deliver live BUY/SELL to subscribers.

**Status:** ready-for-agent

- [ ] Reuse the synthetic runner with the live query revision. Hold partitions, policies, workload shape, per-worker caps and instrumentation fixed; record actual CPU/memory limits, aggregate worker CPU, hardware, and reached load for each run.
- [ ] Specify acceptance rules before scoring: at least 98% achieved input, correct delivery, and no sustained growth in stage backlog after accounting for checkpoint batching. Explore coarse steps, then run longer and repeat near passing/failing boundaries for both worker counts. Mark generator shortfall and missing required telemetry inconclusive.
- [ ] Export highest passing/first failing bounds or an explicitly unbracketed result, repeat spread, steady throughput, tails, drain, lag, backpressure, checkpoint behavior, throttling, and memory. Leave failed attempts in the run ledger.
- [ ] Where a bottleneck is claimed, support it with stage evidence and a one-budget change or Module experiment. Report worker-capacity gain with total resources; 1.5x is a stretch check only for a demonstrably Flink-limited workload.

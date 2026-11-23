# 06: Verify both queries over the entire week

**What to build:** A report writer can cite one full-week reconciliation that shows which rows were processed and whether both query outputs matched independent truth.

**Blocked by:** 02 - Profile the official week and identify the real peak; 04 - Reconcile a reference Historical replay slice.

**Status:** ready-for-agent

- [ ] With verified storage/retention and fresh topics, process all seven files without replay filtering, using the reference mode from the pilot. Record starting and ending offsets, exact input dispositions, acknowledged counts, accepted trades, replay mode, and configuration. Account for all source rows, including rows that cannot be parsed or do not contribute to a query.
- [ ] Compare read-committed EMA and advisory results by symbol, window and type, including the final window. Publish per-day/exchange reconciliation and missing, extra, duplicate, numeric, wrong-direction, and eligible-late-exclusion counts. Exact reference correctness is a pass only when those error counts are zero.
- [ ] Report preprocessing/sorting if applicable, ingestion, processing and drain separately, and include attained throughput, restart/OOM status and evidence links. Label the reference mode explicitly; a preload-and-process run does not measure live latency or live capture-order correctness.

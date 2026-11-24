# 01: Verify and score a chained scenario replay

**What to build:** Run a reproducible synthetic End-to-end experiment that compares `NONE + DROP` with `EXACT + GRACE` on the same chained input. An evaluator can trace differences in committed prices, EMA38/EMA100, BUY/SELL advisories, audits, and measured costs back to the scenario that produced them.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Generate and freeze one input trace and its replay schedule from the scenario emitter. Include clean trends and reversals, exact retries and same-timestamp corrections, closing trades that arrive within and after grace, and a burst with malformed records followed by recovery. Keep fault labels out of Kafka payloads. Use scenario-specific tickers plus a ticker spanning phases to expose carry-over in EMA state.
- [ ] Demo both pilot arms in fresh, isolated state with identical input order, offered schedule, and resources. Synchronize event-time boundaries for the late-close cases, keep real timing for the burst and recovery, and flush final windows without counting marker traffic as scenario output.
- [ ] Verify committed output, WebSocket delivery, and required audits against independent policy-aware expectations. Separately compare signals with a complete-valid-input reference that drops labeled retries but preserves legitimate corrections. Show per-phase EMA agreement within a declared tolerance and BUY/SELL precision and recall by ticker, window, and direction.
- [ ] Publish a reproducible pilot result with input identity, achieved and acknowledged load, price p99, separately defined advisory receipt timing, backlog recovery, CPU-seconds per 1,000 acknowledged originals, peak memory, and Flink-reported checkpoint bytes/s. Report missing telemetry as missing, not zero. Exercise the scoring path with a deliberate mismatch so a broken run cannot pass silently.

## Comments

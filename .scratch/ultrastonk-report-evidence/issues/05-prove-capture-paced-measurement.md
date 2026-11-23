# 05: Prove capture-paced End-to-end measurement

**What to build:** An experimenter can run a small live Historical replay and prove that capture-time pacing and latency measurements describe the publication-to-subscriber path, not historical event age.

**Blocked by:** 03 - Deliver live BUY/SELL to subscribers.

**Status:** ready-for-agent

- [ ] An opt-in mode preserves recorded capture order and gaps without replacing query event time. A controlled fixture distinguishes the two clocks, exercises the missing-capture-time rule, and checks scheduled versus actual pacing. Keep the pre-roll, measured interval, and trailing watermark-advancing input distinct.
- [ ] With a common time basis or documented clock error, correlate enqueue, acknowledgement, committed observation, window eligibility/finalization, and WebSocket receipt where applicable. Distinguish duplicate/tied identities, report sample counts, and separate historical event age and watermark waiting from transport latency.
- [ ] Verify subscribed delivery, lag/backlog, effective limits, and telemetry in a pilot. A missing required metric, insufficient pacing, or missing/duplicate delivery is detected rather than counted as a pass. Use bounded samples and record actual run settings.
- [ ] If checkpoint cadence is tuned toward the latency goal, document a matched comparison of visibility, throughput and checkpoint cost before freezing the scored configuration.

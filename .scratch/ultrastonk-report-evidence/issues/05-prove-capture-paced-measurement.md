# 05: Prove capture-paced End-to-end measurement

**What to build:** An experimenter can run a small live Historical replay and prove that capture-time pacing and latency measurements describe the publication-to-subscriber path, not historical event age.

**Blocked by:** 03 - Deliver live BUY/SELL to subscribers.

**Status:** resolved

- [x] An opt-in mode preserves recorded capture order and gaps without replacing query event time. A controlled fixture distinguishes the two clocks, exercises the missing-capture-time rule, and checks scheduled versus actual pacing. Keep the pre-roll, measured interval, and trailing watermark-advancing input distinct.
- [x] With a common time basis or documented clock error, correlate enqueue, acknowledgement, committed observation, window eligibility/finalization, and WebSocket receipt where applicable. Distinguish duplicate/tied identities, report sample counts, and separate historical event age and watermark waiting from transport latency.
- [x] Verify subscribed delivery, lag/backlog, effective limits, and telemetry in a pilot. A missing required metric, insufficient pacing, or missing/duplicate delivery is detected rather than counted as a pass. Use bounded samples and record actual run settings.
- [x] If checkpoint cadence is tuned toward the latency goal, document a matched comparison of visibility, throughput and checkpoint cost before freezing the scored configuration.

## Answer

Integrated as merge `44376d2` from `capture-pacing` tip `f44464d`. `PACING=capture` (default `event`) keeps exchange event time in the payload while pacing on the recorded capture clock, with a configurable missing-capture-time policy (`fail`/`skip`). The pilot used a 158-row two-symbol fixture where the capture clock trails event time by 250 ms and recorded pre-roll/measured/trailing segments of 10 s/60 s/60 s. Measured-segment achieved pacing was 1.00034 (threshold 0.98), and delivery matched exactly at ingest, committed Kafka, and WebSocket (158 prices + 10 EMAs = 168 records, no missing/extra/duplicate). Boundaries were correlated on one clock with the 250 ms offset documented; historical event age (~1.59e11 ms) and watermark waiting (p50 400 ms) are reported separately from transport. The pilot fails closed on missing telemetry, insufficient pacing, or delivery mismatch.

Verification during integration: 51 benchmark tests pass and 22 `event-emitter` Java tests pass (3 config, 14 `ReplayPacer`, 5 `DataLoaderCapture`), run through the Dockerized Maven image. Evidence: `docs/capture-pilot-evidence.md`, `benchmarks/capture_pilot.py`, `benchmarks/fixtures/capture-pilot.csv`.

## Comments

Criterion 4 was conditional and not triggered: no checkpoint cadence was tuned for a latency target, and the scored configuration is not yet frozen. Ticket 07 must record the checkpoint cadence it freezes and, if it tunes cadence for latency, the matched visibility/throughput/cost comparison. The pilot is synthetic and accelerated 20x with shortened 60 s windows, uncapped containers, and one worker; it is a measurement-path proof, not a capacity, latency, or historical claim. Container clocks share the host kernel clock, so cross-host runs must document clock error.

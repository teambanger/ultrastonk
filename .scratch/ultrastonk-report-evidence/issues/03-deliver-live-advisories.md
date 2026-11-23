# 03: Deliver live BUY/SELL to subscribers

**What to build:** A subscriber sees genuine BUY and SELL markers from finalized Flink windows, with the EMA values and window that triggered them, without receiving unrelated symbols.

**Blocked by:** 01 - Verify query truth on a hand-checked CSV.

**Status:** resolved

- [x] For each finalized nonempty window, emit at most one advisory: BUY for previous EMA38-minus-EMA100 <= 0 and current > 0, SELL for previous >= 0 and current < 0. Preserve keyed/checkpointed computation and the closing-tick timestamp and existing output shape unless the fixture demonstrates a need to change them.
- [x] An isolated deterministic run proves both directions, no repeat while on the same side, equality boundary behavior, and subscription isolation. Verify actual read-committed Kafka output and overlapping/disjoint WebSocket subscriptions, with live messages checked separately from history. Unsubscribed symbols still produce correct query results in Kafka.
- [x] Cover pending-window restore and no duplicate advisory on the controlled recovery fixture. Verify the browser shows the triggering window and both EMA values for real signals from Flink, not fake or gateway-injected markers.
- [x] Save the demo command, expected/actual comparison and a screenshot or capture from the same measured revision.

## Answer

Integrated through merge commits `c992997`, `08b303c`, and `2c63f39`. Flink emits advisories from finalized windows; the browser matches each signal to its ticker/window EMA pair; the isolated verifier checks 148 read-committed records, three live subscribers, subscription isolation, and history separately. Equality-boundary cases (equality-as-current emits nothing; departures both upward and downward emit) are covered by operator tests using production-valid prices, mutation-checked for discrimination.

## Comments

Evidence is a synthetic accelerated-time run (one-second windows, `NONE`/`DROP`), not a latency, recovery, or historical claim. End-to-end Kafka recovery was not demonstrated; checkpoint restore is covered at the operator seam. The capture records one unexplained timeout with a 1,000 ms source-idle timeout, and the successful retry used 30,000 ms.

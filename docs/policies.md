# Duplicate and late-event policy design

## Duplicate identity

Kafka producer idempotence suppresses transport retries from the same producer session. Flink's checkpoint-coordinated Kafka transactions prevent recovery from exposing partially committed output. Neither mechanism removes two separately produced records representing the same business event.

The input contract has no event ID. The implemented identities are explicit approximations, keyed separately by ticker:

| Policy | Identity | Output / accuracy trade-off | State cost |
|---|---|---|---|
| `NONE` | No identity lookup | Every input tick is forwarded; duplicates can change equal-time arrival decisions | No dedup state |
| `EXACT` | Timestamp + IEEE-754 price bits + security type | Suppresses identical payloads; preserves same-time price corrections; can suppress distinct legitimate identical ticks | One TTL entry per distinct payload |
| `TIMESTAMP` | Timestamp only | First arrival wins; suppresses subsequent legitimate same-millisecond prices and corrections | One TTL entry per distinct ticker timestamp |

Dedup runs before raw-price output and before window aggregation. Duplicate suppression therefore affects both feeds consistently. Identity strings use unambiguous delimiters and a fixed hexadecimal representation of the price. `-0.0` and `0.0` are distinct identities.

Entries have processing-time TTL, set on the first accepted occurrence, with incremental cleanup for heap backends and a compaction filter for RocksDB. Seeing a duplicate does not extend the TTL. An identity may be accepted again after expiration, including after recovery if its TTL has elapsed. Expired entries become logically invisible immediately and are physically removed by background cleanup/compaction. Retained identity state scales with distinct event rate multiplied by retention time and cleanup lag. RocksDB avoids placing the entire dedup index in JVM heap.

A future schema with an immutable source event ID would enable stronger business deduplication. Do not infer exactly-once business processing from payload deduplication.

## Late arrivals

The pipeline retains one closing event per open ticker/window. An event-time timer finalizes each window. Finalization uses `window_end - 1`, plus configured grace only under `GRACE`.

| Policy | Inside out-of-order allowance | After nominal finalization | After final grace boundary |
|---|---|---|---|
| `DROP` | Included | Price forwarded; EMA unchanged; counter incremented | Same |
| `SIDE_OUTPUT` | Included | Price forwarded; EMA unchanged; audit record and counter | Same |
| `GRACE` | Included | Included until delayed finalization; grace counter incremented | Price forwarded; EMA unchanged; audit record and late counter |

`MAX_OUT_OF_ORDER_MS` delays the watermark for all policies. Grace is an **additional** delay. With default settings, `GRACE` normally waits about 35 seconds of event-time progress beyond the window before producing the EMA pair, plus checkpoint visibility delay and possible source idleness delay.

All policies finalize once and preserve window order. Changing an older EMA would also require recomputing every subsequent EMA, and the requested output has neither window identifiers nor revision markers. An update/retraction policy would therefore require a protocol change and is deliberately not presented as equivalent to these strategies.

A checkpointed per-ticker finalization boundary prevents already finalized windows from reopening when watermarks reset during task recovery. Watermark timing and idle-source decisions can still vary during replay; the concurrent load test does not assume identical late classification across failure schedules.

Equal-time closing candidates use last accepted arrival; `TIMESTAMP` dedup changes that outcome by keeping only the first. Empty windows are skipped. Completed window state is removed, while the latest two EMA values remain per ticker.

## Comparison structure

Test all nine combinations under the same duration, target rate, seed, symbol skew, duplicate fraction, delayed fraction, window size, grace, checkpoint cadence, and worker count. Compare:

1. Price multiset correctness under the selected identity rule; operator tests and the integration verifier check exact EMA values and late-policy behavior.
2. Throughput including duplicates, accepted output rate, consumer lag, busy/backpressure time, and drain time.
3. Consumer-visible p50/p95/p99 latency, Flink ingress-to-sink latency, operator time, and finalization delay.
4. Duplicate suppression, finalized-window exclusions, accepted grace events, audit volume, RocksDB/checkpoint size, heap, GC, and CPU.

Use multiple runs and inspect variance. The fastest configuration is useful only if its loss of legitimate ticks and delayed finalization satisfy the intended semantics.

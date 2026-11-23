# Subscriber growth on one gateway (ticket 10)

**Status: MEASURED, two runs of the pair — directional, not a gateway limit.**

The question an evaluator can now answer: while the upstream processing workload is
held fixed, what does doubling the live subscriber count do to one gateway's fan-out
rate, per-client delivery, latency tails and resource use?

## Method

Same documented passing baseline as ticket 08: 4,000 offered originals/s, one
TaskManager, 1 s windows, 100 ms disorder budget, 2 s grace, `DEDUP_STRATEGY=NONE`,
`LATE_EVENT_STRATEGY=DROP`, fixed caps
`taskmanager:1:2g,kafka:2:2g,gateway:1:512m,benchmark:2:1g`, 20 s measured after 5 s
warmup, seed 42, one gateway container, measured 200-symbol population with the week's
top-200 measured weights (`symbol-weights-200.json`; top 1/10/100 = 3.50% / 23.05% /
85.85% after prefix normalization).

Both conditions subscribe every client to the **same full ticker set** before the
measurement interval (the assignment method is "broadcast: every subscriber sees every
symbol", fixed across conditions). The only change is the subscriber count, 2 → 4.
Every client is verified independently against the committed Kafka output, so a client
that missed anything fails the run rather than being masked by its peers.

## Results (`benchmarks/evidence/subscribers/summary.json`, monitored run)

| Clients | Committed/s | Per-client prices/s | Fan-out frames/s | Client p50 / p99 (ms) | Gateway CPU (cores of 1) | Gateway throttle | Gateway RSS | Slow-client drops | Delivery |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 2 | 3,845.7 | 3,845.8 | 8,475.1 | ≈674–730 / 2,243–2,261 | 0.204 | 0.04 | ≈202 MB | 0 | exact on both clients |
| 4 | 3,860.8 | 3,861.1 | 17,022.3 | p99 2,451–2,522 | 0.232 | 0.08 | ≈186 MB | 0 | exact on all four clients |

**Fan-out doubled: 2.009×** (8,475 → 17,022 frames/s), with per-client price rate
unchanged (1.004×), zero missing prices, zero missing EMAs, zero missing advisories, zero
client errors, zero slow-client skips and no disconnects. The gateway used **23% of one
CPU core** at four subscribers (8% throttled periods versus 4%), so it was nowhere near
its cap. The saturated components were elsewhere: the TaskManager ran at 0.997 of its
1-core quota with essentially every period throttled, and Kafka at 0.63 with ~35%
throttling. At this baseline a second pair of subscribers is a gateway fan-out exercise,
not a gateway bottleneck.

A prior unmonitored repeat of the same pair agrees (fan-out 8,613 → 17,030 = 1.977×,
exact on every client, p99 2.9 → 2.8 s), so the direction is supported by two runs even
though each run is a single repetition. The monitored rerun, not the first pair, is the
one quoted above because it carries per-service CPU; both session ids are in
`summary.json.source_sessions` and the first pair remains under `benchmarks/results/`.

## Limits

- **One repetition per condition** in each pair; two pairs agree, but neither is a
  repeated measurement with spread.
- "One gateway" means one Node process with `MAX_CLIENTS=1000`, `SLOW_CLIENT_BYTES=1 MiB`,
  `KILL_CLIENT_BYTES=8 MiB`, `HISTORY_LIMIT=500`; the subscriber counts tested (2, 4) are
  far below the connection limit, so this says nothing about the maximum subscriber count.
- Fan-out is broadcast, so offered gateway work grows linearly with subscribers; a
  partitioned (disjoint-symbol) subscription design would not show the same growth.
- The upstream (Flink worker + single Kafka broker) is saturated at 4,000/s in this
  configuration, so the gateway never became the binding constraint; a run at a higher
  sustainable upstream rate (not established — see ticket 08) could find a gateway limit.
- Client latency includes the pipeline's commit visibility and the driver's own Python
  parsing; with four subscribers the benchmark container used 0.354 of its 2-core quota,
  so the driver was not the limiter, but it is not a zero-cost observer either.

## Reproduce

```sh
python3 benchmarks/growth.py --tickets 10 --monitoring
python3 -m unittest benchmarks.tests.test_growth
```

Artifacts: `benchmarks/evidence/subscribers/{summary.json,symbol-weights-200.json,runs/}`;
raw matrix sessions are named in `summary.json.source_sessions`.
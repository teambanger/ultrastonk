# Ticket 10 evidence — subscriber growth on one gateway

Directional measurement, two pairs of runs. Read `docs/subscriber-growth.md` for the
argument; this file is the artifact inventory and the trusted numbers.

- `summary.json` — baseline, both conditions (normalized driver metrics including
  per-client verification and `/health` totals), the paired comparison, per-service
  resource maxima, commands and session ids. The quoted pair is the **monitored** run
  (sessions in `source_sessions`); an earlier unmonitored pair is listed there too and
  agrees directionally.
- `symbol-weights-200.json` — the published 200-symbol distribution input.
- `runs/{clients-2,clients-4}.json` — per-condition normalized metrics, including every
  client's missing/extra counts, latency quantiles and errors, and the gateway drop
  totals.

Trusted numbers (monitored pair):

| Clients | Fan-out frames/s | Per-client prices/s | Exact delivery | Gateway CPU (cores) | Drops |
|---:|---:|---:|---|---:|---:|
| 2 | 8,475.1 | 3,845.8 | both clients exact | 0.204 | 0 |
| 4 | 17,022.3 | 3,861.1 | all four clients exact | 0.232 | 0 |

Fan-out ratio 2.009×, per-client rate ratio 1.004×. Scope: one gateway, broadcast
subscription of a fixed 200-symbol set, one repetition per condition in each pair;
upstream (TaskManager at 0.997 core, Kafka at 0.63) was the saturated component, so this
does not establish a gateway limit.

Run: `python3 benchmarks/growth.py --tickets 10 --monitoring`.
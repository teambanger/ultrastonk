# Ticket 09 evidence — long-tail and symbol growth

Directional measurement. Read `docs/long-tail.md` for the argument; this file is the
artifact inventory and the trusted numbers.

- `summary.json` — baseline, the three conditions (normalized driver metrics), the two
  paired comparisons, distribution coverage, commands and session ids.
- `symbol-weights-1000.json`, `symbol-weights-2000.json` — the published distribution
  inputs (ranked symbols, normalized weights, coverage, source rows). These are byte
  copies of the `/results/growth-weights-*.json` files the container read.
- `runs/{uniform-1000,longtail-1000,longtail-2000}.json` — per-condition normalized
  metrics, including measured per-partition input counts (as imbalance shares/CV).

Trusted numbers (from `summary.json`):

| Condition | Committed/s | Exact delivery | Partition max/mean | Partition CV |
|---|---:|---|---:|---:|
| uniform-1000 | 3,924.7 | 80,000 / 80,000 | 1.122 | 0.094 |
| longtail-1000 | 3,843.8 | exact | 1.692 | 0.320 |
| longtail-2000 | 3,853.9 | exact | 1.103 | 0.108 |

Scope: one repetition per condition, 20 s, one TaskManager, no gateway; visible latency
is commit visibility under 1 s checkpoints. Nothing here is a limit or a scaling law.

Run: `python3 benchmarks/growth.py --tickets 09`. Raw matrix artifacts stay under
`benchmarks/results/` (gitignored) and are named by `summary.json.source_sessions`.
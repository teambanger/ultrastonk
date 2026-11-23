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
| uniform-1000 | 3,982.7 | 80,000 / 80,000 | 1.123 | 0.087 |
| longtail-1000 | 3,879.9 | exact | 1.271 | 0.157 |
| longtail-2000 | 3,848.3 | exact | 1.270 | 0.153 |

The quoted triple ran with a fixed ticker prefix (`--ticker-prefix B`), so every
condition shares an identical key set and the partition mapping is reproducible:
sessions `b6e4a0ece593` / `ddf66eb58ae5` / `ff5408db1ae2`. Two earlier triples with
per-run prefixes (`5058cb4b9046` / `a2800a3eddae` / `c827d6b5db8e`, then
`ff1455612cf5` / `f9e634201e00` / `dd17b66d300a`) agree on exact delivery and the
throughput ordering (−2.1%, −2.0%) but their partition numbers moved with the hash;
their raw artifacts remain under `benchmarks/results/`.

Scope: one repetition per condition (three triples total), 20 s, one TaskManager, no gateway; visible latency
is commit visibility under 1 s checkpoints. Nothing here is a limit or a scaling law.

Run: `python3 benchmarks/growth.py --tickets 09`. Raw matrix artifacts stay under
`benchmarks/results/` (gitignored) and are named by `summary.json.source_sessions`.
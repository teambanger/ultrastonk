# Sustainable-limit evidence

Scored artifacts for ticket 08 land here. Nothing in this directory is a measurement
until the scored phase runs; the files committed before scoring are the frozen
acceptance contract only.

| File | Written by | Contents |
|---|---|---|
| `pre-registered-rules.json` | committed before scoring | The acceptance thresholds. `default_rules()` in `benchmarks/capacity_bracket.py` must equal this file; a unit test enforces it. |
| `run-ledger.jsonl` | `capacity_bracket.py` | Append-only ledger of every evaluated run, including failed and inconclusive attempts. |
| `bracket.json` | `capacity_bracket.py` | Highest-passing and first-failing offered load per worker count (or the unbracketed outcome), per-point medians and repeat spread, worker-capacity gain, and every evaluated run. |
| `bracket.json` `source_sessions` | `capacity_bracket.py` | Matrix session ids whose raw artifacts (`benchmarks/results/SESSION-*`, `RUN.json`, `RUN.limits.json`, `RUN.prometheus.json`) back the bracket. |

The bracket is only valid when the ledger shows the generator achieved at least 98%
of each offered load and all required telemetry was present; a run missing either is
recorded as INCONCLUSIVE and never counts as a pass or a fail. See
[`docs/sustainable-limit.md`](../../../docs/sustainable-limit.md) for the rules, the
scored commands, the resource profiles and the search schedule.

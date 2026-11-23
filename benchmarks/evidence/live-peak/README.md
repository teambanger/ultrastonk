# Live peak-load evidence (ticket 07)

This directory is written by `benchmarks/peak_slice.py` and
`benchmarks/live_peak.py` during the **scored phase**. It is intentionally empty
of results until that phase runs: no measurement has been taken here yet. The
method, pre-registered plan and decision rules are in
[`docs/live-peak.md`](../../../docs/live-peak.md).

## Layout

| Path | Written by | Contents |
|---|---|---|
| `slices/peak-warm.csv`, `slices/peak-cold.csv` | `peak_slice.py` | The offered slice CSVs (pre-roll + measured + trailing), measured rows verified against the fixture. |
| `slices/peak-*-manifest.json` | `peak_slice.py` | Segment counts, measured exchange mix, and the emitter `REPLAY_PREROLL_MS` / `REPLAY_MEASURE_MS`. |
| `run-plan.json` | `live_peak.py plan` | The pre-registered run matrix, captured before any run. |
| `runs/<run-id>-run.json` | `live_peak.py client` | The raw evidence for one run (settings, limits, pacing report, input/committed/WebSocket records, progress, watermarks, consumer lag). |
| `runs/<run-id>-limits.json` | runner | Effective container limits before the run. |
| `oracle-*.jsonl` | `query_truth.py` | Independent complete-input truth over the slice. |
| `compare-*.json` | `live_peak.py compare` | Oracle report plus policy-related vs transport-loss split. |
| `summary.json` | `live_peak.py summarize` | Per-run summaries and the per-condition aggregate. |
| `ledger.json` | `live_peak.py run` / `summarize` | Every attempt, including failed and invalid runs. |

## Ledger

`ledger.template.json` shows the entry shape. The ledger is append-only in
spirit: a failed or invalid attempt stays with its reasons, and a re-run is a new
entry, never an edit of the old one. `status` is one of `valid`, `invalid`,
`failed`; `invalid` means the attempt did not meet the measurement preconditions
(for example insufficient pacing), `failed` means a step errored.

## Reproduce

See the "Extract the slices", "Pre-registered run plan", "Run the scored phase",
"Correctness comparison" and "Summarize" command blocks in
[`docs/live-peak.md`](../../../docs/live-peak.md). Run the scored phase only on an
otherwise idle host: this measures single-workload behaviour.

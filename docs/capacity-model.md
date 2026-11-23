# Predictive capacity model

This document describes `benchmarks/capacity_model.py`, the predictive half of
the evaluation. The model is pure analysis of evidence that already exists: it
starts no container, generates no load, and reads only the artifacts listed
below. It answers the reframed question — *how long does a given workload take,
and what resources does it need?* — for the official week and for a stated
fraction or multiple of it.

Reproduce with:

```sh
python3 benchmarks/capacity_model.py                 # human-readable, official week + 3x
python3 benchmarks/capacity_model.py --json          # machine-readable
python3 benchmarks/capacity_model.py --fraction 0.5 --multiple 3 --workers 1,2
python3 benchmarks/capacity_model.py --volume 100000000 --workers 1
python3 -m unittest discover -s benchmarks/tests     # includes test_capacity_model.py
```

The model is deliberately small. It has one measured anchor today (the ticket 06
whole-week run) and one regime whose evidence has not landed (the ticket 08
ladder). Every number it prints is either read from an artifact named below or
derived by the arithmetic shown in this document. Anything that depends on
ticket 08 is printed as `PENDING`.

---

## 1. Inputs and provenance

Every numeric input, with the artifact it comes from.

| Input | Value | Artifact (path) | Field |
|---|---|---|---|
| Week rows read | `289,116,808` | `/mnt/shared/data/official-week-Ij8jlfeA/results/week-profile.json` | sum of `files[].rows` |
| Week rows emitted (usable) | `289,049,752` | same `week-profile.json` | `replay_screening.emitted` |
| Recorded raw five-minute peak | `1,223,873` rows in 300 s = `4,079.58` rows/s | same `week-profile.json` | `raw_peak.rows`, `raw_peak.start` |
| Trade share | `59,391,670` / `289,049,752` = `20.55%` | same `week-profile.json` | `row_categories.trade` ÷ `replay_screening.emitted` |
| Top-symbol concentration | top1 `2.39%`, top10 `15.71%`, top100 `58.53%` | same `week-profile.json` | `top_symbol_shares` |
| Per-day row volumes | 53,994,554 / 58,811,870 / 64,591,844 / 55,547,846 / 56,111,183 / 33,398 / 26,113 | same `week-profile.json` | `files[].rows` |
| Week source bytes | `24,874,180,296` (23.16 GiB) | same `week-profile.json` | sum of `files[].size`; matches `scripts/official-week-manifest.json` |
| Rate series | 222,844 nonempty one-second buckets, SHA-256 `12883bc1…` | `/mnt/shared/data/official-week-Ij8jlfeA/results/week-rates.jsonl` | `week-profile.json` `series_sha256` |
| Manifest | SHA-256 `b2cf2fb7…` | `scripts/official-week-manifest.json` | whole file |
| Week-run input rows | `289,049,752` | `/mnt/shared/data/ultrastonk-full-week/run2/logs/emitter.log` | `Done: … submitted` |
| Emitter offered rate | `545,563` ev/s | same `emitter.log` | `Done: … ev/s` |
| Emitter wall | `530.386` s | `/mnt/shared/data/ultrastonk-full-week/run2/logs/emitter_inspect.json` | `State.FinishedAt − State.StartedAt` |
| Flink processing wall | `428.031` s | `/mnt/shared/data/ultrastonk-full-week/run2/logs/jobmanager.log` | `RUNNING → FINISHED` timestamps (16:45:44.953 → 16:52:52.984) |
| Output records | `62,380,615` | `/mnt/shared/data/ultrastonk-full-week/run2/evidence/normalize.json` | `counts.records` |
| Output prices | `59,391,670` | same `normalize.json` | `counts.prices` (equals the profile's trade count) |
| Raw output bytes | `5,282,388,854` (4.92 GiB) | `/mnt/shared/data/ultrastonk-full-week/run2/evidence/output.jsonl` | file size |
| Broker footprint | `16,507,840,934` bytes (15.37 GiB) | `/mnt/shared/data/ultrastonk-full-week/run2/logs/kafka_data_bytes.txt` | first field |
| Input topic LEO | `289,049,752` (exactly the submitted count) | `/mnt/shared/data/ultrastonk-full-week/run2/evidence/offsets.json` | sum of `…-in` `high` |
| Output / audit LEO | `62,381,131` / `10,669,014` | same `offsets.json` | sums of `…-out` / `…-audit` `high` |
| Audit eligible-late exclusions | `10,669,014` | same `offsets.json` | Σ(`high − low`) for `…-audit` |
| Output dump | `62,380,615` records in `521.854` s | `/mnt/shared/data/ultrastonk-full-week/run2/logs/dump-output.log` | `records`, `seconds` |
| Per-worker cap (planned) | 1 CPU, 2 GiB | `docs/sustainable-limit.md` §Resource profiles (`taskmanager:1:2g`) | — |
| Ticket 08 ladder | **PENDING** | `benchmarks/evidence/sustainable-limit/bracket.json` (absent) | — |
| Drop-vs-grace penalty | DROP `0` ms / GRACE `30,000` ms finalization lag; +1,100 / +4,344 state bytes | `benchmarks/evidence/drop-vs-grace/comparison.json` | `finalization_lag_ms`, `vs_truth` |

The measured configuration is the ticket 06 run 2: `FLINK_PARALLELISM=2`
(two TaskManagers), six partitions, `DEDUP_STRATEGY=NONE`,
`LATE_EVENT_STRATEGY=SIDE_OUTPUT`, `WINDOW_SIZE_MS=300000`,
`MAX_OUT_OF_ORDER_MS=5000`, `CHECKPOINT_INTERVAL_MS=10000`, `PACING=event`,
`SPEED_MULTIPLIER=1000` (`benchmarks/evidence/README.md`, ticket 06).

### Régime note: the week run is preload-and-process

The composer log `/mnt/shared/data/ultrastonk-full-week/run2/logs/compose_up.log`
shows the emitter container *exited* before the JobManager and TaskManagers
started, and the timestamps agree: the emitter ran 16:36:48 → 16:45:39 and the
Flink job ran 16:45:44 → 16:52:52. The emitter therefore preloaded the whole
week into Kafka first, and Flink then consumed the backlog. The **processing
wall (428 s) is a batch-drain ceiling, not a live-arrival ceiling**, and the
emitter rate is the *generator's* rate, which the model never uses as a capacity
number. This is the same "preload-and-process, not a live latency measurement"
mode the spec's Problem Statement describes.

---

## 2. Fitted parameters and the points they came from

The model uses two quantities.

**Aggregate processing rate** — the pipeline moved the whole week's input to
output in the measured processing wall:

```
R(2) = 289,049,752 rows / 428.031 s = 675,301 rows/s        (ticket 06, 2 workers)
```

**Per-worker service rate** — the aggregate rate divided by the worker count:

```
r    = R(2) / 2 = 337,650 rows/s   (linear-scaling-implied; see §4)
```

This is the only fitted quantity with a measured anchor. It is derived from a
**single repetition** (ticket 06 run 2), so it is directional.

**Scaling efficiency** `η(p) = (R(p)/R(m)) / (p/m)` needs **two** measured
worker counts. With only the two-worker anchor, `η` is `PENDING`; the model
reports it as such and does not invent a scaling law. When ticket 08 lands, the
ladder's one- and two-TaskManager points supply it automatically and
`build_model()` reports it with no code change — as the ladder's *live-regime*
worker-capacity gain, the quantity `docs/sustainable-limit.md` calls
`worker_capacity_gain`. It is deliberately **not** multiplied into the batch
wall-time prediction, because the ladder measures the live regime and the week
anchor measures the batch regime; mixing the two would double-count. The batch
prediction uses the measured per-worker rate with the monotone bracket of §4.

**Latency** is not fitted at all — there is no measured latency point in any
artifact the model reads, so `latency_at()` returns `PENDING` until the ticket 08
points carry `visible_latency_ms`. (Ticket 05's p50/p99 figures exist only as
prose and are not re-derivable from a committed artifact — `docs/evidence-audit.md`.)

**Recorded-peak headroom** compares the peak to the measured batch rate:

```
headroom = 675,301 / 4,079.58 = 165.5×
```

---

## 3. Resource envelope

The envelope is per volume `N` (input rows):

```
worker CPU quota  = p × 1 CPU          (p = TaskManagers; per-worker cap)
worker memory     = p × 2 GiB
disk (broker)     = N × 57.1 MB / 10^6 rows
output bytes      = N × 18.28 bytes
output records    = N × 0.2158
```

The per-row rates are read straight from ticket 06:

```
disk   : 16,507,840,934 bytes / 289,049,752 rows = 57.11 MB per million rows
output :  5,282,388,854 bytes / 289,049,752 rows = 18.28 bytes per input row
records:     62,380,615 rows  / 289,049,752 rows = 0.2158 output records per input row
```

The disk figure is the whole broker footprint (input + output + audit topics on
one broker); the output figure is the raw `output.jsonl` dump. `flink-state`
grew by `0` bytes on disk after the run
(`logs/flink_state_bytes.txt`), though individual checkpoints reached ~9.3 MB
during the run (`logs/flink_finish_lines.txt`).

**Per-worker caps mean total resources grow with worker count.** The quota is
per TaskManager; adding a worker adds 1 CPU and 2 GiB. The model always reports
the aggregate, not the per-worker cap alone.

Because the ticket 06 run was Docker-uncapped (`NanoCpus=0`, `Memory=0` in
`logs/jobmanager_inspect.json`; Flink process size `taskmanager.memory.process.size=1536m`
in `compose.yaml`), the 1 CPU / 2 GiB figure is the **ticket 08 plan**, not a
measurement of the ticket 06 run.

---

## 4. Predictions, with ranges

Two monotonicity assumptions need no second point and bracket every prediction,
so a single-repetition model still yields a range:

1. More workers cannot lower aggregate throughput: `R(p) ≥ R(m)`.
2. Adding workers cannot more than proportionally raise it: `R(p) ≤ (p/m)·R(m)`.

Around the anchor `T = N / R(2)`, wall time lies in `[T·min(1, m/p), T·max(1, m/p)]`.
At the measured worker count the bracket collapses, so the model prints a
`±20%` one-repetition band there instead. The linear-scaling point
(`N / (r · p)`) is printed for reference; away from the anchor it is the upper
end of the bracket.

| Workload | Workers | Predicted processing wall | Range | Basis |
|---|---|---|---|---|
| Official week (`289,049,752` rows) | 2 | `428 s` (measured) | `342 – 514 s` | one-repetition ±20% |
| Official week | 1 | `856 s` (14 min 16 s) | `428 – 856 s` | monotone bracket |
| 3× week (`867,149,256` rows) | 2 | `1,284 s` (21 min 24 s) | `1,027 – 1,541 s` | one-repetition ±20% |
| 3× week | 1 | `2,568 s` (42 min 48 s) | `1,284 – 2,568 s` | monotone bracket |
| Half week (`144,524,876` rows) | 2 | `214 s` | `171 – 257 s` | one-repetition ±20% |

**Resource envelope by volume** (CPU/memory at the ticket 08 plan; disk/output
from ticket 06):

| Workload | Workers | CPU / memory quota | Broker disk | Output bytes / records |
|---|---|---|---|---|
| Official week | 1 | 1 CPU / 2 GiB | 16.51 GB | 5.28 GB / 62.38 M |
| Official week | 2 | 2 CPU / 4 GiB | 16.51 GB | 5.28 GB / 62.38 M |
| 3× week | 1 | 1 CPU / 2 GiB | 49.52 GB | 15.85 GB / 187.14 M |
| 3× week | 2 | 2 CPU / 4 GiB | 49.52 GB | 15.85 GB / 187.14 M |

**Recorded peak.** `4,079.58 rows/s ≤ 675,301 rows/s`, headroom `165.5×`. This
is the batch-drain bound; the *direct* live headroom test is ticket 07 and the
sustainable limit is ticket 08, both **PENDING**.

### Worked example — how long and how many resources for the official week

Arithmetic, all inputs from §1:

```
week input rows          N    = 289,049,752
measured processing wall T    = 428.031 s           (jobmanager.log)
measured aggregate rate  R(2) = 289,049,752 / 428.031 = 675,301 rows/s
per-worker rate          r     = 675,301 / 2          = 337,650 rows/s   (linear-implied)

2 TaskManagers (the measured configuration):
    T(2) = 289,049,752 / 675,301 = 428.0 s
    range: ±20% one repetition  = 342 s .. 514 s

1 TaskManager (not measured; monotone bracket):
    upper bound = 289,049,752 / 337,650 = 856.1 s
    lower bound = T(2)                  = 428.0 s
    range: 428 s .. 856 s

Resources at 2 TaskManagers:
    CPU    = 2 × 1 CPU  = 2 CPU
    memory = 2 × 2 GiB  = 4 GiB
    disk   = 289,049,752 × 57.11 MB / 10^6 = 16.51 GB   (broker: input+output+audit)
    output = 289,049,752 × 18.28 B         =  5.28 GB   → 62.38 M records
```

So the official week, as measured, is a **~7-minute processing job on two
TaskManagers in 2 CPU / 4 GiB**, needing **~16.5 GB of broker disk** and
producing **5.28 GB / 62.4 M output records**. One TaskManager is bracketed at
**428–856 s (7–14 min)** with 1 CPU / 2 GiB. Add the emitter phase if the run is
preload-and-process: `530 s` (generator-side) + `428 s` ≈ **16 min** end to end.

### Second example — 3× volume

```
N = 3 × 289,049,752 = 867,149,256 rows

2 TaskManagers:  T = 867,149,256 / 675,301 = 1,284.1 s  (21 min 24 s)
                 range ±20%               = 1,027 .. 1,541 s
1 TaskManager:   upper = 867,149,256 / 337,650 = 2,568.2 s (42 min 48 s)
                 range = 1,284 .. 2,568 s

Resources at 2 TaskManagers: CPU 2, memory 4 GiB
    disk   = 867,149,256 × 57.11 MB / 10^6 = 49.52 GB
    output = 867,149,256 × 18.28 B         = 15.85 GB → 187.14 M records
```

Throughput is assumed independent of volume, so the 3× time is exactly 3× the
week time; that linearity is an assumption, not a measurement (§6).

---

## 5. Other conditions (as measured penalties, not new claims)

- **Ticket 11 — lateness policy.** On one scripted late window, `GRACE` costs a
  policy-intrinsic `30,000` ms finalization delay versus `DROP`'s `0` ms, and a
  managed-state increase of `+1,100` (R1) / `+4,344` (R2) checkpoint bytes;
  `DROP` loses 1 advisory and 2 EMA values. This is a *correctness/latency* trade,
  not a throughput penalty, so the model applies no throughput factor for it
  (`benchmarks/evidence/drop-vs-grace/comparison.json`).
- **Ticket 03 — advisory delivery.** The advisory path delivered `148` records
  exactly over three WebSocket subscribers with two quarantined inputs
  (`benchmarks/evidence/live-advisory/comparison.json`). That is a correctness
  fixture, not a throughput measurement, so the model assumes the advisory/gateway
  path adds no measured throughput cost — an **assumption**, not evidence.

---

## 6. What this cannot support

- **A validated capacity law.** One measured anchor cannot fit a curve. Scaling
  efficiency and the live per-worker rate need ticket 08's second worker count.
- **A live Sustainable limit or live headroom.** The `165.5×` figure is the
  batch-drain ceiling against a preloaded backlog, not a live rate. Ticket 07 and
  ticket 08 are the direct evidence and are **PENDING**.
- **Latency predictions.** No measured latency point is available; `latency_at()`
  is `PENDING`. Ticket 05's numbers are prose-only.
- **Absolute wall times.** The anchor is a single repetition on a shared,
  partially busy host; every wall time is a range, not a number.
- **Measured per-worker resource quotas.** Ticket 06 ran uncapped; the 1 CPU /
  2 GiB quota is the ticket 08 plan, and per-worker caps mean the total grows with
  worker count rather than being a fixed budget.
- **Volumes outside the tested range.** Extrapolation to ≫3× inherits the
  constant-throughput assumption and unmeasured cache/state behaviour.
- **Workload-shape generality.** The ticket 08 ladder is a synthetic shape
  (1000 symbols, Zipf `skew=1.2`); the real week is long-tailed
  (top100 = 58.53%). A throughput fit from the synthetic ladder may not transfer.
- **Policy and distribution effects on throughput.** Ticket 11's penalty is a
  scripted fixture, and tickets 09/10 (symbol and subscriber growth) are deferred,
  so the model does not predict their effect.

## PENDING numbers (require ticket 08)

Offered-volume sweep rates (`500/1000/2000/4000`, optional `8000` originals/s);
the highest-passing / first-failing sustainable limit per worker count; scaling
efficiency `η` from one vs two TaskManagers; per-point `p50/p95/p99`, drain,
worker CPU throttling, and aggregate worker CPU/memory; the `1.5×` worker-capacity
stretch check. The code reads `benchmarks/evidence/sustainable-limit/bracket.json`
(the canonical output of `capacity_bracket.py`) automatically once the scored
phase writes it, and falls back to `benchmarks/results/<session>-matrix.csv` when
the caller names the sessions with `--session <id>`; until then every one of
these prints as `PENDING`.

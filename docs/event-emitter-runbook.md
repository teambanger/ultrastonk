# Event Emitter — Run & Verification Runbook

How to run the `event-emitter` container (the "Raw event emitter → injestion →
partitioning" part of `docs/excalidraw-planning.excalidraw`) and what to check
by hand so the run matches `CS_E4780_Course_Project_2026.txt`.

---

## 1. Running it

### 1.1 Full stack (recommended): emitter + Kafka via compose

From the **repo root**:

```bash
docker compose up --build --exit-code-from emitter emitter   # one replay run
docker compose down
```

What this does:
1. Starts a single-broker Kafka (KRaft, no ZooKeeper).
2. `kafka-init` creates topic `raw-trade-events` with 8 partitions and
   `retention.ms=-1` (mandatory — see 1.4).
3. `emitter` builds the image, mounts `./data:/data:ro`, and replays
   `data/src/debs2022-gc-trading-day-08-11-21.csv` at `SPEED_MULTIPLIER=10`.

`--exit-code-from emitter` makes compose exit with the emitter's exit code
(`0` = all sends succeeded, `1` = at least one failed).

### 1.2 Standalone image (Kafka already running elsewhere)

```bash
docker build -t event-emitter ./event-emitter

docker run --rm \
  -v "$(pwd)/data:/data:ro" \
  -e KAFKA_SERVER=host.docker.internal:9092 \
  -e TOPIC_NAME=raw-trade-events \
  -e SPEED_MULTIPLIER=100 \
  event-emitter
```

The topic must exist with `retention.ms=-1`:

```bash
kafka-topics.sh --bootstrap-server <broker> --create --if-not-exists \
  --topic raw-trade-events --partitions 8 --config retention.ms=-1
```

### 1.3 Containerized sanity check (no Kafka needed)

Stats mode of `Main` (prints loader accounting for `../data/src/part.csv`,
which is `/data/src/part.csv` with the mount above):

```bash
docker run --rm -v "$(pwd)/data:/data:ro" \
  --entrypoint java event-emitter \
  -cp "/app/app.jar:/app/dependency/*" org.ultrastonk.eventemitter.Main
```

### 1.4 Knobs and traps

| Env var | Default | Notes |
|---|---|---|
| `SPEED_MULTIPLIER` | `1.0` | `1.0` = real time; a full day file then takes **24 h**. Raise it, or cap sleeps with `MAX_SLEEP_MS` to skip the overnight gap. |
| `MAX_SLEEP_MS` | `0` (off) | Cap on a single pacing sleep; kills the 18:00→次日 multi-hour stall. |
| `PRICE_FILTER` | `none` | Keep `none` for any number that goes into the report. `last`/`last-nonzero` drop real events (the run loudly warns about it). |
| `RECORD_TIMESTAMP` | `event` | Records carry **Nov 2021** timestamps → broker default 7-day retention would prune them instantly; that's why the topic needs `retention.ms=-1`. `append` avoids it but downstream must read event time from the payload. |
| `EVENT_TIME_ZONE` | `Europe/Berlin` | Must match the downstream job's window anchoring (see check 5). |
| `CSV_PATH` | full day file | Comma-separated list = multi-day replay in order. |

---

## 2. Manual verification checklist (run → observe → spec clause)

Inspect the stream from the host (works while the emitter runs or after a
finished run, since retention is infinite):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic raw-trade-events \
  --from-beginning --max-messages 50 \
  --property print.timestamp=true --property print.partition=true \
  --property print.key=true
```

| # | Check | How | Expected | Spec |
|---|---|---|---|---|
| 1 | **Faithful replay accounting** | Read the emitter's final `LoadStats` line; cross-check with `awk -F',' 'NR>13 && $22!=""' data/src/part.csv \| wc -l` vs. records with non-null `last` | `read = emitted + droppedTotal`; the only material bucket is `no event time` (~0.4 % of the day file, metadata-only rows with no timestamp); `submitted == emitted` and `failed == 0` | §2.2 "each line = one event"; relaxation 3; excalidraw "sends relevant events vs everything" → decision: everything |
| 2 | **Grouping by symbol / partitioning** | Consume with `print.partition=true --property print.key=true` | Every record with the same key (`A2ASZ7.ETR`, …) appears on exactly one partition for the whole run | Relaxation 2; excalidraw "partitioning" box |
| 3 | **Per-symbol order** | Consume one partition, check `eventTimestampMs` per key | Non-decreasing within each key (idempotent producer also forbids duplicates/reorder on retry) | Prerequisite for EMA chains in Query 1/2 |
| 4 | **Event types from non-NULL attributes** | Pick records of each shape | Ask-only row → JSON has `ask` only; trade row → `last`; housekeeping row (has a Time, no price) → only id/secType/timestamps. Absent fields are omitted, not `null` | §2.2; excalidraw "Event types (e.g., bid, ask, price)" |
| 5 | **Timestamps & window anchor** | First trade row of `part.csv` is `07:00:00.021`: with defaults `eventTimestampMs = 1636351200021`; with `EVENT_TIME_ZONE=UTC` it is `1636354800021` | Exactly +3 600 000 ms (Nov 2021 = CET, UTC+1 — the spec's "CEST" label vs. reality; document the choice). Midnight rows (`00:00:00.000`) map to *local* midnight, where 5-min tumbling windows anchor | §2.2 formats `DD-MM-YYYY`, `HH:MM:SS.ssss`; §3.1 relaxation 5 |
| 6 | **Record timestamp = event time** | `print.timestamp=true` | `CreateTime:163635xxxxxxx` matching the payload's `eventTimestampMs`; with `RECORD_TIMESTAMP=append` it becomes `LogAppendTime` | Enables Flink watermarks / "new window detection" (excalidraw) |
| 7 | **Pacing & out-of-order** | Run a slice at `SPEED_MULTIPLIER=1.0`; count rows per event-time second with awk and compare with the printed ev/s; feed a hand-made CSV with a late row | ev/s tracks the file's density; a late row is emitted immediately (no stall, no sleep); `MAX_SLEEP_MS` caps the overnight gap | Replay fidelity; excalidraw out-of-order brainstorm |
| 8 | **Latency / divergence measurement** | Consumer computes `now − emitTimestampMs` | Small, roughly stable value; spikes identify consumer lag | Excalidraw "measure divergence error" |
| 9 | **Failure handling** | Stop Kafka mid-run | Run aborts within ≤ 120 s (`MAX_BLOCK_MS`), `failed > 0` reported, exit code `1` | Operational correctness |
| 10 | **Multi-day replay** | `CSV_PATH=day08.csv,day09.csv` | One continuous run; event times increase across files; per-file summaries printed | §2 "one file per day" |

Expected emitter stdout (day file):

```
Replaying /data/src/debs2022-...csv -> topic 'raw-trade-events' via kafka:29092 at 10.00x (event zone Europe/Berlin, price filter none)
... 1000000 events sent (… ev/s, … s elapsed)      # every 1M events; too small for part.csv
  debs2022-...csv: read 54,0xx,xxx -> emitted …
Done: N submitted, 0 failed, X ev/s, first event time 1636342867000
Total: read … -> emitted … (…% dropped: …)
```

(`first event time 1636342867000` = `01:01:07.000` CET on 08-11-2021, the first
data row of the day file.)

---

## 3. Scope: what the emitter is *not* responsible for

These boxes of the excalidraw plan are downstream (Flink) and cannot be
verified with the emitter alone — the emitter only guarantees that the raw
stream is complete, per-symbol ordered, and event-timestamped:

- 5-minute tumbling-window evaluation "once the next window starts" (§3.1 (4)–(7))
- EMA38/EMA100 computation (Query 1, §3.2)
- Bullish/bearish crossover detection → buy/sell advise events (Query 2, §3.3)
- `DB.signal` (last price, EMA38, EMA100, ticker, timestamp) and `DB.event`
  (ticker, event_type, timestamp) writes

## 4. Troubleshooting

- **Stream is empty right after a run** → topic was created without
  `retention.ms=-1` while `RECORD_TIMESTAMP=event`; the broker pruned it.
- **`CSV file is not readable: …`** → data dir not mounted at `/data` or wrong
  `CSV_PATH`.
- **Run seems stuck** → you are in the overnight session gap; set
  `MAX_SLEEP_MS` (e.g. `2000`) or raise `SPEED_MULTIPLIER`.
- **Filter warning `topic does NOT hold a faithful replay`** → `PRICE_FILTER`
  was set; don't use this run for report numbers.

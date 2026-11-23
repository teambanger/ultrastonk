# Gateway

Node.js service that sits between Kafka and the browsers:

```
stream processor --Kafka "events-processed"--> GATEWAY --WebSocket :8080--> browsers
```

1. Consumes `EventProcessed` messages from Kafka (its own consumer group, so it sees every message).
2. Browsers connect over WebSocket and subscribe to tickers.
3. Each message is forwarded **unchanged** only to clients subscribed to its ticker.
4. On subscribe, the client first gets the recent history for that ticker.
5. Logs throughput and event age every few seconds.

## Message format

Kafka → gateway (`EventProcessed`, one message per value, Kafka key = ticker):

```json
{ "timestamp_emit": 1636354800021, "ticker": "A2ASZ7.ETR", "type": "EMA38", "value": 12.34 }
```

- `timestamp_emit`: event time in integer epoch milliseconds.
- `ticker`: `STOCK.EXCHANGE`, letters/digits/`.`/`_`/`-`, max 32 chars.
- `type`: any non-empty string (max 32 chars), e.g. `price`, `EMA38`, `EMA100`, `BUY`, `SELL`.
- `value`: required finite number for `price`, `EMA38`, `EMA99`, `EMA100`.
  For any other type (e.g. `BUY`/`SELL`) it may be missing or `null`, but must be a number if present.
- Invalid records are dropped, counted (`invalid` in the metrics) and logged (first 10, then every 1000th).

Browser → gateway:

```json
{ "type": "subscribe",   "tickers": ["A2ASZ7.ETR", "RDSA.NL"] }
{ "type": "unsubscribe", "tickers": ["RDSA.NL"] }
```

Gateway → browser:

- live `EventProcessed` messages, exactly as received from Kafka;
- `{ "type": "history", "ticker": "...", "events": [ ... ] }` once per newly subscribed ticker:
  the last `HISTORY_LIMIT` events for it, oldest first (ordered by `timestamp_emit`);
- `{ "type": "error", "error": "reason" }` for an invalid request.

All format rules live in [`src/protocol.js`](src/protocol.js).

The integrated Flink job emits `price`, `EMA38`, `EMA100`, and crossover `BUY`/`SELL` records. The gateway forwards these records and does not compute signals itself. The browser shows up to ten recent signals per subscribed ticker and matches each with EMA values from the same ticker and closing timestamp; set its window size to the deployed `WINDOW_SIZE_MS` to display the triggering window correctly. Synthetic benchmark messages are not evidence of Flink signals.

## Event age

The gateway measures `now - timestamp_emit` when ingesting a record. The logs and CSV retain the benchmark's latency labels, but on Flink output this is **event age**, not transport latency. Historical replay preserves the exchange timestamp, and EMAs are stamped with the closing trade's time. The synthetic gateway benchmark below instead stamps each batch with the publisher's wall-clock time.

## Running

```bash
cd gateway
npm ci
npm start                  # fake data, no Kafka needed -> open http://localhost:8080
npm test                   # unit + end-to-end tests
npm run smoke              # starts the gateway, subscribes a client, prints what arrives
```

Settings are environment variables (PowerShell: `$env:NAME = "value"` before `npm start`):

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP + WebSocket port (`/` test page, `/health` JSON) |
| `SOURCE` | `fake` | `fake` (built-in generator) or `kafka` |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated; root Compose uses `kafka:9092` or the cluster broker list |
| `KAFKA_TOPIC` | `events-processed` | input topic; root Compose uses `KAFKA_OUTPUT_TOPIC` |
| `KAFKA_GROUP_ID` | `ultrastonk-gateway` | consumer group (each gateway instance needs its own to see every message) |
| `KAFKA_FROM_BEGINNING` | `false` | first start of a new group: `false` = only new messages, `true` = replay the topic |
| `HISTORY_LIMIT` | `500` | events kept per ticker |
| `METRICS_INTERVAL_MS` | `10000` | metrics log period |
| `METRICS_CSV` | (off) | file to append metrics rows to (for the report) |
| `SLOW_CLIENT_BYTES` / `KILL_CLIENT_BYTES` | 1 MB / 8 MB | queued bytes before skipping / disconnecting a client |
| `FAKE_TICKERS`, `FAKE_EXTRA_TICKERS`, `FAKE_INTERVAL_MS` | 5 tickers, 0, 1000 | fake generator |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Running in Docker

```bash
# From the repository root
docker compose up -d --build
curl http://localhost:8080/health
```

The test page and WebSocket are available on `localhost:8080` by default. The service starts after `kafka-init` and reads only committed Kafka records. `/health` reports `source.status` as `connecting` or `running`; Docker's health check only checks the HTTP server.

Root Compose defaults `GATEWAY_KAFKA_FROM_BEGINNING=true` so a fresh group can warm its history from retained output, including bounded replays. Existing groups resume at committed offsets. History is in memory and is not persisted across restarts. Each gateway instance needs a distinct consumer group to receive every ticker.

## Local Kafka smoke test

Run this from the root with Node 22+ and free ports. The test publishes fixtures directly to the output topic, so use a disposable project, not a live or replay stack.

```bash
(
  set -e
  export COMPOSE_PROJECT_NAME="ultrastonk-gateway-test-$(date +%s)"
  export KAFKA_PORT=19092 GATEWAY_PORT=18080 KAFKA_OUTPUT_TOPIC=gateway-smoke
  trap 'docker compose down --volumes' EXIT
  docker compose up -d --build --wait gateway
  npm --prefix gateway ci
  KAFKA_BROKERS=localhost:19092 KAFKA_TOPIC="$KAFKA_OUTPUT_TOPIC" \
    GATEWAY_URL=localhost:18080 npm --prefix gateway run kafka:e2e
)
```

## Performance results

Measured with `npm run bench` on 2026-11-20: AMD Ryzen 7 7800X3D (16 logical CPUs), 32 GB RAM,
Windows 11, Node 24, Kafka 4.0 in Docker Desktop. Publisher, Kafka, gateway and the simulated
browsers all ran on this one machine. Each run: 5 s warm-up, then 15 s measured. One "window"
(price + EMA38 + EMA100 for every ticker, plus BUY/SELL on crossovers) was published every
100 ms, and each simulated browser subscribed to 5 random tickers. Raw data: [`bench-results/latest.csv`](bench-results/latest.csv).

| Tickers | Clients | Events in/s | Messages out/s | Latency p50 / p95 / p99 (ms) | Gateway CPU (% of 1 core) | Memory (MB) | Status |
|---:|---:|---:|---:|---|---:|---:|---|
| 5 | 1 | 138 | 138 | 8 / 15 / 16 | 2 | 57 | OK |
| 5 | 10 | 139 | 1,385 | 9 / 16 / 20 | 4 | 60 | OK |
| 5 | 100 | 138 | 13,842 | 13 / 27 / 30 | 13 | 67 | OK |
| 5 | 500 | 139 | 69,320 | 37 / 157 / 171 | 66 | 72 | OK |
| 205 | 1 | 5,707 | 139 | 71 / 94 / 107 | 5 | 120 | OK |
| 205 | 10 | 5,691 | 1,388 | 72 / 95 / 104 | 7 | 121 | OK |
| 205 | 100 | 5,698 | 13,894 | 80 / 112 / 116 | 23 | 122 | OK |
| 205 | 500 | 5,681 | 69,282 | 192 / 276 / 326 | 66 | 187 | SLOW |
| 1,005 | 1 | 27,988 | 139 | 71 / 109 / 123 | 11 | 249 | OK |
| 1,005 | 10 | 27,926 | 1,390 | 52 / 108 / 118 | 14 | 241 | OK |
| 1,005 | 100 | 27,862 | 13,863 | 62 / 117 / 124 | 26 | 320 | OK |
| 1,005 | 500 | 28,038 | 69,732 | 174 / 289 / 315 | 74 | 339 | SLOW |
| 3,005 | 1 | 83,944 | 140 | 115 / 159 / 168 | 28 | 690 | OK |
| 3,005 | 10 | 81,213 | 1,352 | 146 / 192 / 208 | 43 | 657 | OK |
| 3,005 | 100 | 79,442 | 13,214 | 169 / 216 / 233 | 61 | 606 | OK |
| **3,005** | **500** | **61,589** (77,239 published) | **51,198** | **2,530 / 5,112 / 5,497** | **101** | **685** | **LAG + SLOW** |

Latency is the gateway-side value (`now - timestamp_emit` when an event is forwarded); latency
measured in the browsers on arrival was within a few ms of it. No messages were dropped in any run.
Status: **OK** = keeps up; **SLOW** = p99 above 250 ms; **LAG** = the gateway consumes less than 95 %
of what is published, so it falls further behind every second.

**What the numbers mean**

- **The breaking point is CPU on the gateway's single Node.js thread.** At 3,005 tickers × 500
  clients the gateway would need to take in ~80k events/s *and* send ~70k messages/s. That
  exceeds one core (101 % CPU), so it consumes only 62k of 77k events/s and latency climbs to
  seconds. Every other combination keeps up with what is published.
- **Sending to browsers costs more than reading from Kafka.** Taking in 84k events/s with one
  client uses 28 % of a core, but sending 69k messages/s to 500 clients uses ~66 % even when
  only 139 events/s arrive. Latency starts to climb (p99 > 250 ms) once the gateway sends about
  70k messages/s, whatever the input rate.
- **The ~70 ms latency floor at 205+ tickers does not come from the gateway** (its CPU is only
  5–11 % there). The publisher stamps every event of a window with the same `timestamp_emit`,
  then serialises and sends the whole window as one batch through Kafka in Docker Desktop, so
  the later events in a batch wait longer. With real data the processor emits per symbol as
  windows close, so this effect should be smaller.
- **Memory grows with the number of tickers**, because history keeps the last 500 events per
  ticker (~1.5 M events ≈ 690 MB at 3,005 tickers). Lowering `HISTORY_LIMIT` reduces this
  proportionally.
- **At 3,005 tickers the publisher was at its own limit:** it could not send a window every
  100 ms (~90k events/s target), so it delivered ~77–83k/s (`publisher_behind_windows` in the
  CSV). Higher input rates would need a faster or second publisher.
- **Drops were never triggered.** The simulated browsers run on the same machine and read
  quickly, so their send buffers never reached the 1 MB threshold. Slow-client handling is
  covered by the unit tests instead.
- **Real load is far lower.** The benchmark publishes a window every 100 ms; the real system
  produces one window per ticker every 5 minutes, i.e. 3,000× less per ticker. With ~3,000
  tickers that is about 30 events/s, so the gateway's limit is the number of browser messages
  per second, not Kafka input.
- **How to scale it:** run several gateway instances, each with its own consumer group (each
  needs every event, which is cheap), and spread browsers over them with a load balancer.
  Fan-out, the expensive part, then divides across instances.

**Reproduce**

```powershell
# Isolated Kafka running, port 8097 free; takes about 8 minutes
cd gateway
npm run bench
# quicker: npm run bench -- --extras 0,200 --clients 1,10 --measure 8
```

The script uses its own topic `gateway-bench` (8 partitions, 5-minute retention) and a fresh
consumer group per run. It writes `bench-results/bench-<time>.csv`, `latest.csv` and
`latest.md` (the summary table), and prints the first degraded combination. Absolute numbers
depend on the machine and on everything else running on it; compare runs on the same machine.

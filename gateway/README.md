# Gateway

Node.js service that sits between Kafka and the browsers:

```
stream processor --Kafka "event-processed"--> GATEWAY --WebSocket :8080--> browsers
```

1. Consumes `EventProcessed` messages from Kafka (its own consumer group, so it sees every message).
2. Browsers connect over WebSocket and subscribe to tickers.
3. Each message is forwarded **unchanged** only to clients subscribed to its ticker.
4. On subscribe, the client first gets the recent history for that ticker.
5. Logs throughput and end-to-end latency every few seconds.

## Message format

Kafka → gateway (`EventProcessed`, one message per value, Kafka key = ticker):

```json
{ "timestamp_emit": 1636354800021, "ticker": "A2ASZ7.ETR", "type": "EMA38", "value": 12.34 }
```

- `timestamp_emit`: integer epoch milliseconds (see *Latency* below).
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

## Latency measurement (assumption)

Latency is computed when the gateway forwards an event, as `now - timestamp_emit`, once per event.
**We assume that `timestamp_emit` is the producer's `emitTimestampMs` (the wall-clock time at which
the raw trade was sent to Kafka), copied through unchanged by the stream processor.** Under that
assumption the number covers the whole pipeline: producer → Kafka → processor → Kafka → gateway.
If the processor instead stamps its own send time, the number only covers processor → gateway.
All components run on the same machine/Docker host, so the clocks agree.

Because events are emitted when a 5-minute window *closes*, latency is measured from the
emission of the trade that closed the window, not from the first trade of the window.

## Running

```powershell
cd gateway
npm install
npm start                  # fake data, no Kafka needed -> open http://localhost:8080
npm test                   # unit + end-to-end tests
npm run smoke              # starts the gateway, subscribes a client, prints what arrives
```

Settings are environment variables (PowerShell: `$env:NAME = "value"` before `npm start`):

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8080` | HTTP + WebSocket port (`/` test page, `/health` JSON) |
| `SOURCE` | `fake` | `fake` (built-in generator) or `kafka` |
| `KAFKA_BROKERS` | `localhost:9092` | comma-separated; `kafka:29092` inside docker compose |
| `KAFKA_TOPIC` | `event-processed` | input topic |
| `KAFKA_GROUP_ID` | `ultrastonk-gateway` | consumer group |
| `HISTORY_LIMIT` | `500` | events kept per ticker |
| `METRICS_INTERVAL_MS` | `10000` | metrics log period |
| `METRICS_CSV` | (off) | file to append metrics rows to (for the report) |
| `SLOW_CLIENT_BYTES` / `KILL_CLIENT_BYTES` | 1 MB / 8 MB | queued bytes before skipping / disconnecting a client |
| `FAKE_TICKERS`, `FAKE_EXTRA_TICKERS`, `FAKE_INTERVAL_MS` | 5 tickers, 0, 1000 | fake generator |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Local Kafka for testing (Windows / PowerShell)

Uses the `kafka` service from the `event-emitter` branch's `docker-compose.yml` without
switching branches (the file is piped into `docker compose` with `-f -`).

```powershell
# 1. Start Docker Desktop, then check that the engine runs (Server section must be present):
docker version
docker run --rm hello-world

# 2. Start Kafka (from the repo root):
cd "D:\aalto\scalable systems\ultrastonk"
git fetch origin
git show origin/event-emitter:docker-compose.yml | docker compose -p ultrastonk -f - up -d kafka

# 3. Check it runs and listens on 9092:
docker ps --filter name=ultrastonk-kafka
docker logs ultrastonk-kafka-1 2>&1 | Select-String "Kafka Server started"

# 4. Test the client library and create the topic:
cd gateway
npm run kafka:check

# 5. Publish test events (Ctrl+C to stop):
npm run kafka:publish

# 6. Stop Kafka:
docker compose -p ultrastonk down
```

Useful: list topics / peek at messages inside the container:

```powershell
docker exec ultrastonk-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec ultrastonk-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic event-processed --from-beginning --max-messages 5 --property print.key=true
```

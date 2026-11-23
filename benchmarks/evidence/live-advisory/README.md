# Live advisory fixture capture

The history capture below used source revision `c085b4979ed880c48055061e62f657edb95e829a`.
It was recorded on 2026-11-23 using an isolated
Compose project, fresh Kafka topics, and the production Flink and gateway
images built from that revision.

- `comparison.json`: expected and actual `read_committed` output multisets,
  rounded to nine decimal places by the verifier. Both `missing` and `extra`
  are empty; 148 records, two quarantined inputs and three live WebSocket
  subscribers were validated. History was checked separately.
- `browser.png`: Chromium rendering of the crossover ticker's gateway history
  after the verifier completed. The chart displays BUY/SELL/BUY; the latest
  BUY row shows EMA38 51.6607, EMA100 20.2137 and its one-second UTC window.
  This is a history screenshot, not a live browser update; live WebSocket
  delivery was verified separately. The browser does not display the earlier
  SELL's EMA pair in this screenshot.
- The expected crossover windows 0, 30 and 32 come from the hand-checked
  oracle fixture described in `benchmarks/query-truth.md`. The window size is
  shortened to one second for the demo. This synthetic run is not a historical
  replay, latency measurement, or recovery demonstration.

SHA-256: `browser.png` = `43bef5dc7bd88c7142edb5ecd432a557c4476739c06d2faf49b8798e56711406`;
`comparison.json` = `fe333beb8029d35dea549989579af03151e8f62aaeae7422e2d836b4a6ec6eac`.

## Reproduce

To reproduce the original history capture, use the evidence commit `2d5363d`
and an unused project ID and
ports; the verifier refuses nonempty topics. The commands below create and
destroy only the named disposable project. Install `playwright-core` outside
the repository and use a local Chromium executable for the optional screenshot.

```bash
npm install --prefix /tmp/kilo playwright-core
export COMPOSE_PROJECT_NAME=ultrastonk-verify-screen1123
export KAFKA_INPUT_TOPIC=verify-screen1123-in KAFKA_OUTPUT_TOPIC=verify-screen1123-out KAFKA_REJECTED_TOPIC=verify-screen1123-audit
export KAFKA_PORT=39092 FLINK_UI_PORT=38081 GATEWAY_PORT=38080
export SOURCE_IDLE_TIMEOUT_MS=1000 CHECKPOINT_INTERVAL_MS=1000
export WINDOW_SIZE_MS=1000 MAX_OUT_OF_ORDER_MS=0
export FLINK_RESTORE_PATH= FLINK_BOUNDED_REPLAY=false KAFKA_BOOTSTRAP_SERVERS=kafka:9092
docker compose up -d --build kafka kafka-init jobmanager taskmanager gateway
docker compose --profile benchmark build benchmark
docker compose run --rm --no-deps -v /tmp/kilo:/evidence --entrypoint python benchmark verify.py --gateway-url ws://gateway:8080 --evidence /evidence/live-advisory-comparison.json
NODE_PATH=/tmp/kilo/node_modules node benchmarks/evidence/live-advisory/capture.cjs /tmp/kilo/live-advisory-comparison.json /tmp/kilo/live-advisory-browser.png
docker compose -p ultrastonk-verify-screen1123 down --volumes
```

The capture script in this directory is a portable version of the script used
for the retained screenshot; it uses the same browser interactions. Set
`CHROMIUM` or `GATEWAY_URL` if the executable or port differs. The script must
run before tearing down the stack, because the gateway history is in memory.

## Browser-First Live Capture

The separate live capture used measured source revision `66a9785`, on
2026-11-23, with fresh project `ultrastonk-verify-truelive1123b` and topics.
`browser-frames.json` starts with the empty history for `Vevidence01C.FR`,
followed by actual WebSocket messages received by Chromium. The script creates
the verifier's release file only after receiving that empty history. The live
messages contain BUY, SELL, BUY and their matching EMA values; `browser-live.png`
shows the signal table and UTC windows. `comparison-live.json` contains the
independent expected and actual read-committed Kafka output; 148 records match,
with two quarantined inputs and three separately validated WebSocket clients.
The screenshot and frames were collected before the verifier finished its
late-event and history checks, so the frames are not a complete Kafka audit.

SHA-256: `browser-live.png` = `b7039f3c99a7345728823a65e48a6e782470e80b0b595bcd971c74f901882eca`;
`browser-frames.json` = `8c98a40e2ac96d00a5d1e2ceccc2305bc4ae2909746aafcbef5099acd9c5fbaf`;
`comparison-live.json` = `ab4dfcd25f842a3fdd342772ba743b5bffea6130927cec002360d1a043d9a4a8`.

An earlier fresh run with a 1000 ms source idle timeout received prices and
rejections but timed out waiting for windowed outputs. Its cause was not
established; the successful retry used 30000 ms. Neither run measures latency,
recovery, or historical replay. Synthetic event timestamps can be in the future.

To reproduce, use two terminals from the repository root, an unused project ID,
and a new empty output directory. Install `playwright-core` under `/tmp/kilo` as
above. The capture fails if a stale `ready` file already exists. Terminal 1:

```bash
mkdir /tmp/kilo/live-new
export COMPOSE_PROJECT_NAME=ultrastonk-verify-live-new
export KAFKA_INPUT_TOPIC=verify-live-new-in KAFKA_OUTPUT_TOPIC=verify-live-new-out KAFKA_REJECTED_TOPIC=verify-live-new-audit
export KAFKA_PORT=39192 FLINK_UI_PORT=38181 GATEWAY_PORT=38180
export SOURCE_IDLE_TIMEOUT_MS=30000 CHECKPOINT_INTERVAL_MS=1000 WINDOW_SIZE_MS=1000 MAX_OUT_OF_ORDER_MS=0
export FLINK_RESTORE_PATH= FLINK_BOUNDED_REPLAY=false KAFKA_BOOTSTRAP_SERVERS=kafka:9092
docker compose up -d --build kafka kafka-init jobmanager taskmanager gateway
docker compose --profile benchmark build benchmark
NODE_PATH=/tmp/kilo/node_modules GATEWAY_URL=http://127.0.0.1:38180/ node benchmarks/evidence/live-advisory/capture-live.cjs /tmp/kilo/live-new Vevidence01C.FR
```

While the browser command is waiting, run in terminal 2 with the same exports:

```bash
docker compose run --rm --no-deps -v /tmp/kilo/live-new:/evidence --entrypoint python benchmark verify.py --gateway-url ws://gateway:8080 --run-id evidence01 --wait-for-file /evidence/ready --evidence /evidence/comparison.json
docker compose -p ultrastonk-verify-live-new down --volumes
```

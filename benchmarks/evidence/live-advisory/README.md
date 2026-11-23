# Live advisory fixture capture

Measured source revision: `c085b4979ed880c48055061e62f657edb95e829a`.
The files in this directory were recorded on 2026-11-23 using an isolated
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

From the repository root at the evidence commit containing this document (whose
application and verifier sources match the measured revision), use an unused project ID and
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

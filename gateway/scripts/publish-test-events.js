'use strict';
// Publishes hand-made EventProcessed messages to Kafka (keyed by ticker), so the
// gateway's Kafka consumer can be tested without the stream processor.
//
// Usage (PowerShell, from gateway/):
//   node scripts/publish-test-events.js                  # 5 tickers, 1 window/s, until Ctrl+C
//   node scripts/publish-test-events.js --windows 10     # stop after 10 windows
//   node scripts/publish-test-events.js --duration 30    # stop after 30 seconds
//   node scripts/publish-test-events.js --interval 100 --extra 200   # load test
//   node scripts/publish-test-events.js --bad            # also send a few invalid records
// Env: KAFKA_BROKERS (default localhost:9092), KAFKA_TOPIC (default event-processed)
//
// One "window" = price, EMA38, EMA100 (+ BUY/SELL on crossovers) for every ticker,
// sent as one batch. Windows never overlap: if a send takes longer than --interval,
// the next window starts right after it, and the achieved rate is reported.
const { Kafka, logLevel, Partitioners } = require('kafkajs');
const { FakeSource } = require('../src/fake');
const { hideKafkajsTimeoutWarning } = require('../src/kafka-source');

hideKafkajsTimeoutWarning();

function arg(name, def) {
  const i = process.argv.indexOf(`--${name}`);
  if (i === -1) return def;
  const v = process.argv[i + 1];
  return v === undefined || v.startsWith('--') ? true : v;
}

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const topic = process.env.KAFKA_TOPIC || 'event-processed';
const intervalMs = Number(arg('interval', 1000));
const maxWindows = Number(arg('windows', Infinity));
const durationMs = Number(arg('duration', Infinity)) * 1000;
const tickers = String(arg('tickers', 'A2ASZ7.ETR,RDSA.NL,AIR.FR,SAP.ETR,ASML.NL')).split(',');
const extraTickers = Number(arg('extra', 0));
const sendBad = Boolean(arg('bad', false));

const BAD_RECORDS = [
  'not json at all',
  JSON.stringify({ ticker: 'RDSA.NL', type: 'price', value: 1 }), // no timestamp_emit
  JSON.stringify({ timestamp_emit: Date.now(), ticker: 'RDSA.NL', type: 'EMA38' }), // value missing
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const kafka = new Kafka({ clientId: 'gateway-test-publisher', brokers, logLevel: logLevel.ERROR });
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  console.log(`publishing to "${topic}" on ${brokers.join(',')}: ${tickers.length + extraTickers} tickers, every ${intervalMs} ms`);

  let batch = [];
  const source = new FakeSource({
    tickers, extraTickers, intervalMs,
    onMessage: (value, event) => batch.push({ key: event.ticker, value }),
  });

  let stopping = false;
  process.on('SIGINT', () => { stopping = true; });

  const t0 = Date.now();
  let windows = 0;
  let sent = 0;
  let behind = 0; // windows that took longer than the interval
  while (!stopping && windows < maxWindows && Date.now() - t0 < durationMs) {
    const start = Date.now();
    batch = [];
    source.tick();
    if (sendBad && windows === 0) batch.push(...BAD_RECORDS.map((value) => ({ key: 'RDSA.NL', value })));
    await producer.send({ topic, messages: batch });
    windows++;
    sent += batch.length;
    if (windows % 10 === 1 || windows === maxWindows) {
      const signals = batch.filter((m) => /"type":"(BUY|SELL)"/.test(m.value)).length;
      console.log(`window ${windows}: sent ${batch.length} messages (${signals} BUY/SELL), total ${sent}`);
    }
    const spent = Date.now() - start;
    if (spent > intervalMs) behind++;
    else await sleep(intervalMs - spent);
  }
  const seconds = (Date.now() - t0) / 1000;
  await producer.disconnect();
  console.log(`done: ${windows} windows, ${sent} messages in ${seconds.toFixed(1)} s = ${Math.round(sent / seconds)} msg/s, ${behind} windows behind schedule`);
}

main().catch((e) => {
  console.error(`publisher failed (Kafka at ${brokers.join(',')}): ${e.message}`);
  process.exit(1);
});

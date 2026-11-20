'use strict';
// Publishes hand-made EventProcessed messages to Kafka (keyed by ticker), so the
// gateway's Kafka consumer can be tested without the stream processor.
//
// Usage (PowerShell, from gateway/):
//   node scripts/publish-test-events.js                  # 5 tickers, 1 window/s, until Ctrl+C
//   node scripts/publish-test-events.js --windows 10     # stop after 10 windows
//   node scripts/publish-test-events.js --interval 100 --extra 200   # load test
//   node scripts/publish-test-events.js --bad            # also send a few invalid records
// Env: KAFKA_BROKERS (default localhost:9092), KAFKA_TOPIC (default event-processed)
const { Kafka, logLevel, Partitioners } = require('kafkajs');
const { FakeSource } = require('../src/fake');

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
const tickers = String(arg('tickers', 'A2ASZ7.ETR,RDSA.NL,AIR.FR,SAP.ETR,ASML.NL')).split(',');
const extraTickers = Number(arg('extra', 0));
const sendBad = Boolean(arg('bad', false));

const BAD_RECORDS = [
  'not json at all',
  JSON.stringify({ ticker: 'RDSA.NL', type: 'price', value: 1 }), // no timestamp_emit
  JSON.stringify({ timestamp_emit: Date.now(), ticker: 'RDSA.NL', type: 'EMA38' }), // value missing
];

async function main() {
  const kafka = new Kafka({ clientId: 'gateway-test-publisher', brokers, logLevel: logLevel.ERROR });
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  console.log(`publishing to "${topic}" on ${brokers.join(',')}: ${tickers.length + extraTickers} tickers, every ${intervalMs} ms`);

  // Reuse the fake generator; collect one window's messages, then send them as one batch.
  let batch = [];
  const source = new FakeSource({ tickers, extraTickers, intervalMs, onMessage: (text) => batch.push(text) });
  let windows = 0;
  let sent = 0;
  let stopping = false;

  const stop = async () => {
    if (stopping) return;
    stopping = true;
    clearInterval(timer);
    await producer.disconnect();
    console.log(`done: ${windows} windows, ${sent} messages`);
  };

  const publishWindow = async () => {
    if (stopping) return;
    batch = [];
    source.tick();
    const messages = batch.map((value) => ({ key: JSON.parse(value).ticker, value }));
    if (sendBad && windows === 0) messages.push(...BAD_RECORDS.map((value) => ({ key: 'RDSA.NL', value })));
    await producer.send({ topic, messages });
    windows++;
    sent += messages.length;
    if (windows % 10 === 1 || windows === maxWindows) {
      const signals = messages.filter((m) => /"type":"(BUY|SELL)"/.test(m.value)).length;
      console.log(`window ${windows}: sent ${messages.length} messages (${signals} BUY/SELL), total ${sent}`);
    }
    if (windows >= maxWindows) await stop();
  };

  const timer = setInterval(() => publishWindow().catch((e) => { console.error('send failed:', e.message); stop(); }), intervalMs);
  publishWindow().catch((e) => { console.error('send failed:', e.message); stop(); });
  process.on('SIGINT', stop);
}

main().catch((e) => {
  console.error(`could not connect to Kafka at ${brokers.join(',')}: ${e.message}`);
  process.exit(1);
});

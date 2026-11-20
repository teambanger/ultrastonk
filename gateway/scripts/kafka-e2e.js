'use strict';
// End-to-end test through real Kafka (Kafka must be running on KAFKA_BROKERS):
//   1. start the gateway with SOURCE=kafka (port 8098, fresh consumer group)
//   2. connect a WebSocket client and subscribe to RDSA.NL
//   3. run the test publisher: 10 windows + 3 invalid records
//   4. check the client got RDSA.NL's events and /health counted everything
// Usage: node scripts/kafka-e2e.js   (or npm run kafka:e2e)
// Against an already running gateway (e.g. the Docker container), skip step 1:
//   $env:GATEWAY_URL = "localhost:8080"; npm run kafka:e2e
const { spawn } = require('node:child_process');
const path = require('node:path');
const WebSocket = require('ws');

const PORT = 8098;
const TICKER = 'RDSA.NL';
const WINDOWS = 10;
const root = path.join(__dirname, '..');

function startChild(name, args, env) {
  const child = spawn(process.execPath, args, { cwd: root, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  const lines = [];
  const onData = (d) => d.toString().split(/\r?\n/).filter(Boolean).forEach((l) => { lines.push(l); console.log(`[${name}] ${l}`); });
  child.stdout.on('data', onData);
  child.stderr.on('data', onData);
  child.waitFor = (re, ms) => new Promise((resolve, reject) => {
    const t0 = Date.now();
    const iv = setInterval(() => {
      const hit = lines.find((l) => re.test(l));
      if (hit) { clearInterval(iv); resolve(hit); } else if (Date.now() - t0 > ms) { clearInterval(iv); reject(new Error(`${name}: timeout waiting for ${re}`)); }
    }, 100);
  });
  return child;
}

const getHealth = async (host) => (await fetch(`http://${host}/health`)).json();

async function main() {
  const external = process.env.GATEWAY_URL;
  const host = external || `localhost:${PORT}`;
  const gateway = external ? null : startChild('gateway', ['src/index.js'], {
    SOURCE: 'kafka', PORT: String(PORT), KAFKA_GROUP_ID: `gateway-e2e-${Date.now()}`, METRICS_INTERVAL_MS: '2000',
  });
  try {
    if (gateway) await gateway.waitFor(/kafka group joined/, 30000);
    const before = await getHealth(host);
    if (!before.source || before.source.status !== 'running') throw new Error(`gateway source not running: ${JSON.stringify(before.source)}`);

    const ws = new WebSocket(`ws://${host}/`);
    const received = [];
    ws.on('message', (d) => received.push(JSON.parse(d)));
    await new Promise((r, j) => { ws.once('open', r); ws.once('error', j); });
    ws.send(JSON.stringify({ type: 'subscribe', tickers: [TICKER] }));

    const publisher = startChild('publisher', ['scripts/publish-test-events.js', '--windows', String(WINDOWS), '--interval', '200', '--bad'], {});
    const done = await publisher.waitFor(/^done: /, 30000);
    const published = Number(done.match(/(\d+) messages/)[1]);
    await new Promise((r) => setTimeout(r, 1500)); // let the last messages arrive

    const health = await getHealth(host);
    ws.close();

    const consumed = health.totals.in - before.totals.in; // deltas: an external gateway may have older counts
    const invalid = health.totals.invalid - before.totals.invalid;
    const live = received.filter((m) => m.ticker === TICKER && m.type !== 'history');
    const otherTickers = received.filter((m) => m.ticker && m.ticker !== TICKER).length;
    const checks = [
      ['gateway consumed every valid message', consumed === published - 3, `${consumed} of ${published - 3}`],
      ['3 invalid records rejected', invalid === 3, invalid],
      [`client got >= ${3 * WINDOWS} live ${TICKER} events`, live.length >= 3 * WINDOWS, live.length],
      ['client got nothing for other tickers', otherTickers === 0, otherTickers],
      ['events arrive in timestamp order', live.every((m, i) => i === 0 || m.timestamp_emit >= live[i - 1].timestamp_emit), ''],
      ['source status is running', health.source && health.source.status === 'running', JSON.stringify(health.source)],
    ];
    console.log('');
    for (const [name, ok, detail] of checks) console.log(`${ok ? 'PASS' : 'FAIL'}  ${name} ${detail !== '' ? `(${detail})` : ''}`);
    const allOk = checks.every((c) => c[1]);
    console.log(`\nRESULT: ${allOk ? 'Kafka -> gateway -> WebSocket WORKS' : 'FAILED'}`);
    process.exitCode = allOk ? 0 : 1;
  } catch (err) {
    console.log(`\nRESULT: FAILED: ${err.message}`);
    process.exitCode = 1;
  } finally {
    if (gateway) gateway.kill();
  }
}

main();

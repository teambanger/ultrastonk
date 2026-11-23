'use strict';
// Load / metrics benchmark through the real Kafka path (Kafka must be running).
//
// For every combination of ticker count x client count:
//   1. start a fresh gateway (SOURCE=kafka, own topic + consumer group, metrics CSV)
//   2. connect N WebSocket clients, each subscribed to a few random tickers
//   3. run the publisher (5 + extra tickers, one window every --interval ms)
//   4. after a warm-up, measure for --measure seconds:
//        gateway side (from its metrics CSV): events/s in and out, dropped,
//          latency p50/p95/p99 (now - timestamp_emit when forwarded), CPU %, memory
//        client side: messages/s received, latency on arrival
//   5. stop everything, next combination
// Output: bench-results/bench-<time>.csv (+ latest.csv, latest.md) and a summary table.
//
// Usage: npm run bench
//        npm run bench -- --extras 0,200 --clients 1,10 --measure 8     (quick run)
// Options: --extras, --clients, --interval (ms, default 100), --warmup (s, 5),
//          --measure (s, 15), --subs (tickers per client, 5)
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const WebSocket = require('ws');
const { Kafka, logLevel } = require('kafkajs');
const { hideKafkajsTimeoutWarning } = require('../src/kafka-source');
const { mulberry32 } = require('../src/fake');

hideKafkajsTimeoutWarning();

function arg(name, def) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 || process.argv[i + 1] === undefined ? def : process.argv[i + 1];
}
const nums = (s) => String(s).split(',').map(Number);

const EXTRAS = nums(arg('extras', '0,200,1000,3000'));
const CLIENTS = nums(arg('clients', '1,10,100,500'));
const INTERVAL_MS = Number(arg('interval', 100));
const WARMUP_S = Number(arg('warmup', 5));
const MEASURE_S = Number(arg('measure', 15));
const SUBS = Number(arg('subs', 5));
const METRICS_MS = 2500;
const PORT = 8097;
const TOPIC = 'gateway-bench';
const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const BASE_TICKERS = ['A2ASZ7.ETR', 'RDSA.NL', 'AIR.FR', 'SAP.ETR', 'ASML.NL'];

const root = path.join(__dirname, '..');
const outDir = path.join(root, 'bench-results');
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gateway-bench-'));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- helpers ----------
function startChild(args, env) {
  const child = spawn(process.execPath, args, { cwd: root, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  child.lines = [];
  const onData = (d) => d.toString().split(/\r?\n/).filter(Boolean).forEach((l) => child.lines.push(l));
  child.stdout.on('data', onData);
  child.stderr.on('data', onData);
  child.exited = new Promise((r) => child.on('exit', r));
  child.waitFor = async (re, ms) => {
    const t0 = Date.now();
    while (Date.now() - t0 < ms) {
      const hit = child.lines.find((l) => re.test(l));
      if (hit) return hit;
      await sleep(100);
    }
    throw new Error(`timeout waiting for ${re}; last output: ${child.lines.slice(-3).join(' | ')}`);
  };
  return child;
}

function percentile(sorted, p) {
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] : null;
}

function pickTickers(rand, universe, k) {
  if (universe.length <= k) return [...universe];
  const set = new Set();
  while (set.size < k) set.add(universe[Math.floor(rand() * universe.length)]);
  return [...set];
}

async function ensureTopic() {
  const admin = new Kafka({ clientId: 'gateway-bench-admin', brokers: BROKERS, logLevel: logLevel.NOTHING }).admin();
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: 8, configEntries: [{ name: 'retention.ms', value: '300000' }] }],
    waitForLeaders: true,
  });
  await admin.disconnect();
}

/** Simulated browser: subscribes, then records arrival count + latency while `stats.measuring`. */
function connectClient(port, tickers, stats) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${port}/`);
    let histories = 0;
    ws.on('error', reject);
    ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', tickers })));
    ws.on('message', (data) => {
      const text = data.toString();
      if (text.startsWith('{"type":"history"')) {
        if (++histories === tickers.length) resolve(ws);
        return;
      }
      if (!stats.measuring) return;
      stats.received++;
      const m = /"timestamp_emit":(\d+)/.exec(text); // cheaper than JSON.parse for 100k msg/s
      if (m) {
        stats.seen++;
        const lat = Date.now() - Number(m[1]);
        if (stats.lat.length < 200000) stats.lat.push(lat);
        else { const j = Math.floor(Math.random() * stats.seen); if (j < 200000) stats.lat[j] = lat; }
      }
    });
  });
}

function readGatewayRows(file, fromMs, toMs) {
  if (!fs.existsSync(file)) return [];
  const [header, ...lines] = fs.readFileSync(file, 'utf8').trim().split('\n');
  const cols = header.split(',');
  return lines.map((l) => Object.fromEntries(l.split(',').map((v, i) => [cols[i], cols[i] === 'time' ? v : Number(v)])))
    .filter((r) => {
      const end = Date.parse(r.time);
      return end - r.seconds * 1000 >= fromMs - 200 && end <= toMs + 500; // interval fully inside the window
    });
}

const mean = (xs) => (xs.length ? xs.reduce((s, x) => s + x, 0) / xs.length : null);
const max = (xs) => (xs.length ? Math.max(...xs) : null);
const median = (xs) => percentile([...xs].sort((a, b) => a - b), 50);
const r1 = (x) => (x === null ? null : Math.round(x * 10) / 10);

// ---------- one run ----------
async function runOne(extra, nClients, runNo) {
  const universe = [...BASE_TICKERS];
  for (let i = 1; i <= extra; i++) universe.push(`SYN${String(i).padStart(5, '0')}.FAKE`);
  const csvFile = path.join(tmpDir, `run-${runNo}.csv`);
  const gateway = startChild(['src/index.js'], {
    SOURCE: 'kafka', PORT: String(PORT), KAFKA_TOPIC: TOPIC, KAFKA_GROUP_ID: `gateway-bench-${Date.now()}`,
    METRICS_INTERVAL_MS: String(METRICS_MS), METRICS_CSV: csvFile, LOG_LEVEL: 'info',
  });
  const clients = [];
  const stats = { measuring: false, received: 0, seen: 0, lat: [] };
  try {
    await gateway.waitFor(/kafka group joined/, 30000);
    const rand = mulberry32(runNo + 1);
    for (let i = 0; i < nClients; i++) clients.push(await connectClient(PORT, pickTickers(rand, universe, SUBS), stats));

    const publisher = startChild(['scripts/publish-test-events.js', '--extra', String(extra), '--interval', String(INTERVAL_MS),
      '--duration', String(WARMUP_S + MEASURE_S)], { KAFKA_TOPIC: TOPIC });
    await sleep(WARMUP_S * 1000);
    stats.measuring = true;
    const from = Date.now();
    await sleep(MEASURE_S * 1000);
    stats.measuring = false;
    const to = Date.now();
    const done = await publisher.waitFor(/^done: /, 60000);
    await sleep(METRICS_MS + 500); // let the gateway write its last metrics row

    const rows = readGatewayRows(csvFile, from, to);
    const lat = stats.lat.sort((a, b) => a - b);
    const published = Number(/= (\d+) msg\/s/.exec(done)[1]);
    const behind = Number(/(\d+) windows behind/.exec(done)[1]);
    const result = {
      tickers: universe.length,
      clients: nClients,
      subs_per_client: Math.min(SUBS, universe.length),
      published_per_s: published,
      publisher_behind_windows: behind,
      gw_in_per_s: r1(mean(rows.map((r) => r.in_per_sec))),
      gw_fwd_per_s: r1(mean(rows.map((r) => r.forwarded_per_sec))),
      gw_dropped: rows.reduce((s, r) => s + r.dropped, 0),
      gw_lat_p50: median(rows.map((r) => r.lat_p50)),
      gw_lat_p95: max(rows.map((r) => r.lat_p95)),
      gw_lat_p99: max(rows.map((r) => r.lat_p99)),
      client_recv_per_s: r1(stats.received / MEASURE_S),
      client_lat_p50: percentile(lat, 50),
      client_lat_p95: percentile(lat, 95),
      client_lat_p99: percentile(lat, 99),
      gw_cpu_pct: r1(mean(rows.map((r) => r.cpu_pct))),
      gw_rss_mb: max(rows.map((r) => r.rss_mb)),
      intervals: rows.length,
    };
    result.status = classify(result);
    return result;
  } finally {
    for (const ws of clients) ws.terminate();
    gateway.kill();
    await gateway.exited;
  }
}

// OK = keeps up; LAG = gateway consumes slower than published; DROPS = slow-client
// skips; SLOW = gateway p99 latency above 250 ms.
function classify(r) {
  const flags = [];
  if (r.gw_in_per_s !== null && r.gw_in_per_s < 0.95 * r.published_per_s) flags.push('LAG');
  if (r.gw_dropped > 0) flags.push('DROPS');
  if (r.gw_lat_p99 !== null && r.gw_lat_p99 > 250) flags.push('SLOW');
  return flags.length ? flags.join('+') : 'OK';
}

// ---------- main ----------
const COLUMNS = ['tickers', 'clients', 'subs_per_client', 'published_per_s', 'publisher_behind_windows', 'gw_in_per_s', 'gw_fwd_per_s',
  'gw_dropped', 'gw_lat_p50', 'gw_lat_p95', 'gw_lat_p99', 'client_recv_per_s', 'client_lat_p50', 'client_lat_p95', 'client_lat_p99',
  'gw_cpu_pct', 'gw_rss_mb', 'intervals', 'status'];

function markdownTable(results) {
  const head = '| Tickers | Clients | Published msg/s | Gateway in/s | Out (fwd)/s | Dropped | Gw latency p50/p95/p99 ms | Client latency p50/p99 ms | CPU % | RSS MB | Status |';
  const sep = '|---:|---:|---:|---:|---:|---:|---|---|---:|---:|---|';
  const rows = results.map((r) => `| ${r.tickers} | ${r.clients} | ${r.published_per_s} | ${r.gw_in_per_s} | ${r.gw_fwd_per_s} | ${r.gw_dropped} | `
    + `${r.gw_lat_p50} / ${r.gw_lat_p95} / ${r.gw_lat_p99} | ${r.client_lat_p50} / ${r.client_lat_p99} | ${r.gw_cpu_pct} | ${r.gw_rss_mb} | ${r.status} |`);
  return [head, sep, ...rows].join('\n');
}

async function main() {
  const total = EXTRAS.length * CLIENTS.length;
  console.log(`bench: ${total} runs, tickers 5+${EXTRAS.join('/')} x clients ${CLIENTS.join('/')}, `
    + `window every ${INTERVAL_MS} ms, warm-up ${WARMUP_S} s, measure ${MEASURE_S} s, ${SUBS} tickers/client`);
  console.log(`machine: ${os.cpus()[0].model}, ${os.cpus().length} logical CPUs, ${Math.round(os.totalmem() / 2 ** 30)} GB RAM, Node ${process.version}`);
  await ensureTopic();

  fs.mkdirSync(outDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const csvPath = path.join(outDir, `bench-${stamp}.csv`);
  fs.writeFileSync(csvPath, COLUMNS.join(',') + '\n');

  const results = [];
  let runNo = 0;
  for (const extra of EXTRAS) {
    for (const nClients of CLIENTS) {
      runNo++;
      process.stdout.write(`[${runNo}/${total}] tickers=${5 + extra} clients=${nClients} ... `);
      try {
        const r = await runOne(extra, nClients, runNo);
        results.push(r);
        fs.appendFileSync(csvPath, COLUMNS.map((c) => (r[c] === null ? '' : r[c])).join(',') + '\n');
        console.log(`in ${r.gw_in_per_s}/s (published ${r.published_per_s}/s), out ${r.gw_fwd_per_s}/s, dropped ${r.gw_dropped}, `
          + `gw p99 ${r.gw_lat_p99} ms, client p99 ${r.client_lat_p99} ms, cpu ${r.gw_cpu_pct}%, rss ${r.gw_rss_mb} MB -> ${r.status}`);
      } catch (err) {
        console.log(`FAILED: ${err.message}`);
      }
    }
  }

  fs.copyFileSync(csvPath, path.join(outDir, 'latest.csv'));
  const table = markdownTable(results);
  fs.writeFileSync(path.join(outDir, 'latest.md'), table + '\n');
  console.log(`\n${table}\n`);
  const firstBad = results.find((r) => r.status !== 'OK');
  console.log(firstBad
    ? `First degraded run: tickers=${firstBad.tickers}, clients=${firstBad.clients} (${firstBad.status})`
    : 'No degradation found in the tested range.');
  console.log(`CSV: ${csvPath}`);
  fs.rmSync(tmpDir, { recursive: true, force: true });
}

main().catch((e) => {
  console.error(`bench failed: ${e.message}`);
  process.exit(1);
});

'use strict';

// Entry point: wires config, gateway core, data source, server and metrics.
const fs = require('node:fs');
const { loadConfig } = require('./config');
const { createLogger } = require('./logger');
const { SubscriptionRegistry } = require('./subscriptions');
const { HistoryStore } = require('./history');
const { Metrics, CSV_HEADER, csvRow, processSampler } = require('./metrics');
const { Gateway } = require('./gateway');
const { createServer } = require('./server');
const { FakeSource } = require('./fake');
const { KafkaSource } = require('./kafka-source');

async function main() {
  const config = loadConfig();
  const log = createLogger(config.logLevel);

  const registry = new SubscriptionRegistry({ maxTickersPerClient: config.maxTickersPerClient });
  const history = new HistoryStore(config.historyLimit);
  const metrics = new Metrics();
  const gateway = new Gateway({
    registry, history, metrics, log,
    slowClientBytes: config.slowClientBytes,
    killClientBytes: config.killClientBytes,
  });

  let source;
  const server = createServer({
    gateway, maxClients: config.maxClients, heartbeatMs: config.heartbeatMs, log,
    sourceStatus: () => ({ type: config.source, status: source ? source.status : 'stopped' }),
  });
  const port = await server.listen(config.port);
  log.info('gateway listening', { http: `http://localhost:${port}/`, ws: `ws://localhost:${port}/`, source: config.source });

  if (config.source === 'fake') {
    source = new FakeSource({ ...config.fake, onMessage: (text) => gateway.ingest(text) });
    source.start();
    log.info('fake source started', { tickers: source.tickers.length, intervalMs: config.fake.intervalMs });
  } else {
    source = new KafkaSource({ ...config.kafka, log, onMessage: (value) => gateway.ingest(value) });
    source.start(); // retries in the background until Kafka is reachable
  }

  if (config.metricsCsv && !fs.existsSync(config.metricsCsv)) fs.writeFileSync(config.metricsCsv, CSV_HEADER);
  const sampleProcess = processSampler();
  const metricsTimer = setInterval(() => {
    const r = metrics.report();
    const conn = registry.stats();
    const proc = sampleProcess();
    const l = r.latencyMs;
    log.info('metrics', {
      in_per_s: r.inPerSec, fwd_per_s: r.forwardedPerSec, dropped: r.dropped, invalid: r.invalid,
      clients: conn.clients, tickers: conn.tickers,
      lat_ms: l.p50 === null ? 'n/a' : `p50=${l.p50},p95=${l.p95},p99=${l.p99},max=${l.max}`,
      cpu_pct: proc.cpuPct, rss_mb: proc.rssMb,
    });
    if (config.metricsCsv) fs.appendFile(config.metricsCsv, csvRow(r, { ...conn, ...proc }), () => {});
  }, config.metricsIntervalMs);

  let stopping = false;
  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    log.info('shutting down', { signal });
    clearInterval(metricsTimer);
    if (source) await source.stop();
    await server.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown); // Ctrl+C (works on Windows too)
  process.on('SIGTERM', shutdown); // docker stop
}

main().catch((err) => {
  console.error(`${new Date().toISOString()} ERROR gateway failed to start: ${err.message}`);
  process.exit(1);
});

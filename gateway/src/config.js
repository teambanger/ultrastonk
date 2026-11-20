'use strict';

// All settings come from environment variables, with defaults for local use.
// PowerShell example:  $env:PORT = "8081"; npm start

function int(env, name, def, min = 0) {
  const raw = env[name];
  if (raw === undefined || raw.trim() === '') return def;
  const n = Number(raw);
  if (!Number.isSafeInteger(n) || n < min) throw new Error(`${name} must be an integer >= ${min}, got "${raw}"`);
  return n;
}

function str(env, name, def) {
  const raw = env[name];
  return raw === undefined || raw.trim() === '' ? def : raw.trim();
}

function list(env, name, def) {
  return str(env, name, def).split(',').map((s) => s.trim()).filter(Boolean);
}

function loadConfig(env = process.env) {
  const source = str(env, 'SOURCE', 'fake').toLowerCase();
  if (source !== 'fake' && source !== 'kafka') throw new Error(`SOURCE must be "fake" or "kafka", got "${source}"`);
  return {
    port: int(env, 'PORT', 8080),
    source,
    historyLimit: int(env, 'HISTORY_LIMIT', 500, 1),
    maxClients: int(env, 'MAX_CLIENTS', 1000, 1),
    maxTickersPerClient: int(env, 'MAX_TICKERS_PER_CLIENT', 1000, 1),
    // Slow clients: above this many queued bytes we skip messages for that
    // client; above the kill limit we disconnect it.
    slowClientBytes: int(env, 'SLOW_CLIENT_BYTES', 1024 * 1024, 1),
    killClientBytes: int(env, 'KILL_CLIENT_BYTES', 8 * 1024 * 1024, 1),
    heartbeatMs: int(env, 'HEARTBEAT_MS', 30000, 1000),
    metricsIntervalMs: int(env, 'METRICS_INTERVAL_MS', 10000, 100),
    metricsCsv: str(env, 'METRICS_CSV', ''), // optional file to append metrics rows to
    logLevel: str(env, 'LOG_LEVEL', 'info'),
    kafka: {
      brokers: list(env, 'KAFKA_BROKERS', 'localhost:9092'),
      topic: str(env, 'KAFKA_TOPIC', 'event-processed'),
      groupId: str(env, 'KAFKA_GROUP_ID', 'ultrastonk-gateway'),
      clientId: str(env, 'KAFKA_CLIENT_ID', 'ultrastonk-gateway'),
      // Only used the first time a group starts (no committed offsets yet):
      // false = only new messages, true = replay the whole topic.
      fromBeginning: str(env, 'KAFKA_FROM_BEGINNING', 'false').toLowerCase() === 'true',
    },
    fake: {
      tickers: list(env, 'FAKE_TICKERS', 'A2ASZ7.ETR,RDSA.NL,AIR.FR,SAP.ETR,ASML.NL'),
      extraTickers: int(env, 'FAKE_EXTRA_TICKERS', 0), // adds SYN00001.FAKE ... for load tests
      intervalMs: int(env, 'FAKE_INTERVAL_MS', 1000, 1),
    },
  };
}

module.exports = { loadConfig };

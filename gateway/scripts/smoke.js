'use strict';
// Smoke test: starts the gateway (fake mode) as a child process, subscribes one
// WebSocket client, prints what arrives plus /health, then stops the child.
// Usage: node scripts/smoke.js   (uses port 8099 so it doesn't clash with npm start)
const { spawn } = require('node:child_process');
const path = require('node:path');
const WebSocket = require('ws');

const PORT = 8099;
const child = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'index.js')], {
  env: { ...process.env, PORT: String(PORT), SOURCE: 'fake', FAKE_INTERVAL_MS: '200', METRICS_INTERVAL_MS: '2000' },
  stdio: ['ignore', 'pipe', 'pipe'],
});
child.stdout.on('data', (d) => process.stdout.write(`[gateway] ${d}`));
child.stderr.on('data', (d) => process.stdout.write(`[gateway] ${d}`));

const stop = (code) => { child.kill(); process.exitCode = code; };

setTimeout(() => {
  const ws = new WebSocket(`ws://localhost:${PORT}/`);
  let n = 0;
  ws.on('error', (e) => { console.log('client error:', e.message); stop(1); });
  ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', tickers: ['RDSA.NL'] })));
  ws.on('message', (d) => {
    const m = JSON.parse(d);
    if (n++ < 5) console.log('[client]', m.type === 'history' ? `history ${m.ticker} events=${m.events.length}` : d.toString());
  });
  setTimeout(async () => {
    console.log(`[client] received ${n} messages in 4 s`);
    console.log('[health]', await (await fetch(`http://localhost:${PORT}/health`)).text());
    ws.close();
    stop(n > 10 ? 0 : 1);
  }, 4000);
}, 1000);

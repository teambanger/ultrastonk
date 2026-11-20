'use strict';
// End-to-end over a real WebSocket: start the server on a free port,
// connect with the ws client library, subscribe, push an event, receive it.
const test = require('node:test');
const assert = require('node:assert/strict');
const WebSocket = require('ws');
const { createServer } = require('../src/server');
const { Gateway } = require('../src/gateway');
const { SubscriptionRegistry } = require('../src/subscriptions');
const { HistoryStore } = require('../src/history');
const { Metrics } = require('../src/metrics');

function nextMessage(ws) {
  return new Promise((resolve) => ws.once('message', (d) => resolve(JSON.parse(d))));
}

test('WebSocket subscribe -> history -> live event; HTTP health and page', async (t) => {
  const gateway = new Gateway({ registry: new SubscriptionRegistry(), history: new HistoryStore(10), metrics: new Metrics() });
  const server = createServer({ gateway });
  const port = await server.listen(0); // 0 = any free port
  t.after(() => server.close());

  gateway.ingest(JSON.stringify({ timestamp_emit: 1, ticker: 'X.FR', type: 'price', value: 10 }));

  const ws = new WebSocket(`ws://localhost:${port}/`);
  await new Promise((r) => ws.once('open', r));
  ws.send(JSON.stringify({ type: 'subscribe', tickers: ['X.FR'] }));
  const history = await nextMessage(ws);
  assert.equal(history.type, 'history');
  assert.equal(history.events.length, 1);

  const live = { timestamp_emit: Date.now(), ticker: 'X.FR', type: 'BUY', value: 11 };
  gateway.ingest(JSON.stringify(live));
  assert.deepEqual(await nextMessage(ws), live);

  ws.send('nonsense');
  assert.equal((await nextMessage(ws)).type, 'error');

  const health = await (await fetch(`http://localhost:${port}/health`)).json();
  assert.equal(health.status, 'ok');
  assert.equal(health.clients, 1);

  const page = await fetch(`http://localhost:${port}/`);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /<title>Gateway Test Page<\/title>/);

  ws.close();
  await new Promise((r) => ws.once('close', r));
  // give the server a moment to process the close
  await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(gateway.registry.stats(), { clients: 0, tickers: 0 });
});

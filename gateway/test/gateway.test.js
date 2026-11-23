'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Gateway } = require('../src/gateway');
const { SubscriptionRegistry } = require('../src/subscriptions');
const { HistoryStore } = require('../src/history');
const { Metrics } = require('../src/metrics');

// Stand-in for a WebSocket: records what is sent to it.
function fakeClient(id) {
  return {
    id, readyState: 1, bufferedAmount: 0, sent: [], terminated: false,
    send(text) { this.sent.push(JSON.parse(text)); },
    terminate() { this.terminated = true; this.readyState = 3; },
  };
}

function setup(opts = {}) {
  return new Gateway({
    registry: new SubscriptionRegistry(),
    history: new HistoryStore(opts.historyLimit || 500),
    metrics: new Metrics(),
    now: () => 1000,
    ...opts,
  });
}

const ev = (ticker, ts, type = 'price', value = 1.5) => JSON.stringify({ timestamp_emit: ts, ticker, type, value });
const sub = (tickers) => JSON.stringify({ type: 'subscribe', tickers });

test('events go only to clients subscribed to that ticker, unchanged', () => {
  const g = setup();
  const a = fakeClient(1);
  const b = fakeClient(2);
  g.onClientMessage(a, sub(['X']));
  g.onClientMessage(b, sub(['Y']));
  a.sent = []; b.sent = []; // drop the history messages

  const raw = JSON.stringify({ timestamp_emit: 900, ticker: 'X', type: 'EMA38', value: 2.5, extra: 'kept' });
  g.ingest(raw);
  assert.deepEqual(a.sent, [JSON.parse(raw)]);
  assert.deepEqual(b.sent, []);
});

test('subscribe sends history ordered by timestamp_emit', () => {
  const g = setup();
  g.ingest(ev('X', 20, 'EMA38'));
  g.ingest(ev('X', 10, 'price'));
  g.ingest(ev('Y', 15));
  const a = fakeClient(1);
  g.onClientMessage(a, sub(['X', 'Z']));
  assert.equal(a.sent.length, 2);
  assert.equal(a.sent[0].type, 'history');
  assert.equal(a.sent[0].ticker, 'X');
  assert.deepEqual(a.sent[0].events.map((e) => e.timestamp_emit), [10, 20]);
  assert.deepEqual(a.sent[1], { type: 'history', ticker: 'Z', events: [] });
});

test('unsubscribe and disconnect stop delivery', () => {
  const g = setup();
  const a = fakeClient(1);
  const b = fakeClient(2);
  g.onClientMessage(a, sub(['X']));
  g.onClientMessage(b, sub(['X']));
  g.onClientMessage(a, JSON.stringify({ type: 'unsubscribe', tickers: ['X'] }));
  g.onDisconnect(b);
  a.sent = []; b.sent = [];
  g.ingest(ev('X', 1));
  assert.deepEqual(a.sent, []);
  assert.deepEqual(b.sent, []);
  assert.deepEqual(g.registry.stats(), { clients: 0, tickers: 0 });
});

test('bad client message gets an error reply', () => {
  const g = setup();
  const a = fakeClient(1);
  g.onClientMessage(a, '{"type":"subscribe","tickers":"X"}');
  assert.equal(a.sent[0].type, 'error');
});

test('invalid events are counted and not stored or forwarded', () => {
  const g = setup();
  const a = fakeClient(1);
  g.onClientMessage(a, sub(['X']));
  a.sent = [];
  assert.equal(g.ingest('garbage'), false);
  assert.equal(g.ingest(JSON.stringify({ ticker: 'X', type: 'price', value: 1 })), false);
  assert.deepEqual(a.sent, []);
  assert.equal(g.metrics.totals.invalid, 2);
  assert.deepEqual(g.history.get('X'), []);
});

test('latency = now - timestamp_emit, recorded per event', () => {
  const g = setup(); // now() = 1000
  g.ingest(ev('X', 900));
  g.ingest(ev('X', 990));
  assert.deepEqual(g.metrics.latencies, [100, 10]);
  assert.equal(g.metrics.totals.in, 2);
});

test('slow client: messages are skipped, very slow client is disconnected', () => {
  const g = setup({ slowClientBytes: 100, killClientBytes: 1000 });
  const slow = fakeClient(1);
  const fine = fakeClient(2);
  g.onClientMessage(slow, sub(['X']));
  g.onClientMessage(fine, sub(['X']));
  slow.sent = []; fine.sent = [];

  slow.bufferedAmount = 500;
  g.ingest(ev('X', 1));
  assert.equal(slow.sent.length, 0);
  assert.equal(fine.sent.length, 1);
  assert.equal(g.metrics.totals.dropped, 1);

  slow.bufferedAmount = 5000;
  g.ingest(ev('X', 2));
  assert.equal(slow.terminated, true);
  assert.equal(g.registry.subscribersOf('X').size, 1);
  assert.equal(fine.sent.length, 2);
});

test('closed sockets are not written to', () => {
  const g = setup();
  const a = fakeClient(1);
  g.onClientMessage(a, sub(['X']));
  a.sent = [];
  a.readyState = 2; // CLOSING
  g.ingest(ev('X', 1));
  assert.deepEqual(a.sent, []);
});

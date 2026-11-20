'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { SubscriptionRegistry } = require('../src/subscriptions');

test('subscribe / lookup / unsubscribe', () => {
  const reg = new SubscriptionRegistry();
  const a = {}; // stands in for a WebSocket
  const b = {};
  assert.deepEqual(reg.subscribe(a, ['X', 'Y']), ['X', 'Y']);
  assert.deepEqual(reg.subscribe(b, ['Y']), ['Y']);
  assert.deepEqual(reg.subscribe(a, ['X']), [], 'second subscribe is a no-op');

  assert.deepEqual([...reg.subscribersOf('Y')], [a, b]);
  assert.deepEqual([...reg.subscribersOf('X')], [a]);
  assert.equal(reg.subscribersOf('Z').size, 0);

  assert.deepEqual(reg.unsubscribe(a, ['Y', 'Z']), ['Y']);
  assert.deepEqual([...reg.subscribersOf('Y')], [b]);
});

test('removeClient cleans up everything (no memory leak)', () => {
  const reg = new SubscriptionRegistry();
  const a = {};
  const b = {};
  reg.subscribe(a, ['X', 'Y']);
  reg.subscribe(b, ['Y']);
  reg.removeClient(a);
  assert.deepEqual(reg.stats(), { clients: 1, symbols: 1 });
  reg.removeClient(b);
  reg.removeClient(b); // twice is harmless
  assert.deepEqual(reg.stats(), { clients: 0, symbols: 0 });
});

test('per-client symbol limit', () => {
  const reg = new SubscriptionRegistry({ maxSymbolsPerClient: 2 });
  const a = {};
  assert.deepEqual(reg.subscribe(a, ['A', 'B', 'C']), ['A', 'B']);
  assert.equal(reg.symbolsOf(a).size, 2);
});

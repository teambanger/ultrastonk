'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { HistoryStore } = require('../src/history');

const ev = (ticker, ts, type = 'price') => ({ timestamp_emit: ts, ticker, type, value: ts });
const stamps = (h, t) => h.get(t).map((e) => e.timestamp_emit);

test('keeps only the last N events, oldest dropped', () => {
  const h = new HistoryStore(3);
  for (let ts = 1; ts <= 5; ts++) h.add(ev('X', ts));
  assert.deepEqual(stamps(h, 'X'), [3, 4, 5]);
});

test('ordered by timestamp_emit; late event is inserted in place', () => {
  const h = new HistoryStore(10);
  h.add(ev('X', 10));
  h.add(ev('X', 30));
  h.add(ev('X', 20));
  assert.deepEqual(stamps(h, 'X'), [10, 20, 30]);
});

test('no dedupe: equal timestamps are all kept, in arrival order', () => {
  const h = new HistoryStore(10);
  h.add(ev('X', 5, 'price'));
  h.add(ev('X', 5, 'EMA38'));
  h.add(ev('X', 5, 'EMA100'));
  h.add(ev('X', 5, 'EMA100'));
  assert.deepEqual(h.get('X').map((e) => e.type), ['price', 'EMA38', 'EMA100', 'EMA100']);
});

test('an event older than everything in a full buffer is ignored', () => {
  const h = new HistoryStore(2);
  h.add(ev('X', 5));
  h.add(ev('X', 6));
  h.add(ev('X', 1));
  assert.deepEqual(stamps(h, 'X'), [5, 6]);
});

test('tickers are independent; unknown ticker gives []; result is a copy', () => {
  const h = new HistoryStore(10);
  h.add(ev('X', 1));
  h.add(ev('Y', 2));
  assert.equal(h.tickerCount(), 2);
  assert.deepEqual(h.get('nope'), []);
  h.get('X').push('junk');
  assert.equal(h.get('X').length, 1);
});

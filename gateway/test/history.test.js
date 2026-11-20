'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { HistoryStore } = require('../src/history');

const ema = (symbol, window) => ({ type: 'ema', symbol, window, close: window });
const windows = (h, s) => h.snapshot(s).ema.map((m) => m.window);

test('keeps only the last N windows, oldest dropped', () => {
  const h = new HistoryStore(3);
  for (let w = 1; w <= 5; w++) h.addEma(ema('X', w));
  assert.deepEqual(windows(h, 'X'), [3, 4, 5]);
});

test('duplicate window replaces, out-of-order window is inserted in order', () => {
  const h = new HistoryStore(5);
  h.addEma(ema('X', 1));
  h.addEma(ema('X', 3));
  h.addEma(ema('X', 2));
  h.addEma({ ...ema('X', 3), close: 99 });
  assert.deepEqual(windows(h, 'X'), [1, 2, 3]);
  assert.equal(h.snapshot('X').ema[2].close, 99);
});

test('a window older than everything in a full buffer is ignored', () => {
  const h = new HistoryStore(2);
  h.addEma(ema('X', 5));
  h.addEma(ema('X', 6));
  h.addEma(ema('X', 1));
  assert.deepEqual(windows(h, 'X'), [5, 6]);
});

test('snapshot shape, symbols are independent, unknown symbol is empty', () => {
  const h = new HistoryStore(10);
  h.addEma(ema('X', 1));
  h.addAdvisory({ type: 'advisory', symbol: 'X', window: 1, signal: 'BUY' });
  h.addEma(ema('Y', 7));
  const snap = h.snapshot('X');
  assert.equal(snap.type, 'history');
  assert.equal(snap.symbol, 'X');
  assert.equal(snap.ema.length, 1);
  assert.equal(snap.advisories[0].signal, 'BUY');
  assert.deepEqual(h.snapshot('nope'), { type: 'history', symbol: 'nope', ema: [], advisories: [] });
});

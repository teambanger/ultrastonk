'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { FakeSource } = require('../src/fake');
const { parseEvent } = require('../src/protocol');

function run(ticks, opts = {}) {
  const out = [];
  let t = 0;
  const src = new FakeSource({ tickers: ['A.FR', 'B.NL'], now: () => (t += 1000), onMessage: (s) => out.push(s), ...opts });
  for (let i = 0; i < ticks; i++) src.tick();
  return out;
}

test('every fake message is a valid EventProcessed', () => {
  for (const raw of run(50)) assert.equal(parseEvent(raw).ok, true, raw);
});

test('emits price, EMA38, EMA100 per ticker per tick, and some BUY/SELL', () => {
  const events = run(2000).map((s) => JSON.parse(s));
  const count = (type) => events.filter((e) => e.type === type).length;
  assert.equal(count('price'), 4000);
  assert.equal(count('EMA38'), 4000);
  assert.equal(count('EMA100'), 4000);
  assert.ok(count('BUY') > 0, 'some BUY');
  assert.ok(count('SELL') > 0, 'some SELL');
  assert.ok(Math.abs(count('BUY') - count('SELL')) <= 2, 'BUY and SELL alternate per ticker');
});

test('extraTickers adds synthetic tickers for load tests', () => {
  const tickers = new Set(run(1, { extraTickers: 3 }).map((s) => JSON.parse(s).ticker));
  assert.deepEqual([...tickers], ['A.FR', 'B.NL', 'SYN00001.FAKE', 'SYN00002.FAKE', 'SYN00003.FAKE']);
});

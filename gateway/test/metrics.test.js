'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Metrics, CSV_HEADER, csvRow } = require('../src/metrics');

test('rates and latency percentiles per interval, then reset', () => {
  let t = 0;
  const m = new Metrics(() => t);
  for (let i = 1; i <= 100; i++) {
    m.inc('in');
    m.latency(i);
  }
  m.inc('forwarded', 300);
  t = 2000; // 2 seconds later
  const r = m.report();
  assert.equal(r.inPerSec, 50);
  assert.equal(r.forwardedPerSec, 150);
  assert.deepEqual(r.latencyMs, { min: 1, avg: 50.5, p50: 51, p95: 96, p99: 100, max: 100 });

  t = 3000;
  const r2 = m.report();
  assert.equal(r2.in, 0);
  assert.equal(r2.latencyMs.p50, null);
  assert.equal(m.totals.in, 100, 'totals survive the reset');
});

test('CSV row has one value per header column', () => {
  const m = new Metrics(() => 0);
  const row = csvRow(m.report(), { clients: 1, tickers: 2 }, new Date(0));
  assert.equal(row.split(',').length, CSV_HEADER.split(',').length);
});

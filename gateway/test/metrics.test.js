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
  const row = csvRow(m.report(), { clients: 1, tickers: 2, cpuPct: 5, rssMb: 50, heapMb: 20 }, new Date(0));
  assert.equal(row.split(',').length, CSV_HEADER.split(',').length);
});

test('latency sampling stays bounded and unbiased above the cap', () => {
  const m = new Metrics(() => 0);
  for (let i = 0; i < 400000; i++) m.latency(i < 200000 ? 0 : 1); // second half all 1s
  assert.equal(m.latencies.length, 200000);
  const ones = m.latencies.filter((x) => x === 1).length / m.latencies.length;
  assert.ok(ones > 0.45 && ones < 0.55, `about half the samples come from the second half (${ones})`);
});

test('processSampler reports CPU % and memory', () => {
  const { processSampler } = require('../src/metrics');
  const sample = processSampler();
  const s = sample();
  assert.ok(s.cpuPct >= 0 && s.rssMb > 0 && s.heapMb > 0);
});

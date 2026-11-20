'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { parseClientMessage, parseEvent } = require('../src/protocol');

test('parses a valid subscribe and removes duplicate tickers', () => {
  const r = parseClientMessage('{"type":"subscribe","tickers":["A2ASZ7.ETR","RDSA.NL","RDSA.NL"]}');
  assert.deepEqual(r, { ok: true, msg: { type: 'subscribe', tickers: ['A2ASZ7.ETR', 'RDSA.NL'] } });
});

test('accepts a Buffer (what the ws library hands us)', () => {
  const r = parseClientMessage(Buffer.from('{"type":"unsubscribe","tickers":["X.FR"]}'));
  assert.equal(r.ok, true);
  assert.equal(r.msg.type, 'unsubscribe');
});

test('rejects bad client messages with a reason', () => {
  const bad = [
    'not json',
    '[]',
    'null',
    '{"type":"hello","tickers":["A.FR"]}',
    '{"type":"subscribe"}',
    '{"type":"subscribe","symbols":["A.FR"]}', // old draft field name
    '{"type":"subscribe","tickers":[]}',
    '{"type":"subscribe","tickers":[42]}',
    '{"type":"subscribe","tickers":["<script>"]}',
    JSON.stringify({ type: 'subscribe', tickers: Array.from({ length: 501 }, (_, i) => `S${i}.FR`) }),
  ];
  for (const raw of bad) {
    const r = parseClientMessage(raw);
    assert.equal(r.ok, false, raw.slice(0, 50));
    assert.equal(typeof r.error, 'string');
  }
});

const ev = { timestamp_emit: 1636354800021, ticker: 'A2ASZ7.ETR', type: 'EMA38', value: 12.34 };

test('valid EventProcessed: returns the event and the original text unchanged', () => {
  const raw = JSON.stringify({ ...ev, extra: 1 });
  const r = parseEvent(Buffer.from(raw));
  assert.equal(r.ok, true);
  assert.equal(r.json, raw);
  assert.equal(r.event.value, 12.34);
});

test('type is opaque: any non-empty string passes', () => {
  for (const type of ['price', 'EMA38', 'EMA99', 'EMA100', 'BUY', 'SELL', 'something-new']) {
    assert.equal(parseEvent(JSON.stringify({ ...ev, type })).ok, true, type);
  }
});

test('malformed EventProcessed records are rejected', () => {
  const bad = [
    'nope',
    '[1]',
    { ...ev, timestamp_emit: '1636354800021' },
    { ...ev, timestamp_emit: 1.5 },
    { ...ev, ticker: '' },
    { ...ev, ticker: undefined },
    { ...ev, type: '' },
    { ...ev, type: 5 },
    { ...ev, value: '12.3' },
    { ...ev, value: null },
  ];
  for (const b of bad) {
    const raw = typeof b === 'string' ? b : JSON.stringify(b);
    assert.equal(parseEvent(raw).ok, false, raw);
  }
});

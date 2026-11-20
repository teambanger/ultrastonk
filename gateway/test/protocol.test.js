'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { parseClientMessage, toEmaMessage, toAdvisoryMessage } = require('../src/protocol');

test('parses a valid subscribe and removes duplicate symbols', () => {
  const r = parseClientMessage('{"type":"subscribe","symbols":["A2ASZ7.ETR","RDSA.NL","RDSA.NL"]}');
  assert.deepEqual(r, { ok: true, msg: { type: 'subscribe', symbols: ['A2ASZ7.ETR', 'RDSA.NL'] } });
});

test('accepts a Buffer (what the ws library hands us)', () => {
  const r = parseClientMessage(Buffer.from('{"type":"unsubscribe","symbols":["X.FR"]}'));
  assert.equal(r.ok, true);
  assert.equal(r.msg.type, 'unsubscribe');
});

test('rejects bad client messages with a reason', () => {
  const bad = [
    'not json',
    '[]',
    'null',
    '{"type":"hello","symbols":["A.FR"]}',
    '{"type":"subscribe"}',
    '{"type":"subscribe","symbols":[]}',
    '{"type":"subscribe","symbols":[42]}',
    '{"type":"subscribe","symbols":["<script>"]}',
    JSON.stringify({ type: 'subscribe', symbols: Array.from({ length: 501 }, (_, i) => `S${i}.FR`) }),
  ];
  for (const raw of bad) {
    const r = parseClientMessage(raw);
    assert.equal(r.ok, false, raw.slice(0, 50));
    assert.equal(typeof r.error, 'string');
  }
});

const ema = { symbol: 'A.FR', window: 12, windowStart: 1636329600000, close: 10.5, ema38: 10.1, ema100: 9.9, producedAt: 1700000000000 };

test('EMA record -> browser message', () => {
  assert.deepEqual(toEmaMessage({ ...ema, extra: 'ignored' }), { type: 'ema', ...ema });
});

test('EMA record without producedAt is still valid', () => {
  const { producedAt, ...rest } = ema;
  assert.deepEqual(toEmaMessage(rest), { type: 'ema', ...rest });
});

test('malformed EMA records are rejected', () => {
  assert.equal(toEmaMessage(null), null);
  assert.equal(toEmaMessage({ ...ema, close: 'x' }), null);
  assert.equal(toEmaMessage({ ...ema, window: 1.5 }), null);
  assert.equal(toEmaMessage({ ...ema, window: -1 }), null);
  assert.equal(toEmaMessage({ ...ema, symbol: undefined }), null);
});

test('advisory record validation', () => {
  const adv = { symbol: 'A.FR', window: 12, signal: 'BUY', ema38: 1, ema100: 0.9 };
  assert.deepEqual(toAdvisoryMessage(adv), { type: 'advisory', ...adv });
  assert.equal(toAdvisoryMessage({ ...adv, signal: 'HOLD' }), null);
  assert.equal(toAdvisoryMessage({ ...adv, ema38: NaN }), null);
});

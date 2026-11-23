'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

test('browser displays the window and matching EMA values for live and history advisories', () => {
  const html = fs.readFileSync(require.resolve('../public/index.html'), 'utf8');
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  const elements = Object.fromEntries(['url', 'tickers', 'sub', 'unsub', 'status', 'rows', 'signalRows', 'chart', 'chartTitle', 'log', 'windowSize'].map((id) => [id, {
    value: id === 'tickers' ? 'AAA.FR' : id === 'windowSize' ? '300000' : '',
    textContent: '', innerHTML: '', clientWidth: 500, clientHeight: 260,
    getContext: () => new Proxy({}, { get: () => () => {} }),
  }]));
  let socket;
  class WebSocket { constructor() { socket = this; this.readyState = 1; } send() {} }
  const context = { document: { getElementById: (id) => elements[id], documentElement: {} },
    window: { addEventListener() {} }, location: { search: '', protocol: 'http:', host: 'localhost' },
    WebSocket, URLSearchParams, requestAnimationFrame: (fn) => fn(), getComputedStyle: () => ({ getPropertyValue: () => '' }), setTimeout() {} };
  vm.runInNewContext(script, context);
  elements.sub.onclick();
  const event = (type, timestamp_emit, value) => ({ ticker: 'AAA.FR', type, timestamp_emit, value });
  const receive = (message) => socket.onmessage({ data: JSON.stringify(message) });
  receive({ type: 'history', ticker: 'AAA.FR', events: [event('EMA38', 301000, 12.34567), event('EMA100', 301000, 10.12345), event('BUY', 301000, 100)] });
  assert.match(elements.rows.innerHTML, /BUY/);
  assert.match(elements.rows.innerHTML, /12\.3457/);
  assert.match(elements.rows.innerHTML, /10\.1235/);
  assert.match(elements.rows.innerHTML, /1970-01-01T00:05:00\.000Z.*1970-01-01T00:10:00\.000Z/);
  receive(event('EMA38', 601000, 88));
  receive(event('EMA100', 601000, 99));
  assert.match(elements.rows.innerHTML, /BUY.*12\.3457.*10\.1235/);
  receive(event('SELL', 601000, 0));
  assert.match(elements.rows.innerHTML, /SELL.*88\.0000.*99\.0000/);
  assert.match(elements.signalRows.innerHTML, /BUY.*12\.3457.*10\.1235.*1970-01-01T00:05:00\.000Z/);
  assert.match(elements.signalRows.innerHTML, /SELL.*88\.0000.*99\.0000.*1970-01-01T00:10:00\.000Z/);
  receive({ ticker: 'BBB.FR', type: 'BUY', timestamp_emit: 601000, value: 100 });
  assert.doesNotMatch(elements.signalRows.innerHTML, /BBB\.FR/);
  assert.match(elements.rows.innerHTML, /1970-01-01T00:10:00\.000Z.*1970-01-01T00:15:00\.000Z/);
  receive(event('BUY', 901000, 50));
  assert.match(elements.rows.innerHTML, /BUY.*<td>–<\/td><td>–<\/td>/);
  receive(event('EMA100', 901000, 11));
  receive(event('EMA38', 901000, 22));
  assert.match(elements.rows.innerHTML, /BUY.*22\.0000.*11\.0000/);
  assert.match(elements.signalRows.innerHTML, /BUY.*22\.0000.*11\.0000.*1970-01-01T00:15:00\.000Z/);
  elements.windowSize.value = '600000';
  elements.windowSize.oninput();
  assert.match(elements.rows.innerHTML, /1970-01-01T00:10:00\.000Z.*1970-01-01T00:20:00\.000Z/);
  receive({ type: 'history', ticker: 'AAA.FR', events: [event('SELL', 1201000, 5)] });
  assert.match(elements.rows.innerHTML, /SELL.*<td>–<\/td><td>–<\/td>/);
  assert.doesNotMatch(elements.signalRows.innerHTML, /BUY/);
});

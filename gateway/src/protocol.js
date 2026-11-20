'use strict';

// Message formats and validation. This is the ONE place that knows the JSON
// shapes, so if the processor's output format changes, only this file changes.

const SYMBOL_RE = /^[A-Za-z0-9._-]{1,32}$/; // e.g. "A2ASZ7.ETR", "RDSA.NL"
const MAX_SYMBOLS_PER_REQUEST = 500;
const SIGNALS = new Set(['BUY', 'SELL']);

const isFiniteNumber = (v) => typeof v === 'number' && Number.isFinite(v);
const isWindow = (v) => Number.isSafeInteger(v) && v >= 0;

/**
 * Parse a message from a browser. Returns { ok: true, msg } or { ok: false, error }.
 * msg = { type: 'subscribe' | 'unsubscribe', symbols: string[] } (deduplicated).
 */
function parseClientMessage(raw) {
  let data;
  try {
    data = JSON.parse(typeof raw === 'string' ? raw : raw.toString('utf8'));
  } catch {
    return { ok: false, error: 'invalid JSON' };
  }
  if (data === null || typeof data !== 'object' || Array.isArray(data)) {
    return { ok: false, error: 'message must be a JSON object' };
  }
  if (data.type !== 'subscribe' && data.type !== 'unsubscribe') {
    return { ok: false, error: 'type must be "subscribe" or "unsubscribe"' };
  }
  if (!Array.isArray(data.symbols) || data.symbols.length === 0) {
    return { ok: false, error: 'symbols must be a non-empty array' };
  }
  if (data.symbols.length > MAX_SYMBOLS_PER_REQUEST) {
    return { ok: false, error: `at most ${MAX_SYMBOLS_PER_REQUEST} symbols per request` };
  }
  const symbols = [];
  for (const s of data.symbols) {
    if (typeof s !== 'string' || !SYMBOL_RE.test(s)) {
      return { ok: false, error: `invalid symbol: ${JSON.stringify(s).slice(0, 40)}` };
    }
    symbols.push(s);
  }
  return { ok: true, msg: { type: data.type, symbols: [...new Set(symbols)] } };
}

/**
 * Validate an EMA result coming from Kafka (topic ema-results) and return the
 * message we send to browsers, or null if the record is malformed.
 */
function toEmaMessage(data) {
  if (!data || typeof data !== 'object') return null;
  const { symbol, window, windowStart, close, ema38, ema100, producedAt } = data;
  if (typeof symbol !== 'string' || !SYMBOL_RE.test(symbol)) return null;
  if (!isWindow(window)) return null;
  if (![windowStart, close, ema38, ema100].every(isFiniteNumber)) return null;
  const msg = { type: 'ema', symbol, window, windowStart, close, ema38, ema100 };
  if (isFiniteNumber(producedAt)) msg.producedAt = producedAt;
  return msg;
}

/** Same as toEmaMessage, for the advisories topic. */
function toAdvisoryMessage(data) {
  if (!data || typeof data !== 'object') return null;
  const { symbol, window, signal, ema38, ema100, producedAt } = data;
  if (typeof symbol !== 'string' || !SYMBOL_RE.test(symbol)) return null;
  if (!isWindow(window)) return null;
  if (!SIGNALS.has(signal)) return null;
  if (![ema38, ema100].every(isFiniteNumber)) return null;
  const msg = { type: 'advisory', symbol, window, signal, ema38, ema100 };
  if (isFiniteNumber(producedAt)) msg.producedAt = producedAt;
  return msg;
}

module.exports = {
  parseClientMessage,
  toEmaMessage,
  toAdvisoryMessage,
  MAX_SYMBOLS_PER_REQUEST,
};

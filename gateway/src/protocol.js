'use strict';

// Message formats and validation. This is the ONE place that knows the JSON
// shapes, so if the team changes the format, only this file changes.
//
// Kafka -> gateway (EventProcessed, one message per value, from the Excalidraw board):
//   { "timestamp_emit": 1636354800021, "ticker": "A2ASZ7.ETR", "type": "EMA38", "value": 12.34 }
//   `type` is opaque: any non-empty string passes (price, EMA38, EMA100, BUY, SELL, ...).
//   `value` is required (finite number) for the numeric types below; for any other
//   type (BUY, SELL, ...) it may be missing or null, but if present it must be a number.
// Browser -> gateway:
//   { "type": "subscribe",   "tickers": ["A2ASZ7.ETR", ...] }
//   { "type": "unsubscribe", "tickers": [...] }
// Gateway -> browser:
//   EventProcessed messages, forwarded unchanged
//   { "type": "history", "ticker": "...", "events": [EventProcessed, ...] }  (on subscribe)
//   { "type": "error", "error": "..." }                                    (bad request)

const TICKER_RE = /^[A-Za-z0-9._-]{1,32}$/; // STOCK.EXCHANGE, e.g. "A2ASZ7.ETR"
const MAX_TYPE_LENGTH = 32;
const MAX_TICKERS_PER_REQUEST = 500;
const VALUE_REQUIRED_TYPES = new Set(['price', 'EMA38', 'EMA99', 'EMA100']);

const isTicker = (v) => typeof v === 'string' && TICKER_RE.test(v);

/**
 * Parse a message from a browser. Returns { ok: true, msg } or { ok: false, error }.
 * msg = { type: 'subscribe' | 'unsubscribe', tickers: string[] } (deduplicated).
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
  if (!Array.isArray(data.tickers) || data.tickers.length === 0) {
    return { ok: false, error: 'tickers must be a non-empty array' };
  }
  if (data.tickers.length > MAX_TICKERS_PER_REQUEST) {
    return { ok: false, error: `at most ${MAX_TICKERS_PER_REQUEST} tickers per request` };
  }
  for (const t of data.tickers) {
    if (!isTicker(t)) return { ok: false, error: `invalid ticker: ${String(JSON.stringify(t)).slice(0, 40)}` };
  }
  return { ok: true, msg: { type: data.type, tickers: [...new Set(data.tickers)] } };
}

/**
 * Validate an EventProcessed object. Returns null if valid, otherwise a reason string.
 */
function eventError(e) {
  if (e === null || typeof e !== 'object' || Array.isArray(e)) return 'not a JSON object';
  if (!Number.isSafeInteger(e.timestamp_emit) || e.timestamp_emit < 0) return 'bad timestamp_emit';
  if (!isTicker(e.ticker)) return 'bad ticker';
  if (typeof e.type !== 'string' || e.type.length === 0 || e.type.length > MAX_TYPE_LENGTH) return 'bad type';
  const hasValue = e.value !== undefined && e.value !== null;
  if (hasValue && (typeof e.value !== 'number' || !Number.isFinite(e.value))) return 'bad value';
  if (!hasValue && VALUE_REQUIRED_TYPES.has(e.type)) return `value required for type ${e.type}`;
  return null;
}

/**
 * Parse an EventProcessed record as it arrives from Kafka (string or Buffer).
 * Returns { ok: true, event, json } where `json` is the original text (forwarded
 * unchanged, no re-serialisation), or { ok: false, error }.
 */
function parseEvent(raw) {
  const json = typeof raw === 'string' ? raw : raw.toString('utf8');
  let event;
  try {
    event = JSON.parse(json);
  } catch {
    return { ok: false, error: 'invalid JSON' };
  }
  const error = eventError(event);
  return error ? { ok: false, error } : { ok: true, event, json };
}

const historyMessage = (ticker, events) => ({ type: 'history', ticker, events });
const errorMessage = (error) => ({ type: 'error', error });

module.exports = {
  parseClientMessage,
  parseEvent,
  eventError,
  historyMessage,
  errorMessage,
  MAX_TICKERS_PER_REQUEST,
};

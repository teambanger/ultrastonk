'use strict';

/**
 * Last N EMA results and last N advisories per symbol, kept sorted by window.
 * Sent to a client when it subscribes, so its chart is not empty.
 * A result for a window we already have replaces the old one (no duplicates),
 * which also makes the store safe if Kafka re-delivers messages.
 */
class HistoryStore {
  constructor(limit = 100) {
    this.limit = limit;
    this.ema = new Map(); // symbol -> array of messages, ascending window
    this.advisories = new Map();
  }

  addEma(msg) {
    insert(this.ema, msg, this.limit);
  }

  addAdvisory(msg) {
    insert(this.advisories, msg, this.limit);
  }

  /** The history message sent to a client on subscribe. */
  snapshot(symbol) {
    return {
      type: 'history',
      symbol,
      ema: [...(this.ema.get(symbol) || [])],
      advisories: [...(this.advisories.get(symbol) || [])],
    };
  }

  symbolCount() {
    return this.ema.size;
  }
}

function insert(map, msg, limit) {
  let list = map.get(msg.symbol);
  if (!list) {
    list = [];
    map.set(msg.symbol, list);
  }
  // Usual case: newest window arrives last, so scan from the end.
  let i = list.length;
  while (i > 0 && list[i - 1].window > msg.window) i--;
  if (i > 0 && list[i - 1].window === msg.window) {
    list[i - 1] = msg; // same window again: replace
    return;
  }
  if (list.length >= limit && i === 0) return; // older than everything we keep
  list.splice(i, 0, msg);
  if (list.length > limit) list.shift(); // drop the oldest
}

module.exports = { HistoryStore };

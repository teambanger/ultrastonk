'use strict';

/**
 * Last N events per ticker, ordered by timestamp_emit (oldest first).
 * Sent to a client when it subscribes, so its view is not empty.
 * No deduplication: one EventProcessed message = one entry, as on the board.
 */
class HistoryStore {
  constructor(limit = 500) {
    this.limit = limit;
    this.byTicker = new Map(); // ticker -> array of events
  }

  add(event) {
    let list = this.byTicker.get(event.ticker);
    if (!list) {
      list = [];
      this.byTicker.set(event.ticker, list);
    }
    // Usual case: newest arrives last, so scan from the end. Events with an
    // equal timestamp keep arrival order (price, EMA38, EMA100 of one window).
    let i = list.length;
    while (i > 0 && list[i - 1].timestamp_emit > event.timestamp_emit) i--;
    if (i === 0 && list.length >= this.limit) return; // older than everything kept
    list.splice(i, 0, event);
    if (list.length > this.limit) list.shift(); // drop the oldest
  }

  /** Copy of the stored events for a ticker (empty array if unknown). */
  get(ticker) {
    return [...(this.byTicker.get(ticker) || [])];
  }

  tickerCount() {
    return this.byTicker.size;
  }
}

module.exports = { HistoryStore };

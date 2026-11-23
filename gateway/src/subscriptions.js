'use strict';

/**
 * Who is subscribed to what. Two maps kept in sync:
 *   client -> Set<ticker>   (to clean up when a client disconnects)
 *   ticker -> Set<client>   (to find recipients fast when an event arrives)
 * A "client" is any object (in the server it is the WebSocket).
 */
class SubscriptionRegistry {
  constructor({ maxTickersPerClient = 1000 } = {}) {
    this.maxTickersPerClient = maxTickersPerClient;
    this.byClient = new Map();
    this.byTicker = new Map();
  }

  /** Returns the tickers that were newly added (already-subscribed ones are skipped). */
  subscribe(client, tickers) {
    let mine = this.byClient.get(client);
    if (!mine) {
      mine = new Set();
      this.byClient.set(client, mine);
    }
    const added = [];
    for (const ticker of tickers) {
      if (mine.has(ticker)) continue;
      if (mine.size >= this.maxTickersPerClient) break;
      mine.add(ticker);
      let clients = this.byTicker.get(ticker);
      if (!clients) {
        clients = new Set();
        this.byTicker.set(ticker, clients);
      }
      clients.add(client);
      added.push(ticker);
    }
    if (mine.size === 0) this.byClient.delete(client);
    return added;
  }

  /** Returns the tickers that were actually removed. */
  unsubscribe(client, tickers) {
    const mine = this.byClient.get(client);
    if (!mine) return [];
    const removed = [];
    for (const ticker of tickers) {
      if (!mine.delete(ticker)) continue;
      const clients = this.byTicker.get(ticker);
      clients.delete(client);
      if (clients.size === 0) this.byTicker.delete(ticker);
      removed.push(ticker);
    }
    if (mine.size === 0) this.byClient.delete(client);
    return removed;
  }

  /** Call on disconnect: drops every subscription of this client. */
  removeClient(client) {
    const mine = this.byClient.get(client);
    if (mine) this.unsubscribe(client, [...mine]);
  }

  /** Clients subscribed to a ticker (empty set if none). Do not modify the result. */
  subscribersOf(ticker) {
    return this.byTicker.get(ticker) || EMPTY;
  }

  tickersOf(client) {
    return this.byClient.get(client) || EMPTY;
  }

  stats() {
    return { clients: this.byClient.size, tickers: this.byTicker.size };
  }
}

const EMPTY = new Set();

module.exports = { SubscriptionRegistry };

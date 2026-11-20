'use strict';

/**
 * Who is subscribed to what. Two maps kept in sync:
 *   client -> Set<symbol>   (to clean up when a client disconnects)
 *   symbol -> Set<client>   (to find recipients fast when a result arrives)
 * A "client" is any object (in the server it is the WebSocket).
 */
class SubscriptionRegistry {
  constructor({ maxSymbolsPerClient = 1000 } = {}) {
    this.maxSymbolsPerClient = maxSymbolsPerClient;
    this.byClient = new Map();
    this.bySymbol = new Map();
  }

  /** Returns the symbols that were newly added (already-subscribed ones are skipped). */
  subscribe(client, symbols) {
    let mine = this.byClient.get(client);
    if (!mine) {
      mine = new Set();
      this.byClient.set(client, mine);
    }
    const added = [];
    for (const symbol of symbols) {
      if (mine.has(symbol)) continue;
      if (mine.size >= this.maxSymbolsPerClient) break;
      mine.add(symbol);
      let clients = this.bySymbol.get(symbol);
      if (!clients) {
        clients = new Set();
        this.bySymbol.set(symbol, clients);
      }
      clients.add(client);
      added.push(symbol);
    }
    return added;
  }

  /** Returns the symbols that were actually removed. */
  unsubscribe(client, symbols) {
    const mine = this.byClient.get(client);
    if (!mine) return [];
    const removed = [];
    for (const symbol of symbols) {
      if (!mine.delete(symbol)) continue;
      const clients = this.bySymbol.get(symbol);
      clients.delete(client);
      if (clients.size === 0) this.bySymbol.delete(symbol);
      removed.push(symbol);
    }
    if (mine.size === 0) this.byClient.delete(client);
    return removed;
  }

  /** Call on disconnect: drops every subscription of this client. */
  removeClient(client) {
    const mine = this.byClient.get(client);
    if (!mine) return;
    this.unsubscribe(client, [...mine]);
  }

  /** Clients subscribed to a symbol (empty set if none). Do not modify the result. */
  subscribersOf(symbol) {
    return this.bySymbol.get(symbol) || EMPTY;
  }

  symbolsOf(client) {
    return this.byClient.get(client) || EMPTY;
  }

  stats() {
    return { clients: this.byClient.size, symbols: this.bySymbol.size };
  }
}

const EMPTY = new Set();

module.exports = { SubscriptionRegistry };

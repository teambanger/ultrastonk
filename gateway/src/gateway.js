'use strict';

const { parseClientMessage, parseEvent, historyMessage, errorMessage } = require('./protocol');
const { silentLogger } = require('./logger');

const OPEN = 1; // WebSocket.OPEN

/**
 * The gateway's brain, independent of WebSocket and Kafka libraries so it can be
 * unit-tested with fake clients. A client only needs: readyState, bufferedAmount,
 * send(text), terminate().
 *
 *   source (Kafka/fake) --ingest(raw)--> history + forward to subscribers
 *   browser --onClientMessage--> subscribe (+ send history) / unsubscribe
 */
class Gateway {
  constructor({ registry, history, metrics, log = silentLogger, slowClientBytes = 1 << 20, killClientBytes = 8 << 20, now = Date.now }) {
    Object.assign(this, { registry, history, metrics, log, slowClientBytes, killClientBytes, now });
  }

  /** One raw EventProcessed record (string/Buffer) from Kafka or the fake source. */
  ingest(raw) {
    const r = parseEvent(raw);
    if (!r.ok) {
      this.metrics.inc('invalid');
      const n = this.metrics.totals.invalid;
      if (n <= 10 || n % 1000 === 0) {
        this.log.warn('invalid event dropped', { reason: r.error, total: n, sample: String(raw).slice(0, 120) });
      }
      return false;
    }
    const { event, json } = r;
    this.metrics.inc('in');
    this.history.add(event);
    this.metrics.latency(this.now() - event.timestamp_emit);
    for (const client of this.registry.subscribersOf(event.ticker)) this.send(client, json);
    return true;
  }

  onClientMessage(client, raw) {
    const r = parseClientMessage(raw);
    if (!r.ok) {
      this.log.debug('bad client message', { client: client.id, reason: r.error });
      this.send(client, JSON.stringify(errorMessage(r.error)));
      return;
    }
    const { type, tickers } = r.msg;
    if (type === 'subscribe') {
      const added = this.registry.subscribe(client, tickers);
      if (added.length < tickers.length && this.registry.tickersOf(client).size >= this.registry.maxTickersPerClient) {
        this.send(client, JSON.stringify(errorMessage(`ticker limit ${this.registry.maxTickersPerClient} reached`)));
      }
      for (const ticker of added) {
        this.send(client, JSON.stringify(historyMessage(ticker, this.history.get(ticker))));
      }
      this.log.debug('subscribe', { client: client.id, added });
    } else {
      const removed = this.registry.unsubscribe(client, tickers);
      this.log.debug('unsubscribe', { client: client.id, removed });
    }
  }

  onDisconnect(client) {
    this.registry.removeClient(client);
  }

  /**
   * Send with back-pressure: if the client's outgoing queue is too large
   * (slow network/tab in background) we skip the message instead of letting
   * memory grow; if it is far too large, we disconnect the client.
   */
  send(client, text) {
    if (client.readyState !== OPEN) return false;
    const queued = client.bufferedAmount;
    if (queued > this.killClientBytes) {
      this.log.warn('disconnecting slow client', { client: client.id, queuedBytes: queued });
      this.registry.removeClient(client);
      client.terminate();
      return false;
    }
    if (queued > this.slowClientBytes) {
      this.metrics.inc('dropped');
      return false;
    }
    client.send(text);
    this.metrics.inc('forwarded');
    return true;
  }
}

module.exports = { Gateway };

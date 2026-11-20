'use strict';

/**
 * Fake data source: emits made-up EventProcessed messages so the gateway and
 * the browser page work without Kafka or the stream processor.
 * Every tick (= one simulated 5-minute window), per ticker, it emits:
 *   price, EMA38, EMA100   and   BUY/SELL when EMA38 crosses EMA100.
 * EMA formula as in the spec: EMA_j = close*2/(1+j) + prev*(1-2/(1+j)).
 * (Here the EMAs start at the first price instead of 0, so the chart is
 * readable immediately; real data comes from the processor anyway.)
 */

// Small seeded random generator, so tests are repeatable.
function mulberry32(seed) {
  return function rand() {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const ema = (close, prev, j) => close * (2 / (1 + j)) + prev * (1 - 2 / (1 + j));

class FakeTicker {
  constructor(ticker, rand) {
    this.ticker = ticker;
    this.rand = rand;
    this.price = 20 + rand() * 180;
    this.ema38 = this.price;
    this.ema100 = this.price;
    this.drift = 0;
    this.ticksLeft = 0;
  }

  /** Advance one window; returns the EventProcessed objects for it. */
  next(timestamp) {
    // Trend "regimes" of random length make crossovers (BUY/SELL) happen now and then.
    if (this.ticksLeft-- <= 0) {
      this.drift = (this.rand() - 0.5) * 0.01;
      this.ticksLeft = 20 + Math.floor(this.rand() * 80);
    }
    const noise = (this.rand() - 0.5) * 0.01;
    this.price = Math.max(0.01, this.price * (1 + this.drift + noise));
    const prev38 = this.ema38;
    const prev100 = this.ema100;
    this.ema38 = ema(this.price, prev38, 38);
    this.ema100 = ema(this.price, prev100, 100);

    const r = (x) => Math.round(x * 10000) / 10000;
    const base = { timestamp_emit: timestamp, ticker: this.ticker };
    const out = [
      { ...base, type: 'price', value: r(this.price) },
      { ...base, type: 'EMA38', value: r(this.ema38) },
      { ...base, type: 'EMA100', value: r(this.ema100) },
    ];
    if (this.ema38 > this.ema100 && prev38 <= prev100) out.push({ ...base, type: 'BUY', value: r(this.price) });
    if (this.ema38 < this.ema100 && prev38 >= prev100) out.push({ ...base, type: 'SELL', value: r(this.price) });
    return out;
  }
}

class FakeSource {
  /** onMessage(text) receives JSON strings, exactly like Kafka record values. */
  constructor({ tickers, extraTickers = 0, intervalMs = 1000, seed = 42, now = Date.now, onMessage }) {
    const rand = mulberry32(seed);
    const all = [...tickers];
    for (let i = 1; i <= extraTickers; i++) all.push(`SYN${String(i).padStart(5, '0')}.FAKE`);
    this.tickers = all.map((t) => new FakeTicker(t, rand));
    Object.assign(this, { intervalMs, now, onMessage, timer: null });
  }

  tick() {
    const ts = this.now();
    for (const t of this.tickers) for (const e of t.next(ts)) this.onMessage(JSON.stringify(e));
  }

  start() {
    this.tick();
    this.timer = setInterval(() => this.tick(), this.intervalMs);
  }

  stop() {
    clearInterval(this.timer);
    this.timer = null;
  }
}

module.exports = { FakeSource, FakeTicker, mulberry32 };

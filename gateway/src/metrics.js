'use strict';

const MAX_SAMPLES = 200000; // latency samples kept per interval (bounded memory)

/**
 * Counters and latency samples. `report()` returns the numbers for the
 * interval since the previous report and starts a new interval.
 *   in        = valid events received from the source (Kafka or fake)
 *   invalid   = records rejected by validation
 *   forwarded = WebSocket messages sent to clients (one event can go to many)
 *   dropped   = messages skipped because a client was too slow
 *   latency   = now - timestamp_emit, measured once per event when it is forwarded
 */
class Metrics {
  constructor(now = Date.now) {
    this.now = now;
    this.totals = { in: 0, invalid: 0, forwarded: 0, dropped: 0 };
    this.startInterval();
  }

  startInterval() {
    this.intervalStart = this.now();
    this.counts = { in: 0, invalid: 0, forwarded: 0, dropped: 0 };
    this.latencies = [];
  }

  inc(name, n = 1) {
    this.counts[name] += n;
    this.totals[name] += n;
  }

  latency(ms) {
    if (this.latencies.length < MAX_SAMPLES) this.latencies.push(ms);
  }

  report() {
    const seconds = Math.max(this.now() - this.intervalStart, 1) / 1000;
    const lat = this.latencies.sort((a, b) => a - b);
    const pct = (p) => (lat.length ? lat[Math.min(lat.length - 1, Math.floor((p / 100) * lat.length))] : null);
    const r = {
      seconds: round(seconds),
      inPerSec: round(this.counts.in / seconds),
      forwardedPerSec: round(this.counts.forwarded / seconds),
      ...this.counts,
      latencySamples: lat.length,
      latencyMs: {
        min: lat.length ? lat[0] : null,
        avg: lat.length ? round(lat.reduce((s, x) => s + x, 0) / lat.length) : null,
        p50: pct(50),
        p95: pct(95),
        p99: pct(99),
        max: lat.length ? lat[lat.length - 1] : null,
      },
    };
    this.startInterval();
    return r;
  }
}

const round = (x) => Math.round(x * 10) / 10;

const CSV_HEADER =
  'time,seconds,in_per_sec,forwarded_per_sec,in,invalid,forwarded,dropped,clients,tickers,lat_samples,lat_min,lat_avg,lat_p50,lat_p95,lat_p99,lat_max\n';

function csvRow(r, extra, time = new Date()) {
  const l = r.latencyMs;
  return [time.toISOString(), r.seconds, r.inPerSec, r.forwardedPerSec, r.in, r.invalid, r.forwarded, r.dropped,
    extra.clients, extra.tickers, r.latencySamples, l.min, l.avg, l.p50, l.p95, l.p99, l.max]
    .map((v) => (v === null ? '' : v)).join(',') + '\n';
}

module.exports = { Metrics, CSV_HEADER, csvRow };

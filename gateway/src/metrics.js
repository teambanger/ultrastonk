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
    this.latencySeen = 0;
  }

  inc(name, n = 1) {
    this.counts[name] += n;
    this.totals[name] += n;
  }

  /** Reservoir sampling: above MAX_SAMPLES every event still has an equal chance to be kept. */
  latency(ms) {
    this.latencySeen++;
    if (this.latencies.length < MAX_SAMPLES) {
      this.latencies.push(ms);
    } else {
      const j = Math.floor(Math.random() * this.latencySeen);
      if (j < MAX_SAMPLES) this.latencies[j] = ms;
    }
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
  'time,seconds,in_per_sec,forwarded_per_sec,in,invalid,forwarded,dropped,clients,tickers,lat_samples,lat_min,lat_avg,lat_p50,lat_p95,lat_p99,lat_max,cpu_pct,rss_mb,heap_mb\n';

/** extra = { clients, tickers, cpuPct, rssMb, heapMb } */
function csvRow(r, extra, time = new Date()) {
  const l = r.latencyMs;
  return [time.toISOString(), r.seconds, r.inPerSec, r.forwardedPerSec, r.in, r.invalid, r.forwarded, r.dropped,
    extra.clients, extra.tickers, r.latencySamples, l.min, l.avg, l.p50, l.p95, l.p99, l.max,
    extra.cpuPct, extra.rssMb, extra.heapMb]
    .map((v) => (v === null || v === undefined ? '' : v)).join(',') + '\n';
}

/**
 * CPU use of this process since the previous call, in % of one core
 * (can exceed 100 when libuv threads work in parallel), plus memory in MB.
 */
function processSampler(now = Date.now) {
  let lastCpu = process.cpuUsage();
  let lastTime = now();
  return () => {
    const cpu = process.cpuUsage(lastCpu);
    const t = now();
    const wallMs = Math.max(t - lastTime, 1);
    lastCpu = process.cpuUsage();
    lastTime = t;
    const mem = process.memoryUsage();
    return {
      cpuPct: round(((cpu.user + cpu.system) / 1000 / wallMs) * 100),
      rssMb: round(mem.rss / 1048576),
      heapMb: round(mem.heapUsed / 1048576),
    };
  };
}

module.exports = { Metrics, CSV_HEADER, csvRow, processSampler };

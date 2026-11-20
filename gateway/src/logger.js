'use strict';

// Minimal timestamped logger: "2026-11-20T15:04:05.123Z INFO message key=value".
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };

function createLogger(level = 'info', out = console) {
  const min = LEVELS[level] ?? LEVELS.info;
  const line = (lvl, msg, fields) => {
    const extra = fields
      ? ' ' + Object.entries(fields).map(([k, v]) => `${k}=${typeof v === 'string' ? v : JSON.stringify(v)}`).join(' ')
      : '';
    return `${new Date().toISOString()} ${lvl.toUpperCase()} ${msg}${extra}`;
  };
  const make = (lvl, fn) => (msg, fields) => {
    if (LEVELS[lvl] >= min) fn.call(out, line(lvl, msg, fields));
  };
  return {
    debug: make('debug', out.log),
    info: make('info', out.log),
    warn: make('warn', out.warn),
    error: make('error', out.error),
  };
}

const silentLogger = createLogger('silent');

module.exports = { createLogger, silentLogger };

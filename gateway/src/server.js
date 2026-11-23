'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');
const { silentLogger } = require('./logger');

const PAGE = path.join(__dirname, '..', 'public', 'index.html');
const MAX_CLIENT_MESSAGE_BYTES = 64 * 1024;

/**
 * One HTTP server on one port:
 *   GET /        -> test page (public/index.html)
 *   GET /health  -> JSON status
 *   WebSocket    -> ws://host:PORT/  (any path)
 */
function createServer({ gateway, maxClients = 1000, heartbeatMs = 30000, log = silentLogger, sourceStatus = () => null }) {
  let nextId = 1;

  const httpServer = http.createServer((req, res) => {
    const url = (req.url || '/').split('?')[0];
    if (req.method === 'GET' && url === '/health') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        uptimeSec: Math.round(process.uptime()),
        source: sourceStatus(),
        ...gateway.registry.stats(),
        historyTickers: gateway.history.tickerCount(),
        totals: gateway.metrics.totals,
        memoryMb: Math.round(process.memoryUsage().rss / 1048576),
      }));
    } else if (req.method === 'GET' && (url === '/' || url === '/index.html')) {
      fs.readFile(PAGE, (err, body) => {
        if (err) {
          res.writeHead(500).end('test page missing');
          return;
        }
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        res.end(body);
      });
    } else {
      res.writeHead(404, { 'content-type': 'text/plain' }).end('not found');
    }
  });

  const wss = new WebSocketServer({ server: httpServer, maxPayload: MAX_CLIENT_MESSAGE_BYTES });

  wss.on('connection', (ws, req) => {
    ws.id = nextId++;
    if (wss.clients.size > maxClients) {
      log.warn('too many clients, rejecting', { client: ws.id });
      ws.close(1013, 'server full');
      return;
    }
    ws.isAlive = true;
    log.info('client connected', { client: ws.id, ip: req.socket.remoteAddress, clients: wss.clients.size });

    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        gateway.send(ws, JSON.stringify({ type: 'error', error: 'binary messages not supported' }));
        return;
      }
      gateway.onClientMessage(ws, data);
    });
    ws.on('close', (code) => {
      gateway.onDisconnect(ws);
      log.info('client disconnected', { client: ws.id, code, clients: wss.clients.size });
    });
    ws.on('error', (err) => log.warn('client socket error', { client: ws.id, error: err.message }));
  });

  // Heartbeat: ping every client; one that did not answer the previous ping is dead.
  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) {
        log.info('terminating unresponsive client', { client: ws.id });
        gateway.onDisconnect(ws);
        ws.terminate();
        continue;
      }
      ws.isAlive = false;
      ws.ping();
    }
  }, heartbeatMs);
  heartbeat.unref();

  return {
    httpServer,
    wss,
    listen(port) {
      return new Promise((resolve, reject) => {
        httpServer.once('error', reject);
        httpServer.listen(port, () => resolve(httpServer.address().port));
      });
    },
    close() {
      clearInterval(heartbeat);
      for (const ws of wss.clients) ws.terminate();
      return new Promise((resolve) => wss.close(() => httpServer.close(() => resolve())));
    },
  };
}

module.exports = { createServer };

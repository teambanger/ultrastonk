'use strict';

const { Kafka, logLevel } = require('kafkajs');
const { silentLogger } = require('./logger');

// kafkajs on Node >= 23 emits a harmless TimeoutNegativeWarning from its
// request queue (Node treats the negative delay as 1 ms). Hide only that one.
let warningFilterInstalled = false;
function hideKafkajsTimeoutWarning() {
  if (warningFilterInstalled) return;
  warningFilterInstalled = true;
  const listeners = process.listeners('warning');
  process.removeAllListeners('warning');
  process.on('warning', (w) => {
    if (w.name === 'TimeoutNegativeWarning') return;
    if (listeners.length) listeners.forEach((l) => l(w));
    else console.warn(`${w.name}: ${w.message}`);
  });
}

// Route kafkajs' own log lines into our logger.
function kafkajsLogCreator(log) {
  return () => ({ level, log: entry }) => {
    const { message, timestamp, logger, ...fields } = entry;
    const text = `kafkajs: ${message}`;
    if (level === logLevel.ERROR) log.warn(text, fields); // kafkajs retries most errors itself
    else if (level === logLevel.WARN) log.warn(text, fields);
    else log.debug(text, fields);
  };
}

/**
 * Kafka data source: consumes the EventProcessed topic in its own consumer group
 * and hands each record value (a Buffer) to onMessage. Keeps retrying while
 * Kafka is unreachable, so the WebSocket side stays up meanwhile.
 *
 * status: 'stopped' | 'connecting' | 'running'
 */
class KafkaSource {
  constructor({ brokers, topic, groupId, clientId, fromBeginning = false, retryDelayMs = 5000, log = silentLogger, onMessage, kafka }) {
    Object.assign(this, { brokers, topic, groupId, fromBeginning, retryDelayMs, log, onMessage });
    hideKafkajsTimeoutWarning();
    this.kafka = kafka || new Kafka({
      clientId,
      brokers,
      logLevel: logLevel.WARN,
      logCreator: kafkajsLogCreator(log),
    });
    this.status = 'stopped';
    this.consumer = null;
    this.stopping = false;
  }

  /** Resolves once consuming has started (retries until then, unless stop() is called). */
  async start() {
    this.stopping = false;
    this.status = 'connecting';
    for (let attempt = 1; !this.stopping; attempt++) {
      try {
        await this.connectAndRun();
        this.status = 'running';
        this.log.info('kafka consumer running', { topic: this.topic, group: this.groupId, brokers: this.brokers.join(',') });
        return;
      } catch (err) {
        this.log.warn('kafka not reachable, retrying', { attempt, error: err.message, retryInMs: this.retryDelayMs });
        await this.safeDisconnect();
        await new Promise((r) => {
          this.wakeUp = r; // stop() calls this so the wait ends immediately
          this.retryTimer = setTimeout(r, this.retryDelayMs);
        });
      }
    }
  }

  async connectAndRun() {
    // Flink publishes transactionally; do not forward pending or aborted output.
    const consumer = this.kafka.consumer({ groupId: this.groupId, readUncommitted: false });
    this.consumer = consumer;
    const { CRASH, GROUP_JOIN } = consumer.events;
    consumer.on(GROUP_JOIN, (e) => this.log.info('kafka group joined', { group: this.groupId, partitions: countPartitions(e.payload.memberAssignment) }));
    consumer.on(CRASH, (e) => {
      // kafkajs restarts the consumer itself for retriable errors (restartOnFailure).
      this.log.error('kafka consumer crashed', { error: e.payload.error && e.payload.error.message, restart: e.payload.restart });
      if (!e.payload.restart && !this.stopping) {
        this.status = 'connecting';
        this.safeDisconnect().then(() => this.start());
      }
    });
    await consumer.connect();
    await consumer.subscribe({ topics: [this.topic], fromBeginning: this.fromBeginning });
    await consumer.run({
      // Messages of one partition arrive in order; one ticker is always on one partition.
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        for (const message of batch.messages) {
          if (!isRunning() || isStale()) break;
          if (message.value) this.onMessage(message.value);
          resolveOffset(message.offset);
        }
        await heartbeat();
      },
    });
  }

  async safeDisconnect() {
    const c = this.consumer;
    this.consumer = null;
    if (c) await c.disconnect().catch(() => {});
  }

  async stop() {
    this.stopping = true;
    clearTimeout(this.retryTimer);
    if (this.wakeUp) this.wakeUp();
    await this.safeDisconnect();
    this.status = 'stopped';
  }
}

function countPartitions(assignment) {
  return Object.values(assignment || {}).reduce((n, parts) => n + parts.length, 0);
}

module.exports = { KafkaSource, hideKafkajsTimeoutWarning };

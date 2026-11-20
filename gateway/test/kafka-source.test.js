'use strict';
// KafkaSource with a stand-in for the kafkajs client (no broker needed).
const test = require('node:test');
const assert = require('node:assert/strict');
const { KafkaSource } = require('../src/kafka-source');

function fakeKafka({ failConnects = 0 } = {}) {
  const state = { connects: 0, subscribed: null, disconnects: 0, eachBatch: null };
  return {
    state,
    consumer: ({ groupId }) => ({
      events: { CRASH: 'crash', GROUP_JOIN: 'join' },
      on() {},
      async connect() {
        state.connects++;
        if (state.connects <= failConnects) throw new Error('ECONNREFUSED');
        state.groupId = groupId;
      },
      async subscribe(opts) { state.subscribed = opts; },
      async run({ eachBatch }) { state.eachBatch = eachBatch; },
      async disconnect() { state.disconnects++; },
    }),
  };
}

const batchOf = (values) => ({
  batch: { messages: values.map((v, i) => ({ offset: String(i), value: v === null ? null : Buffer.from(v) })) },
  resolveOffset() {},
  heartbeat: async () => {},
  isRunning: () => true,
  isStale: () => false,
});

test('subscribes to the topic in its group and passes record values on', async () => {
  const kafka = fakeKafka();
  const got = [];
  const src = new KafkaSource({ kafka, topic: 'event-processed', groupId: 'g1', brokers: ['x:1'], onMessage: (v) => got.push(v.toString()) });
  await src.start();
  assert.equal(src.status, 'running');
  assert.equal(kafka.state.groupId, 'g1');
  assert.deepEqual(kafka.state.subscribed, { topics: ['event-processed'], fromBeginning: false });

  await kafka.state.eachBatch(batchOf(['{"a":1}', null, '{"b":2}'])); // null = tombstone, skipped
  assert.deepEqual(got, ['{"a":1}', '{"b":2}']);

  await src.stop();
  assert.equal(src.status, 'stopped');
});

test('keeps retrying while Kafka is unreachable', async () => {
  const kafka = fakeKafka({ failConnects: 2 });
  const src = new KafkaSource({ kafka, topic: 't', groupId: 'g', brokers: ['x:1'], retryDelayMs: 5, onMessage() {} });
  await src.start();
  assert.equal(kafka.state.connects, 3);
  assert.equal(src.status, 'running');
  await src.stop();
});

test('stop() during retries ends the retry loop', async () => {
  const kafka = fakeKafka({ failConnects: Infinity });
  const src = new KafkaSource({ kafka, topic: 't', groupId: 'g', brokers: ['x:1'], retryDelayMs: 20, onMessage() {} });
  const started = src.start();
  await new Promise((r) => setTimeout(r, 50));
  await src.stop();
  await started; // must resolve, not hang
  assert.equal(src.status, 'stopped');
});

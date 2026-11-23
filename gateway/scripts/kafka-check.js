'use strict';
// Does kafkajs work against our Kafka 4.0 broker? Runs every operation the
// gateway needs and prints PASS/FAIL per step:
//   connect (admin) -> list/create topic -> produce -> consume in a consumer group
// Usage: node scripts/kafka-check.js      (env: KAFKA_BROKERS, KAFKA_TOPIC)
const { Kafka, logLevel, Partitioners } = require('kafkajs');
const { hideKafkajsTimeoutWarning } = require('../src/kafka-source');

hideKafkajsTimeoutWarning();

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const topic = process.env.KAFKA_TOPIC || 'events-processed';
const kafka = new Kafka({ clientId: 'gateway-kafka-check', brokers, logLevel: logLevel.ERROR, retry: { retries: 3 } });

async function step(name, fn) {
  const t0 = Date.now();
  try {
    const out = await fn();
    console.log(`PASS  ${name} (${Date.now() - t0} ms)${out ? ` -> ${out}` : ''}`);
  } catch (err) {
    console.log(`FAIL  ${name}: ${err.name}: ${err.message}`);
    throw err;
  }
}

async function main() {
  console.log(`kafkajs check against ${brokers.join(',')}, topic "${topic}"`);
  const admin = kafka.admin();
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  const consumer = kafka.consumer({ groupId: `gateway-kafka-check-${Date.now()}` });
  try {
    await step('admin connect', () => admin.connect());
    await step('describe cluster', async () => {
      const c = await admin.describeCluster();
      return `${c.brokers.length} broker(s), controller ${c.controller}`;
    });
    await step(`ensure topic ${topic}`, async () => {
      const created = await admin.createTopics({ topics: [{ topic, numPartitions: 8 }], waitForLeaders: true });
      return created ? 'created with 8 partitions' : 'already exists';
    });

    const marker = `kafka-check-${Date.now()}`;
    await step('producer connect + send (keyed)', async () => {
      await producer.connect();
      const value = JSON.stringify({ timestamp_emit: Date.now(), ticker: 'CHECK.TEST', type: marker, value: 1 });
      const [r] = await producer.send({ topic, messages: [{ key: 'CHECK.TEST', value }] });
      return `partition ${r.partition}, offset ${r.baseOffset}`;
    });

    await step('consumer group join + receive', async () => {
      await consumer.connect();
      await consumer.subscribe({ topics: [topic], fromBeginning: true });
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('message not received within 30 s')), 30000);
        consumer.run({
          eachMessage: async ({ message }) => {
            if (message.value.toString().includes(marker)) {
              clearTimeout(timer);
              resolve();
            }
          },
        }).catch(reject);
      });
      return 'got our message back';
    });
    console.log('\nRESULT: kafkajs WORKS with this broker');
  } catch {
    console.log('\nRESULT: kafkajs FAILED (see the step above)');
    process.exitCode = 1;
  } finally {
    await Promise.allSettled([consumer.disconnect(), producer.disconnect(), admin.disconnect()]);
  }
}

main();

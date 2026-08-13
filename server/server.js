'use strict';
require('dotenv').config();

const http    = require('http');
const path    = require('path');
const express = require('express');
const { Server } = require('socket.io');
const { Kafka, logLevel } = require('kafkajs');
const { SchemaRegistryClient, AvroDeserializer, SerdeType } = require('@confluentinc/schemaregistry');

// ─── Validate environment ──────────────────────────────────────────────────────
const REQUIRED_ENV = [
  'BOOTSTRAP_SERVERS', 'CONSUMER_API_KEY', 'CONSUMER_API_SECRET',
  'SCHEMA_REGISTRY_URL', 'SCHEMA_REGISTRY_API_KEY', 'SCHEMA_REGISTRY_API_SECRET',
];
REQUIRED_ENV.forEach((key) => {
  if (!process.env[key]) {
    console.error(`[server] Missing required env var: ${key}`);
    process.exit(1);
  }
});

const PORT = parseInt(process.env.PORT || '3000', 10);

// ─── Schema Registry client ────────────────────────────────────────────────────
const registry = new SchemaRegistryClient({
  baseURLs: [process.env.SCHEMA_REGISTRY_URL],
  basicAuthCredentials: {
    credentialsSource: 'USER_INFO',
    userInfo: `${process.env.SCHEMA_REGISTRY_API_KEY}:${process.env.SCHEMA_REGISTRY_API_SECRET}`,
  },
});
const avroDeserializer = new AvroDeserializer(registry, SerdeType.VALUE, {});

// ─── Express + Socket.io setup ────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const io     = new Server(server);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Kafka consumer factory ────────────────────────────────────────────────────
function makeKafka(clientId) {
  return new Kafka({
    clientId,
    brokers: [process.env.BOOTSTRAP_SERVERS],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: process.env.CONSUMER_API_KEY,
      password: process.env.CONSUMER_API_SECRET,
    },
    logLevel: logLevel.WARN,
  });
}

// ─── Decode a Kafka message value (Avro if magic byte present, JSON fallback) ──
async function decode(topic, value) {
  if (!value) return null;
  // Confluent Avro wire format starts with magic byte 0x00
  if (value[0] === 0) {
    return avroDeserializer.deserialize(topic, value);
  }
  return JSON.parse(value.toString());
}

// ─── Consumer: cell-tower-telemetry ───────────────────────────────────────────
async function startTelemetryConsumer() {
  const consumer = makeKafka('celltower-telemetry-consumer')
    .consumer({ groupId: 'dashboard-telemetry-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'cell-tower-telemetry', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const payload = await decode(topic, message.value);
        if (payload) io.emit('telemetry', payload);
      } catch (_) { /* malformed message — skip */ }
    },
  });

  return consumer;
}

// ─── Consumer: anomaly-alerts ─────────────────────────────────────────────────
async function startAnomalyConsumer() {
  const consumer = makeKafka('celltower-anomaly-consumer')
    .consumer({ groupId: 'dashboard-anomaly-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'anomaly-alerts', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const payload = await decode(topic, message.value);
        if (payload) {
          io.emit('anomaly', payload);
          console.log(`[server] 🚨 Anomaly received for ${payload.tower_id}`);
        }
      } catch (_) { /* malformed message — skip */ }
    },
  });

  return consumer;
}

// ─── Socket.io connection log ─────────────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`[server] Browser connected: ${socket.id}`);
  socket.on('disconnect', () => {
    console.log(`[server] Browser disconnected: ${socket.id}`);
  });
});

// ─── Start everything ─────────────────────────────────────────────────────────
(async () => {
  const [telemetryConsumer, anomalyConsumer] = await Promise.all([
    startTelemetryConsumer(),
    startAnomalyConsumer(),
  ]);

  server.listen(PORT, () => {
    console.log(`[server] Dashboard running → http://localhost:${PORT}`);
  });

  const shutdown = async () => {
    console.log('[server] Shutting down...');
    const forceExit = setTimeout(() => {
      console.error('[server] Force exit after timeout.');
      process.exit(1);
    }, 5000).unref();
    try {
      await Promise.all([
        telemetryConsumer.disconnect(),
        anomalyConsumer.disconnect(),
      ]);
    } catch (err) {
      console.error('[server] Error during disconnect:', err.message);
    }
    clearTimeout(forceExit);
    io.close(() => server.close(() => process.exit(0)));
  };
  process.on('SIGINT',  shutdown);
  process.on('SIGTERM', shutdown);
})().catch((err) => {
  console.error('[server] Fatal error:', err);
  process.exit(1);
});

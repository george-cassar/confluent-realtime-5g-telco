'use strict';
require('dotenv').config();

const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { SchemaRegistryClient, AvroSerializer, SerdeType } = require('@confluentinc/schemaregistry');

// ─── Validate required environment variables ───────────────────────────────────
const REQUIRED_ENV = [
  'BOOTSTRAP_SERVERS', 'PRODUCER_API_KEY', 'PRODUCER_API_SECRET',
  'SCHEMA_REGISTRY_URL', 'SCHEMA_REGISTRY_API_KEY', 'SCHEMA_REGISTRY_API_SECRET',
];
REQUIRED_ENV.forEach((key) => {
  if (!process.env[key]) {
    console.error(`[generator] Missing required env var: ${key}`);
    process.exit(1);
  }
});

// ─── Avro schema — must match the version already registered in Schema Registry ─
const TELEMETRY_SCHEMA = {
  type: 'record',
  name: 'CellTowerTelemetry',
  namespace: 'com.celltower',
  doc: 'Raw telemetry reading emitted by a 5G cell tower every 2 seconds.',
  fields: [
    { name: 'tower_id',        type: 'string', doc: 'Unique tower identifier, e.g. TWR-001' },
    { name: 'tower_name',      type: 'string', doc: 'Human-readable tower location name, e.g. Retiro' },
    { name: 'latitude',        type: 'double', doc: 'WGS-84 latitude of the tower' },
    { name: 'longitude',       type: 'double', doc: 'WGS-84 longitude of the tower' },
    { name: 'temperature',     type: 'double', doc: 'Tower hardware temperature in degrees Celsius. Anomaly threshold: > 85.0 °C' },
    { name: 'signal_strength', type: 'double', doc: 'Downlink signal strength in dBm. Anomaly threshold: < -90.0 dBm' },
    { name: 'cpu_load',        type: 'double', doc: 'CPU utilisation percentage (0–100)' },
    { name: 'event_time',      type: { type: 'long', logicalType: 'timestamp-millis' }, doc: 'Unix epoch milliseconds UTC of the reading. Avro timestamp-millis maps to Flink TIMESTAMP_LTZ(3), required for TUMBLE() windowing.' },
    { name: 'is_anomaly',      type: 'boolean', doc: 'True when the generator deliberately produced an anomalous reading' },
  ],
};

// ─── Schema Registry client ────────────────────────────────────────────────────
const schemaRegistryClient = new SchemaRegistryClient({
  baseURLs: [process.env.SCHEMA_REGISTRY_URL],
  basicAuthCredentials: {
    credentialsSource: 'USER_INFO',
    userInfo: `${process.env.SCHEMA_REGISTRY_API_KEY}:${process.env.SCHEMA_REGISTRY_API_SECRET}`,
  },
});

const serializer = new AvroSerializer(schemaRegistryClient, SerdeType.VALUE, {
  useLatestVersion: true,
});

// ─── Kafka client ──────────────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: 'celltower-producer',
  brokers: [process.env.BOOTSTRAP_SERVERS],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.PRODUCER_API_KEY,
    password: process.env.PRODUCER_API_SECRET,
  },
  logLevel: logLevel.WARN,
  requestTimeout: 30000,
  connectionTimeout: 10000,
});

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

// ─── Tower definitions — Madrid, Spain ────────────────────────────────────────
const TOWERS = [
  { id: 'TWR-001', name: 'Retiro',          lat: 40.4153, lon: -3.6844 },
  { id: 'TWR-002', name: 'Salamanca',       lat: 40.4308, lon: -3.6800 },
  { id: 'TWR-003', name: 'Chamberí',        lat: 40.4390, lon: -3.7014 },
  { id: 'TWR-004', name: 'Moncloa',         lat: 40.4356, lon: -3.7194 },
  { id: 'TWR-005', name: 'Vallecas',        lat: 40.3895, lon: -3.6513 },
  // Tetuán cluster (40.4595, -3.7007) and nearby
  { id: 'TWR-006', name: 'Tetuán Centro',   lat: 40.4608, lon: -3.7012 },
  { id: 'TWR-007', name: 'Tetuán Norte',    lat: 40.4671, lon: -3.6981 },
  { id: 'TWR-008', name: 'Cuatro Caminos',  lat: 40.4498, lon: -3.7034 },
  // Additional Madrid coverage
  { id: 'TWR-009', name: 'Hortaleza',       lat: 40.4763, lon: -3.6421 },
  { id: 'TWR-010', name: 'Carabanchel',     lat: 40.3863, lon: -3.7363 },
  // Extended coverage — south, east, and airport corridor
  { id: 'TWR-011', name: 'Arganzuela',      lat: 40.3990, lon: -3.6974 },
  { id: 'TWR-012', name: 'Usera',           lat: 40.3878, lon: -3.7112 },
  { id: 'TWR-013', name: 'Villaverde',      lat: 40.3494, lon: -3.7054 },
  { id: 'TWR-014', name: 'Moratalaz',       lat: 40.4050, lon: -3.6412 },
  { id: 'TWR-015', name: 'Barajas',         lat: 40.4786, lon: -3.5830 },
  { id: 'TWR-016', name: 'La Latina',       lat: 40.4124, lon: -3.7118 },
];

const TOPIC        = 'cell-tower-telemetry';
const INTERVAL_MS  = 2000;

const rand = (min, max) => Math.random() * (max - min) + min;

function generateReading(tower) {
  const ANOMALY_TOWERS = new Set(['TWR-002', 'TWR-004']);
  const isAnomaly = ANOMALY_TOWERS.has(tower.id) && Math.random() < 0.20;
  return {
    tower_id:        tower.id,
    tower_name:      tower.name,
    latitude:        tower.lat,
    longitude:       tower.lon,
    temperature:     isAnomaly ? rand(88, 95)    : rand(60, 84),
    signal_strength: isAnomaly ? rand(-100, -91) : rand(-89, -65),
    cpu_load:        isAnomaly ? rand(85, 99)    : rand(20, 75),
    event_time:      Date.now(),
    is_anomaly:      isAnomaly,
  };
}

// ─── ANSI colour helpers ───────────────────────────────────────────────────────
const ANSI = {
  reset:  '\x1b[0m',
  bold:   '\x1b[1m',
  red:    '\x1b[31m',
  yellow: '\x1b[33m',
  cyan:   '\x1b[36m',
  gray:   '\x1b[90m',
};

function printBatchTable(payloads) {
  const cols = {
    id:      8,
    name:    16,
    temp:     9,
    signal:  10,
    cpu:      7,
    anomaly:  9,
  };

  const header =
    `${ANSI.bold}${ANSI.cyan}` +
    'TOWER-ID'.padEnd(cols.id)  + '  ' +
    'NAME'.padEnd(cols.name)    + '  ' +
    'TEMP(°C)'.padStart(cols.temp)  + '  ' +
    'SIGNAL(dBm)'.padStart(cols.signal) + '  ' +
    'CPU(%)'.padStart(cols.cpu)  + '  ' +
    'ANOMALY'.padEnd(cols.anomaly) +
    ANSI.reset;

  const separator = ANSI.gray + '─'.repeat(
    cols.id + cols.name + cols.temp + cols.signal + cols.cpu + cols.anomaly + 10,
  ) + ANSI.reset;

  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  console.log(`\n${ANSI.bold}${ANSI.gray}[generator] Batch @ ${ts}${ANSI.reset}`);
  console.log(separator);
  console.log(header);
  console.log(separator);

  for (const p of payloads) {
    const colour = p.is_anomaly ? ANSI.red : '';
    const flag   = p.is_anomaly ? `${ANSI.bold}${ANSI.red}⚠ YES${ANSI.reset}${colour}` : 'no';
    const row =
      colour +
      p.tower_id.padEnd(cols.id)                           + '  ' +
      p.tower_name.padEnd(cols.name).slice(0, cols.name)   + '  ' +
      p.temperature.toFixed(1).padStart(cols.temp)         + '  ' +
      p.signal_strength.toFixed(1).padStart(cols.signal)   + '  ' +
      p.cpu_load.toFixed(1).padStart(cols.cpu)             + '  ' +
      flag +
      ANSI.reset;
    console.log(row);
  }

  console.log(separator);
}

// ─── Main loop ─────────────────────────────────────────────────────────────────
async function run() {
  await producer.connect();
  console.log('[generator] Connected. Producing Avro telemetry for Madrid towers...');

  const interval = setInterval(async () => {
    const payloads = TOWERS.map(generateReading);

    const messages = await Promise.all(
      payloads.map(async (payload) => {
        const value = await serializer.serialize(TOPIC, payload);
        return { key: payload.tower_id, value };
      }),
    );

    printBatchTable(payloads);

    try {
      await producer.send({ topic: TOPIC, messages });
    } catch (err) {
      console.error('[generator] Failed to send batch:', err.message);
    }
  }, INTERVAL_MS);

  const shutdown = async () => {
    console.log('[generator] Shutting down...');
    clearInterval(interval);
    await producer.disconnect();
    process.exit(0);
  };
  process.on('SIGINT',  shutdown);
  process.on('SIGTERM', shutdown);
}

run().catch((err) => {
  console.error('[generator] Fatal error:', err);
  process.exit(1);
});

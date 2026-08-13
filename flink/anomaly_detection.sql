-- =============================================================================
-- anomaly_detection.sql
-- Submitted as a confluent_flink_statement via Terraform.
--
-- The producer registers Avro schemas in Schema Registry; Terraform also
-- registers them explicitly via confluent_schema resources. Confluent
-- Flink's managed catalog resolves column types from SR automatically —
-- no CREATE TABLE statement is needed.
--
-- Detects anomalous 5G cell-tower readings using a 60-second tumbling window:
--   temperature     > 85.0  °C
--   signal_strength < -90.0 dBm
--
-- Time attribute: $rowtime is Confluent Flink's built-in event-time watermark
-- sourced from the Kafka message timestamp. It is the ONLY column that carries
-- the time-attribute marker without a CREATE TABLE / WATERMARK declaration.
-- Avro timestamp-millis fields (like event_time) are plain TIMESTAMP_LTZ(3)
-- data columns — they cannot be used directly in TUMBLE(DESCRIPTOR(...)).
--
-- Sink schema: Confluent Flink exposes the Kafka message key as a leading
-- `key: BYTES` column. The INSERT must supply it as the first column.
-- window_start/window_end from TUMBLE are TIMESTAMP(3); the sink expects
-- TIMESTAMP_LTZ(3) so we cast them explicitly.
-- =============================================================================

INSERT INTO `anomaly-alerts`
SELECT
  -- Kafka message key column (first in sink schema)
  CAST(tower_id AS BYTES)                           AS key,
  tower_id,
  -- Location fields: constant per tower so safe to include in GROUP BY
  tower_name,
  latitude,
  longitude,
  AVG(temperature)                                  AS avg_temperature,
  AVG(signal_strength)                              AS avg_signal,
  MAX(temperature)                                  AS max_temperature,
  MIN(signal_strength)                              AS min_signal,
  COUNT(*)                                          AS event_count,
  CAST(window_start AS TIMESTAMP_LTZ(3))            AS window_start,
  CAST(window_end   AS TIMESTAMP_LTZ(3))            AS window_end
FROM TABLE(
  -- Use $rowtime (Kafka message timestamp) — the only true time attribute
  -- available in the managed catalog without a CREATE TABLE statement.
  TUMBLE(TABLE `cell-tower-telemetry`, DESCRIPTOR(`$rowtime`), INTERVAL '60' SECOND)
)
WHERE temperature > 85.0
   OR signal_strength < -90.0
GROUP BY
  tower_id,
  tower_name,
  latitude,
  longitude,
  window_start,
  window_end;

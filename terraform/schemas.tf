# ============================================================================
# Schema Registry — Avro schemas for both Kafka topics
# ============================================================================
# Schemas are registered under the default TopicNameStrategy:
#   subject = "<topic-name>-value"
#
# The .avsc source files live in schemas/ at the project root.
# Confluent Flink's managed catalog reads the registered schema from SR
# to resolve column types automatically — no CREATE TABLE statement needed.
# ============================================================================

# ─── cell-tower-telemetry-value ──────────────────────────────────────────────
#
# The event_time field was changed from Avro string → timestamp-millis (long)
# so that Flink maps it to TIMESTAMP_LTZ(3) for TUMBLE() windowing.
# That is a breaking change under BACKWARD compatibility. The provider has no
# subject-level compatibility resource, so we use local-exec curl calls to
# set the subject compatibility to NONE before registration and restore
# BACKWARD afterwards.

resource "null_resource" "telemetry_compat_none" {
  triggers = {
    schema_hash = filesha256("${path.module}/../schemas/cell-tower-telemetry-value.avsc")
  }

  provisioner "local-exec" {
    command = <<-EOT
      curl -s -X PUT \
        -u "${confluent_api_key.schema_registry.id}:${confluent_api_key.schema_registry.secret}" \
        -H "Content-Type: application/json" \
        -d '{"compatibility":"NONE"}' \
        "${data.confluent_schema_registry_cluster.main.rest_endpoint}/config/cell-tower-telemetry-value"
    EOT
  }

  depends_on = [confluent_api_key.schema_registry]
}

resource "confluent_schema" "telemetry_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.main.id
  }

  rest_endpoint = data.confluent_schema_registry_cluster.main.rest_endpoint

  subject_name = "cell-tower-telemetry-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/cell-tower-telemetry-value.avsc")

  credentials {
    key    = confluent_api_key.schema_registry.id
    secret = confluent_api_key.schema_registry.secret
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_kafka_topic.telemetry,
    confluent_api_key.schema_registry,
    null_resource.telemetry_compat_none,
  ]
}

resource "null_resource" "telemetry_compat_restore" {
  triggers = {
    schema_id = confluent_schema.telemetry_value.id
  }

  provisioner "local-exec" {
    command = <<-EOT
      curl -s -X PUT \
        -u "${confluent_api_key.schema_registry.id}:${confluent_api_key.schema_registry.secret}" \
        -H "Content-Type: application/json" \
        -d '{"compatibility":"BACKWARD"}' \
        "${data.confluent_schema_registry_cluster.main.rest_endpoint}/config/cell-tower-telemetry-value"
    EOT
  }

  depends_on = [confluent_schema.telemetry_value]
}

# ─── anomaly-alerts-value ────────────────────────────────────────────────────

resource "confluent_schema" "anomaly_alerts_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.main.id
  }

  rest_endpoint = data.confluent_schema_registry_cluster.main.rest_endpoint

  subject_name = "anomaly-alerts-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/anomaly-alerts-value.avsc")

  credentials {
    key    = confluent_api_key.schema_registry.id
    secret = confluent_api_key.schema_registry.secret
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_kafka_topic.anomalies,
    confluent_api_key.schema_registry,
  ]
}

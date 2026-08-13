# ============================================================================
# Kafka Topics for Cell Tower Telemetry Demo
# ============================================================================

# Raw telemetry events from mock cell towers
resource "confluent_kafka_topic" "telemetry" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  topic_name       = "cell-tower-telemetry"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }

  config = {
    "cleanup.policy"    = "delete"
    "retention.ms"      = "604800000" # 7 days
    "max.message.bytes" = "1048588"
  }

  lifecycle {
    prevent_destroy = false
    ignore_changes = [
      config,
      partitions_count
    ]
  }

  depends_on = [
    confluent_role_binding.producer_cluster_admin,
    time_sleep.cluster_admin_key_ready,
  ]
}

# Aggregated anomaly alerts produced by the Flink SQL job
resource "confluent_kafka_topic" "anomalies" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  topic_name       = "anomaly-alerts"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }

  config = {
    "cleanup.policy"    = "delete"
    "retention.ms"      = "604800000" # 7 days
    "max.message.bytes" = "1048588"
  }

  lifecycle {
    prevent_destroy = false
    ignore_changes = [
      config,
      partitions_count
    ]
  }

  depends_on = [
    confluent_role_binding.cluster_admin_env,
    time_sleep.cluster_admin_key_ready,
  ]
}

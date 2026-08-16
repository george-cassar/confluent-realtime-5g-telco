# ─── Core IDs ────────────────────────────────────────────────────────────────────

output "environment_id" {
  description = "Confluent Cloud environment ID"
  value       = confluent_environment.main.id
}

output "kafka_cluster_id" {
  description = "Kafka cluster ID"
  value       = confluent_kafka_cluster.main.id
}

output "flink_compute_pool_id" {
  description = "Flink compute pool ID"
  value       = confluent_flink_compute_pool.main.id
}

# ─── Kafka Endpoints ─────────────────────────────────────────────────────────────

output "bootstrap_endpoint" {
  description = "Kafka cluster bootstrap endpoint (used in .env files)"
  value       = confluent_kafka_cluster.main.bootstrap_endpoint
  sensitive   = true
}

output "kafka_rest_endpoint" {
  description = "Kafka cluster REST endpoint"
  value       = confluent_kafka_cluster.main.rest_endpoint
}

# ─── Flink Endpoints ─────────────────────────────────────────────────────────────

output "flink_rest_endpoint" {
  description = "Flink REST endpoint (sourced from confluent_flink_region data source)"
  value       = data.confluent_flink_region.main.rest_endpoint
}

# ─── Schema Registry ─────────────────────────────────────────────────────────────

output "schema_registry_url" {
  description = "Schema Registry REST endpoint"
  value       = data.confluent_schema_registry_cluster.main.rest_endpoint
}

output "schema_registry_api_key" {
  description = "Schema Registry API key ID"
  value       = confluent_api_key.schema_registry.id
  sensitive   = true
}

output "schema_registry_api_secret" {
  description = "Schema Registry API key secret"
  value       = confluent_api_key.schema_registry.secret
  sensitive   = true
}

# ─── Producer API Key ────────────────────────────────────────────────────────────

output "producer_api_key" {
  description = "API key ID for the telemetry producer service account"
  value       = confluent_api_key.producer.id
  sensitive   = true
}

output "producer_api_secret" {
  description = "API key secret for the telemetry producer service account"
  value       = confluent_api_key.producer.secret
  sensitive   = true
}

# ─── Consumer API Key ────────────────────────────────────────────────────────────

output "consumer_api_key" {
  description = "API key ID for the dashboard consumer service account"
  value       = confluent_api_key.consumer.id
  sensitive   = true
}

output "consumer_api_secret" {
  description = "API key secret for the dashboard consumer service account"
  value       = confluent_api_key.consumer.secret
  sensitive   = true
}

# ─── Flink API Key ───────────────────────────────────────────────────────────────

output "flink_api_key" {
  description = "Flink API key ID"
  value       = confluent_api_key.flink_admin.id
  sensitive   = true
}

output "flink_api_secret" {
  description = "Flink API key secret"
  value       = confluent_api_key.flink_admin.secret
  sensitive   = true
}

# ─── Consolidated Connection Info ────────────────────────────────────────────────

output "connection_info" {
  description = "Connection information for the Cell Tower Telemetry infrastructure"
  value = {
    environment = {
      id   = confluent_environment.main.id
      name = confluent_environment.main.display_name
    }
    kafka = {
      cluster_id        = confluent_kafka_cluster.main.id
      bootstrap_servers = confluent_kafka_cluster.main.bootstrap_endpoint
      rest_endpoint     = confluent_kafka_cluster.main.rest_endpoint
      region            = "${var.cloud_provider}/${var.region}"
    }
    flink = {
      compute_pool_id = confluent_flink_compute_pool.main.id
      rest_endpoint   = data.confluent_flink_region.main.rest_endpoint
      region          = "${var.cloud_provider}/${var.flink_region}"
    }
    schema_registry = {
      endpoint = data.confluent_schema_registry_cluster.main.rest_endpoint
    }
  }
}

# ─── Console URLs ────────────────────────────────────────────────────────────────

output "confluent_cloud_console_urls" {
  description = "Direct links to the Confluent Cloud console"
  value = {
    environment   = "https://confluent.cloud/environments/${confluent_environment.main.id}"
    kafka_cluster = "https://confluent.cloud/environments/${confluent_environment.main.id}/clusters/${confluent_kafka_cluster.main.id}"
    flink_console = "https://confluent.cloud/environments/${confluent_environment.main.id}/flink"
    topics        = "https://confluent.cloud/environments/${confluent_environment.main.id}/clusters/${confluent_kafka_cluster.main.id}/topics"
  }
}

# ─── .env Generator ──────────────────────────────────────────────────────────────
# Usage: terraform output -raw env_config >> ../generator/.env
#        terraform output -raw env_config >> ../server/.env

output "env_config" {
  description = "Ready-to-paste .env content for the generator and server services"
  value       = <<EOT
# ============================================================
# Confluent Cloud — Cell Tower Telemetry Demo
# Generated by: terraform output -raw env_config
# ============================================================

# Kafka — shared broker endpoint
BOOTSTRAP_SERVERS=${replace(confluent_kafka_cluster.main.bootstrap_endpoint, "SASL_SSL://", "")}

# Kafka (producer — generator/)
PRODUCER_API_KEY=${confluent_api_key.producer.id}
PRODUCER_API_SECRET=${confluent_api_key.producer.secret}

# Kafka (consumer — server/)
CONSUMER_API_KEY=${confluent_api_key.consumer.id}
CONSUMER_API_SECRET=${confluent_api_key.consumer.secret}

# Schema Registry
SCHEMA_REGISTRY_URL=${data.confluent_schema_registry_cluster.main.rest_endpoint}
SCHEMA_REGISTRY_API_KEY=${confluent_api_key.schema_registry.id}
SCHEMA_REGISTRY_API_SECRET=${confluent_api_key.schema_registry.secret}

# Confluent Cloud API (for MCP)
CONFLUENT_CLOUD_API_KEY=${var.confluent_cloud_api_key}
CONFLUENT_CLOUD_API_SECRET=${var.confluent_cloud_api_secret}
CONFLUENT_CLOUD_REST_ENDPOINT=https://api.confluent.cloud

# Flink
FLINK_API_KEY=${confluent_api_key.flink_admin.id}
FLINK_API_SECRET=${confluent_api_key.flink_admin.secret}
FLINK_COMPUTE_POOL_ID=${confluent_flink_compute_pool.main.id}
FLINK_ENV_ID=${confluent_environment.main.id}
FLINK_ORG_ID=${local.org_id}
FLINK_REST_ENDPOINT=${data.confluent_flink_region.main.rest_endpoint}
FLINK_DATABASE_NAME=${confluent_kafka_cluster.main.id}
EOT
  sensitive   = true
}

# ─── AI Agent ────────────────────────────────────────────────────────────────────

output "agent_flink_statement_name" {
  description = "Name of the Flink SQL INSERT statement running the Gemini Streaming Agent"
  value       = confluent_flink_statement.agent_insert.statement_name
}

# ─── Next Steps ──────────────────────────────────────────────────────────────────

output "next_steps" {
  description = "Next steps after deployment"
  value       = <<-EOT
    🎉 Confluent Cloud infrastructure deployed successfully!

    📋 Next Steps:
    1. View environment:   https://confluent.cloud/environments/${confluent_environment.main.id}
    2. Kafka cluster:      https://confluent.cloud/environments/${confluent_environment.main.id}/clusters/${confluent_kafka_cluster.main.id}
    3. Flink console:      https://confluent.cloud/environments/${confluent_environment.main.id}/flink
    4. Populate .env files:
         terraform output -raw env_config > ../generator/.env
         terraform output -raw env_config > ../server/.env
    5. Start generator:    cd ../generator && npm start
    6. Start server:       cd ../server && npm start

    💡 Tip: Use 'terraform output -json' to get all outputs in JSON format.
  EOT
}

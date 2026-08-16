# Configure the Confluent Provider
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# ─── Organisation ────────────────────────────────────────────────────────────────

data "confluent_organization" "main" {}

# Extract Organisation ID from the environment resource name
# Format: crn://confluent.cloud/organization=<org-id>/environment=<env-id>
locals {
  org_id              = regex("organization=([^/]+)", confluent_environment.main.resource_name)[0]
  flink_rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  flink_properties = {
    "sql.current-catalog"  = confluent_environment.main.id
    "sql.current-database" = confluent_kafka_cluster.main.id
  }
}

# ─── Flink Region ────────────────────────────────────────────────────────────────
# Resolves the Flink region resource (id, api_version, kind, rest_endpoint)
# needed for the Flink API key managed_resource block and REST calls.

data "confluent_flink_region" "main" {
  cloud  = var.cloud_provider
  region = var.flink_region
}

# ─── Environment (with Stream Governance Essentials for Schema Registry) ─────────

resource "confluent_environment" "main" {
  display_name = var.environment_name

  stream_governance {
    package = "ESSENTIALS"
  }

  lifecycle {
    prevent_destroy = false
  }
}

# ─── Schema Registry ─────────────────────────────────────────────────────────────
# The Stream Governance Essentials package provisions a Schema Registry cluster
# automatically. We look it up with a data source so we can create an API key.

data "confluent_schema_registry_cluster" "main" {
  environment {
    id = confluent_environment.main.id
  }

  depends_on = [confluent_kafka_cluster.main]
}

# ─── Kafka Cluster ──────────────────────────────────────────────────────────────

resource "confluent_kafka_cluster" "main" {
  display_name = var.cluster_name
  availability = "SINGLE_ZONE"
  cloud        = var.cloud_provider
  region       = var.region

  basic {}

  environment {
    id = confluent_environment.main.id
  }

  lifecycle {
    prevent_destroy = false
  }
}

# ─── Flink Compute Pool ─────────────────────────────────────────────────────────

resource "confluent_flink_compute_pool" "main" {
  display_name = "celltower-flink-${var.env_suffix}"
  cloud        = var.cloud_provider
  region       = var.flink_region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.main.id
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_kafka_cluster.main]
}

# ─── Service Accounts ───────────────────────────────────────────────────────────

resource "confluent_service_account" "producer" {
  display_name = "celltower-producer-sa-${var.env_suffix}"
  description  = "Service account for the mock telemetry producer"

  lifecycle {
    prevent_destroy = false
  }
}

resource "confluent_service_account" "consumer" {
  display_name = "celltower-consumer-sa-${var.env_suffix}"
  description  = "Service account for the dashboard Kafka consumer"

  lifecycle {
    prevent_destroy = false
  }
}

# Admin SA used by Terraform to manage topics and submit Flink statements
resource "confluent_service_account" "cluster_admin" {
  display_name = "celltower-admin-sa-${var.env_suffix}"
  description  = "Terraform-only admin service account"

  lifecycle {
    prevent_destroy = false
  }
}

# ─── Role Bindings ───────────────────────────────────────────────────────────────

# EnvironmentAdmin on the environment — allows the admin SA to manage all
# resources within the environment including Kafka and Flink.
resource "confluent_role_binding" "cluster_admin_env" {
  principal   = "User:${confluent_service_account.cluster_admin.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name

  lifecycle {
    prevent_destroy = false
  }
}

# CloudClusterAdmin on the Kafka cluster — grants the producer SA full cluster
# admin access, including topic-level produce/consume rights.
resource "confluent_role_binding" "producer_cluster_admin" {
  principal   = "User:${confluent_service_account.producer.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn

  lifecycle {
    prevent_destroy = false
  }
}

# CloudClusterAdmin on the Kafka cluster — grants the consumer SA full cluster
# admin access, including topic-level read rights.
resource "confluent_role_binding" "consumer_cluster_admin" {
  principal   = "User:${confluent_service_account.consumer.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.main.rbac_crn

  lifecycle {
    prevent_destroy = false
  }
}

# FlinkDeveloper on the Flink compute pool — grants permission to submit
# Flink SQL statements to the specific pool.
resource "confluent_role_binding" "flink_developer" {
  principal   = "User:${confluent_service_account.cluster_admin.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = confluent_flink_compute_pool.main.resource_name

  lifecycle {
    prevent_destroy = false
  }
}

# ResourceOwner on all Schema Registry subjects — allows the admin SA
# to register and manage schemas.
resource "confluent_role_binding" "schema_registry_resource_owner" {
  principal   = "User:${confluent_service_account.cluster_admin.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.main.resource_name}/subject=*"

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [data.confluent_schema_registry_cluster.main]
}

# ─── API Keys ───────────────────────────────────────────────────────────────────

resource "confluent_api_key" "cluster_admin" {
  display_name = "celltower-admin-key-${var.env_suffix}"
  description  = "Kafka API key for Terraform cluster management"

  owner {
    id          = confluent_service_account.cluster_admin.id
    api_version = confluent_service_account.cluster_admin.api_version
    kind        = confluent_service_account.cluster_admin.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  lifecycle {
    prevent_destroy      = false
    replace_triggered_by = [confluent_kafka_cluster.main]
  }

  depends_on = [confluent_role_binding.cluster_admin_env]
}

# Confluent Cloud Kafka API keys take a few seconds to propagate after creation.
# Without this pause, topic resources that depend on the key receive a 401 on
# their first REST call. 15 seconds is sufficient; the rest of the plan runs
# in parallel so the wall-clock cost is minimal.
resource "time_sleep" "cluster_admin_key_ready" {
  create_duration = "15s"
  depends_on      = [confluent_api_key.cluster_admin]
}

resource "confluent_api_key" "producer" {
  display_name = "celltower-producer-key-${var.env_suffix}"
  description  = "Kafka API key for the telemetry producer"

  owner {
    id          = confluent_service_account.producer.id
    api_version = confluent_service_account.producer.api_version
    kind        = confluent_service_account.producer.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_role_binding.producer_cluster_admin]
}

resource "confluent_api_key" "consumer" {
  display_name = "celltower-consumer-key-${var.env_suffix}"
  description  = "Kafka API key for the dashboard consumer"

  owner {
    id          = confluent_service_account.consumer.id
    api_version = confluent_service_account.consumer.api_version
    kind        = confluent_service_account.consumer.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.main.id
    api_version = confluent_kafka_cluster.main.api_version
    kind        = confluent_kafka_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_role_binding.consumer_cluster_admin]
}

# Flink API key — scoped to the Flink region via the confluent_flink_region data source.
resource "confluent_api_key" "flink_admin" {
  display_name = "celltower-flink-key-${var.env_suffix}"
  description  = "Flink API key used to submit the anomaly detection statement"

  owner {
    id          = confluent_service_account.cluster_admin.id
    api_version = confluent_service_account.cluster_admin.api_version
    kind        = confluent_service_account.cluster_admin.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.main.id
    api_version = data.confluent_flink_region.main.api_version
    kind        = data.confluent_flink_region.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_role_binding.flink_developer]
}

resource "confluent_api_key" "schema_registry" {
  display_name = "celltower-sr-key-${var.env_suffix}"
  description  = "Schema Registry API key for producer and Flink"

  owner {
    id          = confluent_service_account.cluster_admin.id
    api_version = confluent_service_account.cluster_admin.api_version
    kind        = confluent_service_account.cluster_admin.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.main.id
    api_version = data.confluent_schema_registry_cluster.main.api_version
    kind        = data.confluent_schema_registry_cluster.main.kind

    environment {
      id = confluent_environment.main.id
    }
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [confluent_role_binding.schema_registry_resource_owner]
}

# ─── Flink SQL Statement ─────────────────────────────────────────────────────────
# The producer registers the telemetry Avro schema in Schema Registry.
# Confluent Flink's managed catalog auto-resolves column types from SR,
# so the INSERT INTO job can be submitted directly — no CREATE TABLE needed.

resource "confluent_flink_statement" "anomaly_detection" {
  statement      = file("${path.module}/../flink/00_stream_anomaly_detection.sql")
  statement_name = "celltower-anomaly-detection-${var.env_suffix}"
  rest_endpoint  = local.flink_rest_endpoint
  properties     = local.flink_properties

  credentials {
    key    = confluent_api_key.flink_admin.id
    secret = confluent_api_key.flink_admin.secret
  }

  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = confluent_service_account.cluster_admin.id }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_kafka_topic.telemetry,
    confluent_kafka_topic.anomalies,
    confluent_api_key.schema_registry,
    confluent_flink_compute_pool.main,
    confluent_api_key.flink_admin,
    confluent_schema.telemetry_value,
    confluent_schema.anomaly_alerts_value,
  ]
}

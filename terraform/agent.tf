# ============================================================================
# AI Streaming Pipeline — Confluent Flink + Google Gemini
# ============================================================================
#
# Uses AI_COMPLETE (direct model call) rather than AI_RUN_AGENT / CREATE AGENT.
# AI_RUN_AGENT adds multi-turn conversation-state overhead that causes token
# limit errors on gemini-3.5-flash even with max_iterations=1. AI_COMPLETE
# is a plain prompt → response call with zero framework overhead.
#
# Flink catalog objects (CONNECTION, MODEL) have their own lifecycle independent
# of the confluent_flink_statement that creates them. Each object requires two
# Terraform resources: DROP IF EXISTS then CREATE.
#
# IMPORTANT: The Confluent Flink statement API accepts EXACTLY ONE SQL statement
# per resource. Compound SQL (two statements separated by ;) is rejected with
# "Only a single statement is supported at a time."
#
# Full dependency chain:
#
#   topic + schema
#     └─► agent_drop_connection  (DROP CONNECTION IF EXISTS)
#           └─► agent_connection       (CREATE CONNECTION)
#                 └─► agent_drop_model  (DROP MODEL IF EXISTS $all)
#                       └─► agent_model       (CREATE MODEL with system_prompt)
#                                 └─► agent_insert  (INSERT … AI_COMPLETE)
# ============================================================================

# ─── Kafka topic: ai-agent-responses ─────────────────────────────────────────

resource "confluent_kafka_topic" "agent_responses" {
  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  topic_name       = "ai-agent-responses"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.main.rest_endpoint

  credentials {
    key    = confluent_api_key.cluster_admin.id
    secret = confluent_api_key.cluster_admin.secret
  }

  config = {
    "cleanup.policy"    = "delete"
    "retention.ms"      = "28800000" # 8 hours
    "max.message.bytes" = "1048588"
  }

  lifecycle {
    prevent_destroy = false
    ignore_changes  = [partitions_count]
  }

  depends_on = [
    confluent_role_binding.cluster_admin_env,
    time_sleep.cluster_admin_key_ready,
  ]
}

# ─── Avro schema: ai-agent-responses-value ───────────────────────────────────

resource "confluent_schema" "agent_responses_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.main.id
  }

  rest_endpoint = data.confluent_schema_registry_cluster.main.rest_endpoint

  subject_name = "ai-agent-responses-value"
  format       = "AVRO"
  schema       = file("${path.module}/../schemas/ai-agent-responses-value.avsc")

  credentials {
    key    = confluent_api_key.schema_registry.id
    secret = confluent_api_key.schema_registry.secret
  }

  lifecycle {
    prevent_destroy = false
  }

  depends_on = [
    confluent_kafka_topic.agent_responses,
    confluent_api_key.schema_registry,
  ]
}

# ─── Flink statement 1a: DROP CONNECTION IF EXISTS ───────────────────────────
# Must run before CREATE CONNECTION to ensure idempotency.
# ALTER CONNECTION cannot change the endpoint — drop-then-create is required.

resource "confluent_flink_statement" "agent_drop_connection" {
  statement      = file("${path.module}/../flink/01a_drop_gemini_connection.sql")
  statement_name = "celltower-agent-drop-conn-${var.env_suffix}"
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
    confluent_kafka_topic.agent_responses,
    confluent_schema.agent_responses_value,
    confluent_api_key.flink_admin,
    confluent_flink_compute_pool.main,
  ]
}

# ─── Flink statement 1b: CREATE CONNECTION ───────────────────────────────────
# Runs after the DROP. Uses templatefile() to inject the API key at plan time.

resource "confluent_flink_statement" "agent_connection" {
  statement = templatefile(
    "${path.module}/../flink/01b_create_gemini_connection.sql",
    { google_api_key = var.google_api_key }
  )
  statement_name = "celltower-agent-connection-${var.env_suffix}"
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

  depends_on = [confluent_flink_statement.agent_drop_connection]
}

# ─── Sentinel: tracks the model SQL file hash ─────────────────────────────────
# confluent_flink_statement is immutable — the provider cannot update a running
# statement in-place. replace_triggered_by only fires when the referenced
# resource itself replaces, NOT when an input attribute changes. A change to
# 02b_create_gemini_model.sql would therefore be silently ignored.
#
# This terraform_data resource holds the SHA256 of the model SQL. When the file
# changes its hash changes, terraform_data replaces, and the replace_triggered_by
# chain (drop_model → model → insert) fires automatically.

resource "terraform_data" "model_sql_hash" {
  input = sha256(file("${path.module}/../flink/02b_create_gemini_model.sql"))
}

# ─── Flink statement 2a: DROP MODEL IF EXISTS ────────────────────────────────
# Clears all versions of the model so the subsequent CREATE MODEL always
# registers a fresh version 1 pointing to the current connection.

resource "confluent_flink_statement" "agent_drop_model" {
  statement      = file("${path.module}/../flink/02a_drop_gemini_model.sql")
  statement_name = "celltower-agent-drop-model-${var.env_suffix}"
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
    prevent_destroy      = false
    replace_triggered_by = [
      confluent_flink_statement.agent_connection,
      terraform_data.model_sql_hash,
    ]
  }

  depends_on = [confluent_flink_statement.agent_connection]
}

# ─── Flink statement 2b: CREATE MODEL ────────────────────────────────────────

resource "confluent_flink_statement" "agent_model" {
  statement      = file("${path.module}/../flink/02b_create_gemini_model.sql")
  statement_name = "celltower-agent-model-${var.env_suffix}"
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
    prevent_destroy      = false
    replace_triggered_by = [confluent_flink_statement.agent_drop_model]
  }

  depends_on = [confluent_flink_statement.agent_drop_model]
}

# ─── Flink statement 3: INSERT (streaming pipeline) ─────────────────────────
# Reads anomaly-alerts, calls AI_COMPLETE on the model, writes to ai-agent-responses.
# This is the only STREAMING statement; all statements above are DDL.

resource "confluent_flink_statement" "agent_insert" {
  statement      = file("${path.module}/../flink/04_stream_agent_responses.sql")
  statement_name = "celltower-gemini-agent-${var.env_suffix}"
  rest_endpoint  = local.flink_rest_endpoint

  # latest-offset: start from the tail of anomaly-alerts, not the beginning.
  # Prevents replaying the full backlog through ML_PREDICT on every re-deploy,
  # which would fire hundreds of Gemini calls at once and trip the 429 quota.
  #
  # sql.tables.scan.source-operator-parallelism=1 constrains the source scan
  # to a single task thread so ML_PREDICT calls are strictly serialised —
  # one Gemini request at a time.
  # Combined with retry_interval_ms=20000 in the SQL this gives Gemini 20 s to
  # recover between retry attempts on a 429.
  properties = merge(local.flink_properties, {
    "sql.tables.scan.startup.mode"              = "latest-offset",
    "sql.tables.scan.source-operator-parallelism" = "1"
  })

  credentials {
    key    = confluent_api_key.flink_admin.id
    secret = confluent_api_key.flink_admin.secret
  }

  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = confluent_service_account.cluster_admin.id }

  lifecycle {
    prevent_destroy      = false
    replace_triggered_by = [confluent_flink_statement.agent_model]
  }

  depends_on = [
    confluent_flink_statement.agent_model,
    confluent_flink_statement.anomaly_detection,
  ]
}

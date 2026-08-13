#!/usr/bin/env bash
# =============================================================================
# generate-mcp-config.sh
#
# Reads all required values from Terraform outputs and writes a fully-populated
# .bob/mcp.json for IBM Bob's MCP integration with this project's Confluent
# Cloud infrastructure.
#
# Usage:
#   cd <project-root>
#   ./scripts/generate-mcp-config.sh
#
# Prerequisites:
#   - terraform apply has been run successfully in terraform/
#   - The Terraform state is present and up-to-date
#   - jq is installed (brew install jq)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TERRAFORM_DIR="$PROJECT_ROOT/terraform"
OUTPUT_FILE="$PROJECT_ROOT/.bob/mcp.json"
MCP_CONFIG_FILE="$PROJECT_ROOT/server/mcp-config/config.yaml"

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
die()     { error "$*"; exit 1; }

# ── Dependency checks ─────────────────────────────────────────────────────────
command -v terraform >/dev/null 2>&1 || die "terraform is not installed or not on PATH"
command -v jq        >/dev/null 2>&1 || die "jq is not installed. Run: brew install jq"

# ── Read Terraform outputs ────────────────────────────────────────────────────
info "Reading Terraform outputs from $TERRAFORM_DIR ..."
cd "$TERRAFORM_DIR"

# Validate state is present
terraform output -json >/dev/null 2>&1 || die "No Terraform state found. Run 'terraform apply' first."

tf_raw() { terraform output -raw "$1" 2>/dev/null || die "Terraform output '$1' not found"; }
tf_val()  { terraform output     "$1" 2>/dev/null | tr -d '"' || die "Terraform output '$1' not found"; }

BOOTSTRAP_SERVERS=$(tf_raw bootstrap_endpoint | sed 's|SASL_SSL://||')
KAFKA_REST_ENDPOINT=$(tf_val kafka_rest_endpoint)
SCHEMA_REGISTRY_ENDPOINT=$(tf_val schema_registry_url)
ENVIRONMENT_ID=$(tf_val environment_id)
KAFKA_CLUSTER_ID=$(tf_val kafka_cluster_id)
FLINK_COMPUTE_POOL_ID=$(tf_val flink_compute_pool_id)
FLINK_REST_ENDPOINT=$(tf_val flink_rest_endpoint)

# Sensitive — use -raw
KAFKA_API_KEY=$(tf_raw consumer_api_key)
KAFKA_API_SECRET=$(tf_raw consumer_api_secret)
SCHEMA_REGISTRY_API_KEY=$(tf_raw schema_registry_api_key)
SCHEMA_REGISTRY_API_SECRET=$(tf_raw schema_registry_api_secret)
CONFLUENT_CLOUD_API_KEY=$(tf_raw flink_api_key)
CONFLUENT_CLOUD_API_SECRET=$(tf_raw flink_api_secret)
FLINK_API_KEY=$(tf_raw flink_api_key)
FLINK_API_SECRET=$(tf_raw flink_api_secret)

# Organisation ID: parse from the environment resource_name in Terraform state.
# resource_name format: crn://confluent.cloud/organization=<org-id>/environment=<env-id>
ORGANIZATION_ID=$(terraform show -json 2>/dev/null \
  | jq -r '.. | objects | select(.type == "confluent_environment") | .values.resource_name // empty' 2>/dev/null \
  | sed 's|.*organization=\([^/]*\).*|\1|' | head -1 || echo "")

[[ -z "$ORGANIZATION_ID" ]] && die "Could not determine ORGANIZATION_ID from Terraform state. Check your state is up to date."

info "All Terraform outputs retrieved successfully."

# ── Preserve the bob-marketplace block if it already exists ──────────────────
cd "$PROJECT_ROOT"
BOB_MARKETPLACE_BLOCK='{
      "type": "streamable-http",
      "url": "http://127.0.0.1:39247/mcp",
      "headers": {
        "Bob-Marketplace-Token": "bob-marketplace-local"
      },
      "disabled": false,
      "alwaysAllow": [
        "search_assets",
        "get_asset",
        "list_installed",
        "list_favorites",
        "suggest_assets",
        "list_updates"
      ]
    }'

if [[ -f "$OUTPUT_FILE" ]]; then
  EXISTING_MARKETPLACE=$(jq -c '.mcpServers["bob-marketplace"] // empty' "$OUTPUT_FILE" 2>/dev/null || echo "")
  if [[ -n "$EXISTING_MARKETPLACE" ]]; then
    BOB_MARKETPLACE_BLOCK="$EXISTING_MARKETPLACE"
  fi
fi

# ── Generate server/mcp-config/config.yaml ───────────────────────────────────
info "Writing $MCP_CONFIG_FILE ..."
mkdir -p "$(dirname "$MCP_CONFIG_FILE")"

cat > "$MCP_CONFIG_FILE" <<YAML
server:
  transports: [stdio]
  log_level: "\${LOG_LEVEL:-info}"

connections:
  default:
    type: direct
    description: "Confluent Cloud - Cell Tower Demo"

    kafka:
      bootstrap_servers: "\${BOOTSTRAP_SERVERS}"
      auth:
        type: api_key
        key: "\${KAFKA_API_KEY}"
        secret: "\${KAFKA_API_SECRET}"
      rest_endpoint: "\${KAFKA_REST_ENDPOINT}"
      cluster_id: "\${KAFKA_CLUSTER_ID}"
      env_id: "\${ENVIRONMENT_ID}"

    schema_registry:
      endpoint: "\${SCHEMA_REGISTRY_ENDPOINT}"
      auth:
        type: api_key
        key: "\${SCHEMA_REGISTRY_API_KEY}"
        secret: "\${SCHEMA_REGISTRY_API_SECRET}"

    confluent_cloud:
      endpoint: "https://api.confluent.cloud"
      auth:
        type: api_key
        key: "\${CONFLUENT_CLOUD_API_KEY}"
        secret: "\${CONFLUENT_CLOUD_API_SECRET}"

    flink:
      endpoint: "\${FLINK_REST_ENDPOINT}"
      auth:
        type: api_key
        key: "\${FLINK_API_KEY}"
        secret: "\${FLINK_API_SECRET}"
      organization_id: "\${ORGANIZATION_ID}"
      environment_id: "\${ENVIRONMENT_ID}"
      compute_pool_id: "\${FLINK_COMPUTE_POOL_ID}"
YAML

info "✅ MCP YAML config written to $MCP_CONFIG_FILE"

# ── Generate mcp.json ─────────────────────────────────────────────────────────
info "Writing $OUTPUT_FILE ..."
mkdir -p "$(dirname "$OUTPUT_FILE")"

jq -n \
  --arg bootstrap        "$BOOTSTRAP_SERVERS" \
  --arg kafka_key        "$KAFKA_API_KEY" \
  --arg kafka_secret     "$KAFKA_API_SECRET" \
  --arg kafka_rest       "$KAFKA_REST_ENDPOINT" \
  --arg sr_endpoint      "$SCHEMA_REGISTRY_ENDPOINT" \
  --arg sr_key           "$SCHEMA_REGISTRY_API_KEY" \
  --arg sr_secret        "$SCHEMA_REGISTRY_API_SECRET" \
  --arg cc_key           "$CONFLUENT_CLOUD_API_KEY" \
  --arg cc_secret        "$CONFLUENT_CLOUD_API_SECRET" \
  --arg org_id           "$ORGANIZATION_ID" \
  --arg env_id           "$ENVIRONMENT_ID" \
  --arg cluster_id       "$KAFKA_CLUSTER_ID" \
  --arg flink_pool_id    "$FLINK_COMPUTE_POOL_ID" \
  --arg flink_rest       "$FLINK_REST_ENDPOINT" \
  --arg flink_key        "$FLINK_API_KEY" \
  --arg flink_secret     "$FLINK_API_SECRET" \
  --arg mcp_config_file  "$MCP_CONFIG_FILE" \
  --argjson marketplace  "$BOB_MARKETPLACE_BLOCK" \
  '{
    mcpServers: {
      "bob-marketplace": $marketplace,
      "confluent": {
        command: "npx",
        args: ["-y", "@confluentinc/mcp-confluent", "--config", $mcp_config_file],
        env: {
          BOOTSTRAP_SERVERS:          $bootstrap,
          KAFKA_API_KEY:              $kafka_key,
          KAFKA_API_SECRET:           $kafka_secret,
          KAFKA_REST_ENDPOINT:        $kafka_rest,
          SCHEMA_REGISTRY_ENDPOINT:   $sr_endpoint,
          SCHEMA_REGISTRY_API_KEY:    $sr_key,
          SCHEMA_REGISTRY_API_SECRET: $sr_secret,
          CONFLUENT_CLOUD_API_KEY:    $cc_key,
          CONFLUENT_CLOUD_API_SECRET: $cc_secret,
          ORGANIZATION_ID:            $org_id,
          ENVIRONMENT_ID:             $env_id,
          KAFKA_CLUSTER_ID:           $cluster_id,
          FLINK_COMPUTE_POOL_ID:      $flink_pool_id,
          FLINK_REST_ENDPOINT:        $flink_rest,
          FLINK_API_KEY:              $flink_key,
          FLINK_API_SECRET:           $flink_secret
        },
        alwaysAllow: [
          "list-topics",
          "list-flink-statements",
          "list-flink-tables",
          "list-flink-catalogs",
          "list-flink-databases",
          "list-connectors",
          "list-schemas",
          "list-available-metrics",
          "list-clusters",
          "list-compute-pools",
          "search-product-docs"
        ]
      }
    }
  }' > "$OUTPUT_FILE"

info "✅ MCP config written to $OUTPUT_FILE"
echo
echo "  Next step: reload the MCP server in IBM Bob"
echo "  (Cmd+Shift+P → 'MCP: Reload Servers')"
echo

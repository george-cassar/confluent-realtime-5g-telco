#!/usr/bin/env bash
# =============================================================================
# scripts/infra-provision.sh
# Interactive wizard — provisions all Confluent Cloud infrastructure via
# Terraform and writes .env files for the generator and dashboard.
#
# Usage:  ./scripts/infra-provision.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

# ── Step 1 — Tool check ────────────────────────────────────────────────────────
banner "5G Cell Tower Demo — Infrastructure Provisioning" "Confluent Cloud + Terraform"

step "1" "Checking required tools"
require_tools terraform node npm

# ── Step 2 — Confluent credentials ────────────────────────────────────────────
step "2" "Confluent Cloud credentials"

c "$BOLD"
cat <<'MSG'
   Before continuing you need a Confluent Cloud API key.
   This is a "Cloud-level" key used only by Terraform to create resources —
   it is NOT a Kafka API key and will NOT be put into any application config.

   How to get one (takes about 1 minute):
     1. Log in at https://confluent.cloud
     2. Click your account name in the top-right corner
     3. Select "API Keys"
     4. Click "+ Add key"  →  choose "Global access"  →  click "Download & continue"
     5. Copy the Key and Secret shown on screen

MSG
res

ask_secret CF_API_KEY    "Confluent Cloud API Key"    \
  "Paste the Key ID (looks like: ABCD1234EFGH5678)"
ask_secret CF_API_SECRET "Confluent Cloud API Secret" \
  "Paste the Secret (longer string — it is only shown once, so keep a copy safe)"

# ── Step 3 — Cloud provider & region ──────────────────────────────────────────
step "3" "Cloud provider & region"

info "Choose where to host the Kafka cluster and Flink pool."
info "Press Enter at any prompt to accept the default shown in [brackets]."

# ── Cloud provider (default: AWS) ─────────────────────────────────────────────
printf "\n"
info "AWS is recommended and has the widest Confluent region coverage."
printf "\n"
c "$BOLD"
printf "   1) AWS   ← default\n"
printf "   2) GCP\n"
printf "   3) AZURE\n"
res
c "$BOLD"; printf "\n   → Cloud provider [default: AWS]: "; res
read -r _cloud_input
case "$_cloud_input" in
  2|GCP|gcp)     CF_CLOUD="GCP"   ;;
  3|AZURE|azure) CF_CLOUD="AZURE" ;;
  *)             CF_CLOUD="AWS"   ;;
esac
info "Selected: $CF_CLOUD"

# ── Region (default depends on provider, AWS default = eu-central-1) ──────────
declare -a REGION_OPTIONS
if [[ "$CF_CLOUD" == "AWS" ]]; then
  REGION_OPTIONS=(
    "eu-central-1     (Frankfurt, Germany — Flink available ✓)"
    "eu-west-1        (Ireland            — Flink available ✓)"
    "eu-west-2        (London, UK         — Flink available ✓)"
    "eu-west-3        (Paris, France      — Flink available ✓)"
    "eu-south-1       (Milan, Italy       — Flink available ✓)"
    "eu-north-1       (Stockholm, Sweden  — Flink available ✓)"
    "us-east-1        (N. Virginia, USA   — Flink available ✓)"
    "us-east-2        (Ohio, USA          — Flink available ✓)"
    "us-west-2        (Oregon, USA        — Flink available ✓)"
  )
  _region_default="eu-central-1"
elif [[ "$CF_CLOUD" == "GCP" ]]; then
  REGION_OPTIONS=(
    "europe-west3     (Frankfurt, Germany — Flink available ✓)"
    "europe-west1     (Belgium            — Flink available ✓)"
    "europe-west2     (London, UK         — Flink available ✓)"
    "europe-west4     (Netherlands        — Flink available ✓)"
    "europe-west6     (Zurich, Switzerland)"
    "europe-west8     (Milan, Italy)"
    "europe-west9     (Paris, France      — Flink available ✓)"
    "europe-north1    (Finland)"
    "us-central1      (Iowa, USA          — Flink available ✓)"
    "us-east1         (S. Carolina, USA   — Flink available ✓)"
  )
  _region_default="europe-west3"
else
  REGION_OPTIONS=(
    "germanywestcentral   (Frankfurt, Germany — Flink available ✓)"
    "westeurope           (Netherlands        — Flink available ✓)"
    "northeurope          (Ireland            — Flink available ✓)"
    "uksouth              (London, UK         — Flink available ✓)"
    "francecentral        (Paris, France      — Flink available ✓)"
    "switzerlandnorth     (Zurich, Switzerland)"
    "norwayeast           (Oslo, Norway)"
    "swedencentral        (Gävle, Sweden)"
    "italynorth           (Milan, Italy)"
    "eastus               (Virginia, USA      — Flink available ✓)"
    "westus2              (Washington, USA)"
  )
  _region_default="germanywestcentral"
fi

printf "\n"
info "All listed regions support Confluent Flink."
printf "\n"
for _ri in "${!REGION_OPTIONS[@]}"; do
  if [[ "${REGION_OPTIONS[_ri]%%[[:space:]]*}" == "$_region_default" ]]; then
    c "$BOLD"; printf "   %d) %s  ← default\n" "$((_ri+1))" "${REGION_OPTIONS[_ri]}"; res
  else
    c "$BOLD"; printf "   %d) %s\n" "$((_ri+1))" "${REGION_OPTIONS[_ri]}"; res
  fi
done
c "$BOLD"; printf "\n   → Region [default: %s]: " "$_region_default"; res
read -r _region_input

if [[ -z "$_region_input" ]]; then
  region_full="$_region_default"
  info "Using default: $_region_default"
elif [[ "$_region_input" =~ ^[0-9]+$ ]] && \
     (( _region_input >= 1 && _region_input <= ${#REGION_OPTIONS[@]} )); then
  region_full="${REGION_OPTIONS[$((_region_input-1))]%%[[:space:]]*}"
  info "Selected: $region_full"
else
  region_full="$_region_input"
  info "Using: $region_full"
fi

# Extract only the region code — the first whitespace-delimited token
CF_REGION="${region_full%%[[:space:]]*}"

# Flink uses the same region as Kafka — all options in the list above support Flink.
CF_FLINK_REGION="$CF_REGION"

# ── Step 4 — Resource naming ───────────────────────────────────────────────────
step "4" "Resource naming"

info "All Confluent Cloud resources are named automatically."
info "You can add a short label (like 'dev' or 'test') to tell environments apart."
info "Press Enter on any question to accept the suggested default."

ask_with_default ENV_SUFFIX "Environment label" "dev" \
  "Short label appended to resource names (e.g. 'dev' gives: celltower-kafka-dev)"

ask_with_default ENV_NAME "Confluent environment name" "celltower-env-${ENV_SUFFIX}" \
  "A logical folder inside Confluent Cloud that groups all your demo resources"

ask_with_default CLUSTER_NAME "Kafka cluster name" "celltower-kafka-${ENV_SUFFIX}" \
  "The name of the Kafka cluster that stores the telemetry and anomaly data"

ask_with_default FLINK_MAX_CFU "Maximum Flink compute units (CFU)" "5" \
  "How much processing power to give Apache Flink. 5 is plenty for this demo (valid range: 5–40)"

# ── Step 5 — Review ────────────────────────────────────────────────────────────
step "5" "Review — please confirm before anything is created"

printf "\n"
c "$BOLD"
printf "   %-32s %s\n" "Cloud provider:"    "$CF_CLOUD"
printf "   %-32s %s\n" "Region (Kafka + Flink):" "$CF_REGION"
printf "   %-32s %s\n" "Environment name:"  "$ENV_NAME"
printf "   %-32s %s\n" "Cluster name:"      "$CLUSTER_NAME"
printf "   %-32s %s\n" "Resource label:"    "$ENV_SUFFIX"
printf "   %-32s %s\n" "Flink max CFU:"     "$FLINK_MAX_CFU"
printf "   %-32s %s\n" "API Key (masked):"  "${CF_API_KEY:0:4}…${CF_API_KEY: -4}"
res
printf "\n"

ask_yes_no confirmed "Everything looks correct — proceed with provisioning?"
if [[ "$confirmed" != "y" ]]; then
  warn "Provisioning cancelled. Run this script again to start over."
  exit 0
fi

# ── Step 6 — Write terraform.tfvars ───────────────────────────────────────────
step "6" "Writing Terraform configuration file"

TFVARS="${ROOT_DIR}/terraform/terraform.tfvars"
cat > "$TFVARS" <<EOF
# Auto-generated by infra-provision.sh — do not commit this file
confluent_cloud_api_key    = "${CF_API_KEY}"
confluent_cloud_api_secret = "${CF_API_SECRET}"

environment_name = "${ENV_NAME}"
cluster_name     = "${CLUSTER_NAME}"
cloud_provider   = "${CF_CLOUD}"
region           = "${CF_REGION}"
flink_region     = "${CF_FLINK_REGION}"
flink_max_cfu    = ${FLINK_MAX_CFU}
env_suffix       = "${ENV_SUFFIX}"
EOF

success "Written: terraform/terraform.tfvars  (gitignored — will not be committed)"

# ── Step 7 — Terraform init ────────────────────────────────────────────────────
step "7" "Initialising Terraform"
info "Downloading the Confluent Cloud provider plugin. This may take ~30 seconds…"
info "(-upgrade ensures the latest compatible provider version is used)"
printf "\n"

(cd "${ROOT_DIR}/terraform" && terraform init -input=false -upgrade)
success "Terraform initialised."

# ── Step 8 — Import orphaned service accounts ──────────────────────────────────
# Service accounts are org-level resources in Confluent Cloud — they are NOT
# deleted when an environment is deleted manually in the UI. If they still
# exist in the org but are absent from Terraform state, the subsequent apply
# will fail with 409 Conflict. This step detects and imports them first.
step "8" "Checking for orphaned service accounts"
info "Querying the Confluent Cloud API for existing service accounts with matching names…"
import_orphaned_sas "${ROOT_DIR}/terraform" "${ENV_SUFFIX}" "${CF_API_KEY}" "${CF_API_SECRET}"
success "Service account check complete."

# ── Step 9 — Terraform plan ────────────────────────────────────────────────────
step "9" "Previewing infrastructure changes"
info "Terraform will list every resource it will create. Nothing is built yet."
printf "\n"

(cd "${ROOT_DIR}/terraform" && terraform plan -var-file=terraform.tfvars -out=tfplan)

printf "\n"
ask_yes_no apply_ok "The plan above looks good — create all resources in Confluent Cloud now?"
if [[ "$apply_ok" != "y" ]]; then
  warn "Apply cancelled. Your terraform.tfvars is saved."
  info "To apply later:  cd terraform && terraform apply -var-file=terraform.tfvars"
  exit 0
fi

# ── Step 10 — Terraform apply ──────────────────────────────────────────────────
step "10" "Provisioning infrastructure  (this takes ~3–5 minutes)"
info "Creating: environment · Schema Registry · Kafka cluster · Flink pool · topics · service accounts · API keys · role bindings · schemas · Flink statement"
printf "\n"

(cd "${ROOT_DIR}/terraform" && terraform apply -input=false tfplan)
success "All Confluent Cloud resources provisioned."

# ── Step 11 — Write .env files ─────────────────────────────────────────────────
step "11" "Writing application credentials"
info "Reading API keys and the cluster endpoint from Terraform outputs…"
printf "\n"

# Strip the "SASL_SSL://" protocol prefix — KafkaJS brokers expect "host:port" only
BOOTSTRAP_RAW=$(cd "${ROOT_DIR}/terraform" && terraform output -raw bootstrap_endpoint)
BOOTSTRAP="${BOOTSTRAP_RAW#SASL_SSL://}"
PROD_KEY=$(cd "${ROOT_DIR}/terraform"    && terraform output -raw producer_api_key)
PROD_SECRET=$(cd "${ROOT_DIR}/terraform" && terraform output -raw producer_api_secret)
CONS_KEY=$(cd "${ROOT_DIR}/terraform"    && terraform output -raw consumer_api_key)
CONS_SECRET=$(cd "${ROOT_DIR}/terraform" && terraform output -raw consumer_api_secret)
SR_URL=$(cd "${ROOT_DIR}/terraform"      && terraform output -raw schema_registry_url)
SR_KEY=$(cd "${ROOT_DIR}/terraform"      && terraform output -raw schema_registry_api_key)
SR_SECRET=$(cd "${ROOT_DIR}/terraform"   && terraform output -raw schema_registry_api_secret)

cat > "${ROOT_DIR}/generator/.env" <<EOF
# Auto-generated by infra-provision.sh — do not commit this file
BOOTSTRAP_SERVERS=${BOOTSTRAP}
PRODUCER_API_KEY=${PROD_KEY}
PRODUCER_API_SECRET=${PROD_SECRET}
SCHEMA_REGISTRY_URL=${SR_URL}
SCHEMA_REGISTRY_API_KEY=${SR_KEY}
SCHEMA_REGISTRY_API_SECRET=${SR_SECRET}
EOF
success "Written: generator/.env"

cat > "${ROOT_DIR}/server/.env" <<EOF
# Auto-generated by infra-provision.sh — do not commit this file
BOOTSTRAP_SERVERS=${BOOTSTRAP}
CONSUMER_API_KEY=${CONS_KEY}
CONSUMER_API_SECRET=${CONS_SECRET}
SCHEMA_REGISTRY_URL=${SR_URL}
SCHEMA_REGISTRY_API_KEY=${SR_KEY}
SCHEMA_REGISTRY_API_SECRET=${SR_SECRET}
PORT=3000
EOF
success "Written: server/.env"

# ── Step 12 — npm install ──────────────────────────────────────────────────────
step "12" "Installing Node.js dependencies"

info "Installing generator packages (kafkajs, @confluentinc/schemaregistry, dotenv)…"
(cd "${ROOT_DIR}/generator" && npm install --silent)
success "generator/ packages installed."

info "Installing dashboard packages (express, socket.io, kafkajs, @confluentinc/schemaregistry, dotenv)…"
(cd "${ROOT_DIR}/server" && npm install --silent)
success "server/ packages installed."

# ── Final summary ──────────────────────────────────────────────────────────────
printf "\n"
c "$GREEN$BOLD"
printf "╔══════════════════════════════════════════════════════════════╗\n"
printf "║         ✔  Infrastructure ready — you're set to go!          ║\n"
printf "╚══════════════════════════════════════════════════════════════╝\n"
res

printf "\n"
c "$DIM"
printf "  Resources created in Confluent Cloud:\n"
printf "    • Environment   : %s\n"    "$ENV_NAME"
printf "    • Kafka cluster : %s  (%s / %s)\n" "$CLUSTER_NAME" "$CF_CLOUD" "$CF_REGION"
printf "    • Topics        : cell-tower-telemetry, anomaly-alerts\n"
printf "    • Flink pool    : celltower-flink-%s  (max %s CFU)\n" "$ENV_SUFFIX" "$FLINK_MAX_CFU"
printf "    • Flink job     : celltower-anomaly-detection-%s\n"    "$ENV_SUFFIX"
res

printf "\n"
c "$BOLD"
printf "  Start the demo:\n"
res
c "$CYAN"
printf "    ./scripts/demo-start.sh       ← starts everything at once\n"
printf "\n"
printf "    ./scripts/generator-start.sh  ← start only the data generator\n"
printf "    ./scripts/dashboard-start.sh  ← start only the web dashboard\n"
res
printf "\n"

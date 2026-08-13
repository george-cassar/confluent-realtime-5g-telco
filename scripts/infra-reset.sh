#!/usr/bin/env bash
# =============================================================================
# scripts/infra-reset.sh
# Hard-reset the demo environment when the Terraform state is out of sync
# with Confluent Cloud (e.g. after a partial destroy or a re-provision
# attempt that left orphaned resources).
#
# What this script does:
#   1. Reads your Confluent Cloud API credentials from terraform.tfvars
#   2. Calls the Confluent REST API to delete the live environment
#      (cascade-deletes: Kafka cluster, topics, Flink pool, schemas)
#   3. Purges any orphaned service accounts for this env_suffix
#   4. Removes all local Terraform state, plan, and lock files
#   5. Removes generated .env files and terraform.tfvars
#
# After this script completes you have a clean slate and can run:
#   ./scripts/infra-provision.sh
#
# Usage:  ./scripts/infra-reset.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

banner "5G Cell Tower Demo — Hard Reset" "Wipe Confluent Cloud resources + local state"

# ── Step 1 — Read credentials from terraform.tfvars ───────────────────────────
step "1" "Reading credentials"

TFVARS="${ROOT_DIR}/terraform/terraform.tfvars"
if [[ ! -f "$TFVARS" ]]; then
  fail "terraform/terraform.tfvars not found.
   Cannot read Confluent Cloud API credentials.
   If you still have your API key & secret, create a minimal tfvars:
     echo 'confluent_cloud_api_key    = \"<key>\"'  > terraform/terraform.tfvars
     echo 'confluent_cloud_api_secret = \"<secret>\"' >> terraform/terraform.tfvars
     echo 'env_suffix = \"dev\"'                       >> terraform/terraform.tfvars
   Then re-run this script."
fi

CF_API_KEY="$(grep '^confluent_cloud_api_key'    "$TFVARS" | sed 's/.*= *"//' | sed 's/"//')"
CF_API_SECRET="$(grep '^confluent_cloud_api_secret' "$TFVARS" | sed 's/.*= *"//' | sed 's/"//')"
ENV_SUFFIX="$(grep '^env_suffix' "$TFVARS" | sed 's/.*= *"//' | sed 's/"//')"
ENV_SUFFIX="${ENV_SUFFIX:-dev}"
ENV_NAME="celltower-env-${ENV_SUFFIX}"

if [[ -z "$CF_API_KEY" || -z "$CF_API_SECRET" ]]; then
  fail "Could not parse confluent_cloud_api_key / confluent_cloud_api_secret from terraform.tfvars."
fi

success "Credentials loaded. env_suffix = '${ENV_SUFFIX}'"

# ── Step 2 — Confirm ──────────────────────────────────────────────────────────
step "2" "Confirmation"

printf "\n"
c "$RED$BOLD"
printf "   ⚠  WARNING: This will PERMANENTLY DELETE the Confluent Cloud\n"
printf "      environment '%s' and everything inside it:\n" "$ENV_NAME"
printf "        • Kafka cluster + all topic data\n"
printf "        • Flink compute pool + statements\n"
printf "        • Schema Registry subjects\n"
printf "        • All API keys for this environment\n"
printf "      It will also wipe local Terraform state and .env files.\n"
printf "      This action CANNOT be undone.\n"
res
printf "\n"

ask_yes_no confirmed "Are you sure you want to perform a hard reset?"
if [[ "$confirmed" != "y" ]]; then
  warn "Reset cancelled. Nothing was changed."
  exit 0
fi

printf "\n"
c "$BOLD"
printf "   → Type  RESET  (all caps) to confirm: "
res
read -r final_confirm
if [[ "$final_confirm" != "RESET" ]]; then
  warn "Confirmation not matched. Reset cancelled."
  exit 0
fi

# ── Step 3 — Delete live Confluent Cloud environment via REST API ──────────────
step "3" "Deleting Confluent Cloud environment via REST API"

info "Looking up environment '${ENV_NAME}' in your Confluent Cloud org…"

# List all environments and find the one matching our display_name
ENV_LIST=$(curl -fsSL \
  -u "${CF_API_KEY}:${CF_API_SECRET}" \
  "https://api.confluent.cloud/org/v2/environments?page_size=100" 2>/dev/null)

ENV_ID=$(printf '%s' "$ENV_LIST" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for e in data.get('data', []):
    if e.get('display_name') == '${ENV_NAME}':
        print(e['id'])
        break
" 2>/dev/null || true)

if [[ -z "$ENV_ID" ]]; then
  warn "Environment '${ENV_NAME}' not found in Confluent Cloud (may already be deleted)."
else
  info "Found environment: ${ENV_ID} — deleting…"
  HTTP_STATUS=$(curl -fsSL -o /dev/null -w "%{http_code}" \
    -X DELETE \
    -u "${CF_API_KEY}:${CF_API_SECRET}" \
    "https://api.confluent.cloud/org/v2/environments/${ENV_ID}" 2>/dev/null || echo "error")

  if [[ "$HTTP_STATUS" == "204" || "$HTTP_STATUS" == "200" ]]; then
    success "Environment ${ENV_ID} deleted. (Kafka cluster, Flink pool, and schemas cascade-deleted.)"
  elif [[ "$HTTP_STATUS" == "404" ]]; then
    info "Environment ${ENV_ID} was already gone."
  else
    warn "DELETE returned HTTP ${HTTP_STATUS}. Check the Confluent Cloud console to confirm deletion."
  fi
fi

# ── Step 4 — Purge orphaned service accounts ──────────────────────────────────
step "4" "Purging orphaned service accounts"
info "Checking for service accounts that survive environment deletion…"
purge_orphaned_sas "${ENV_SUFFIX}" "${CF_API_KEY}" "${CF_API_SECRET}"
success "Service account purge complete."

# ── Step 5 — Wipe local Terraform state ───────────────────────────────────────
step "5" "Wiping local Terraform state"

TF_DIR="${ROOT_DIR}/terraform"

info "Removing terraform.tfstate and terraform.tfstate.backup…"
rm -f "${TF_DIR}/terraform.tfstate"
rm -f "${TF_DIR}/terraform.tfstate.backup"
success "State files removed."

info "Removing tfplan (saved plan file)…"
rm -f "${TF_DIR}/tfplan"
success "Plan file removed."

info "Removing .terraform/ provider cache and .terraform.lock.hcl…"
rm -rf "${TF_DIR}/.terraform"
rm -f  "${TF_DIR}/.terraform.lock.hcl"
success "Provider cache removed."

# ── Step 6 — Remove generated credential files ────────────────────────────────
step "6" "Removing generated credential files"

rm -f "${ROOT_DIR}/generator/.env"
success "Removed: generator/.env"

rm -f "${ROOT_DIR}/server/.env"
success "Removed: server/.env"

rm -f "${TF_DIR}/terraform.tfvars"
success "Removed: terraform/terraform.tfvars"

# ── Done ──────────────────────────────────────────────────────────────────────
printf "\n"
c "$GREEN$BOLD"
printf "╔══════════════════════════════════════════════════════════════╗\n"
printf "║           ✔  Hard reset complete — clean slate!             ║\n"
printf "╚══════════════════════════════════════════════════════════════╝\n"
res

printf "\n"
c "$DIM"
printf "  Deleted from Confluent Cloud:\n"
printf "    • Environment  : %s (%s)\n" "$ENV_NAME" "${ENV_ID:-not found}"
printf "    • Service accounts for suffix '%s'\n" "$ENV_SUFFIX"
printf "\n"
printf "  Removed locally:\n"
printf "    • terraform/terraform.tfstate\n"
printf "    • terraform/terraform.tfstate.backup\n"
printf "    • terraform/tfplan\n"
printf "    • terraform/.terraform/  (provider cache)\n"
printf "    • terraform/.terraform.lock.hcl\n"
printf "    • generator/.env\n"
printf "    • server/.env\n"
printf "    • terraform/terraform.tfvars\n"
res

printf "\n"
c "$CYAN$BOLD"
printf "  ▶  Ready for a fresh provision:\n\n"
printf "       ./scripts/infra-provision.sh\n"
res
printf "\n"

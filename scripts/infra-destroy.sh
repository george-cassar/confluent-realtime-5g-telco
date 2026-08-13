#!/usr/bin/env bash
# =============================================================================
# scripts/infra-destroy.sh
# Interactive wizard — tears down all Confluent Cloud resources created by
# infra-provision.sh and optionally removes local .env files.
#
# Usage:  ./scripts/infra-destroy.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

banner "5G Cell Tower Demo — Infrastructure Teardown" "Remove all Confluent Cloud resources"

# ── Guard: check terraform.tfvars exists ──────────────────────────────────────
step "1" "Checking prerequisites"
require_tools terraform

TFVARS="${ROOT_DIR}/terraform/terraform.tfvars"
if [[ ! -f "$TFVARS" ]]; then
  fail "terraform/terraform.tfvars not found.
   The infrastructure may already have been destroyed, or was never provisioned.
   If you provisioned manually, recreate the file from terraform/terraform.tfvars.example."
fi
success "terraform.tfvars found."

# Read credentials and suffix from tfvars — needed for the SA purge after destroy
_DESTROY_API_KEY="$(grep '^confluent_cloud_api_key' "$TFVARS" | sed 's/.*= *"//' | sed 's/"//')"
_DESTROY_API_SECRET="$(grep '^confluent_cloud_api_secret' "$TFVARS" | sed 's/.*= *"//' | sed 's/"//')"
_DESTROY_SUFFIX="$(grep '^env_suffix' "$TFVARS" | sed 's/.*= *"//' | sed 's/"//')"
_DESTROY_SUFFIX="${_DESTROY_SUFFIX:-dev}"

# Check for active running services and warn
for svc in generator dashboard; do
  if is_running "$svc"; then
    warn "The $svc is currently running. Stop it first with: ./scripts/${svc}-stop.sh"
  fi
done

# ── Show what will be destroyed ────────────────────────────────────────────────
step "2" "Preview — what will be removed"

info "Running terraform plan -destroy so you can see exactly what will be deleted."
info "Nothing is removed yet."
printf "\n"

(cd "${ROOT_DIR}/terraform" && terraform plan -destroy -var-file=terraform.tfvars)

# ── Confirmation — two-step to prevent accidents ───────────────────────────────
step "3" "Confirmation"

printf "\n"
c "$RED$BOLD"
printf "   ⚠  WARNING: This will permanently delete ALL Confluent Cloud resources\n"
printf "      for this demo, including the Kafka cluster and all topic data.\n"
printf "      This action CANNOT be undone.\n"
res
printf "\n"

ask_yes_no confirmed "Are you sure you want to destroy all resources?"
if [[ "$confirmed" != "y" ]]; then
  warn "Teardown cancelled. Nothing was changed."
  exit 0
fi

# Second confirmation — type the word DELETE
printf "\n"
c "$BOLD"
printf "   → Type  DELETE  (all caps) to confirm: "
res
read -r final_confirm
if [[ "$final_confirm" != "DELETE" ]]; then
  warn "Confirmation not matched. Teardown cancelled."
  exit 0
fi

# ── Terraform destroy ─────────────────────────────────────────────────────────
step "4" "Destroying infrastructure"
info "Removing all Confluent Cloud resources. This may take 1–2 minutes…"
printf "\n"

(cd "${ROOT_DIR}/terraform" && terraform destroy -var-file=terraform.tfvars -auto-approve)
success "All Confluent Cloud resources destroyed."

# ── Step 5 — Purge any orphaned service accounts ──────────────────────────────
# Service accounts are org-scoped and occasionally survive terraform destroy
# (e.g. if the environment was deleted manually before running this script).
step "5" "Purging orphaned service accounts"
info "Checking Confluent Cloud for any service accounts that weren't removed by Terraform…"
purge_orphaned_sas "${_DESTROY_SUFFIX}" "${_DESTROY_API_KEY}" "${_DESTROY_API_SECRET}"
success "Service account purge complete."

# ── Remove local credential files ─────────────────────────────────────────────
step "6" "Cleaning up local credential files"

ask_yes_no clean_env "Remove the generated .env files (generator/.env and server/.env)?"
if [[ "$clean_env" == "y" ]]; then
  rm -f "${ROOT_DIR}/generator/.env"
  success "Removed: generator/.env"
  rm -f "${ROOT_DIR}/server/.env"
  success "Removed: server/.env"
else
  info "Credential files kept. Delete them manually when no longer needed."
fi

# ── Step 7 ────────────────────────────────────────────────────────────────────
ask_yes_no clean_tfvars "Remove terraform/terraform.tfvars (contains your Confluent API key)?"
if [[ "$clean_tfvars" == "y" ]]; then
  rm -f "$TFVARS"
  success "Removed: terraform/terraform.tfvars"
else
  info "terraform.tfvars kept. This file is gitignored and will not be committed."
fi

# ── Summary ───────────────────────────────────────────────────────────────────
printf "\n"
c "$GREEN$BOLD"
printf "╔══════════════════════════════════════════════════════════════╗\n"
printf "║              ✔  Teardown complete.                           ║\n"
printf "╚══════════════════════════════════════════════════════════════╝\n"
res

printf "\n"
c "$DIM"
printf "  All Confluent Cloud resources have been deleted.\n"
printf "  To provision again from scratch, run:\n"
res
c "$CYAN$BOLD"
printf "    ./scripts/infra-provision.sh\n"
res
printf "\n"

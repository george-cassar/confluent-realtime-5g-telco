#!/usr/bin/env bash
# =============================================================================
# scripts/generator-start.sh
# Starts the mock telemetry generator in the background.
# Validates the .env file, optionally lets the user tune emission rate,
# and tails the log so you can see messages flowing immediately.
#
# Usage:  ./scripts/generator-start.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

banner "5G Cell Tower Demo — Mock Data Generator" "Produces live telemetry for 5 Madrid towers"

# ── Step 1 — Pre-flight checks ────────────────────────────────────────────────
step "1" "Pre-flight checks"
require_tools node npm

ENV_FILE="${ROOT_DIR}/generator/.env"
check_env_file "$ENV_FILE" "Generator credentials file (generator/.env)"

# Verify the three required variables are present and non-empty
for var in BOOTSTRAP_SERVERS PRODUCER_API_KEY PRODUCER_API_SECRET; do
  if env_var_set "$ENV_FILE" "$var"; then
    success "$var is configured."
  else
    fail "$var is missing or empty in generator/.env
   Re-run: ./scripts/infra-provision.sh  to regenerate credentials."
  fi
done

# ── Step 2 — Check for already-running instance ───────────────────────────────
step "2" "Checking for an existing generator process"

if is_running "generator"; then
  PID=$(read_pid "generator")
  warn "The generator is already running (PID $PID)."
  ask_yes_no restart "Stop the existing instance and start a fresh one?"
  if [[ "$restart" == "y" ]]; then
    stop_service "generator" "Generator"
  else
    info "Keeping existing instance. Nothing was changed."
    exit 0
  fi
else
  success "No existing generator process found."
fi

# ── Step 3 — Install dependencies if needed ───────────────────────────────────
step "3" "Checking Node.js packages"

if [[ ! -d "${ROOT_DIR}/generator/node_modules" ]]; then
  info "node_modules not found — running npm install…"
  (cd "${ROOT_DIR}/generator" && npm install --silent)
  success "Packages installed."
else
  success "node_modules already present. Skipping install."
fi

# ── Step 4 — Configuration ────────────────────────────────────────────────────
step "4" "Generator configuration"

info "The generator will produce a telemetry reading for each of the 16 Madrid"
info "towers every 2 seconds. TWR-002 (Salamanca) and TWR-004 (Moncloa) randomly trigger anomalies."

ask_yes_no use_defaults "Use the default settings and start now?"

LOG_FILE="${ROOT_DIR}/.pids/generator.log"

if [[ "$use_defaults" != "y" ]]; then
  printf "\n"
  c "$DIM"
  printf "   Note: To change emission interval or anomaly probability, edit:\n"
  printf "         generator/producer.js  (INTERVAL_MS and the 0.20 threshold)\n"
  res
  printf "\n"
  info "Starting with current settings from generator/producer.js."
fi

# ── Step 5 — Launch ───────────────────────────────────────────────────────────
step "5" "Starting the generator"

mkdir -p "${ROOT_DIR}/.pids"

# Launch in background, redirect output to log file
(cd "${ROOT_DIR}/generator" && node producer.js >> "$LOG_FILE" 2>&1) &
GEN_PID=$!
save_pid "generator" "$GEN_PID"

# Give it two seconds to connect and verify it didn't immediately crash
sleep 2
if ! kill -0 "$GEN_PID" 2>/dev/null; then
  fail "Generator process exited immediately. Check the log for errors:
   cat ${LOG_FILE}"
fi

success "Generator started  (PID $GEN_PID)"
success "Log file: ${LOG_FILE}"

# ── Step 6 — Live output ──────────────────────────────────────────────────────
step "6" "Live output  (press Ctrl+C to stop watching — generator keeps running)"
printf "\n"
c "$DIM"
printf "   The generator continues running in the background when you press Ctrl+C.\n"
printf "   To stop it:  ./scripts/generator-stop.sh\n\n"
res

# Tail the log — trap Ctrl+C so we exit cleanly without stopping the background process
trap 'printf "\n"; info "Detached from log. Generator is still running (PID $GEN_PID)."; exit 0' INT
tail -f "$LOG_FILE"

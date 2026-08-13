#!/usr/bin/env bash
# =============================================================================
# scripts/dashboard-start.sh
# Starts the Express + Socket.io web dashboard in the background.
# Validates the .env file, confirms the port, installs packages if needed,
# and opens (or prints) the dashboard URL.
#
# Usage:  ./scripts/dashboard-start.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

banner "5G Cell Tower Demo — Web Dashboard" "Real-time anomaly map + AI chat"

# ── Step 1 — Pre-flight checks ────────────────────────────────────────────────
step "1" "Pre-flight checks"
require_tools node npm

ENV_FILE="${ROOT_DIR}/server/.env"
check_env_file "$ENV_FILE" "Dashboard credentials file (server/.env)"

for var in BOOTSTRAP_SERVERS CONSUMER_API_KEY CONSUMER_API_SECRET; do
  if env_var_set "$ENV_FILE" "$var"; then
    success "$var is configured."
  else
    fail "$var is missing or empty in server/.env
   Re-run: ./scripts/infra-provision.sh  to regenerate credentials."
  fi
done

# ── Step 2 — Check for already-running instance ───────────────────────────────
step "2" "Checking for an existing dashboard process"

if is_running "dashboard"; then
  PID=$(read_pid "dashboard")
  warn "The dashboard is already running (PID $PID)."
  ask_yes_no restart "Stop the existing instance and start a fresh one?"
  if [[ "$restart" == "y" ]]; then
    stop_service "dashboard" "Dashboard"
  else
    # Read the configured port and print the URL even if not restarting
    PORT=$(grep -E "^PORT=" "$ENV_FILE" | cut -d= -f2 || echo "3000")
    info "Dashboard is already available at: http://localhost:${PORT}"
    exit 0
  fi
else
  success "No existing dashboard process found."
fi

# ── Step 3 — Install dependencies if needed ───────────────────────────────────
step "3" "Checking Node.js packages"

if [[ ! -d "${ROOT_DIR}/server/node_modules" ]]; then
  info "node_modules not found — running npm install…"
  (cd "${ROOT_DIR}/server" && npm install --silent)
  success "Packages installed."
else
  success "node_modules already present. Skipping install."
fi

# ── Step 4 — Port configuration ───────────────────────────────────────────────
step "4" "Dashboard configuration"

CURRENT_PORT=$(grep -E "^PORT=" "$ENV_FILE" | cut -d= -f2 || echo "3000")

info "The dashboard will be available in your browser at: http://localhost:${CURRENT_PORT}"
info "The page shows a live Leaflet map of Madrid with 5 cell towers."
info "Tower markers turn red when an anomaly is detected by Flink."

ask_yes_no change_port "Change the port? (default: ${CURRENT_PORT})"
if [[ "$change_port" == "y" ]]; then
  ask_required NEW_PORT "New port number" \
    "Enter a port number between 1024 and 65535 (e.g. 8080)"
  # Update the PORT line in server/.env
  if grep -qE "^PORT=" "$ENV_FILE"; then
    # Replace existing line (works on both macOS and Linux)
    sed -i.bak "s|^PORT=.*|PORT=${NEW_PORT}|" "$ENV_FILE" && rm -f "${ENV_FILE}.bak"
  else
    echo "PORT=${NEW_PORT}" >> "$ENV_FILE"
  fi
  CURRENT_PORT="$NEW_PORT"
  success "Port updated to $CURRENT_PORT in server/.env"
fi

# ── Step 5 — Launch ───────────────────────────────────────────────────────────
step "5" "Starting the dashboard server"

LOG_FILE="${ROOT_DIR}/.pids/dashboard.log"
mkdir -p "${ROOT_DIR}/.pids"

(cd "${ROOT_DIR}/server" && node server.js >> "$LOG_FILE" 2>&1) &
DASH_PID=$!
save_pid "dashboard" "$DASH_PID"

# Wait up to 5 seconds for the server to bind
printf "   "
for i in {1..5}; do
  sleep 1
  if ! kill -0 "$DASH_PID" 2>/dev/null; then
    printf "\n"
    fail "Dashboard process exited immediately. Check the log:
   cat ${LOG_FILE}"
  fi
  printf "."
done
printf "\n\n"

success "Dashboard started  (PID $DASH_PID)"
success "Log file: ${LOG_FILE}"

# ── Step 6 — Open browser ─────────────────────────────────────────────────────
step "6" "Opening the dashboard"

URL="http://localhost:${CURRENT_PORT}"

printf "\n"
c "$CYAN$BOLD"
printf "   Dashboard URL:  %s\n" "$URL"
res
printf "\n"

# Try to open a browser automatically on macOS and Linux
if command -v open &>/dev/null; then
  open "$URL"
  success "Browser opened automatically."
elif command -v xdg-open &>/dev/null; then
  xdg-open "$URL" &>/dev/null &
  success "Browser opened automatically."
else
  info "Open your browser and navigate to: $URL"
fi

printf "\n"
c "$DIM"
printf "  The dashboard runs in the background.\n"
printf "  To stop it:  ./scripts/dashboard-stop.sh\n"
res
printf "\n"

#!/usr/bin/env bash
# =============================================================================
# scripts/demo-start.sh
# One-command launcher — starts both the mock data generator and the web
# dashboard, then prints their URLs and live status.
#
# Usage:  ./scripts/demo-start.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

banner "5G Cell Tower Anomaly Demo — Start Everything" "Madrid · Confluent + Flink + Leaflet"

# ── Step 1 — Pre-flight checks ────────────────────────────────────────────────
step "1" "Pre-flight checks"
require_tools node npm

# Check both .env files exist and have the required variables
GEN_ENV="${ROOT_DIR}/generator/.env"
SRV_ENV="${ROOT_DIR}/server/.env"

check_env_file "$GEN_ENV" "Generator credentials (generator/.env)"
check_env_file "$SRV_ENV" "Dashboard credentials (server/.env)"

for var in BOOTSTRAP_SERVERS PRODUCER_API_KEY PRODUCER_API_SECRET; do
  env_var_set "$GEN_ENV" "$var" || fail "$var missing in generator/.env — run ./scripts/infra-provision.sh"
done
for var in BOOTSTRAP_SERVERS CONSUMER_API_KEY CONSUMER_API_SECRET; do
  env_var_set "$SRV_ENV" "$var" || fail "$var missing in server/.env — run ./scripts/infra-provision.sh"
done
success "All required credentials are present."

# ── Step 2 — Handle already-running services ──────────────────────────────────
step "2" "Checking for already-running services"

RESTART_NEEDED=false
for svc in generator dashboard; do
  if is_running "$svc"; then
    warn "$svc is already running (PID $(read_pid "$svc"))."
    RESTART_NEEDED=true
  else
    success "$svc is not running."
  fi
done

if $RESTART_NEEDED; then
  ask_yes_no do_restart "Stop existing services and restart everything fresh?"
  if [[ "$do_restart" == "y" ]]; then
    stop_service "generator" "Generator"
    stop_service "dashboard" "Dashboard"
  else
    warn "Keeping existing processes. Nothing was changed."
    exit 0
  fi
fi

# ── Step 3 — Install dependencies ─────────────────────────────────────────────
step "3" "Checking Node.js packages"

if [[ ! -d "${ROOT_DIR}/generator/node_modules" ]]; then
  info "Installing generator packages…"
  (cd "${ROOT_DIR}/generator" && npm install --silent)
  success "generator/ packages installed."
else
  success "generator/ packages already installed."
fi

if [[ ! -d "${ROOT_DIR}/server/node_modules" ]]; then
  info "Installing dashboard packages…"
  (cd "${ROOT_DIR}/server" && npm install --silent)
  success "server/ packages installed."
else
  success "server/ packages already installed."
fi

# ── Step 4 — Start services ───────────────────────────────────────────────────
step "4" "Starting services"

mkdir -p "${ROOT_DIR}/.pids"

GEN_LOG="${ROOT_DIR}/.pids/generator.log"
DASH_LOG="${ROOT_DIR}/.pids/dashboard.log"

# Start generator
info "Launching mock data generator…"
(cd "${ROOT_DIR}/generator" && node producer.js >> "$GEN_LOG" 2>&1) &
GEN_PID=$!
save_pid "generator" "$GEN_PID"
sleep 1
kill -0 "$GEN_PID" 2>/dev/null || fail "Generator failed to start. Check: cat ${GEN_LOG}"
success "Generator started  (PID $GEN_PID)"

# Start dashboard
info "Launching web dashboard…"
(cd "${ROOT_DIR}/server" && node server.js >> "$DASH_LOG" 2>&1) &
DASH_PID=$!
save_pid "dashboard" "$DASH_PID"

# Wait for dashboard to bind (up to 5 s)
printf "   Waiting for dashboard to become ready"
for i in {1..5}; do
  sleep 1
  kill -0 "$DASH_PID" 2>/dev/null || { printf "\n"; fail "Dashboard failed to start. Check: cat ${DASH_LOG}"; }
  printf "."
done
printf "\n\n"
success "Dashboard started  (PID $DASH_PID)"

# ── Step 5 — Open browser ─────────────────────────────────────────────────────
PORT=$(grep -E "^PORT=" "$SRV_ENV" | cut -d= -f2 || echo "3000")
URL="http://localhost:${PORT}"

step "5" "Demo is live"

printf "\n"
c "$GREEN$BOLD"
printf "   ✔  Both services are running.\n"
res
printf "\n"
c "$BOLD"
printf "   %-20s %s\n"  "Dashboard:"  "$URL"
printf "   %-20s %s\n"  "Generator log:"  "$GEN_LOG"
printf "   %-20s %s\n"  "Dashboard log:"  "$DASH_LOG"
res
printf "\n"

if command -v open &>/dev/null; then
  open "$URL"
  success "Browser opened automatically."
elif command -v xdg-open &>/dev/null; then
  xdg-open "$URL" &>/dev/null &
  success "Browser opened automatically."
else
  c "$CYAN"; printf "   Open your browser at: %s\n" "$URL"; res
fi

printf "\n"
c "$DIM"
printf "  What to expect:\n"
printf "    • The map shows 16 towers in Madrid across all major districts\n"
printf "    • Tower stats (temperature, signal, CPU) update every 2 seconds\n"
printf "    • TWR-002 (Salamanca) and TWR-004 (Moncloa) randomly trigger anomalies\n"
printf "    • Use the AI chat panel to ask questions about the stream\n"
printf "\n"
printf "  To stop the demo:\n"
printf "    ./scripts/demo-stop.sh\n"
res
printf "\n"

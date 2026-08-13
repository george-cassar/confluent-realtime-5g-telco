#!/usr/bin/env bash
# =============================================================================
# scripts/demo-stop.sh
# Gracefully stops both the mock data generator and the web dashboard.
#
# Usage:  ./scripts/demo-stop.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

banner "5G Cell Tower Demo — Stop All Services"

step "1" "Stopping the mock data generator"
stop_service "generator" "Generator"

step "2" "Stopping the web dashboard"
stop_service "dashboard" "Dashboard"

printf "\n"
c "$GREEN$BOLD"
printf "   ✔  All services stopped.\n"
res
printf "\n"
c "$DIM"
printf "  To start again:         ./scripts/demo-start.sh\n"
printf "  To destroy cloud infra: ./scripts/infra-destroy.sh\n"
res
printf "\n"

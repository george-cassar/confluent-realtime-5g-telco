#!/usr/bin/env bash
# =============================================================================
# scripts/dashboard-stop.sh
# Gracefully stops the web dashboard that was started by dashboard-start.sh.
#
# Usage:  ./scripts/dashboard-stop.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

banner "5G Cell Tower Demo — Stop Dashboard"

step "1" "Stopping the web dashboard"
stop_service "dashboard" "Dashboard"

printf "\n"
c "$DIM"
printf "  To start it again:  ./scripts/dashboard-start.sh\n"
res
printf "\n"

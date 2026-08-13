#!/usr/bin/env bash
# =============================================================================
# scripts/generator-stop.sh
# Gracefully stops the mock telemetry generator that was started by
# generator-start.sh.
#
# Usage:  ./scripts/generator-stop.sh
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"

banner "5G Cell Tower Demo — Stop Generator"

step "1" "Stopping the mock data generator"
stop_service "generator" "Generator"

printf "\n"
c "$DIM"
printf "  To start it again:  ./scripts/generator-start.sh\n"
res
printf "\n"

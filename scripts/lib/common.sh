#!/usr/bin/env bash
# =============================================================================
# scripts/lib/common.sh
# Shared helpers sourced by every script in scripts/.
# Do NOT execute directly.
# =============================================================================

# ── Colour codes ───────────────────────────────────────────────────────────────
BOLD="\033[1m"
DIM="\033[2m"
CYAN="\033[0;36m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
RESET="\033[0m"

# Only emit colour when writing to a real terminal
if [ -t 1 ]; then COLOUR=true; else COLOUR=false; fi
c()   { $COLOUR && printf "%b" "$1" || true; }
res() { $COLOUR && printf "%b" "$RESET" || true; }

# ── Print helpers ──────────────────────────────────────────────────────────────
# banner <title> <subtitle>
banner() {
  local title="${1:-5G Cell Tower Anomaly Demo}"
  local subtitle="${2:-}"
  printf "\n"
  c "$CYAN$BOLD"
  printf "╔══════════════════════════════════════════════════════════════╗\n"
  printf "║  %-60s║\n" "  $title"
  if [[ -n "$subtitle" ]]; then
    printf "║  %-60s║\n" "  $subtitle"
  fi
  printf "╚══════════════════════════════════════════════════════════════╝\n"
  res
  printf "\n"
}

# step <number> <description>
step() {
  printf "\n"
  c "$BOLD$CYAN"
  printf "▶  Step %s — %s\n" "$1" "$2"
  res
  printf "\n"
}

info()    { c "$DIM";    printf "   ℹ  %s\n" "$*"; res; }
success() { c "$GREEN";  printf "   ✔  %s\n" "$*"; res; }
warn()    { c "$YELLOW"; printf "   ⚠  %s\n" "$*"; res; }
fail()    { c "$RED";    printf "\n   ✘  ERROR: %s\n\n" "$*"; res; exit 1; }

# ── Input helpers ──────────────────────────────────────────────────────────────
# All ask_* functions accept the TARGET VARIABLE NAME as $1 and write into it
# using eval-based assignment — fully compatible with bash 3.2+.
# (local -n namerefs require bash 4.3+ and break on macOS system bash.)

# ask_required <varname> <prompt> <explanation>
ask_required() {
  local _ar_var="$1"
  local _ar_prompt="$2"
  local _ar_explain="$3"
  local _ar_val=""
  printf "\n"
  info "$_ar_explain"
  while true; do
    c "$BOLD"; printf "   → %s: " "$_ar_prompt"; res
    read -r _ar_val
    if [[ -n "$_ar_val" ]]; then
      eval "${_ar_var}=\$_ar_val"
      break
    fi
    warn "This field is required. Please enter a value."
  done
}

# ask_secret <varname> <prompt> <explanation>
ask_secret() {
  local _as_var="$1"
  local _as_prompt="$2"
  local _as_explain="$3"
  local _as_val=""
  printf "\n"
  info "$_as_explain"
  while true; do
    c "$BOLD"; printf "   → %s: " "$_as_prompt"; res
    read -rs _as_val; printf "\n"
    if [[ -n "$_as_val" ]]; then
      eval "${_as_var}=\$_as_val"
      break
    fi
    warn "This field is required. Please enter a value."
  done
}

# ask_with_default <varname> <prompt> <default> <explanation>
ask_with_default() {
  local _ad_var="$1"
  local _ad_prompt="$2"
  local _ad_default="$3"
  local _ad_explain="$4"
  local _ad_val=""
  printf "\n"
  info "$_ad_explain"
  c "$BOLD"; printf "   → %s [default: %s]: " "$_ad_prompt" "$_ad_default"; res
  read -r _ad_val
  if [[ -z "$_ad_val" ]]; then
    _ad_val="$_ad_default"
    info "Using default: $_ad_default"
  fi
  eval "${_ad_var}=\$_ad_val"
}

# ask_choice <varname> <prompt> <explanation> <option1> [option2 ...]
# Sets varname to the chosen option string.
ask_choice() {
  local _ac_var="$1"
  local _ac_prompt="$2"
  local _ac_explain="$3"
  shift 3
  local _ac_options=("$@")
  local _ac_num=""
  local _ac_chosen=""
  printf "\n"
  info "$_ac_explain"
  printf "\n"
  local i
  for i in "${!_ac_options[@]}"; do
    c "$BOLD"; printf "   %d) %s\n" "$((i+1))" "${_ac_options[$i]}"; res
  done
  while true; do
    c "$BOLD"; printf "\n   → %s (enter number): " "$_ac_prompt"; res
    read -r _ac_num
    if [[ "$_ac_num" =~ ^[0-9]+$ ]] && (( _ac_num >= 1 && _ac_num <= ${#_ac_options[@]} )); then
      _ac_chosen="${_ac_options[$((_ac_num-1))]}"
      info "Selected: $_ac_chosen"
      eval "${_ac_var}=\$_ac_chosen"
      break
    fi
    warn "Please enter a number between 1 and ${#_ac_options[@]}."
  done
}

# ask_yes_no <varname> <prompt>
# Sets varname to "y" or "n".
ask_yes_no() {
  local _yn_var="$1"
  local _yn_prompt="$2"
  local _yn_raw=""
  local _yn_result=""
  c "$BOLD"; printf "\n   → %s [y/N]: " "$_yn_prompt"; res
  read -r _yn_raw
  # Portable lowercase — bash 3.2 does not support ${var,,}
  _yn_raw="$(printf '%s' "$_yn_raw" | tr '[:upper:]' '[:lower:]')"
  if [[ "$_yn_raw" == "y" || "$_yn_raw" == "yes" ]]; then
    _yn_result="y"
  else
    _yn_result="n"
  fi
  eval "${_yn_var}=\$_yn_result"
}

# ── Environment file helpers ───────────────────────────────────────────────────

# check_env_file <path> <label>
# Exits with a clear message if the .env file is missing.
check_env_file() {
  local path="$1"
  local label="$2"
  if [[ ! -f "$path" ]]; then
    fail "$label not found at: $path
   Has the infrastructure been provisioned yet?
   Run:  ./scripts/infra-provision.sh"
  fi
}

# env_var_set <file> <varname>
# Returns 0 if varname is non-empty in the env file, 1 otherwise.
env_var_set() {
  local file="$1"
  local varname="$2"
  grep -qE "^${varname}=.+" "$file" 2>/dev/null
}

# ── OS / distro detection ──────────────────────────────────────────────────────

# _os_type  →  "macos" | "debian" | "redhat" | "unknown"
_os_type() {
  case "$(uname -s)" in
    Darwin) echo "macos" ;;
    Linux)
      if [[ -f /etc/os-release ]]; then
        local _id_like
        # shellcheck disable=SC1091
        _id_like="$(. /etc/os-release && echo "${ID_LIKE:-${ID:-}}")"
        case "$_id_like" in
          *debian*|*ubuntu*) echo "debian" ;;
          *rhel*|*fedora*|*centos*) echo "redhat" ;;
          *) echo "unknown" ;;
        esac
      else
        echo "unknown"
      fi ;;
    *) echo "unknown" ;;
  esac
}

# ── Auto-installer helpers ─────────────────────────────────────────────────────

# _install_brew  (macOS only)
_install_brew() {
  if command -v brew &>/dev/null; then return 0; fi
  info "Homebrew not found — installing it now (this is the standard macOS package manager)…"
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  # Add brew to PATH for the rest of this shell session
  if [[ -x /opt/homebrew/bin/brew ]]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
  elif [[ -x /usr/local/bin/brew ]]; then
    eval "$(/usr/local/bin/brew shellenv)"
  fi
}

# _install_node  →  installs node + npm via nvm (all platforms), brew/apt/dnf fallback
_install_node() {
  local os; os="$(_os_type)"

  info "Attempting to install Node.js via nvm (Node Version Manager)…"
  info "nvm installs Node.js into your home directory — no admin password needed."

  # Download and run the nvm installer
  curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash

  # Source nvm into the current shell session
  export NVM_DIR="${HOME}/.nvm"
  # shellcheck disable=SC1091
  [[ -s "${NVM_DIR}/nvm.sh" ]] && source "${NVM_DIR}/nvm.sh"

  if [[ -s "${NVM_DIR}/nvm.sh" ]]; then
    nvm install --lts
    nvm use --lts
    success "Node.js $(node --version) installed via nvm."
    return 0
  fi

  # nvm fallback — use system package manager
  warn "nvm install did not activate in this shell session. Trying system package manager…"
  case "$os" in
    macos)
      _install_brew
      brew install node
      ;;
    debian)
      sudo apt-get update -qq
      sudo apt-get install -y nodejs npm
      ;;
    redhat)
      sudo dnf install -y nodejs npm
      ;;
    *)
      fail "Cannot auto-install Node.js on this system.
   Please install it manually from: https://nodejs.org"
      ;;
  esac
}

# _install_terraform  →  brew (macOS) or HashiCorp repo (Linux)
_install_terraform() {
  local os; os="$(_os_type)"
  case "$os" in
    macos)
      _install_brew
      info "Installing Terraform via Homebrew…"
      brew tap hashicorp/tap
      brew install hashicorp/tap/terraform
      ;;
    debian)
      info "Installing Terraform via the official HashiCorp apt repository…"
      sudo apt-get update -qq
      sudo apt-get install -y gnupg software-properties-common curl
      curl -fsSL https://apt.releases.hashicorp.com/gpg \
        | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
      echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
        | sudo tee /etc/apt/sources.list.d/hashicorp.list > /dev/null
      sudo apt-get update -qq
      sudo apt-get install -y terraform
      ;;
    redhat)
      info "Installing Terraform via the official HashiCorp dnf repository…"
      sudo dnf install -y dnf-plugins-core
      sudo dnf config-manager --add-repo \
        https://rpm.releases.hashicorp.com/fedora/hashicorp.repo
      sudo dnf install -y terraform
      ;;
    *)
      fail "Cannot auto-install Terraform on this system.
   Please install it manually from: https://developer.hashicorp.com/terraform/install"
      ;;
  esac
}

# ── Tool-check + auto-install ──────────────────────────────────────────────────

# require_tools <tool1> [tool2 ...]
# For each missing tool, asks permission and installs it.
# Exits if a tool is still missing after the install attempt.
require_tools() {
  local missing=()
  local tool
  for tool in "$@"; do
    if command -v "$tool" &>/dev/null; then
      success "$tool found: $(command -v "$tool")"
    else
      missing+=("$tool")
      warn "$tool not found."
    fi
  done

  (( ${#missing[@]} == 0 )) && return 0

  # Describe what is missing and why
  printf "\n"
  c "$BOLD"
  printf "   The following tools are required but not installed:\n\n"
  local t
  for t in "${missing[@]}"; do
    case "$t" in
      terraform) printf "   • terraform — provisions Confluent Cloud infrastructure\n" ;;
      node)      printf "   • node      — runs the mock data generator and web dashboard\n" ;;
      npm)       printf "   • npm       — installs Node.js packages\n" ;;
      *)         printf "   • %s\n" "$t" ;;
    esac
  done
  res
  printf "\n"

  # Ask permission to install
  local do_install
  ask_yes_no do_install "Install the missing tools automatically now?"
  if [[ "$do_install" != "y" ]]; then
    printf "\n"
    c "$DIM"
    printf "   Install them manually, then run this script again:\n"
    printf "     • terraform : https://developer.hashicorp.com/terraform/install\n"
    printf "     • node / npm: https://nodejs.org\n"
    res
    printf "\n"
    exit 1
  fi

  # Install each missing tool (node+npm handled together to avoid running twice)
  local node_done=false
  for tool in "${missing[@]}"; do
    printf "\n"
    c "$BOLD$CYAN"
    printf "   ── Installing: %s ──\n" "$tool"
    res
    printf "\n"
    case "$tool" in
      node|npm)
        if ! $node_done; then
          _install_node
          node_done=true
        fi
        ;;
      terraform)
        _install_terraform
        ;;
      *)
        fail "No auto-installer available for '$tool'. Please install it manually."
        ;;
    esac
  done

  # Re-source nvm in case it was just installed
  export NVM_DIR="${HOME}/.nvm"
  # shellcheck disable=SC1091
  [[ -s "${NVM_DIR}/nvm.sh" ]] && source "${NVM_DIR}/nvm.sh" || true

  # Re-verify
  local still_missing=()
  for tool in "${missing[@]}"; do
    if command -v "$tool" &>/dev/null; then
      success "$tool is now available: $(command -v "$tool")"
    else
      still_missing+=("$tool")
    fi
  done

  if (( ${#still_missing[@]} > 0 )); then
    printf "\n"
    c "$YELLOW"
    printf "   ⚠  The following tools were installed but are not yet visible in this\n"
    printf "      terminal session. This is normal — the installer updated your shell\n"
    printf "      profile (.bashrc / .zshrc) but the current session has not reloaded.\n\n"
    printf "      Please run one of:\n\n"
    printf "        source ~/.zshrc    (if you use zsh — default on macOS)\n"
    printf "        source ~/.bashrc   (if you use bash)\n\n"
    printf "      Then re-run this script.\n"
    res
    printf "\n"
    exit 1
  fi
}

# ── Confluent Cloud SA helpers ────────────────────────────────────────────────
#
# These helpers call the Confluent Cloud IAM REST API directly (using the
# Cloud-level API key / secret) to manage service accounts that are
# org-scoped and survive environment deletion.
#
# Both functions require CF_API_KEY and CF_API_SECRET to be set in the caller.

# _cf_list_sas <api_key> <api_secret>
# Prints a JSON array of all service accounts in the org.
_cf_list_sas() {
  local key="$1" secret="$2"
  curl -fsSL -u "${key}:${secret}" \
    "https://api.confluent.cloud/iam/v2/service-accounts?page_size=100" \
    2>/dev/null
}

# import_orphaned_sas <tf_dir> <env_suffix> <api_key> <api_secret>
#
# For each of the three well-known SA display-names used by this project
# (celltower-{producer,consumer,admin}-sa-<suffix>), checks whether the SA
# already exists in Confluent Cloud but is absent from Terraform state.
# If found, runs "terraform import" so the subsequent apply can adopt it
# instead of failing with 409 Conflict.
import_orphaned_sas() {
  local tf_dir="$1"
  local suffix="$2"
  local key="$3"
  local secret="$4"

  local sa_json
  sa_json="$(_cf_list_sas "$key" "$secret")" || {
    warn "Could not reach Confluent Cloud API — skipping SA import check."
    return 0
  }

  # Map of terraform resource address → display_name
  local resources="producer:celltower-producer-sa-${suffix} consumer:celltower-consumer-sa-${suffix} cluster_admin:celltower-admin-sa-${suffix}"

  local pair tf_addr display_name sa_id already_in_state
  for pair in $resources; do
    tf_addr="confluent_service_account.${pair%%:*}"
    display_name="${pair#*:}"

    # Check if already tracked in state
    already_in_state="$(cd "$tf_dir" && terraform state list 2>/dev/null | grep -Fx "$tf_addr" || true)"
    if [[ -n "$already_in_state" ]]; then
      info "SA '${display_name}' already in Terraform state — skipping import."
      continue
    fi

    # Look up by display_name in the org
    sa_id="$(printf '%s' "$sa_json" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for sa in data.get('data', []):
    if sa.get('display_name') == '${display_name}':
        print(sa['id'])
        break
" 2>/dev/null || true)"

    if [[ -z "$sa_id" ]]; then
      info "SA '${display_name}' not found in Confluent Cloud — will be created fresh."
      continue
    fi

    warn "Found orphaned SA '${display_name}' (${sa_id}) — importing into Terraform state."
    (cd "$tf_dir" && terraform import -var-file=terraform.tfvars "${tf_addr}" "${sa_id}") && \
      success "Imported: ${tf_addr} = ${sa_id}" || \
      warn "Import failed for ${tf_addr} — apply may still encounter a 409. Check manually."
  done
}

# purge_orphaned_sas <env_suffix> <api_key> <api_secret>
#
# Deletes any service accounts whose display_name matches the project pattern
# for the given suffix that still exist in Confluent Cloud after a destroy.
# Called by infra-destroy.sh after terraform destroy completes.
purge_orphaned_sas() {
  local suffix="$1"
  local key="$2"
  local secret="$3"

  local sa_json
  sa_json="$(_cf_list_sas "$key" "$secret")" || {
    warn "Could not reach Confluent Cloud API — skipping SA purge."
    return 0
  }

  local names="celltower-producer-sa-${suffix} celltower-consumer-sa-${suffix} celltower-admin-sa-${suffix}"
  local display_name sa_id http_status

  for display_name in $names; do
    sa_id="$(printf '%s' "$sa_json" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for sa in data.get('data', []):
    if sa.get('display_name') == '${display_name}':
        print(sa['id'])
        break
" 2>/dev/null || true)"

    if [[ -z "$sa_id" ]]; then
      info "SA '${display_name}' not found — already gone."
      continue
    fi

    info "Deleting orphaned SA '${display_name}' (${sa_id})…"
    http_status="$(curl -fsSL -o /dev/null -w "%{http_code}" \
      -X DELETE -u "${key}:${secret}" \
      "https://api.confluent.cloud/iam/v2/service-accounts/${sa_id}" 2>/dev/null || echo "error")"

    if [[ "$http_status" == "204" || "$http_status" == "200" ]]; then
      success "Deleted SA '${display_name}' (${sa_id})."
    elif [[ "$http_status" == "404" ]]; then
      info "SA '${display_name}' was already deleted."
    else
      warn "DELETE returned HTTP ${http_status} for SA '${display_name}' — check manually in Confluent Cloud UI."
    fi
  done
}

# ── PID file helpers ───────────────────────────────────────────────────────────
PIDS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../" && pwd)/.pids"

# pid_file <service-name>  →  prints the path to that service's PID file
pid_file() { echo "${PIDS_DIR}/$1.pid"; }

# save_pid <service-name> <pid>
save_pid() {
  mkdir -p "$PIDS_DIR"
  echo "$2" > "$(pid_file "$1")"
}

# read_pid <service-name>  →  prints the saved PID, or empty string
read_pid() {
  local f; f="$(pid_file "$1")"
  [[ -f "$f" ]] && cat "$f" || echo ""
}

# remove_pid <service-name>
remove_pid() { rm -f "$(pid_file "$1")"; }

# is_running <service-name>  →  0 if process is alive, 1 otherwise
is_running() {
  local pid; pid="$(read_pid "$1")"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

# stop_service <service-name> <display-label>
stop_service() {
  local name="$1"
  local label="$2"
  local pid; pid="$(read_pid "$name")"

  if [[ -z "$pid" ]]; then
    warn "$label is not recorded as running (no PID file found)."
    return 0
  fi

  if ! kill -0 "$pid" 2>/dev/null; then
    warn "$label (PID $pid) is no longer running — cleaning up stale PID file."
    remove_pid "$name"
    return 0
  fi

  info "Sending SIGTERM to $label (PID $pid)…"
  kill -TERM "$pid"

  # Wait up to 10 seconds for clean exit
  local waited=0
  while kill -0 "$pid" 2>/dev/null && (( waited < 10 )); do
    sleep 1; (( waited++ ))
  done

  if kill -0 "$pid" 2>/dev/null; then
    warn "Process did not stop cleanly — sending SIGKILL."
    kill -KILL "$pid" 2>/dev/null || true
  fi

  remove_pid "$name"
  success "$label stopped."
}

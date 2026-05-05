#!/bin/bash

REQUIRED_IMPORTS="import requests; import google.cloud.storage"

log_runner_message() {
  echo "$(date -Iseconds) - RUNNER - $*" >> "$LOG_FILE"
}

activate_if_usable() {
  local activate_file="$1"
  local label="$2"

  if [ ! -f "$activate_file" ]; then
    return 1
  fi

  # shellcheck disable=SC1090
  source "$activate_file"

  if python3 -c "$REQUIRED_IMPORTS" >> "$LOG_FILE" 2>&1; then
    log_runner_message "Using $label: $(command -v python3)"
    python3 --version >> "$LOG_FILE" 2>&1
    return 0
  fi

  log_runner_message "$label is present but missing required dependencies; trying next environment."
  deactivate 2>/dev/null || true
  return 1
}

prepare_orchestrator_env() {
  mkdir -p "$(dirname "$LOG_FILE")"
  cd "$PROJECT_ROOT" || exit 1

  local default_venv="$PROJECT_ROOT/.venv/bin/activate"
  local fallback_venv="$HOME/scraper-env/bin/activate"

  if activate_if_usable "$default_venv" "$PROJECT_ROOT/.venv"; then
    :
  elif activate_if_usable "$fallback_venv" "$HOME/scraper-env"; then
    :
  elif python3 -c "$REQUIRED_IMPORTS" >> "$LOG_FILE" 2>&1; then
    log_runner_message "Using system python3: $(command -v python3)"
    python3 --version >> "$LOG_FILE" 2>&1
  else
    log_runner_message "No usable Python environment found. Run: python3 -m venv .venv && . .venv/bin/activate && python -m pip install -r requirements.txt"
    exit 1
  fi

  export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
}

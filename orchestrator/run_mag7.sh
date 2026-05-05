#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$PROJECT_ROOT/orchestrator/logs/orchestrator_mag7.log"

source "$SCRIPT_DIR/run_common.sh"
prepare_orchestrator_env
export CHROME_BINARY="${CHROME_BINARY:-/usr/bin/chromium}"
python3 orchestrator/orchestrator_mag7.py >> "$LOG_FILE" 2>&1

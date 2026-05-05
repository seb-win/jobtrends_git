#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$PROJECT_ROOT/orchestrator/logs/orchestrator_tech.log"

source "$SCRIPT_DIR/run_common.sh"
prepare_orchestrator_env
python3 orchestrator/orchestrator_tech.py >> "$LOG_FILE" 2>&1

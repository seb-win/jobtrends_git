#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_VENV="$PROJECT_ROOT/.venv/bin/activate"
FALLBACK_VENV="$HOME/scraper-env/bin/activate"
LOG_FILE="$PROJECT_ROOT/orchestrator/logs/orchestrator_consult.log"

cd "$PROJECT_ROOT" || exit 1
mkdir -p "$(dirname "$LOG_FILE")"

if [ -f "$DEFAULT_VENV" ]; then
  # shellcheck disable=SC1090
  source "$DEFAULT_VENV"
elif [ -f "$FALLBACK_VENV" ]; then
  # shellcheck disable=SC1090
  source "$FALLBACK_VENV"
fi

export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
python3 orchestrator/orchestrator_consult.py >> "$LOG_FILE" 2>&1

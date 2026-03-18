#!/bin/bash

PROJECT_ROOT="/home/sebwinkler84/project_root"
VENV_PATH="/home/sebwinkler84/scraper-env/bin/activate"

########## hier den Logfile-Namen eingeben
LOG_FILE="${PROJECT_ROOT}/orchestrator/logs/orchestrator_finance.log"

# Navigate to the project root
cd "$PROJECT_ROOT" || exit 1

# Activate the virtual environment
source "$VENV_PATH"

# Set PYTHONPATH to include the project root
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Run the orchestrator script
######### hier den Orchestrator angeben
python3 orchestrator/orchestrator_finance.py >> "$LOG_FILE" 2>&1
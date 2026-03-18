
import os
import json
import subprocess
import logging
from pathlib import Path
from util_v2 import run_script_with_retries

########
# Es muss nur der Name des Script JSON im scripts_json_path geändert werden

# Resolve CONFIG_PATH based on the script's directory
CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "configs/"))

# Construct full paths for the JSON files
scripts_json_path = os.path.join(CONFIG_PATH, "consult.json") 
orchestrator_json_path = os.path.join(CONFIG_PATH, "orchestrator.json")

# Verify that the files exist
if not os.path.exists(scripts_json_path):
    raise FileNotFoundError(f"File not found: {scripts_json_path}")
if not os.path.exists(orchestrator_json_path):
    raise FileNotFoundError(f"File not found: {orchestrator_json_path}")

# Load the JSON files
with open(scripts_json_path, "r") as f:
    SCRAPING_JOBS = json.load(f)

with open(orchestrator_json_path, "r") as f:
    ORCHESTRATOR_CONFIG = json.load(f)


# Logging setup
logging.basicConfig(
    filename=ORCHESTRATOR_CONFIG['logging']['file'],
    level=getattr(logging, ORCHESTRATOR_CONFIG['logging']['level'].upper()),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    logging.info("Orchestrator started.")
    for job in SCRAPING_JOBS:
        try:
            logging.info(f"Starting script: {job['name']}")
            result = run_script_with_retries(
                job["path"],
                max_retries=ORCHESTRATOR_CONFIG["max_retries"],
                timeout=ORCHESTRATOR_CONFIG["timeout"]
            )
            if result:
                logging.info(f"Script {job['name']} completed successfully.")
            else:
                logging.warning(f"Script {job['name']} failed after retries.")
        except Exception as e:
            logging.error(f"Unexpected error while running {job['name']}: {e}")

    logging.info("Orchestrator finished.")

if __name__ == "__main__":
    main()

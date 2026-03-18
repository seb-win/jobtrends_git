import requests
import json
import os
import time
import subprocess
import logging
import json
import html as html_lib
import re
from functools import lru_cache
from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

######
# V2:
#   get and post option in the fetch_url method implemented
#   get_nested_value method added
###### 

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fixed proxy credentials and GCS path
PROXY_USERNAME = 'swerch_DqgN3'
PROXY_PASSWORD = 'oxy_M0d3na4ever'
CREDENTIALS_PATH = "/Users/sebastianwinkler/Documents/Jobseite/AI/work_for_elon/service_account_key.json"
country_code = 'US'
GCS_TIMEOUT = (20, 300)
GCS_RETRY_DEADLINE = 600
GCS_UPLOAD_ATTEMPTS = 4


def _upload_blob_from_string(blob, payload, *, content_type="application/json; charset=utf-8", timeout=GCS_TIMEOUT):
    """
    Upload blob content with explicit retries for transient GCS/network failures.
    """
    retry_policy = DEFAULT_RETRY.with_deadline(GCS_RETRY_DEADLINE)
    last_error = None

    for attempt in range(1, GCS_UPLOAD_ATTEMPTS + 1):
        try:
            blob.upload_from_string(
                payload,
                content_type=content_type,
                timeout=timeout,
                retry=retry_policy,
            )
            return True
        except requests.exceptions.RequestException as exc:
            last_error = exc
            logging.warning(
                "GCS upload attempt %s/%s failed for %s: %s",
                attempt,
                GCS_UPLOAD_ATTEMPTS,
                blob.name,
                exc,
            )
        except Exception as exc:
            last_error = exc
            logging.warning(
                "Unexpected GCS upload failure on attempt %s/%s for %s: %s",
                attempt,
                GCS_UPLOAD_ATTEMPTS,
                blob.name,
                exc,
            )

        if attempt < GCS_UPLOAD_ATTEMPTS:
            time.sleep(min(30, 2 ** attempt))

    raise last_error


def upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME):
    """Upload job details to Google Cloud Storage."""
    text_filename = f"{FOLDER_NAME}_{job_id}.txt"
    upload_path = f"{FOLDER_NAME}/job_texts/{text_filename}"
    storage_client = get_storage_client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(upload_path)
    try:
        _upload_blob_from_string(blob, job_text, content_type="application/json; charset=utf-8")
        return True
    except Exception as exc:
        logging.error("Failed to upload job details for job %s to %s: %s", job_id, upload_path, exc)
        return False


def update_job_status(existing_entry, current_date):
    """Update the status of an existing job entry."""
    existing_entry['last_updated'] = current_date
    if existing_entry['status'] == 'inactive':
        existing_entry['status'] = 'reactivated'
        # logging.info(f"Job ID {existing_entry['id']} reactivated.")
    else:
        existing_entry['status'] = 'active'
        # logging.info(f"Job ID {existing_entry['id']} is still active.")


def get_proxy():
    """Fetch a new proxy for each request."""
    proxy_url_base = f'http://customer-{PROXY_USERNAME}-cc-{country_code}:{PROXY_PASSWORD}@pr.oxylabs.io:7777'
    proxies = {
        'http': proxy_url_base,
        'https': proxy_url_base,
    }
    return proxies


def fetch_url(url, headers, params, json, data, use_proxy=False, max_retries=3, timeout=60, request_type='get'):
    """Fetch a URL with adaptive retries, falling back to proxies if needed."""
    attempt = 0
    while attempt < max_retries:
        try:
            # Determine whether to use a proxy for this attempt
            proxies = get_proxy() if use_proxy else None

            # Determine the request type (GET or POST)
            if request_type.lower() == 'post':
                response = requests.post(url, headers=headers, params=params, json=json, data=data, proxies=proxies, timeout=timeout)
            else:
                response = requests.get(url, headers=headers, params=params, json=json, data=data, proxies=proxies, timeout=timeout)

            response.raise_for_status()  # Raises an HTTPError for 4xx/5xx responses
            return response  # Return the successful response if no error is raised
        except requests.exceptions.RequestException as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed: {e}")

            # If initial attempts without proxy fail, set use_proxy to True for subsequent attempts
            if not use_proxy:
                logging.info("Retrying with a proxy.")
                use_proxy = True

            # Back off before retrying
            time.sleep(attempt * 5)

    logging.error("Failed to fetch URL after multiple attempts.")
    return None


@lru_cache(maxsize=1)
def get_storage_client():
    """Initialize Google Cloud Storage client with specified credentials."""
    if os.path.exists(CREDENTIALS_PATH):
        # logging.info(f"Using service account credentials from {CREDENTIALS_PATH}")
        return storage.Client.from_service_account_json(CREDENTIALS_PATH)
    else:
        # logging.info("Using default service account on GCE.")
        return storage.Client()


def get_current_date():
    """Return the current date as a string in yyyymmdd format."""
    return datetime.now().strftime('%Y%m%d')


def get_master_list_filename(folder_name):
    """Generate the master list filename using the bucket name."""
    return f"{folder_name}_master.json"


def load_master_list(bucket_name, folder_name):
    """Download the master list from GCS or initialize an empty one."""
    storage_client = get_storage_client()
    blob_path = f"{folder_name}/{get_master_list_filename(folder_name)}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if blob.exists():
        content = blob.download_as_text()
        logging.info(f"Downloaded master list from {blob_path}")
        return json.loads(content)
    else:
        logging.info(f"Master list {blob_path} does not exist. Initializing an empty master list.")
        return []


def save_master_list(bucket_name, folder_name, master_list):
    """Save the updated master list to GCS."""
    storage_client = get_storage_client()
    blob_path = f"{folder_name}/{get_master_list_filename(folder_name)}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    _upload_blob_from_string(blob, json.dumps(master_list, indent=4), content_type="application/json; charset=utf-8")
    logging.info(f"Updated master list saved to {blob_path}.")

def get_nested_value(data, keys):
    """Recursively get the value from a nested dictionary or list given a list of keys or indices."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, {})
        elif isinstance(data, list) and isinstance(key, int):
            if 0 <= key < len(data):
                data = data[key]
            else:
                return None  # Index out of range
        else:
            return None  # Invalid key or index for the given data type
    return data

def send_metrics_to_cloud_function(
    script_name, execution_time, cpu_usage, scraped_jobs, new_jobs, inactive_jobs, skipped_jobs):
    """
    Send the metrics to the Cloud Function using a POST request with a JSON body.
    """
    cloud_function_url = "https://europe-west3-trim-artifact-357617.cloudfunctions.net/error_log"

    payload = {
        "script_name": script_name,
        "status": "success",
        "execution_time": execution_time,
        "cpu_usage": cpu_usage,
        "memory_usage": None,
        "data_volume": None,
        "scraped_jobs": scraped_jobs,
        "new_jobs": new_jobs,
        "inactive_jobs": inactive_jobs,
        "skipped_jobs": skipped_jobs,
        "error_message": None
    }

    try:
        # Send POST request with the payload
        response = requests.post("https://europe-west3-trim-artifact-357617.cloudfunctions.net/error_log",json=payload,headers={"Content-Type": "application/json"},timeout=10)

        if response.status_code == 200:
            logging.info(f"Metrics successfully sent to Cloud Function: {response.text}")
        else:
            logging.error(f"Failed to send metrics. Status code: {response.status_code}, Response: {response.text}")
    except requests.RequestException as e:
        logging.error(f"Error sending metrics to Cloud Function: {e}")


def run_script_with_retries(script_path, max_retries, timeout):
    for attempt in range(max_retries):
        try:
            result = subprocess.run(
                ["python3", script_path],
                timeout=timeout,
                check=True,
                capture_output=True
            )
            return True  # Script succeeded
        except subprocess.CalledProcessError as e:
            print(f"Attempt {attempt + 1} failed: {e.stderr.decode().strip()}")
        except subprocess.TimeoutExpired:
            print(f"Attempt {attempt + 1} timed out.")
        time.sleep(5)  # Wait before retrying
    return False  # Script failed after all retries

def fix_encoding(text: str) -> str:
    try:
        return text.encode("latin1").decode("utf-8")
    except Exception:
        return text

def clean_html_block(block_html: str) -> str:
    if not block_html:
        return ""

    soup = BeautifulSoup(block_html, "html.parser")

    # (Optional) Überschriften etwas markieren
    for h in soup.find_all(["h1","h2","h3","h4"]):
        h.insert_before("\n")
        h.insert_after("\n")

    # Bullet-Listen als "- " markieren (super billig, aber semantisch stark)
    for li in soup.find_all("li"):
        li.insert_before("\n- ")
        li.insert_after("")

    # Text ziehen (mit \n als Separator, damit Struktur grob bleibt)
    text = soup.get_text("\n", strip=True)

    # HTML entities & whitespace normalisieren
    text = html_lib.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    text = re.sub(r"\n-\n", "\n- ", text)
    text = fix_encoding(text)
    
    return text



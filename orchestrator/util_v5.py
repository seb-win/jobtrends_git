import requests
import json
import os
import time
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from google.cloud import storage

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
BASE_DIR = Path(__file__).resolve().parent
CREDENTIALS_PATH = str(BASE_DIR / "configs" / "service_account_key.json")
country_code = 'US'


def upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME):
    """Upload job details to Google Cloud Storage."""
    text_filename = f"{FOLDER_NAME}_{job_id}.txt"
    upload_path = f"{FOLDER_NAME}/job_texts/{text_filename}"
    storage_client = get_storage_client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(upload_path)
    blob.upload_from_string(job_text, timeout=150)
    # logging.info(f"Uploaded job details for Job ID {job_id} to {upload_path}")

def upload_detailjob_json_to_gcs(detail_obj, job_id, BUCKET_NAME, FOLDER_NAME):
    """Upload standardized detailjob JSON (schema_versioned) to Google Cloud Storage."""
    import json

    json_filename = f"{FOLDER_NAME}_{job_id}.json"
    upload_path = f"{FOLDER_NAME}/job_details/{json_filename}"
    storage_client = get_storage_client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(upload_path)
    blob.upload_from_string(
        json.dumps(detail_obj, ensure_ascii=False, indent=2),
        content_type="application/json; charset=utf-8",
        timeout=150,
    )
    return upload_path


def update_job_status(existing_entry, current_date):
    """Update the status of an existing job entry."""
    existing_entry['last_updated'] = current_date
    if existing_entry['status'] == 'inactive':
        existing_entry['status'] = 'active'


def save_master_list(bucket_name, folder_name, master_list):
    """Save updated master list back to Cloud Storage."""
    filename = get_master_list_filename(folder_name)
    storage_client = get_storage_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/{filename}")
    blob.upload_from_string(json.dumps(master_list, indent=2), content_type="application/json")
    # logging.info(f"Saved updated master list with {len(master_list)} entries to {folder_name}/{filename}")


def get_proxy():
    """Fetch a new proxy for each request."""
    proxy_url_base = f'http://customer-{PROXY_USERNAME}-cc-{country_code}:{PROXY_PASSWORD}@pr.oxylabs.io:7777'
    proxies = {
        'http': proxy_url_base,
        'https': proxy_url_base,
    }
    return proxies


def _init_http_stats(http_stats: dict) -> None:
    """Initialize the mutable http_stats dict with the keys we expect.

    We keep this lightweight on purpose:
    - We do NOT log every request as a DB row (too much noise).
    - We aggregate counts per Scrape-Run so we can quickly debug blocks (403/429),
      bad requests (400s), server errors (5xx), timeouts, etc.
    """
    http_stats.setdefault("requests_total", 0)
    http_stats.setdefault("requests_ok", 0)          # 2xx + 3xx
    http_stats.setdefault("requests_failed", 0)      # exceptions / timeouts / no response
    http_stats.setdefault("status_counts", {})       # e.g. {"200": 12, "403": 2, "400": 1}
    http_stats.setdefault("exception_counts", {})    # e.g. {"ReadTimeout": 3}
    http_stats.setdefault("timeouts", 0)
    http_stats.setdefault("latency_ms_total", 0.0)
    http_stats.setdefault("last_error", None)


def _record_response(http_stats: dict, status_code: int, latency_ms: float) -> None:
    """Record a received HTTP response (even if it is a 4xx/5xx)."""
    http_stats["requests_total"] += 1
    http_stats["latency_ms_total"] += latency_ms

    code = str(status_code)
    http_stats["status_counts"][code] = http_stats["status_counts"].get(code, 0) + 1

    if 200 <= status_code <= 399:
        http_stats["requests_ok"] += 1


def _record_exception(http_stats: dict, exc: Exception, latency_ms: float) -> None:
    """Record an exception where we did not (reliably) get a response object."""
    http_stats["requests_total"] += 1
    http_stats["requests_failed"] += 1
    http_stats["latency_ms_total"] += latency_ms

    name = type(exc).__name__
    http_stats["exception_counts"][name] = http_stats["exception_counts"].get(name, 0) + 1
    http_stats["last_error"] = f"{name}: {str(exc)[:200]}"

    # Best-effort timeout detection (requests uses several timeout exception classes)
    if name in ("Timeout", "ReadTimeout", "ConnectTimeout", "ProxyError"):
        http_stats["timeouts"] += 1


def fetch_url(
    url,
    headers,
    params,
    json,
    data,
    use_proxy=False,
    max_retries=3,
    timeout=60,
    request_type='get',
    http_stats=None,
):
    """Fetch a URL with retries, optionally collecting aggregated HTTP stats.

    Warum `http_stats`?
    - Wir wollen beweisen, dass ein Run wirklich Requests gemacht hat (statt nur "kein Crash").
    - Wir wollen sehen, ob es eher 4xx/5xx/Timeout/Proxy/WAF ist.
    - Wir aggregieren pro Run (ein Dict) statt pro Request (kein Log-Spam).

    Wichtig:
    - Statuscodes werden für *alle* Responses gezählt (auch 400/403/500).
    - raise_for_status() bleibt aktiv, damit dein Retry-Verhalten unverändert bleibt.
    """
    if http_stats is not None:
        _init_http_stats(http_stats)

    attempt = 0
    while attempt < max_retries:
        t0 = time.time()
        try:
            # Determine whether to use a proxy for this attempt
            proxies = get_proxy() if use_proxy else None

            # Determine the request type (GET or POST)
            if request_type.lower() == 'post':
                response = requests.post(
                    url,
                    headers=headers,
                    params=params,
                    json=json,
                    data=data,
                    proxies=proxies,
                    timeout=timeout
                )
            else:
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    json=json,
                    data=data,
                    proxies=proxies,
                    timeout=timeout
                )

            latency_ms = (time.time() - t0) * 1000.0
            if http_stats is not None and response is not None:
                # Wir zählen jede Response – auch wenn sie 4xx/5xx ist und danach ein HTTPError geworfen wird.
                _record_response(http_stats, response.status_code, latency_ms)

            # Raises an HTTPError for 4xx/5xx responses (keeps old behavior)
            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            latency_ms = (time.time() - t0) * 1000.0

            # Wenn keine Response existiert (DNS/Timeout/Connection), zählen wir als Exception-Failure.
            # HTTPError hat i.d.R. eine Response – deren Statuscode wurde oben bereits gezählt.
            if http_stats is not None and getattr(e, "response", None) is None:
                _record_exception(http_stats, e, latency_ms)

            attempt += 1
            logging.warning(f"Attempt {attempt} failed: {e}")

            # If initial attempts without proxy fail, set use_proxy to True for subsequent attempts
            if not use_proxy:
                logging.info("Retrying with a proxy.")
                use_proxy = True

            # Back off before retrying
            time.sleep(2)

    logging.error("Failed to fetch URL after multiple attempts.")
    return None


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
    """Load existing master list JSON from Cloud Storage."""
    filename = get_master_list_filename(folder_name)
    storage_client = get_storage_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/{filename}")

    if blob.exists():
        data = blob.download_as_text()
        return json.loads(data)
    else:
        return []


def get_nested_value(data_dict, path):
    """Get a nested value from a dict using a list path."""
    current = data_dict
    for key in path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current

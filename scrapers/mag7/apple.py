import logging
import time
import psutil
from bs4 import BeautifulSoup
import json
import gc

from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)
from orchestrator.schemas.hyd_extract import get_hydration_json

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'mag7'
FOLDER_NAME = 'apple'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 20
PAGE_START = 1
REQUESTS_PER_BLOCK = 5
DETAIL_FETCH_RETRIES = 3
DETAIL_FETCH_RETRY_SLEEP_S = 2
DETAIL_UPLOAD_RETRIES = 3
DETAIL_UPLOAD_RETRY_SLEEP_S = 3
MASTER_SAVE_RETRIES = 3
MASTER_SAVE_RETRY_SLEEP_S = 5

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'page'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['res','searchResults']
TOTAL_JOBS_KEY = ['res','totalRecords']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'browserlocale': 'de-de',
    'content-type': 'application/json',
    'locale': 'de_DE',
    'origin': 'https://jobs.apple.com',
    'priority': 'u=1, i',
    'referer': 'https://jobs.apple.com/en-us/search?sort=newest',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://jobs.apple.com/api/v1/search'

JOB_DATA_KEYS = {
    'created': ['postDateInGMT'],
    'jobTitle': ['postingTitle'],
    'department': ['team', 'teamName'],
    'team': [],
    'location': ['locations', '0', 'city'],
    'country': ['locations', '0', 'countryName'],
    'contract': [],
    'id': ['positionId'],
    'link': [],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = None
JSON_PAYLOAD = {
    'query': '',
    'filters': {},
    'page': PAGE_START,
    'locale': 'en-us',
    'sort': 'newest',
    'format': {
        'longDate': 'MMMM D, YYYY',
        'mediumDate': 'MMM D, YYYY',
    },
}
DATA = None


def process_jobs(job_data, job_data_keys):
    """
    Extract relevant job details from job data using JOB_DATA_KEYS.
    This function uses get_nested_value() to retrieve values from the JSON structure.
    If a path is empty, that field is skipped.
    If extraction_logic defines a special extractor, it's applied to the retrieved value.
    """
    jobs = []
    for listing in job_data:
        job_details = {
            'scraping_date': None,
            'last_updated': None,
            'status': None,
            'keywords': []
        }

        for key, path in job_data_keys.items():
            if not path:
                # If path is empty, skip this field
                continue

            value = get_nested_value(listing, path)
            extractor = extraction_logic.get(key, lambda x: x)
            job_details[key] = extractor(value)

        jobs.append(job_details)
    return jobs


def fetch_job_list_page(page):
    """
    Fetch a single page of job listings in JSON format.
    Adjusts the request parameters based on PAGINATION_MODE.
    Returns a tuple (job_data_list, total_jobs) where job_data_list is a list of jobs 
    for that page and total_jobs is the total number of jobs in the entire dataset 
    (only set when page == PAGE_START).
    """

    # Copy existing params
    # params = {**PARAMS}

    # # Adjust pagination parameters based on PAGINATION_MODE
    # if PAGINATION_MODE == 'page':
    #     # Standard page-based pagination
    #     params['page'] = page
    # elif PAGINATION_MODE == 'offset':
    #     # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
    #     offset = (page * MAX_JOBS_PER_PAGE)
    #     params['offset'] = offset
    # elif PAGINATION_MODE == 'firstItem':
    #     # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
    #     first_item = (page * MAX_JOBS_PER_PAGE) + 1
    #     params['firstItem'] = first_item

    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=PARAMS,
        json=JSON_PAYLOAD,
        data=DATA,
        use_proxy=USE_PROXY_DAILY_LIST,
        max_retries=3,
        timeout=10,
        request_type=REQUEST_TYPE_LIST
    )

    if not response:
        logging.error("Failed to fetch daily job list after multiple attempts.")
        return None, 0

    try:
        job_data = response.json()
    except ValueError as e:
        logging.error(f"Failed to parse response to JSON: {e}")
        return None, 0

    total_jobs = 0
    if page == PAGE_START:
        total_jobs = get_nested_value(job_data, TOTAL_JOBS_KEY)

    job_list = get_nested_value(job_data, JOBS_LIST_KEY)
    if not isinstance(job_list, list):
        logging.error(f"Unexpected response format: job data is not a list or doesn't contain '{JOBS_LIST_KEY}' key.")
        return None, total_jobs

    return job_list, total_jobs


def _save_master_list_with_retry(master_list):
    """Persist master list with retries so transient network errors do not abort the run."""
    for attempt in range(1, MASTER_SAVE_RETRIES + 1):
        try:
            save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)
            return True
        except Exception as exc:
            logging.warning(
                f"Master list save failed (attempt {attempt}/{MASTER_SAVE_RETRIES}): {exc}"
            )
            if attempt < MASTER_SAVE_RETRIES:
                time.sleep(MASTER_SAVE_RETRY_SLEEP_S)

    logging.error("Master list could not be saved after retries.")
    return False


def _upload_job_details_with_retry(job_text, job_id):
    """Upload one job detail document with retries and non-fatal failure."""
    for attempt in range(1, DETAIL_UPLOAD_RETRIES + 1):
        try:
            upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)
            return True
        except Exception as exc:
            logging.warning(
                f"Detail upload failed for job {job_id} "
                f"(attempt {attempt}/{DETAIL_UPLOAD_RETRIES}): {exc}"
            )
            if attempt < DETAIL_UPLOAD_RETRIES:
                time.sleep(DETAIL_UPLOAD_RETRY_SLEEP_S)

    logging.error(f"Skipping detail upload for job {job_id}: upload failed after retries.")
    return False


def _process_and_persist_block(raw_jobs_block, master_list, block_index):
    """
    Process one block of raw jobs, enrich/save details for new jobs, and persist immediately.
    """
    if not raw_jobs_block:
        return 0, 0, 0, 0

    jobs = process_jobs(raw_jobs_block, JOB_DATA_KEYS)
    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(jobs, master_list)
    _save_master_list_with_retry(master_list)

    logging.info(
        f"Saved block {block_index}: {len(jobs)} jobs processed, "
        f"{new_jobs_count} new, {inactive_jobs_count} marked inactive, {skipped_jobs_count} skipped."
    )

    raw_jobs_block.clear()
    gc.collect()

    return len(jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count


def fetch_all_jobs(master_list):
    """
    Fetch and process job postings in blocks of REQUESTS_PER_BLOCK pages.
    After each block (5 pages = 100 jobs), details are scraped and data is persisted.
    """
    total_jobs_from_response = 0
    total_processed_jobs = 0
    total_new_jobs = 0
    total_inactive_jobs = 0
    total_skipped_jobs = 0

    if USE_PAGINATION:
        page = PAGE_START
        raw_jobs_block = []
        requests_in_block = 0
        block_index = 1

        while True:
            logging.info(f"Fetching page {page} of job listings...")
            JSON_PAYLOAD['page'] = page
            job_data, total_jobs = fetch_job_list_page(page)
            if job_data is None:
                break

            if page == PAGE_START:
                total_jobs_from_response = total_jobs

            if not job_data:
                break

            raw_jobs_block.extend(job_data)
            requests_in_block += 1

            should_flush_block = requests_in_block >= REQUESTS_PER_BLOCK
            retrieved_all_jobs = total_jobs_from_response and (
                (total_processed_jobs + len(raw_jobs_block)) >= total_jobs_from_response
            )

            if should_flush_block or retrieved_all_jobs:
                processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                    raw_jobs_block, master_list, block_index
                )
                total_processed_jobs += processed
                total_new_jobs += new_jobs
                total_inactive_jobs += inactive_jobs
                total_skipped_jobs += skipped_jobs
                requests_in_block = 0
                block_index += 1

            if retrieved_all_jobs:
                break

            page += 1

        if raw_jobs_block:
            processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                raw_jobs_block, master_list, block_index
            )
            total_processed_jobs += processed
            total_new_jobs += new_jobs
            total_inactive_jobs += inactive_jobs
            total_skipped_jobs += skipped_jobs
    else:
        JSON_PAYLOAD['page'] = PAGE_START
        job_data, total_jobs_from_response = fetch_job_list_page(PAGE_START)
        if job_data:
            processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                list(job_data), master_list, 1
            )
            total_processed_jobs += processed
            total_new_jobs += new_jobs
            total_inactive_jobs += inactive_jobs
            total_skipped_jobs += skipped_jobs
            logging.info(f"Fetched and processed {len(job_data)} jobs from the single page.")
        else:
            logging.info("No jobs found on the single page.")

    return total_processed_jobs, total_new_jobs, total_inactive_jobs, total_skipped_jobs


def update_master_list_with_jobs(jobs, master_list):
    """
    Update the master list with new or existing jobs, fetch job details if needed,
    and mark old jobs as inactive.
    """
    current_date = get_current_date()
    new_jobs_count = 0
    inactive_jobs_count = 0
    skipped_jobs_count = 0

    for job in jobs:
        job_id = job.get('id')
        if not job_id:
            skipped_jobs_count += 1
            continue

        existing_entry = next((entry for entry in master_list if entry['id'] == job_id), None)

        if existing_entry:
            update_job_status(existing_entry, current_date)
        else:
            # Add new job
            job['scraping_date'] = current_date
            job['last_updated'] = current_date
            job['status'] = 'active'
            master_list.append(job)
            new_jobs_count += 1

            # Fetch job details if a link is provided
            job_link = 'https://jobs.apple.com/en-us/details/' + job['id']
            if job_link:
                data = None
                for attempt in range(1, DETAIL_FETCH_RETRIES + 1):
                    try:
                        data = get_hydration_json(job_link, '__staticRouterHydrationData')
                        break
                    except Exception as exc:
                        logging.warning(
                            f"Detail fetch failed for job {job_id} (attempt {attempt}/{DETAIL_FETCH_RETRIES}): {exc}"
                        )
                        if attempt < DETAIL_FETCH_RETRIES:
                            time.sleep(DETAIL_FETCH_RETRY_SLEEP_S)

                if not isinstance(data, dict):
                    logging.error(f"Skipping detail upload for job {job_id}: no hydration data after retries.")
                    continue

                # if response:
                #     soup = BeautifulSoup(response.text, 'html.parser')
                #     job_text = soup.get_text()
                #     upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)
                jobs = data.get("loaderData", {}).get("jobDetails", {}).get("jobsData", {})
                job_details = data.get("loaderData", {}).get("jobDetails", {})

                # --- Pay & Benefits (nur einmal, bevorzugt en_US) ---
                ppld = jobs.get("postingPostLocationData", {}) or {}
                locale = "en_US" if "en_US" in ppld else (next(iter(ppld.keys())) if ppld else None)

                loc_id = None
                if jobs.get("locations"):
                    loc_id = jobs["locations"][0].get("id")

                pay = None
                other = None
                if locale and loc_id:
                    pay = ppld.get(locale, {}).get(loc_id, {}).get("postingSupplementFooter")
                    other = ppld.get(locale, {}).get(loc_id, {}).get("otherPostingSupplementFooter")

                # --- Ziel-JSON bauen (mit .get(), damit nichts crasht wenn Felder fehlen) ---
                out = {
                    "positionId": jobs.get("positionId"),
                    "postingTitle": jobs.get("postingTitle"),
                    "transformedPostingTitle": jobs.get("transformedPostingTitle"),
                    "requestUrl": job_details.get("requestUrl"),
                    "description": jobs.get("description"),
                    "minimumQualifications": jobs.get("minimumQualifications"),
                    "preferredQualifications": jobs.get("preferredQualifications"),
                    "teamNames": jobs.get("teamNames"),
                    "locations": jobs.get("locations"),
                    "postDateInGMT": jobs.get("postDateInGMT"),
                    "jobType": jobs.get("jobType"),
                    "employmentType": jobs.get("employmentType"),  # kann bei manchen Jobs fehlen -> None ok
                    "payAndBenefits": {
                        "locale": locale,
                        "locationId": loc_id,
                        "label": (pay or {}).get("label"),
                        "content_html": (pay or {}).get("content"),
                    } if pay else None,
                    "otherPostingSupplement": {
                        "locale": locale,
                        "locationId": loc_id,
                        "content_html": (other or {}).get("content"),
                    } if other else None,
                }

                # --- Als String für GCS ---
                out_json_str = json.dumps(out, ensure_ascii=False)
                _upload_job_details_with_retry(out_json_str, job_id)

    # Mark old jobs as inactive
    for entry in master_list:
        if entry['last_updated'] != current_date:
            entry['status'] = 'inactive'
            inactive_jobs_count += 1

    return new_jobs_count, inactive_jobs_count, skipped_jobs_count


def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")
    starting_time = time.time()
    cpu_usage = psutil.cpu_percent(interval=1)

    # Step 1: Load master list
    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)

    # Step 2: Fetch/process jobs in blocks and persist after each block
    jobs_processed, new_jobs_count, inactive_jobs_count, skipped_jobs_count = fetch_all_jobs(master_list)

    execution_time = time.time() - starting_time

    # Summary
    logging.info(f"Scraping completed successfully. {jobs_processed} jobs processed.")
    logging.info(f"{new_jobs_count} new jobs added.")
    logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(
        FOLDER_NAME,
        execution_time,
        cpu_usage,
        jobs_processed,
        new_jobs_count,
        inactive_jobs_count,
        skipped_jobs_count
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()

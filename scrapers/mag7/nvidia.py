import logging
import time
import psutil
from bs4 import BeautifulSoup
import json
import re
from html import unescape
from datetime import datetime, UTC

from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'mag7'
FOLDER_NAME = 'nvidia'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 10
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['data', 'positions']
TOTAL_JOBS_KEY = ['data', 'count']

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://jobs.nvidia.com/careers?start=0&pid=893392854946&sort_by=timestamp',
    'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': 'ceb9161bd08f40c690c66a330a14c7f8-ba8a693e2bd0deed-0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://jobs.nvidia.com/api/pcsx/search'

JOB_DATA_KEYS = {
    'created': ['postedTs'],
    'jobTitle': ['name'],
    'department': ['department'],
    'team': [],
    'location': ['locations', 0],
    'country': [],
    'contract': [],
    'id': ['id'],
    'link': ['positionUrl'],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    "domain": "nvidia.com",
    "query": "",
    "location": "",
    "start": 0,
    "sort_by": "timestamp"
}
JSON_PAYLOAD = None
DATA = None

def format_time_ts(creation_ts_raw):
    """Convert Micron creationTs values from seconds or milliseconds to UTC ISO."""
    if creation_ts_raw in (None, ""):
        return None

    try:
        creation_ts = float(creation_ts_raw)
    except (TypeError, ValueError):
        logging.warning(f"Invalid creationTs value: {creation_ts_raw!r}")
        return None

    # Detail postings may return epoch seconds, while list endpoints often use
    # milliseconds. Dividing seconds by 1000 produces dates around 1970.
    if creation_ts > 100_000_000_000:
        creation_ts /= 1000

    return datetime.fromtimestamp(creation_ts, UTC).isoformat()


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
    params = {**PARAMS}

    # Adjust pagination parameters based on PAGINATION_MODE
    if PAGINATION_MODE == 'page':
        # Standard page-based pagination
        params['page'] = page
    elif PAGINATION_MODE == 'offset':
        # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
        offset = (page * MAX_JOBS_PER_PAGE)
        params['start'] = offset
    elif PAGINATION_MODE == 'firstItem':
        # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
        first_item = (page * MAX_JOBS_PER_PAGE) + 1
        params['firstItem'] = first_item

    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=params,
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


def fetch_all_jobs():
    """
    Fetch all job postings (paginated or not) based on USE_PAGINATION.
    For pagination, it will loop through pages or offsets until it retrieves all jobs 
    or reaches a stopping condition.
    """
    all_jobs = []
    total_jobs_from_response = 0

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings...")
            job_data, total_jobs = fetch_job_list_page(page)
            if job_data is None:
                break

            # Set total_jobs_from_response if this is the first page
            if page == PAGE_START:
                total_jobs_from_response = total_jobs

            if not job_data:
                # No more jobs
                break

            all_jobs.extend(job_data)

            # Stop if we've retrieved all jobs
            if total_jobs_from_response and len(all_jobs) >= total_jobs_from_response:
                break

            page += 1
    else:
        job_data, total_jobs_from_response = fetch_job_list_page(PAGE_START)
        if job_data:
            all_jobs.extend(job_data)
            logging.info(f"Fetched {len(job_data)} jobs from the single page.")
        else:
            logging.info("No jobs found on the single page.")

    return all_jobs


def update_master_list_with_jobs(jobs, master_list):
    """
    Update the master list with new or existing jobs, fetch job details if needed,
    and mark old jobs as inactive.
    """
    current_date = get_current_date()
    new_jobs_count = 0
    inactive_jobs_count = 0
    skipped_jobs_count = 0
    processed_jobs_count = 0

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
            job_link = 'https://jobs.nvidia.com/api/pcsx/position_details'

            params = {
                'position_id': job_id,
                'domain': 'nvidia.com',
                'hl': 'de',
            }

            if job_link:
                response = fetch_url(
                    job_link,
                    headers=HEADERS,
                    params=params,
                    json=JSON_PAYLOAD,
                    data=DATA,
                    use_proxy=USE_PROXY_DETAILED_POSTINGS,
                    max_retries=3,
                    timeout=10,
                    request_type=REQUEST_TYPE_SINGLE
                )
                if response:
                    json_job = json.loads(response.text)
                    job = json_job.get("data", {})

                    html_text = job.get("jobDescription") or ""
                    html_text = unescape(html_text)
                    soup = BeautifulSoup(html_text, "html.parser")
                    clean_text = soup.get_text(separator=" ", strip=True)
                    clean_text = re.sub(r"\s+", " ", clean_text).strip()

                    cleaned_job = {
                        "creation_ts": format_time_ts(job.get("creationTs")),
                        "posted_ts": format_time_ts(job.get("postedTs")),
                        "department": job.get("department"),
                        "id": job.get("id"),
                        "job_description": clean_text,
                        "location": job.get("location"),
                        "name": job.get("name"),
                    }

                    json_string = json.dumps(cleaned_job, ensure_ascii=False)

                    upload_job_details_to_gcs(json_string, job_id, BUCKET_NAME, FOLDER_NAME)

        processed_jobs_count += 1

        # Save the master list after every 1000 processed jobs
        if processed_jobs_count % 100 == 0:
            logging.info(f"Saving master list after processing {processed_jobs_count} jobs...")
            save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

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

    # Step 1: Fetch all jobs
    raw_job_data = fetch_all_jobs()

    # Step 2: Process jobs using JOB_DATA_KEYS
    jobs = process_jobs(raw_job_data, JOB_DATA_KEYS)

    # Step 3: Update master list
    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)
    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(jobs, master_list)

    # Step 4: Save the updated master list
    save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

    execution_time = time.time() - starting_time

    # Summary
    logging.info(f"Scraping completed successfully. {len(jobs)} jobs processed.")
    logging.info(f"{new_jobs_count} new jobs added.")
    logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(FOLDER_NAME, execution_time, cpu_usage, len(jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()

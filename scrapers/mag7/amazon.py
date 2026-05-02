import logging
import time
import psutil
import json

from orchestrator.util_v2 import (
    fetch_url, get_current_date, get_storage_client, update_job_status,
    get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'mag7'
FOLDER_NAME = 'amazon'

USE_PROXY_DAILY_LIST = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'

MAX_JOBS_PER_PAGE = 100
PAGE_START = 0
MASTER_SAVE_INTERVAL = 300

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['jobs']
TOTAL_JOBS_KEY = ['hits']
BUSINESS_CATEGORY_FACET_KEY = ['facets', 'business_category_facet']

HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    "Accept-Encoding": "gzip, deflate, br",
    'Connection': 'keep-alive',
    'Referer': 'https://www.amazon.jobs/de/search?base_query=&loc_query=',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

DAILY_JOB_URL = 'https://www.amazon.jobs/de/search.json'

JOB_DATA_KEYS = {
    'created': ['posted_date'],
    'jobTitle': ['title'],
    'department': ['job_category'],
    'company': ['company_name'],
    'team': [],
    'location': ['city'],
    'country': ['country'],
    'contract': [],
    'id': ['id'],
    'link': ['job_path'],
    'career_level': [],
    'employment_type': [],
}

JOB_DETAIL_KEYS = [
    'title',
    'description',
    'basic_qualifications',
    'preferred_qualifications',
    'job_category',
    'business_category',
    'company_name',
    'normalized_location',
    'locations',
    'id',
]

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    "business_category[]": '',
    "radius": "24km",
    "facets[]": [
        "business_category",
    ],
    "offset": 0,
    "result_limit": 100,
    "sort": "relevant",
    "latitude": "",
    "longitude": "",
    "loc_group_id": "",
    "loc_query": "",
    "base_query": "",
    "city": "",
    "country": "",
    "region": "",
    "county": "",
    "query_options": ""
}
JSON_PAYLOAD = None
DATA = None


def get_category_folder_name(business_category):
    return f"{FOLDER_NAME}/{business_category}"


def get_category_master_blob_path(business_category):
    category_folder = get_category_folder_name(business_category)
    return f"{category_folder}/{business_category}_master.json"


def load_category_master_list(business_category):
    storage_client = get_storage_client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_path = get_category_master_blob_path(business_category)
    blob = bucket.blob(blob_path)

    if blob.exists():
        content = blob.download_as_text()
        logging.info(f"Downloaded category master list from {blob_path}")
        return json.loads(content)

    logging.info(f"Master list {blob_path} does not exist. Initializing an empty master list.")
    return []


def save_category_master_list(business_category, master_list):
    storage_client = get_storage_client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_path = get_category_master_blob_path(business_category)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(master_list, indent=4, ensure_ascii=False),
        content_type="application/json; charset=utf-8"
    )
    logging.info(f"Updated category master list saved to {blob_path}.")


def upload_category_job_details_to_gcs(job_text, job_id, business_category):
    category_folder = get_category_folder_name(business_category)
    text_filename = f"{business_category}_{job_id}.txt"
    upload_path = f"{category_folder}/job_texts/{text_filename}"
    storage_client = get_storage_client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(upload_path)
    blob.upload_from_string(job_text, content_type="application/json; charset=utf-8")
    logging.info(f"Uploaded job details for {job_id} to {upload_path}.")


def build_request_params(page, business_category=None):
    params = {**PARAMS}

    if business_category:
        params['business_category[]'] = business_category
    else:
        params.pop('business_category[]', None)

    if PAGINATION_MODE == 'page':
        params['page'] = page
    elif PAGINATION_MODE == 'offset':
        offset = (page * MAX_JOBS_PER_PAGE)
        params['offset'] = offset
    elif PAGINATION_MODE == 'firstItem':
        first_item = (page * MAX_JOBS_PER_PAGE) + 1
        params['firstItem'] = first_item

    return params


def extract_business_category_keys(facet_data):
    if isinstance(facet_data, dict):
        return list(facet_data.keys())

    if isinstance(facet_data, list):
        category_keys = []
        for item in facet_data:
            if isinstance(item, dict):
                category_keys.extend(item.keys())
            elif isinstance(item, str):
                category_keys.append(item)
        return category_keys

    logging.error(f"Unexpected business category facet format: {type(facet_data)}")
    return []


def fetch_business_category_keys():
    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=build_request_params(PAGE_START),
        json=JSON_PAYLOAD,
        data=DATA,
        use_proxy=USE_PROXY_DAILY_LIST,
        max_retries=3,
        timeout=10,
        request_type=REQUEST_TYPE_LIST
    )

    if not response:
        logging.error("Failed to fetch business category facets after multiple attempts.")
        return []

    try:
        job_data = response.json()
    except ValueError as e:
        logging.error(f"Failed to parse business category facet response to JSON: {e}")
        return []

    facet_data = get_nested_value(job_data, BUSINESS_CATEGORY_FACET_KEY)
    category_keys = extract_business_category_keys(facet_data)
    logging.info(f"Found {len(category_keys)} business categories.")
    return category_keys


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


def build_job_detail_json(raw_job):
    """
    Build the detail JSON for GCS from the already fetched Amazon search entry.
    """
    detail_payload = {key: raw_job.get(key) for key in JOB_DETAIL_KEYS}
    return json.dumps(detail_payload, ensure_ascii=False)


def fetch_job_list_page(page, business_category=None):
    """
    Fetch a single page of job listings in JSON format.
    Adjusts the request parameters based on PAGINATION_MODE.
    Returns a tuple (job_data_list, total_jobs) where job_data_list is a list of jobs 
    for that page and total_jobs is the total number of jobs in the entire dataset 
    (only set when page == PAGE_START).
    """

    params = build_request_params(page, business_category)

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


def fetch_all_jobs_for_category(business_category):
    """
    Fetch all job postings for one business category based on USE_PAGINATION.
    For pagination, it will loop through pages or offsets until it retrieves all jobs
    or reaches a stopping condition.
    """
    all_jobs = []
    total_jobs_from_response = 0

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings for category {business_category}...")
            job_data, total_jobs = fetch_job_list_page(page, business_category)
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
        job_data, total_jobs_from_response = fetch_job_list_page(PAGE_START, business_category)
        if job_data:
            all_jobs.extend(job_data)
            logging.info(f"Fetched {len(job_data)} jobs from the single page for category {business_category}.")
        else:
            logging.info(f"No jobs found on the single page for category {business_category}.")

    return all_jobs


def deduplicate_jobs_by_id(raw_jobs):
    deduplicated_jobs = []
    seen_job_ids = set()

    for job in raw_jobs:
        job_id = job.get('id')
        if job_id and job_id in seen_job_ids:
            continue
        if job_id:
            seen_job_ids.add(job_id)
        deduplicated_jobs.append(job)

    return deduplicated_jobs


def update_master_list_with_jobs(jobs, master_list, raw_jobs_by_id, business_category):
    """
    Update the master list with new or existing jobs, upload detail JSON for new jobs,
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

            raw_job = raw_jobs_by_id.get(job_id)
            if raw_job:
                job_details_json = build_job_detail_json(raw_job)
                upload_category_job_details_to_gcs(job_details_json, job_id, business_category)
            else:
                logging.warning(f"No raw job entry found for new job {job_id}; skipping detail upload.")

        processed_jobs_count += 1

        if processed_jobs_count % MASTER_SAVE_INTERVAL == 0:
            logging.info(f"Saving master list after processing {processed_jobs_count} jobs...")
            save_category_master_list(business_category, master_list)

    # Mark old jobs as inactive
    for entry in master_list:
        if entry['last_updated'] != current_date:
            entry['status'] = 'inactive'
            inactive_jobs_count += 1

    return new_jobs_count, inactive_jobs_count, skipped_jobs_count


def process_business_category(business_category):
    raw_job_data = fetch_all_jobs_for_category(business_category)
    raw_job_data = deduplicate_jobs_by_id(raw_job_data)
    jobs = process_jobs(raw_job_data, JOB_DATA_KEYS)
    raw_jobs_by_id = {
        raw_job.get('id'): raw_job
        for raw_job in raw_job_data
        if raw_job.get('id')
    }

    master_list = load_category_master_list(business_category)
    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(
        jobs,
        master_list,
        raw_jobs_by_id,
        business_category
    )
    save_category_master_list(business_category, master_list)

    return len(jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count


def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")
    starting_time = time.time()
    cpu_usage = psutil.cpu_percent(interval=1)

    business_categories = fetch_business_category_keys()
    if not business_categories:
        logging.error("No business categories found; aborting job scraping process.")
        return

    jobs_processed = 0
    new_jobs_count = 0
    inactive_jobs_count = 0
    skipped_jobs_count = 0

    for business_category in business_categories:
        logging.info(f"Starting category scraping process for {business_category}")
        processed, new_jobs, inactive_jobs, skipped_jobs = process_business_category(business_category)
        jobs_processed += processed
        new_jobs_count += new_jobs
        inactive_jobs_count += inactive_jobs
        skipped_jobs_count += skipped_jobs

    execution_time = time.time() - starting_time

    # Summary
    logging.info(f"Scraping completed successfully. {jobs_processed} jobs processed.")
    logging.info(f"{new_jobs_count} new jobs added.")
    logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(FOLDER_NAME, execution_time, cpu_usage, jobs_processed, new_jobs_count, inactive_jobs_count, skipped_jobs_count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()

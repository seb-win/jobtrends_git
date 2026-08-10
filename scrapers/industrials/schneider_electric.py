import json
import logging
import re
import time
import psutil
from bs4 import BeautifulSoup
from orchestrator.schemas.job_detail_v1 import clean_text, make_job_detail, make_section
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'industrials_jobs'
FOLDER_NAME = 'schneider_electric'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 100
PAGE_START = 1

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'page'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['jobs']
TOTAL_JOBS_KEY = ['totalCount']

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://careers.se.com/global/jobs?page=1&limit=100',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://careers.se.com/api/jobs'

JOB_DATA_KEYS = {
    'created': ['data', 'create_date'],
    'jobTitle': ['data', 'title'],
    'department': ['data', 'category', 0],
    'team': [],
    'location': ['data', 'city'],
    'country': ['data', 'country'],
    'contract': [],
    'id': ['data', 'req_id'],
    'link': [],
    'career_level': [],
    'employment_type': ['data', 'employment_type']
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    'page': '1',
    'limit': '100',
    'sortBy': 'relevance',
    'descending': 'false',
    'internal': 'false',
}
JSON_PAYLOAD = None
DATA = None


def _detail_data(raw_job):
    if isinstance(raw_job, dict) and isinstance(raw_job.get('data'), dict):
        return raw_job['data']
    return raw_job if isinstance(raw_job, dict) else {}


def _first_list_value(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _category_name(data):
    categories = data.get('categories')
    if isinstance(categories, list) and categories:
        first_category = categories[0]
        if isinstance(first_category, dict):
            return first_category.get('name')
        return first_category

    category = data.get('category')
    return _first_list_value(category)


def _location_values(data):
    values = []
    for key in ('full_location', 'short_location', 'location_name'):
        value = clean_text(data.get(key))
        if value and value not in values:
            values.append(value)

    if not values:
        parts = [data.get('city'), data.get('state'), data.get('country')]
        value = clean_text(', '.join(part for part in parts if part))
        if value:
            values.append(value)

    return values


def _extract_compensation(description):
    text = clean_text(description)
    if not text:
        return {}

    pay_match = re.search(
        r'expected pay range is\s+([A-Z]{3})\s+\$?([0-9][0-9,]*(?:\.[0-9]+)?)\s*-\s*\$?([0-9][0-9,]*(?:\.[0-9]+)?)\s+per\s+([A-Za-z]+)',
        text,
        flags=re.IGNORECASE,
    )
    if not pay_match:
        return {}

    currency, min_value, max_value, period = pay_match.groups()
    raw = pay_match.group(0)
    return {
        'raw': raw,
        'currency': currency.upper(),
        'min': float(min_value.replace(',', '')),
        'max': float(max_value.replace(',', '')),
        'period': period.lower(),
        'text': raw,
        'locale': None,
        'location_id': None,
    }


def build_job_detail_v1_from_json(raw_job):
    data = _detail_data(raw_job)
    description = data.get('description')
    responsibilities = data.get('responsibilities')
    qualifications = data.get('qualifications')

    sections = [
        make_section('responsibilities', heading='Responsibilities', text=responsibilities),
        make_section('qualifications', heading='Qualifications', text=qualifications),
    ]

    return make_job_detail(
        job_id=data.get('req_id') or data.get('slug'),
        title=data.get('title'),
        company=data.get('hiring_organization') or 'Schneider Electric',
        metadata={
            'department': None,
            'job_family': _category_name(data),
            'role_type': _first_list_value(data.get('tags1')),
            'employment_type': data.get('employment_type') or _first_list_value(data.get('tags3')),
            'job_type': _first_list_value(data.get('tags7')),
            'career_level': _first_list_value(data.get('tags2')),
            'locations': _location_values(data),
            'created_at': data.get('create_date'),
            'posted_at': data.get('posted_date'),
            'updated_at': data.get('update_date'),
        },
        full_text=description,
        sections=sections,
        compensation=_extract_compensation(description),
    )


def build_job_detail_json(raw_job):
    return json.dumps(build_job_detail_v1_from_json(raw_job), ensure_ascii=False, indent=2)


def _strip_internal_fields(job):
    job.pop('_raw_listing', None)
    return job


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
        params['offset'] = offset
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

    for job in jobs:
        job_id = job.get('id')
        if not job_id:
            skipped_jobs_count += 1
            continue

        existing_entry = next((entry for entry in master_list if entry['id'] == job_id), None)
        raw_listing = job.get('_raw_listing') or job

        if existing_entry:
            update_job_status(existing_entry, current_date)
        else:
            # Add new job
            _strip_internal_fields(job)
            job['scraping_date'] = current_date
            job['last_updated'] = current_date
            job['status'] = 'active'
            master_list.append(job)
            new_jobs_count += 1

        job_detail_json = build_job_detail_json(raw_listing)
        upload_job_details_to_gcs(job_detail_json, job_id, BUCKET_NAME, FOLDER_NAME)

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
    raw_jobs_by_id = {
        get_nested_value(raw_job, ['data', 'req_id']): raw_job
        for raw_job in raw_job_data
        if get_nested_value(raw_job, ['data', 'req_id'])
    }
    for job in jobs:
        raw_listing = raw_jobs_by_id.get(job.get('id'))
        if raw_listing:
            job['_raw_listing'] = raw_listing

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

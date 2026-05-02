import logging
import time
import psutil
from bs4 import BeautifulSoup

from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'mag7'
FOLDER_NAME = 'meta'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = False
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 50
PAGE_START = 1

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'page'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['data', 'job_search']
TOTAL_JOBS_KEY = ['data', 'job_search']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://www.metacareers.com',
    'priority': 'u=1, i',
    'referer': 'https://www.metacareers.com/jobs',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'x-asbd-id': '129477',
    'x-fb-friendly-name': 'CareersJobSearchResultsQuery',
    'x-fb-lsd': 'AVqje43HUpw',
}

DAILY_JOB_URL = 'https://www.metacareers.com/graphql'

JOB_DATA_KEYS = {
    'created': [''],
    'jobTitle': ['title'],
    'department': ['teams', 0],
    'team': ['subteams', 0],
    'location': ['locations'],
    'country': [],
    'contract': [],
    'id': ['id'],
    'link': [],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = None
JSON_PAYLOAD = None
DATA = {
    'av': '0',
    '__user': '0',
    '__a': '1',
    '__req': '1',
    '__hs': '20063.BP:DEFAULT.2.0..0.0',
    'dpr': '2',
    '__ccg': 'EXCELLENT',
    '__rev': '1018677974',
    '__s': '2h4yzi:j6rv6x:8ip368',
    '__hsi': '7445258137743996438',
    '__dyn': '7xeUmwkHg7ebwKBAg5S1Dxu13wqovzEdEc8uxa1twKzobo1nEhwem0nCq1ewcG0RU2Cwooa81VohwnU14E9k2C0sy0H82NxCawcK1iwmE2ewnE2Lw5XwSyES4E3PwbS1Lwqo3cwbq0x8qw53wtU5K0zU5a',
    '__csr': '',
    'lsd': 'AVqje43HUpw',
    'jazoest': '2962',
    '__spin_r': '1018677974',
    '__spin_b': 'trunk',
    '__spin_t': '1733484243',
    '__jssesw': '1',
    'fb_api_caller_class': 'RelayModern',
    'fb_api_req_friendly_name': 'CareersJobSearchResultsQuery',
    'variables': '{"search_input":{"q":null,"divisions":[],"offices":[],"roles":[],"leadership_levels":[],"saved_jobs":[],"saved_searches":[],"sub_teams":[],"teams":[],"is_leadership":false,"is_remote_only":false,"sort_by_new":false,"results_per_page":null}}',
    'server_timestamps': 'true',
    'doc_id': '9114524511922157',
}


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
            job_link = 'https://www.metacareers.com/jobs/'+job['id']
            if job_link:
                response = fetch_url(
                    job_link,
                    headers=HEADERS,
                    params=PARAMS,
                    json=JSON_PAYLOAD,
                    data=DATA,
                    use_proxy=USE_PROXY_DETAILED_POSTINGS,
                    max_retries=3,
                    timeout=10,
                    request_type=REQUEST_TYPE_SINGLE
                )
                if response:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    job_text = soup.get_text()
                    upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)

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

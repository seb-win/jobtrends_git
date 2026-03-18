import logging
import time
import psutil
import requests
from bs4 import BeautifulSoup
import sys
import os
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'defense_jobs'
FOLDER_NAME = 'rtx'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 500
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['refineSearch', 'data', 'jobs']
TOTAL_JOBS_KEY = ['refineSearch', 'totalHits']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    # 'cookie': 'PLAY_SESSION=eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjp7IkpTRVNTSU9OSUQiOiJiNWQxN2JmYy1mZjAyLTQzNjMtODYzNC0xZjZhMjdhZjgxZDIifSwibmJmIjoxNzM2MjY2NDIzLCJpYXQiOjE3MzYyNjY0MjN9.a4tmr1AU9cT4ngEgKkV_lna01WfjXmH2QVPIzxRiTBc; PHPPPE_ACT=b5d17bfc-ff02-4363-8634-1f6a27af81d2; VISITED_LANG=en; VISITED_COUNTRY=global; __cf_bm=FPeEpa.7a.80Bq49PT1X2WaLLSgjJ0OPj6dVIbuTOkk-1736266423-1.0.1.1-gstxeiKhcOkeNsaYGRtDsu1hHZlcVQbMrsdVH_YTiU3D0AR_8FUFssJLZIzZpE1LYt.kGPYVHAbVQUWVR0VtJw; Per_UniqueID=194418b1f3bc52-1fa400-1dd4-194418b1f3c1736; in_ref=https%3A%2F%2Fwww.google.de%2F; cf_clearance=6Q2MUecpvQDKZY6jSYShOYNjCnyn4_pM9pWzlUzSItY-1736266424-1.2.1.1-im8DTLlBrc0DLAmC9nd07z.wk2aVY6p6GArS5SXP1hCXb2w1q.fyzmhK3QIBPvOiCcGNbfy7SdGSsOUcAHY_XrtEk8.XcXd15QxJGfog41Zx2h6nAXMer.dasEDoYhxbDYWS9GrYrB9L6lI.S69Y96p_qT.GNoGjC_SZUprGSXtbGJFXiwTCRY4p2sHG8C4JgZ4YVZ9x7lu3fWytyT3Yv.3oTf5nYunHnG2nJi46H1tqKk6Krpzd51VJHlmzxaaVs0YYZBsfKcOoumtxzIx.ZlzKPZoiMqp4EoVe6ZPoPrmERUzWCjf5ZOKKfBPzceHxnhafamdR.PgjhXMktyOGmnMsqYuik9QCiYaCpw5q6A2DJTR7AZwnSm9r9brxd4nCiieh3MpJVcLHytuq1TTCZg; ext_trk=pjid%3Db5d17bfc-ff02-4363-8634-1f6a27af81d2&uid%3D194418b1f3bc52-1fa400-1dd4-194418b1f3c1736&p_in_ref%3Dhttps://www.google.de/&p_lang%3Den_global&refNum%3DRAYTGLOBAL; _fbp=fb.1.1736266424467.297699282951922174; PHPPPE_GCC=a; _gcl_au=1.1.47785410.1736266425; _RCRTX03=d644816fa64611ef91416f7168178fdff7f4c21437424f6eba3247b49aac089b; _RCRTX03-samesite=d644816fa64611ef91416f7168178fdff7f4c21437424f6eba3247b49aac089b',
    'origin': 'https://careers.rtx.com',
    'priority': 'u=1, i',
    'referer': 'https://careers.rtx.com/global/en/raytheon-search-results?from=10&s=1&rk=l-raytheon-search-results',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'x-csrf-token': '14ece969d08d484eaf34546c82f5b435',
}

DAILY_JOB_URL = 'https://careers.rtx.com/widgets'

JOB_DATA_KEYS = {
    'created': ['postedDate'],
    'jobTitle': ['title'],
    'department': ['category'],
    'team': [],
    'location': ['city'],
    'country': ['country'],
    'contract': ['type'],
    'id': ['jobId'],
    'link': [],
    'career_level': ['experienceLevel'],
    'employment_type': ['type'],
    'skills': ['ml_skills']
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    KEY_NAME: MAX_JOBS_PER_PAGE
}
JSON_PAYLOAD = {
    'lang': 'en_global',
    'deviceType': 'desktop',
    'country': 'global',
    'pageName': 'search-results',
    'ddoKey': 'refineSearch',
    'sortBy': '',
    'subsearch': '',
    'jobs': True,
    'counts': True,
    'all_fields': [
        'businessUnit',
        'country',
        'state',
        'city',
        'postalCode',
        'type',
        'clearanceType',
        'category',
        'location',
        'locationType',
        'relocation',
        'experienceLevel',
    ],
    'size': 500,
    'clearAll': False,
    'jdsource': 'facets',
    'isSliderEnable': False,
    'pageId': 'page19-ds',
    'siteType': 'external',
    'keywords': '',
    'global': True,
    'selected_fields': {},
    'locationData': {},
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
    json_data = {**JSON_PAYLOAD}

    # Adjust pagination parameters based on PAGINATION_MODE
    if PAGINATION_MODE == 'page':
        # Standard page-based pagination
        json_data['page'] = page
    elif PAGINATION_MODE == 'offset':
        # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
        offset = (page * MAX_JOBS_PER_PAGE)
        json_data['from'] = offset
    elif PAGINATION_MODE == 'firstItem':
        # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
        first_item = (page * MAX_JOBS_PER_PAGE) + 1
        json_data['firstItem'] = first_item

    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=None,
        json=json_data,
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
            job_link = 'https://careers.rtx.com/global/en/job/' + job.get('id')
            if job_link:
                response = requests.get(job_link, headers=HEADERS)
                text = response.text
                # response = fetch_url(
                #     job_link,
                #     headers=HEADERS,
                #     params=PARAMS,
                #     json=JSON_PAYLOAD,
                #     data=DATA,
                #     use_proxy=USE_PROXY_DETAILED_POSTINGS,
                #     max_retries=3,
                #     timeout=10,
                #     request_type=REQUEST_TYPE_SINGLE
                # )
                if text:
                    soup = BeautifulSoup(text, 'html.parser')
                    job_text = soup.get_text()
                    upload_job_details_to_gcs(text, job_id, BUCKET_NAME, FOLDER_NAME)

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

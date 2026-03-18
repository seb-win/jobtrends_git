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
BUCKET_NAME = 'semi_comp'
FOLDER_NAME = 'applied_materials'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = True
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
JOBS_LIST_KEY = ['positions']
TOTAL_JOBS_KEY = ['count']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'content-type': 'application/json',
    # 'cookie': 's_dslv=1735827281958; OptanonAlertBoxClosed=2025-01-02T14:14:44.267Z; OptConsentGroups=%2CC0001%2CC0002%2CC0003%2CC0004%2C; adobeujs-optin=%7B%22aam%22%3Afalse%2C%22adcloud%22%3Afalse%2C%22aa%22%3Atrue%2C%22campaign%22%3Afalse%2C%22ecid%22%3Atrue%2C%22livefyre%22%3Afalse%2C%22target%22%3Atrue%2C%22mediaaa%22%3Afalse%7D; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Jan+02+2025+15%3A14%3A44+GMT%2B0100+(Mitteleurop%C3%A4ische+Normalzeit)&version=202405.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=33783999-7f96-461d-9293-f47a4bc0032d&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&intType=1; AMCVS_A6B7894C5967E3BC0A495D55%40AdobeOrg=1; AMCV_A6B7894C5967E3BC0A495D55%40AdobeOrg=179643557%7CMCMID%7C86537047401631643424560285686900656752%7CMCAAMLH-1736432084%7C6%7CMCAAMB-1736432084%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1735834484s%7CNONE%7CvVersion%7C5.5.0; s_sq=%5B%5BB%5D%5D; s_cc=true; _vs=871386120630828504:1735827286.1078255:8599862435085657918; _vscid=2; _ga=GA1.1.700353744.1735827288; _ga_XLJ82L4YG8=GS1.1.1735827287.1.1.1735827440.16.0.0',
    'priority': 'u=1, i',
    'referer': 'https://careers.appliedmaterials.com/careers?Country=United%20States%20of%20America&domain=appliedmaterials.com&sort_by=relevance&triggerGoButton=false',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': '1a24d225734c4e4dba35a02b760765bb-9a0828ad1b467fa1-0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://careers.appliedmaterials.com/api/apply/v2/jobs'

JOB_DATA_KEYS = {
    'created': ['t_create'],
    'jobTitle': ['name'],
    'department': ['department'],
    'team': [],
    'location': ['location'],
    'country': [],
    'contract': [],
    'id': ['ats_job_id'],
    'link': ['canonicalPositionUrl'],
    'career_level': [],
    'employment_type': [],
    'description': ['job_description']
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    ('domain', 'appliedmaterials.com'),
    ('start', '0'),
    ('num', '10'),
    ('Country', 'United States of America'),
    ('domain', 'appliedmaterials.com'),
    ('sort_by', 'relevance'),
}
JSON_PAYLOAD = None
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

    # Copy existing params (convert set to list for easier modification)
    params = list(PARAMS)

    # Adjust pagination parameters based on PAGINATION_MODE
    if PAGINATION_MODE == 'page':
        # Standard page-based pagination
        params.append(('page', page))  # Adding page to the params

    elif PAGINATION_MODE == 'offset':
        # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
        offset = (page * MAX_JOBS_PER_PAGE)
        for i, param in enumerate(params):
            if param[0] == 'start':
                params[i] = ('start', offset)  # Update the 'start' parameter

    elif PAGINATION_MODE == 'firstItem':
        # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
        first_item = (page * MAX_JOBS_PER_PAGE) + 1
        params.append(('firstItem', first_item))  # Adding firstItem to the params

    # Convert list back to set
    params = set(params)

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
            # job_link = job.get('link')
            # if job_link:
            #     response = fetch_url(
            #         job_link,
            #         headers=HEADERS,
            #         params=PARAMS,
            #         json=JSON_PAYLOAD,
            #         data=DATA,
            #         use_proxy=USE_PROXY_DETAILED_POSTINGS,
            #         max_retries=3,
            #         timeout=10,
            #         request_type=REQUEST_TYPE_SINGLE
            #     )
            #     if response:
            #         soup = BeautifulSoup(response.text, 'html.parser')
            #         job_text = soup.get_text()
            #         upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)

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

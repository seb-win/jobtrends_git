import logging
import time
import psutil
from bs4 import BeautifulSoup
import re
import json
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'pharma_jobs'
FOLDER_NAME = 'merck'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = False
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 1000
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['refineSearch', 'data', 'jobs']
TOTAL_JOBS_KEY = ['refineSearch', 'data', 'totalHits']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    # 'cookie': 'OptanonAlertBoxClosed=2025-01-19T15:24:06.438Z; OptanonConsent=isGpcEnabled=0&datestamp=Sun+Jan+19+2025+16%3A24%3A06+GMT%2B0100+(Mitteleurop%C3%A4ische+Normalzeit)&version=202401.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=f42f497d-5137-4c75-9d31-da2754df0724&interactionCount=2&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A0%2CC0004%3A0%2CC0005%3A0; VISITED_LANG=en; VISITED_COUNTRY=us; _gcl_au=1.1.145405795.1737300251; in_ref=https%3A%2F%2Fwww.merck.com%2F; _fbp=fb.1.1737300251884.179183951565444434; iq_s=556af9fc2f9cf9067090; PHPPPE_GCC=d; PLAY_SESSION=eyJhbGciOiJIUzI1NiJ9.eyJkYXRhIjp7IkpTRVNTSU9OSUQiOiJhZWExNDcxNi1lYWYxLTQzN2EtOTg5Ny01MWQ3MGRjN2FjNDcifSwibmJmIjoxNzM3MzIyODY1LCJpYXQiOjE3MzczMjI4NjV9.c2mTOZ7exT7LH-_WiBiQ843vwCE050rgShI1J_v-QLk; PHPPPE_ACT=aea14716-eaf1-437a-9897-51d70dc7ac47; ext_trk=pjid%3Daea14716-eaf1-437a-9897-51d70dc7ac47&p_in_ref%3Dhttps://www.merck.com/&p_lang%3Den_us&refNum%3DMERCUS; OptanonConsent=isGpcEnabled=0&datestamp=Sun+Jan+19+2025+22%3A41%3A16+GMT%2B0100+(Mitteleurop%C3%A4ische+Normalzeit)&version=6.10.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=f42f497d-5137-4c75-9d31-da2754df0724&interactionCount=2&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A0%2CC0004%3A0%2CC0005%3A0',
    'origin': 'https://jobs.merck.com',
    'priority': 'u=1, i',
    'referer': 'https://jobs.merck.com/us/en/search-results',
    'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    #'x-csrf-token': 'c537d360a74045fcb9830045938aba25',
}

DAILY_JOB_URL = 'https://jobs.merck.com/widgets'

JOB_DATA_KEYS = {
    'created': ['dateCreated'],
    'jobTitle': ['title'],
    'department': ['category'],
    'team': ['subCategory'],
    'location': ['city'],
    'country': ['country'],
    'contract': ['type'],
    'id': ['reqId'],
    'link': ['applyUrl'],
    'career_level': [],
    'employment_type': [],
    'skills': ['ml_skills']
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = None
JSON_PAYLOAD = {
    'lang': 'en_us',
    'deviceType': 'desktop',
    'country': 'us',
    'pageName': 'search-results',
    'ddoKey': 'refineSearch',
    'sortBy': '',
    'subsearch': '',
    'from': 0,
    'jobs': True,
    'counts': True,
    'all_fields': [
        'category',
        'subCategory',
        'country',
        'state',
        'city',
        'type',
        'divisionList',
        'locationType',
    ],
    'size': 500,
    'clearAll': False,
    'jdsource': 'facets',
    'isSliderEnable': False,
    'pageId': 'page2',
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
        time.sleep(1)
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
            apply_link = job.get('link')
            if isinstance(apply_link, str):
                job_link = re.sub(r'/apply$', '', apply_link)
            else:
                print(f"Invalid apply_link: {apply_link}")
                job_link = 'https://jobs.merck.com/us/en/job/' + job.get('id')

            if job_link:
                response = fetch_url(
                    job_link,
                    headers=HEADERS,
                    params=PARAMS,
                    json=None,
                    data=DATA,
                    use_proxy=USE_PROXY_DETAILED_POSTINGS,
                    max_retries=3,
                    timeout=10,
                    request_type=REQUEST_TYPE_SINGLE
                )
                if response:
                    #soup = BeautifulSoup(response.text, 'html.parser')
                    #job_text = soup.get_text()

                    upload_job_details_to_gcs(response.text, job_id, BUCKET_NAME, FOLDER_NAME)

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

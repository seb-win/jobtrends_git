import logging
from bs4 import BeautifulSoup
import re

from orchestrator.util_v2 import get_proxy, fetch_url, load_master_list, save_master_list, get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
import requests
import time
import psutil


# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'semi_comp'
FOLDER_NAME = 'micron'

USE_PROXY_DAILY_LIST = True
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
    # 'cookie': 'AMCVS_39BFB008560A6FB87F000101%40AdobeOrg=1; AMCV_39BFB008560A6FB87F000101%40AdobeOrg=179643557%7CMCIDTS%7C20091%7CMCMID%7C82282883802785932483846498049294844966%7CMCAAMLH-1736441108%7C6%7CMCAAMB-1736441108%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1735843508s%7CNONE%7CvVersion%7C5.5.0; s_cc=true; _gcl_au=1.1.445087954.1735836308; _ga_GVCT99X2WN=GS1.1.1735836308.1.0.1735836308.0.0.0; _ga=GA1.1.377628405.1735836308; _mkto_trk=id:944-LZM-763&token:_mch-micron.com-1c2d17b7cf0c5c2bc083ecee4fb509c8; OptanonAlertBoxClosed=2025-01-02T16:45:16.922Z; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Jan+02+2025+17%3A45%3A16+GMT%2B0100+(Mitteleurop%C3%A4ische+Normalzeit)&version=202402.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=3576bcff-b42d-44e1-be46-0aca00dcc91d&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0003%3A0%2CC0001%3A1%2CC0004%3A0%2CC0002%3A0&hosts=H203%3A0%2CH145%3A0%2CH207%3A0%2CH147%3A0%2CH131%3A1%2CH155%3A1%2CH114%3A1%2CH87%3A1%2CH224%3A1%2CH115%3A1%2CH9%3A0%2CH198%3A0%2CH151%3A0%2CH60%3A0%2CH182%3A0%2CH183%3A0%2CH219%3A0%2CH184%3A0%2CH100%3A0%2CH61%3A0%2CH220%3A0%2CH62%3A0%2CH186%3A0%2CH164%3A0%2CH63%3A0%2CH64%3A0%2CH14%3A0%2CH142%3A0%2CH110%3A0%2CH66%3A0%2CH67%3A0%2CH153%3A0%2CH18%3A0%2CH19%3A0%2CH20%3A0%2CH69%3A0%2CH144%3A0%2CH70%3A0%2CH21%3A0%2CH211%3A0%2CH71%3A0%2CH72%3A0%2CH167%3A0%2CH23%3A0%2CH25%3A0%2CH26%3A0%2CH179%3A0%2CH96%3A0%2CH102%3A0%2CH187%3A0%2CH74%3A0%2CH6%3A0%2CH195%3A0%2CH31%3A0%2CH154%3A0%2CH32%3A0%2CH188%3A0%2CH75%3A0%2CH189%3A0%2CH76%3A0%2CH226%3A0%2CH201%3A0%2CH34%3A0%2CH1%3A0%2CH2%3A0%2CH35%3A0%2CH77%3A0%2CH78%3A0%2CH248%3A0%2CH79%3A0%2CH190%3A0%2CH191%3A0%2CH169%3A0%2CH80%3A0%2CH40%3A0%2CH41%3A0%2CH98%3A0%2CH81%3A0%2CH43%3A0%2CH213%3A0%2CH7%3A0%2CH44%3A0%2CH46%3A0%2CH82%3A0%2CH227%3A0%2CH241%3A0%2CH156%3A0%2CH48%3A0%2CH83%3A0%2CH49%3A0%2CH86%3A0%2CH3%3A0%2CH197%3A0%2CH52%3A0%2CH192%3A0%2CH53%3A0%2CH54%3A0%2CH232%3A0%2CH193%3A0%2CH194%3A0%2CH101%3A0%2CH88%3A0%2CH57%3A0%2CH185%3A0%2CH113%3A0%2CH196%3A0&genVendors=; s_sq=%5B%5BB%5D%5D; _vs=871386120630828504:1735836319.7613335:5477420368698079494; _vscid=3',
    'priority': 'u=1, i',
    'referer': 'https://careers.micron.com/careers',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://careers.micron.com/api/apply/v2/jobs'

JOB_DATA_KEYS = {
    'created': ['t_create'],
    'jobTitle': ['name'],
    'department': ['department'],
    'team': [],
    'location': ['location'],
    'country': [],
    'contract': [],
    'id': ['id'],
    'link': ['canonicalPositionUrl'],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    ('domain', 'micron.com'),
    ('num', '10'),
    ('domain', 'micron.com'),
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
            job_link = 'https://careers.micron.com/api/apply/v2/jobs/'+ str(job.get('id'))
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
                    #soup = BeautifulSoup(response.text, 'html.parser')
                    #job_text = soup.get_text()
                    json_obj = response.json()
                    job_text = json_obj['job_description']
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

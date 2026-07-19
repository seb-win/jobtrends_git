import logging
import re
from bs4 import BeautifulSoup
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list, 
    get_current_date, get_nested_value, get_storage_client, update_job_status, upload_job_details_to_gcs, send_metrics_to_cloud_function
)
import time
import psutil

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'tech_jobs'
FOLDER_NAME = 'salesforce'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = False
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 50
PAGE_START = 1

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'page'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['Report_Entry']
TOTAL_JOBS_KEY = ['Count']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'origin': 'https://www.salesforce.com',
    'priority': 'u=1, i',
    'referer': 'https://www.salesforce.com/',
    'sec-ch-ua': '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://a.sfdcstatic.com/digital/xsf/careers/prod/jobs_1.json'
WORKDAY_CXS_BASE_URL = 'https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce'

JOB_DATA_KEYS = {
    'created': ['dummy_key'],
    'updated': [''],
    'jobTitle': ['Job_Posting_Title'],
    'department': ['Job_Family_Group'],
    'team': [],
    'location': ['Job_Requisition_Primary_Location'],
    'country': [],
    'contract': [],
    'id': ['Job_Requisition_Ref_ID'],
    'link': ['External_Job_Posting_Site'],
    'career_level': [],
    'employment_type': [],
    '_job_description': ['Job_Description'],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    KEY_NAME: MAX_JOBS_PER_PAGE
}
JSON_PAYLOAD = None
DATA = None

def clean_text(value):
    if not value:
        return None

    # HTML entities dekodieren
    value = html.unescape(value)

    # HTML entfernen
    text = BeautifulSoup(value, "html.parser").get_text(separator=" ", strip=True)

    # Geschützte Leerzeichen etc. normalisieren
    text = text.replace("\xa0", " ")

    # Mehrfache Whitespaces entfernen
    text = re.sub(r"\s+", " ", text).strip()

    return text


def build_workday_detail_url(job_link):
    """
    Convert the public Workday job URL from the daily JSON into the CXS JSON URL.

    Example:
    https://salesforce.wd12.myworkdayjobs.com/External_Career_Site/job/.../Role_JR123
    -> https://salesforce.wd12.myworkdayjobs.com/wday/cxs/salesforce/External_Career_Site/job/.../Role_JR123
    """
    if not job_link:
        return None

    parsed_url = urlparse(job_link)
    if not parsed_url.path:
        return None

    return f"{WORKDAY_CXS_BASE_URL}{parsed_url.path}"


def extract_job_details_from_detail_response(response, fallback_title=None, fallback_location=None):
    """Extract selected fields from a Workday CXS detail response for GCS upload."""
    try:
        detail_json = response.json()
    except ValueError:
        return {
            'title': clean_text(fallback_title),
            'jobDescription': clean_text(response.text),
            'posted': None,
            'location': fallback_location,
        }

    job_posting_info = get_nested_value(detail_json, ['jobPostingInfo'])
    if not isinstance(job_posting_info, dict):
        job_posting_info = {}

    return {
        'title': clean_text(job_posting_info.get('title') or fallback_title),
        'jobDescription': clean_text(job_posting_info.get('jobDescription')),
        'posted': job_posting_info.get('startDate'),
        'location': job_posting_info.get('location') or fallback_location,
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
    params = {**PARAMS} if PARAMS else {}

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

            # Fetch job details from the Workday CXS JSON endpoint.
            job_link = build_workday_detail_url(job.get('link'))
            if job_link:
                response = fetch_url(
                    job_link,
                    headers=HEADERS,
                    params=None,
                    json=None,
                    data=None,
                    use_proxy=USE_PROXY_DETAILED_POSTINGS,
                    max_retries=3,
                    timeout=10,
                    request_type=REQUEST_TYPE_SINGLE
                )
                if response:
                    job_details = extract_job_details_from_detail_response(
                        response,
                        fallback_title=job.get('jobTitle'),
                        fallback_location=job.get('location'),
                    )
                    upload_job_details_to_gcs(
                        json.dumps(job_details, ensure_ascii=False, indent=2),
                        job_id,
                        BUCKET_NAME,
                        FOLDER_NAME,
                    )
            else:
                logging.warning(f"Could not build Workday detail URL for job {job_id}; skipping detail upload.")

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

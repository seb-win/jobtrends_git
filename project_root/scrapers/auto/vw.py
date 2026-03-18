import logging
import re
import random
from bs4 import BeautifulSoup
import time
import psutil
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list, 
    get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'automotive_comp'
FOLDER_NAME = 'vw'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = True
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 25
PAGE_START = 0
JOB_LIST_KEY = 'tr.data-row'

HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'de-DE,de;q=0.9',
        'Connection': 'keep-alive',
        # 'Cookie': 'JSESSIONID=w1~E8EDD62F98F9A55DDDBB58B33CB85CE6',
        'Referer': 'https://jobs.volkswagen-group.com/Volkswagen/search/?searchby=location&createNewAlert=false&q=&locationsearch=&geolocation=&optionsFacetsDD_city=&optionsFacetsDD_facility=Volkswagen+AG&optionsFacetsDD_department=&optionsFacetsDD_customfield2=&optionsFacetsDD_shifttype=&optionsFacetsDD_customfield3=',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
}

params = {
    'q': '',
    'sortColumn': 'referencedate',
    'sortDirection': 'desc',
    'searchby': 'location',
    'd': '15',
    'optionsFacetsDD_facility': 'Volkswagen AG',
    'locale': 'de_DE'
}

DAILY_JOB_URL = 'https://jobs.volkswagen-group.com/Volkswagen/search/'

# Define job detail keys for customization, including optional fields
JOB_DATA_KEYS = {
    'created': 'span.jobDate',
    'jobTitle': 'a.jobTitle-link',
    'department': 'span.jobDepartment',
    'team': None,
    'location': 'span.jobLocation',
    'country': None,
    'contract': None,
    'id': 'a.jobTitle-link[href]',
    'link': 'a.jobTitle-link[href]',
    'career_level': None,
    'employment_type': None,
    'company': 'span.jobFacility',
}

# Special extraction logic for certain keys if needed
extraction_logic = {
    #'id': lambda el: el.get_text(strip=True)
    # Add more special cases if needed
}


def process_jobs(job_postings):
    """
    Extract relevant job details from job postings using JOB_DATA_KEYS.
    If the selector contains an attribute (e.g., [href]), extract that attribute.
    Otherwise, extract text content.
    """
    jobs = []
    for job in job_postings:
        job_details = {
            'scraping_date': None,
            'last_updated': None,
            'status': None,
            'keywords': []
        }

        for key, selector in JOB_DATA_KEYS.items():
            if not selector:
                continue

            element = job.select_one(selector)
            if not element:
                continue

            # Check for an attribute pattern like [href]
            match = re.search(r'\[(.*?)\]', selector)
            if match:
                attribute_name = match.group(1)
                extractor = extraction_logic.get(key, lambda el: el.get(attribute_name, None))
                job_details[key] = extractor(element)
            else:
                # No attribute pattern, fallback to special logic or text extraction
                extractor = extraction_logic.get(key, lambda el: el.get_text(strip=True))
                job_details[key] = extractor(element)

        jobs.append(job_details)
    return jobs


def fetch_job_list_page(url, headers, params, use_proxy=False):
    """Fetch a single page of the job list."""
    response = fetch_url(
        url,
        headers=headers,
        params=params,
        json=None,
        data=None,
        use_proxy=use_proxy,
        max_retries=3,
        timeout=10,
        request_type=REQUEST_TYPE_LIST
    )
    if not response:
        logging.error("Failed to fetch job list page after multiple attempts.")
        return None

    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        job_postings = soup.select(JOB_LIST_KEY)
        return job_postings
    except Exception as e:
        logging.error(f"Failed to parse response HTML: {e}")
        return None


def fetch_all_jobs():
    """
    Fetch all job postings. If USE_PAGINATION is True, iterate through multiple pages.
    Otherwise, fetch only a single page.
    """
    all_jobs = []

    params = {
        'q': '',
        'sortColumn': 'referencedate',
        'sortDirection': 'desc',
        'searchby': 'location',
        'd': '15',
        'optionsFacetsDD_facility': 'Volkswagen AG',
        'locale': 'de_DE'
    }

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings...")
            params['startrow'] = 25 * page
            job_postings = fetch_job_list_page(DAILY_JOB_URL, HEADERS, params, use_proxy=USE_PROXY_DAILY_LIST)
            print(len(job_postings))
            if not job_postings:
                logging.info("No more job postings found, or failed to retrieve job postings.")
                break

            jobs = process_jobs(job_postings)
            all_jobs.extend(jobs)

            if len(job_postings) < MAX_JOBS_PER_PAGE:
                logging.info("Fewer jobs than MAX_JOBS_PER_PAGE found, ending pagination.")
                break

            page += 1
    else:
        logging.info("Fetching a single page of job listings...")
        job_postings = fetch_job_list_page(DAILY_JOB_URL, HEADERS, params, use_proxy=USE_PROXY_DAILY_LIST)
        print(len(job_postings))
        if job_postings:
            jobs = process_jobs(job_postings)
            all_jobs.extend(jobs)
        else:
            logging.info("No job postings found.")

    return all_jobs


def update_master_list_with_jobs(all_jobs, master_list):
    """
    Update the master list with new or existing jobs, fetch job details when needed,
    and mark old jobs as inactive.
    """
    current_date = get_current_date()
    new_jobs_count = 0
    inactive_jobs_count = 0
    skipped_jobs_count = 0

    for job in all_jobs:
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

            # # Fetch job details if link exists
            job_link = 'https://jobs.volkswagen-group.com'+job.get('link')
            if job_link:
                response = fetch_url(
                    job_link,
                    headers=HEADERS,
                    params=params,
                    json=None,
                    data=None,
                    use_proxy=USE_PROXY_DETAILED_POSTINGS,
                    max_retries=3,
                    timeout=10,
                    request_type=REQUEST_TYPE_SINGLE
                )
                if response:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    fake_id = get_current_date() + str(random.randint(10000, 99999))
                    job_text = soup.get_text()
                    upload_job_details_to_gcs(job_text, fake_id, BUCKET_NAME, FOLDER_NAME)

    # Mark old jobs as inactive
    for entry in master_list:
        if entry['last_updated'] != current_date:
            entry['status'] = 'inactive'
            inactive_jobs_count += 1

    return new_jobs_count, inactive_jobs_count, skipped_jobs_count


def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")

    # Set starting time and initiate cpu usage measurement
    start_time = time.time()
    cpu_usage = psutil.cpu_percent(interval=1)

    # Step 1: Fetch all jobs (with or without pagination)
    all_jobs = fetch_all_jobs()

    # Step 2: Load master list
    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)

    # Step 3: Update master list with the fetched jobs
    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(all_jobs, master_list)

    # Step 4: Save the updated master list
    save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

    # Calculate execution time
    execution_time = time.time() - start_time

    # Summary Log
    logging.info(f"Scraping completed successfully. {len(all_jobs)} jobs processed.")
    logging.info(f"{new_jobs_count} new jobs added.")
    logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(FOLDER_NAME, execution_time, cpu_usage, len(all_jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()

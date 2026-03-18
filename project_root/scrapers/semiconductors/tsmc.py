import logging
from bs4 import BeautifulSoup
import re
from orchestrator.util_v2 import get_proxy, fetch_url, load_master_list, save_master_list, get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
import requests
import time
import psutil
import inspect

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'semi_comp'
FOLDER_NAME = 'tsmc'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = True
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 10
PAGE_START = 0
JOB_LIST_KEY = 'div.article__header'

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    # 'cookie': 'portalLanguage-6=en_US; userCookieConsent-6=%7B%221%22%3Atrue%2C%222%22%3Atrue%2C%223%22%3Atrue%7D; ScustomPortal-Np44PUbg2mK35qZa=8jtiae8mante0oh3q9ht18otqk',
    'priority': 'u=0, i',
    'referer': 'https://careers.tsmc.com/en_US/careers/SearchJobs',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

params = {
    'jobRecordsPerPage': '10',
}

DAILY_JOB_URL = 'https://careers.tsmc.com/en_US/careers/SearchJobs/'

# Define job detail keys for customization, including optional fields
JOB_DATA_KEYS = {
    'created': 'span.list-item-posted',
    'jobTitle': 'a.link',
    'department': 'span.list-item-careerArea',
    'team': None,
    'location': 'span.list-item-location',
    'country': None,
    'contract': 'span.list-item-employmentType',
    'id': 'a.link[href]',
    'link': 'a.link[href]',
    'career_level': None,
    'employment_type': None,
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

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings...")
            params['jobOffset'] = page * 10
            job_postings = fetch_job_list_page(DAILY_JOB_URL, HEADERS, params=params, use_proxy=USE_PROXY_DAILY_LIST)
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
        params['jobOffset'] = 630
        job_postings = fetch_job_list_page(DAILY_JOB_URL, HEADERS, params, use_proxy=USE_PROXY_DAILY_LIST)
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
        
        if isinstance(job_id, str):
            match = re.search(r"jobId=(\d+)", job_id)
            if match:
                id_append = match.group(1)
        else:
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

            # Fetch job details if link exists
            job_link = job.get('link')
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
                    job_text = soup.get_text()
                    upload_job_details_to_gcs(job_text, id_append, BUCKET_NAME, FOLDER_NAME)

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

import logging
import re
from bs4 import BeautifulSoup
import sys
import os
import json
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list, 
    get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, send_metrics_to_cloud_function
)
import time
import psutil

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'defense_jobs'
FOLDER_NAME = 'lockheed'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = False
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 50
PAGE_START = 1
JOB_LIST_KEY = 'table#tblResultSet tbody'

HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json; charset=utf-8',
    # 'Cookie': 'SearchVisitorId=21ff1498-d9ac-fefb-30f4-a57331b80842; _gid=GA1.2.993376861.1736166579; _gcl_au=1.1.1369621740.1736166579; _fbp=fb.1.1736166579615.777090341290539721; SearchSessionId={%22SearchSessionId%22:%227d9ff16f-ee2f-3665-82b0-dc60d20db088%22%2C%22ImpressionParentId%22:%22%22%2C%22ViewParentId%22:%22%22%2C%22GoogleSearchRequestId%22:%22%22%2C%22GoogleJobId%22:%22%22%2C%22Created%22:%221736166579678%22}; cb-enabled=enabled; BannerDisplayed=true; _pk_id.7.29ed=e1909f214639eba6.1736166580.; _pk_ses.7.29ed=1; ConsentCapture=Mon Jan 06 2025 13:29:43 GMT+0100 (MitteleuropÃ¤ische Normalzeit); _sp_ses.fdc6=*; _sp_id.fdc6=69a93b75-1faf-4ef2-8395-a4f2ced9c906.1736166591.1.1736166595.1736166591.25569382-095b-486b-96e1-c70d8ef07e0c; viewedJobs=vBbmooTx%252bGjTxjShPZDGgyhUOdcD4b0HyURmsZmuppVSY%252bRcvyVUwxqjWKKmCMgJdePmKoPfYBv6Qk6gpln0OYF%252fnsf%252fUmB8tZZedT3B7QiSUY8dvQWfA0GuAfyUBffPXPVW%252flk9SKrOk8fjLhj%252b5dhlt4ItyyOWbH0kTe0%252bVLx%252b%252fGPPyUmmbCS24taYWXko; _ga=GA1.1.613845219.1736166579; PersonalizationCookie=[{%22Locations%22:[{%22Path%22:%226252001-4155751-4167060-4167147%22%2C%22FacetType%22:4}]%2C%22Categories%22:[%2257621%22]%2C%22PersonalizationType%22:1%2C%22DateCreated%22:%222025-01-06T12:29:54.630Z%22%2C%22CustomFacets%22:[{%22CustomFacetValue%22:%22E1973:Structural%20Engineer%20Sr%22%2C%22CustomFacetTerm%22:%22campaign%22}]%2C%22TenantId%22:694}%2C{%22Locations%22:[{%22Path%22:%226252001%22%2C%22FacetType%22:2}]%2C%22Categories%22:[]%2C%22PersonalizationType%22:1%2C%22DateCreated%22:%222025-01-06T12:30:21.424Z%22%2C%22CustomFacets%22:[{%22CustomFacetValue%22:%22Experienced%20Professional%22%2C%22CustomFacetTerm%22:%22job_status%22}]%2C%22TenantId%22:694}]; _4c_=%7B%22_4c_s_%22%3A%22fZHLboMwEEV%2FpfI6Jn5hHruqlaouuuwage0UGhIjY%2BKmEf9eD4mSVpHKBnPn3sFz5oRCa%2FaopBmXVErJuCTZCm3NcUTlCblOw%2BuASmSaPG%2FMhuK6Jg0WNWe4yHmKCyKF4IoVpk7RCn1BL8ELwdKsyFkxr5AaLj1OSFltYi9aJFQkAm%2FGmPDfUZEkngZn9aR85Y8DuIJpHka9jQVtDp0yVei0b5c4Ize1Nd1H60Em%2BSIPDj4SBtcJ3V7bcE1KIm7iNZhJGdXG2TAayD61zu7MA%2BU0yjaCQG%2B1ikdnNsa5xdJ6P4zleh1CSHqrtq0xelc73%2B0TZXfraB47D0P8LX7aZgTDpR4x%2F7LgsyfWJtdXaojYEFAB%2BnDt6Kt76DkBtpfH6v31GWaiPBcpA6bnJUbwaL5sgkuWcsKopGkk7XtU5lIQeOZz52Ux4s4t7t1nPhhGwNP4T%2FbuT%2FP8Aw%3D%3D%22%7D; _ga_P6FDZHMP9X=GS1.2.1736166579.1.1.1736166659.60.0.0; _ga_8LPVTQ4PCG=GS1.1.1736166579.1.1.1736166659.60.0.1857807469',
    'Referer': 'https://www.lockheedmartinjobs.com/search-jobs',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

DAILY_JOB_URL = 'https://www.lockheedmartinjobs.com/search-jobs/results'

params = {
    'ActiveFacetID': '0',
    'CurrentPage': '1',
    'RecordsPerPage': '3000',
    'TotalContentResults': '',
    'Distance': '50',
    'RadiusUnitType': '0',
    'Keywords': '',
    'Location': '',
    'ShowRadius': 'False',
    'IsPagination': 'False',
    'CustomFacetName': '',
    'FacetTerm': '',
    'FacetType': '0',
    'SearchResultsModuleName': 'Search Results',
    'SearchFiltersModuleName': 'Search Filters',
    'SortCriteria': '0',
    'SortDirection': '0',
    'SearchType': '5',
    'PostalCode': '',
    'ResultsType': '0',
    'fc': '',
    'fl': '',
    'fcf': '',
    'afc': '',
    'afl': '',
    'afcf': '',
    'TotalContentPages': 'NaN',
}

# Define job detail keys for customization, including optional fields
JOB_DATA_KEYS = {
    'created': 'span.job-date-posted',
    'jobTitle': 'span.job-title',
    'department': None,
    'team': None,
    'location': 'span.job-location',
    'country': None,
    'contract': None,
    'id': 'a[data-job-id]',
    'link': 'a[href]',
    'career_level': None,
    'employment_type': None,
}

# Special extraction logic for certain keys if needed
extraction_logic = {
    'id': lambda el: el.get('data-job-id', 'No ID'),
    'href': lambda el: el.get('href', 'No HREF'),
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
        json_obj = json.loads(response.text)
        text = json_obj['results']
        soup = BeautifulSoup(text, 'html.parser')
        job_postings = soup.find_all('li')
        return job_postings
    except Exception as e:
        logging.error(f"Failed to parse response HTML: {e}")
        return None


def fetch_all_jobs():
    global params
    """
    Fetch all job postings. If USE_PAGINATION is True, iterate through multiple pages.
    Otherwise, fetch only a single page.
    """
    all_jobs = []

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings...")
            params = {'page': page, 'limit': MAX_JOBS_PER_PAGE}
            job_postings = fetch_job_list_page(DAILY_JOB_URL, HEADERS, params, use_proxy=USE_PROXY_DAILY_LIST)
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
        print(f'Length: {len(job_postings)}')
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

            # Fetch job details if link exists
            job_link = 'https://www.lockheedmartinjobs.com' + job.get('link')
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

import logging
import re
from bs4 import BeautifulSoup
import sys
import os
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list, 
    get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, send_metrics_to_cloud_function
)
import time
import psutil

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'consulting_jobs'
FOLDER_NAME = 'deloitte_de'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = True
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 10
PAGE_START = 1
JOB_LIST_KEY = 'a.list-item-title'

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    # 'cookie': '_fbp=fb.1.1733493225250.849372753289862653; CookieConsent={stamp:%27I4DI49Bt3mfXNjNaocWJvug7oYBUhrEqOmPM+AcOPFpY1ejQD4TXOA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:2%2Cutc:1733493235389%2Cregion:%27de%27}; _gcl_au=1.1.575355730.1733493235; _pk_id.5.c26e=b3fa1696a66ec82c.1733493238.; OneTrustConsentShare_GLOBAL=optboxclosed=true&optboxexpiry=2024-12-06T13:55:41.584Z&isGpcEnabled=0&datestamp=Fri+Dec+06+2024+14:55:42+GMT+0100+(MitteleuropÃ¤ische+Normalzeit)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=2ca41cd1-753c-4178-a477-5da0cbca2376&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=1:1,2:1,3:1,4:1&geolocation=DE; _hjSessionUser_3494500=eyJpZCI6IjI1ZmM4MDlmLTEwZDAtNTNiYS1iNjExLWI0ZmJjNDlmNmNmMSIsImNyZWF0ZWQiOjE3MzM0OTMzNDI1MzcsImV4aXN0aW5nIjp0cnVlfQ==; s_ecid=MCMID%7C82461604411762886323828479749047058366; AKA_A2=A; OneTrustConsentShare_DE=optboxclosed=true&optboxexpiry=2024-12-06T13:53:44.116Z&isGpcEnabled=0&datestamp=Fri+Jan+03+2025+13:29:25+GMT+0100+(MitteleuropÃ¤ische+Normalzeit)&version=202411.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=0ca8b032-74ad-464a-9d32-dd474835c63a&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=1:1,2:1,3:1,4:1&geolocation=DE&AwaitingReconsent=false; at_check=true; AMCVS_5742550D515CABFF0A490D44%40AdobeOrg=1; AMCV_5742550D515CABFF0A490D44%40AdobeOrg=179643557%7CMCMID%7C82461604411762886323828479749047058366%7CMCAAMLH-1736512166%7C6%7CMCAAMB-1736512166%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1735914566s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.5.0; mbox=PC#69312bacdb2049edbc402745a0fc574b.37_0#1799152167|session#51c5ea83daf245ef900044e35a816483#1735909227; gpv_Page=%2Fde%2Fde%2Fservices%2Fconsulting%2Fservices%2Fmonitor-deloitte; s_cc=true; s_sq=deloittecomnewplatformprod%3D%2526c.%2526a.%2526activitymap.%2526page%253D%25252Fde%25252Fde%25252Fservices%25252Fconsulting%25252Fservices%25252Fmonitor-deloitte%2526link%253DJobsuche%2526region%253Dheader%2526pageIDType%253D1%2526.activitymap%2526.a%2526.c%2526pid%253D%25252Fde%25252Fde%25252Fservices%25252Fconsulting%25252Fservices%25252Fmonitor-deloitte%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Fjob.deloitte.com%25252F%25253Ficid%25253Dtop_https%25253A%25252F%25252Fjob.deloitte.com%25252F%2526ot%253DA; sessionid=0m3rihpvud3plp7ebbvf45vzk3nt8iqk; _pk_ref.5.c26e=%5B%22%22%2C%22%22%2C1735907374%2C%22https%3A%2F%2Fwww.deloitte.com%2F%22%5D; _pk_ses.5.c26e=1; results_pp=10',
    'priority': 'u=0, i',
    'referer': 'https://job.deloitte.com/search?search=&job_function=6',
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

DAILY_JOB_URL = 'https://job.deloitte.com/search'

params = {
    'search': '',
}

# Define job detail keys for customization, including optional fields
JOB_DATA_KEYS = {
    'created': None,
    'jobTitle': None,
    'department': None,
    'team': None,
    'location': None,
    'country': None,
    'contract': None,
    'id': 'a.list-item-title',
    'link': None,
    'career_level': None,
    'employment_type': None,
}

# Special extraction logic for certain keys if needed
extraction_logic = {
    'id': lambda el: el.get('href'),
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
        link = job['href']
        id = link.split('_')[-1]
        title = job.h4.text.strip()
        job_details = {
            'scraping_date': None,
            'last_updated': None,
            'title': title,
            'link': link,
            'id': id,
            'status': None,
            'keywords': []
        }

        # for key, selector in JOB_DATA_KEYS.items():
            
        #     if not selector:
        #         continue

        #     element = job.select_one(selector)
        #     print(element)
        #     if not element:
        #         continue

        #     # Check for an attribute pattern like [href]
        #     match = re.search(r'\[(.*?)\]', selector)
        #     if match:
        #         attribute_name = match.group(1)
        #         extractor = extraction_logic.get(key, lambda el: el.get(attribute_name, None))
        #         job_details[key] = extractor(element)
        #     else:
        #         # No attribute pattern, fallback to special logic or text extraction
        #         extractor = extraction_logic.get(key, lambda el: el.get_text(strip=True))
        #         job_details[key] = extractor(element)

        

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
            params = {'page': page}
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
        params = {'limit': MAX_JOBS_PER_PAGE}
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
            job_link = 'https://job.deloitte.com/' + job.get('link')
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

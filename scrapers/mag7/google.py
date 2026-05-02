import logging
import re
from bs4 import BeautifulSoup
import json
import gc

from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list, 
    get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, send_metrics_to_cloud_function, clean_html_block
)
import time
import psutil

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'mag7'
FOLDER_NAME = 'google'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 20
PAGE_START = 1
JOB_LIST_KEY = 'li.lLd3Je'
REQUESTS_PER_BLOCK = 5

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-arch': '"arm"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-form-factors': '"Desktop"',
    'sec-ch-ua-full-version': '"131.0.6778.86"',
    'sec-ch-ua-full-version-list': '"Google Chrome";v="131.0.6778.86", "Chromium";v="131.0.6778.86", "Not_A Brand";v="24.0.0.0"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"macOS"',
    'sec-ch-ua-platform-version': '"14.6.1"',
    'sec-ch-ua-wow64': '?0',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'x-browser-channel': 'stable',
    'x-browser-copyright': 'Copyright 2024 Google LLC. All rights reserved.',
    'x-browser-validation': 'f17l9rSuHmrgo31qEeG2bl8fxeI=',
    'x-browser-year': '2024',
    'x-client-data': 'CKW1yQEIl7bJAQiltskBCKmdygEIk47LAQiUocsBCIagzQEI/aXOAQjSzc4BCPTPzgEIn9LOAQiL084BCLLTzgEY5NPNAQ==',
}

DAILY_JOB_URL = 'https://www.google.com/about/careers/applications/jobs/results?page=1'

# Define job detail keys for customization, including optional fields
JOB_DATA_KEYS = {
    'created': None,
    'jobTitle': 'h3.QJPWVe',
    'department': None,
    'team': None,
    'location': 'span.pwO9Dc span.r0wTof',
    'country': None,
    'contract': None,
    'id': 'attr:ssk',
    'link': 'a',
    'career_level': 'span.wVSTAb',
    'employment_type': None,
    'company': 'span.RP7SMd > span',
    'fallback': 'div.op1BBf',
}
params = None

# Special extraction logic for certain keys if needed
extraction_logic = {
    #'id': lambda el: el.get_text(strip=True),
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

        # Extract job details from HTML using job_data_keys and BeautifulSoup
        for key, selector in JOB_DATA_KEYS.items():
            if selector:
                if selector.startswith('attr:'):
                    # Handle attributes like 'ssk'
                    attribute_name = selector.split(':', 1)[1]
                    attribute_value = job.get(attribute_name)
                    if attribute_value:
                        if key == 'id':
                            job_details[key] = attribute_value.split(":", 1)[-1]
                else:
                    # Handle CSS selectors for elements
                    element = job.select_one(selector)
                    if element:
                        if key == 'link':
                            # Extract the href attribute from the <a> tag
                            job_details[key] = element['href'] if element.has_attr('href') else None
                        else:
                            # Extract text content for jobTitle and other fields
                            job_details[key] = element.get_text(strip=True)

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


def _process_and_persist_block(jobs_block, master_list, block_index):
    """
    Process one block of jobs, update/enrich the master list, and persist immediately.
    """
    if not jobs_block:
        return 0, 0, 0, 0

    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(jobs_block, master_list)
    processed_jobs_count = len(jobs_block)
    save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

    logging.info(
        f"Saved block {block_index}: {processed_jobs_count} jobs processed, "
        f"{new_jobs_count} new, {inactive_jobs_count} marked inactive, {skipped_jobs_count} skipped."
    )

    jobs_block.clear()
    gc.collect()

    return processed_jobs_count, new_jobs_count, inactive_jobs_count, skipped_jobs_count


def fetch_all_jobs(master_list):
    """
    Fetch job postings and process them in blocks of REQUESTS_PER_BLOCK pages.
    After each block (5 pages = 100 jobs), details are scraped and data is persisted.
    """
    total_processed_jobs = 0
    total_new_jobs = 0
    total_inactive_jobs = 0
    total_skipped_jobs = 0
    params = None

    if USE_PAGINATION:
        page = PAGE_START
        jobs_block = []
        requests_in_block = 0
        block_index = 1

        while True:
            logging.info(f"Fetching page {page} of job listings...")
            daily_job_url = f'https://www.google.com/about/careers/applications/jobs/results?page={page}'
            job_postings = fetch_job_list_page(daily_job_url, HEADERS, params, use_proxy=USE_PROXY_DAILY_LIST)
            if not job_postings:
                logging.info("No more job postings found, or failed to retrieve job postings.")
                break

            jobs = process_jobs(job_postings)
            jobs_block.extend(jobs)
            requests_in_block += 1

            should_flush_block = requests_in_block >= REQUESTS_PER_BLOCK
            reached_last_page = len(job_postings) < MAX_JOBS_PER_PAGE

            if should_flush_block or reached_last_page:
                processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                    jobs_block, master_list, block_index
                )
                total_processed_jobs += processed
                total_new_jobs += new_jobs
                total_inactive_jobs += inactive_jobs
                total_skipped_jobs += skipped_jobs
                requests_in_block = 0
                block_index += 1

            if reached_last_page:
                logging.info("Fewer jobs than MAX_JOBS_PER_PAGE found, ending pagination.")
                break

            page += 1

        if jobs_block:
            processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                jobs_block, master_list, block_index
            )
            total_processed_jobs += processed
            total_new_jobs += new_jobs
            total_inactive_jobs += inactive_jobs
            total_skipped_jobs += skipped_jobs
    else:
        logging.info("Fetching a single page of job listings...")
        job_postings = fetch_job_list_page(DAILY_JOB_URL, HEADERS, params, use_proxy=USE_PROXY_DAILY_LIST)
        if job_postings:
            jobs = process_jobs(job_postings)
            processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                jobs, master_list, 1
            )
            total_processed_jobs += processed
            total_new_jobs += new_jobs
            total_inactive_jobs += inactive_jobs
            total_skipped_jobs += skipped_jobs
        else:
            logging.info("No job postings found.")

    return total_processed_jobs, total_new_jobs, total_inactive_jobs, total_skipped_jobs


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
            job_link = 'https://www.google.com/about/careers/applications/' + job['link']
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

                    root = soup.select_one("div.DkhPwc[data-id]")

                    # --- basics ---
                    title_el = root.select_one("h2.p1N2lc")
                    title = title_el.get_text(" ", strip=True) if title_el else None

                    meta_el = root.select_one("div.op1BBf")

                    company_el = root.select_one("div.op1BBf span.RP7SMd > span")
                    company = company_el.get_text(" ", strip=True) if company_el else None

                    # multiple locations possible
                    location_els = root.select("div.op1BBf span.pwO9Dc span.r0wTof")
                    locations = [
                        el.get_text(" ", strip=True)
                        for el in location_els
                        if el.get_text(strip=True)
                    ]

                    exp_el = root.select_one("div.op1BBf span.wVSTAb")
                    experience_level = exp_el.get_text(" ", strip=True) if exp_el else None


                    # --- qualification blocks ---
                    qual_container = root.select_one("div.KwJkGe")

                    min_text = None
                    pref_text = None
                    min_items = []
                    pref_items = []

                    if qual_container:
                        h3s = qual_container.select(":scope > h3")
                        for h3 in h3s:
                            heading = h3.get_text(" ", strip=True).lower()
                            ul = h3.find_next_sibling("ul")
                            if not ul:
                                continue

                            items = [
                                li.get_text(" ", strip=True)
                                for li in ul.select(":scope > li")
                                if li.get_text(strip=True)
                            ]

                            cleaned_text = clean_html_block(str(ul))

                            if "minimum" in heading:
                                min_text = cleaned_text
                                min_items = items
                            elif "preferred" in heading:
                                pref_text = cleaned_text
                                pref_items = items


                    # --- content blocks ---
                    about_el = root.select_one("div.aG5W3")
                    about_text = clean_html_block(str(about_el)) if about_el else None

                    resp_el = root.select_one("div.BDNOWe")
                    responsibilities_text = clean_html_block(str(resp_el)) if resp_el else None


                    # --- build JSON object ---
                    data = {
                        "title": title,
                        "company": company,
                        "locations": locations,
                        "experience_level": experience_level,

                        "minimum_qualifications": {
                            "text": min_text,
                            "items": min_items,
                        },
                        "preferred_qualifications": {
                            "text": pref_text,
                            "items": pref_items,
                        },

                        "about_text": about_text,
                        "responsibilities_text": responsibilities_text,
                    }

                    job_text = json.dumps(data, ensure_ascii=False)

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

    # Step 1: Load master list
    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)

    # Step 2: Fetch/process jobs in blocks and persist after each block
    jobs_processed, new_jobs_count, inactive_jobs_count, skipped_jobs_count = fetch_all_jobs(master_list)

    # Calculate execution time
    execution_time = time.time() - start_time

    # Summary Log
    logging.info(f"Scraping completed successfully. {jobs_processed} jobs processed.")
    logging.info(f"{new_jobs_count} new jobs added.")
    logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(
        FOLDER_NAME,
        execution_time,
        cpu_usage,
        jobs_processed,
        new_jobs_count,
        inactive_jobs_count,
        skipped_jobs_count
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()

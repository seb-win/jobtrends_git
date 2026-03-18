import logging
import re
import unicodedata
from bs4 import BeautifulSoup
import requests
import sys
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
FOLDER_NAME = 'kearney'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 25
PAGE_START = 1
JOB_LIST_KEY = 'li.search-results__item'

HEADERS = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://kearney.recsolu.com/job_boards/1',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'x-csrf-token': 'PJiC+Br7A1o0r2xgSad51PRCIuMqQSj93ix8aU88IgLlFSObGYznw92i+KtpiPO1BtOwn1l4inZzuzZGGKx8TA==',
    'x-requested-with': 'XMLHttpRequest',
}

DAILY_JOB_URL = 'https://kearney.recsolu.com/job_boards/EPd41qlA4_03IncZMnWyRQ/search'

params = {
    'query': '',
    'filters': '',
    'page_number': '1',
    'job_board_tab_identifier': 'bb53e5a1-3b49-2783-061b-5188f80713ea',
}

def clean_job_description(text):
    """Cleans up job descriptions by removing excessive whitespace, fixing encodings, and formatting properly."""
    
    # Fix encoding issues (remove unwanted special characters)
    text = unicodedata.normalize("NFKD", text)  # Normalize Unicode
    text = text.encode("ascii", "ignore").decode("utf-8")  # Remove non-ASCII characters
    
    # Remove multiple spaces, newlines, and fix weird formatting
    text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces/newlines with a single space
    text = re.sub(r'(?<=[.,])(?=[^\s])', ' ', text)  # Ensure space after punctuation (e.g., "skills.Experience" -> "skills. Experience")
    
    # Fix common encoding artifacts (e.g., "â€™" → "'")
    replacements = {
        "â€™": "'", "â€œ": '"', "â€?": '"', "â€“": "-", "â€”": "-",
        "Â ": "", "Ã©": "é", "Ã¤": "ä", "Ã¶": "ö", "Ã¼": "ü",
        "ÃŸ": "ß", "Ã¨": "è", "Ã´": "ô", "Ã²": "ò"
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    # Trim leading/trailing spaces
    text = text.strip()

    return text



def process_jobs(job_postings):
    """
    Extract relevant job details from job postings using JOB_DATA_KEYS.
    If the selector contains an attribute (e.g., [href]), extract that attribute.
    Otherwise, extract text content.
    """

    jobs = []
    for job in job_postings:

        ### Define beautifulsoup paths to extract all available information
        title_tag = job.select_one("a.search-results__req_title")
        title = title_tag.text.strip() if title_tag else None
        url = title_tag["href"] if title_tag else None
        job_id = url.split("/jobs/")[1].split("?")[0] if url else None
        
        details = job.select("div.search-results__jobinfo span")
        contract_type = details[0].text.strip() if len(details) > 0 else None
        region = details[1].text.strip() if len(details) > 1 else None
        location = details[2].text.strip() if len(details) > 2 else None
        
        posting_age_tag = job.select_one("div.search-results__post-time")
        posting_date = posting_age_tag.text.strip() if posting_age_tag else None


        job_details = {
            'created': posting_date,
            'jobTitle': title,
            'department': None,
            'team': None,
            'location': location,
            'country': region,
            'contract': contract_type,
            'id': job_id,
            'link': url,
            'career_level': None,
            'employment_type': None,
            'scraping_date': None,
            'last_updated': None,
            'status': None,
            'keywords': []
        }


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
        html_block_path = json.loads(response.text)
        html_block = html_block_path['html']
        soup = BeautifulSoup(html_block, 'html.parser')
        job_postings = soup.select(JOB_LIST_KEY)
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
            params['page_number'] = page
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
            job_link = 'https://kearney.recsolu.com' + job.get('link')
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
                'cache-control': 'max-age=0',
                'priority': 'u=0, i',
                'referer': 'https://kearney.recsolu.com/job_boards/1',
                'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            }
            params = {
                'job_board_id': 'EPd41qlA4_03IncZMnWyRQ',
            }
            if job_link:
                response = fetch_url(
                    job_link,
                    headers=headers,
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
                    title_text = soup.find('div', class_="details-top__title pull-left").get_text()
                    job_text = soup.select_one("div.inner.clearfix.ck-rendered-content").get_text()
                    text = f"{title_text}\n{job_text}"
                    text = clean_job_description(text)
                    upload_job_details_to_gcs(text, job_id, BUCKET_NAME, FOLDER_NAME)


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

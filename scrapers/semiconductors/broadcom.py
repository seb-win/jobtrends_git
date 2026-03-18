import logging
from bs4 import BeautifulSoup
import time
import psutil
from orchestrator.util_v2 import get_proxy, fetch_url, load_master_list, save_master_list, get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function

# Site-specific configuration
BUCKET_NAME = 'semi_comp'
FOLDER_NAME = 'broadcom'
USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = True
USE_PAGINATION = True  # Boolean to choose between pagination and non-pagination
request_type_list = 'post'
request_type_single = 'get'
MAX_JOBS_PER_PAGE = 20  # Maximum number of jobs per page
PAGE_START = 0  # Starting page for pagination
key_name = 'limit'  # Key name for limit parameter
JOBS_LIST_KEY = ['jobPostings']  # Key path for extracting the list of job postings
TOTAL_JOBS_KEY = ['total']  # Key path for extracting the total number of jobs

# Define job detail keys for customization, including optional fields
job_data_keys = {
    'created': ['postedOn'],
    'jobTitle': ['title'],
    'department': [],
    'team': [],
    'location': ['locationsText'],
    'country': [],
    'contract': ['timeType'],
    'id': ['bulletFields', 0],
    'link': ['externalPath'],
    'career_level': [],
    'employment_type': [],
}

# Define headers and daily job list URL unique to the site
headers = {
    'accept': 'application/json',
    'accept-language': 'en-US',
    'content-type': 'application/json',
    # 'cookie': 'wd-browser-id=6e5aa20f-28e4-4c05-afd3-688de17f95f9; CALYPSO_CSRF_TOKEN=1e3ff962-8ad3-4033-9151-bb939082ed92; PLAY_SESSION=d2bc7c4763896551b2853c03b5ef5a71ddd83451-broadcom_pSessionId=a25l4al84pon9m3ev1nd571rvc&instance=vps-prod-54i0d1pz.prod-vps.pr502.cust.ash.wd; wday_vps_cookie=2558097418.53810.0000; __cf_bm=ssYYzW8AWi_UArF4O_c7KMInmms_8xlXLzup6YtwaLM-1733983927-1.0.1.1-bLSuY_GG5pUu8bwyhLbuwZqz7TlY6obq1ge0sPOM0pgE01gE00rXhvDyO93S.xPKKrrom3hhF8ccOhaZDDGlug; __cflb=02DiuEyZJzFVW6zKk23ddk9h3x7TomPC2QVA4HWiCV15n; _cfuvid=Hm.6WP_o6r8TEbaFPuubQ_tobKfWgJ70K71PnGlXhyo-1733983927063-0.0.1.1-604800000; timezoneOffset=-60',
    'origin': 'https://broadcom.wd1.myworkdayjobs.com',
    'priority': 'u=1, i',
    'referer': 'https://broadcom.wd1.myworkdayjobs.com/External_Career',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'x-calypso-csrf-token': '1e3ff962-8ad3-4033-9151-bb939082ed92',
}
params = None
json_data = {
    'appliedFacets': {},
    'limit': 20,
    'searchText': '',
}
data = None
daily_job_url = 'https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career/jobs'

def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")

    # Set starting time and initiate cpu usage measurement
    start_time = time.time()
    cpu_usage = psutil.cpu_percent(interval=1)

    # Step 1: Fetch the daily job list from the site
    total_jobs_from_response = 0  # Variable to store the total number of jobs from the initial request
    all_jobs = []

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            json_data['offset'] = 20 * page  # Update params for pagination
            response = fetch_url(daily_job_url, headers=headers, params=params, json=json_data, data=data, use_proxy=USE_PROXY_DAILY_LIST, max_retries=3, timeout=10, request_type=request_type_list)
            if response:
                try:
                    job_data = response.json()  # Parse response to JSON
                    if page == PAGE_START:  # Extract total job count only on the first page
                        total_jobs_from_response = get_nested_value(job_data, TOTAL_JOBS_KEY)
                    job_data = get_nested_value(job_data, JOBS_LIST_KEY)  # Extract the nested jobs list using the configured key
                    if not isinstance(job_data, list):
                        logging.error(f"Unexpected response format: job data is not a list or doesn't contain '{JOBS_LIST_KEY}' key.")
                        break
                except ValueError as e:
                    logging.error(f"Failed to parse response to JSON: {e}")
                    break
            else:
                logging.error("Failed to fetch daily job list after multiple attempts.")
                break

            if not job_data:
                break  # Exit loop if no more jobs are available

            all_jobs.extend(job_data)

            # Check if we have fetched enough jobs
            if total_jobs_from_response and len(all_jobs) >= total_jobs_from_response:
                break

            page += 1  # Increment page for the next iteration

    else:
        # Fetch the single page of job listings
        response = fetch_url(daily_job_url, headers=headers, params=params, json=json_data, data=data, use_proxy=USE_PROXY_DAILY_LIST, max_retries=3, timeout=10, request_type=request_type_list)
        if response:
            try:
                job_data = response.json()  # Parse response to JSON
                # Extract total job count for logging (optional for non-paginated mode)
                total_jobs_from_response = get_nested_value(job_data, TOTAL_JOBS_KEY)
                job_data = get_nested_value(job_data, JOBS_LIST_KEY)  # Extract the nested jobs list using the configured key
                if not isinstance(job_data, list):
                    logging.error(f"Unexpected response format: job data is not a list or doesn't contain '{JOBS_LIST_KEY}' key.")
                    return
            except ValueError as e:
                logging.error(f"Failed to parse response to JSON: {e}")
                return
        else:
            logging.error("Failed to fetch job list after multiple attempts.")
            return

        # Extend the all_jobs list
        if job_data:
            all_jobs.extend(job_data)
            logging.info(f"Fetched {len(job_data)} jobs from the single page.")
        else:
            logging.info("No jobs found on the single page.")

    # Process jobs using the job data keys
    jobs = process_jobs(all_jobs, job_data_keys)

    # Step 2: Download and update the master list
    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)
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
            # Add the new job details to the master list
            job['scraping_date'] = current_date
            job['last_updated'] = current_date
            job['status'] = 'active'
            master_list.append(job)
            new_jobs_count += 1

            # Fetch job details from the job link using the adaptive retry mechanism (using proxy if needed)
            job_link = 'https://broadcom.wd1.myworkdayjobs.com/wday/cxs/broadcom/External_Career' + job['link']
            response = fetch_url(job_link, headers=headers, params=params, json=json_data, data=data, use_proxy=USE_PROXY_DETAILED_POSTINGS, max_retries=3, timeout=10, request_type=request_type_single)
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                job_text = soup.get_text()
                upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)

    # Step 3: Mark jobs as inactive if they were not updated in the current scrape
    for entry in master_list:
        if entry['last_updated'] != current_date:
            entry['status'] = 'inactive'
            inactive_jobs_count += 1

    # Step 4: Save the updated master list
    save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

    # Calculate execution time
    execution_time = time.time() - start_time

    # Summary Log
    logging.info(f"Scraping completed successfully. {len(jobs)} jobs processed. {new_jobs_count} new jobs added. {inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(execution_time, cpu_usage, len(jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count)

def process_jobs(job_data, job_data_keys):
    """Extract relevant job details from job data (customize per site)."""
    jobs = []
    for listing in job_data:
        # Start with the required keys that always need to be present (if needed)
        job_details = {
            'scraping_date': None,
            'last_updated': None,
            'status': None,
            'keywords': []
        }

        # Iterate over all job_data_keys to populate job_details using get_nested_value
        for key, path in job_data_keys.items():
            if path:  # Only add if there is a defined path (non-empty list)
                job_details[key] = get_nested_value(listing, path)

        jobs.append(job_details)
    return jobs

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
    main()

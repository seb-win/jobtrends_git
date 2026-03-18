import logging
from bs4 import BeautifulSoup
from orchestrator.util_v2 import get_proxy, fetch_url, load_master_list, save_master_list, get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
import time
import psutil

# Site-specific configuration
BUCKET_NAME = 'luxu_jobs'
FOLDER_NAME = 'hermes'
USE_PROXY_DAILY_LIST = True
USE_PROXY_DETAILED_POSTINGS = True
USE_PAGINATION = True  # Boolean to choose between pagination and non-pagination
request_type_list = 'get'
request_type_single = 'get'
MAX_JOBS_PER_PAGE = 200  # Maximum number of jobs per page
PAGE_START = 0  # Starting page for pagination
key_name = 'limit'  # Key name for limit parameter
JOBS_LIST_KEY = ['items', 0,'requisitionList' ]  # Key path for extracting the list of job postings
TOTAL_JOBS_KEY = ['items', 0, 'TotalJobsCount']  # Key path for extracting the total number of jobs

# Define job detail keys for customization, including optional fields
job_data_keys = {
    'created': ['PostedDate'],
    'jobTitle': ['Title'],
    'department': [],
    'team': [],
    'location': ['PrimaryLocation'],
    'country': ['PrimaryLocationCountry'],
    'contract': [],
    'id': ['Id'],
    'link': [],
    'career_level': [],
    'employment_type': [],
}

# Define headers and daily job list URL unique to the site
headers = {
    'Accept': '*/*',
    'Accept-Language': 'en',
    'Connection': 'keep-alive',
    'Content-Type': 'application/vnd.oracle.adf.resourceitem+json;charset=utf-8',
    'Ora-Irc-Cx-UserId': 'a19407c1-151c-474f-bc26-84411e9be3e3',
    'Ora-Irc-Language': 'en',
    'Origin': 'https://talents.hermes.com',
    'Referer': 'https://talents.hermes.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}
params = {
    key_name: MAX_JOBS_PER_PAGE
}
json = None
#daily_job_url = 'https://fa-eoic-saasfaprod1.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.secondaryLocations,flexFieldsFacet.values,requisitionList.requisitionFlexFields&finder=findReqs;siteNumber=CX_10003,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=200,sortBy=POSTING_DATES_DESC,offset=0'

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
            daily_job_url = f'https://fa-eoic-saasfaprod1.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.secondaryLocations,flexFieldsFacet.values,requisitionList.requisitionFlexFields&finder=findReqs;siteNumber=CX_10003,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=200,sortBy=POSTING_DATES_DESC,offset={page*200}'
            response = fetch_url(daily_job_url, headers=headers, params=params, json=json, use_proxy=USE_PROXY_DAILY_LIST, max_retries=3, timeout=10, request_type=request_type_list)
            if response:
                try:
                    job_data = response.json()  # Parse response to JSON
                    if page == PAGE_START:  # Extract total job count only on the first page
                        total_jobs_from_response = get_nested_value(job_data, TOTAL_JOBS_KEY)
                    job_data = get_nested_value(job_data, JOBS_LIST_KEY)  # Extract the nested jobs list using the configured key
                    print(len(job_data))
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
        response = fetch_url(daily_job_url, headers=headers, params=params, json=json, use_proxy=USE_PROXY_DAILY_LIST, max_retries=3, timeout=10, request_type=request_type_list)
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
            job_link = f'https://fa-eoic-saasfaprod1.fa.ocs.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails?expand=all&onlyData=true&finder=ById;Id="{job_id}",siteNumber=CX_10003'
            response = fetch_url(job_link, headers=headers, params=params, json=json, use_proxy=USE_PROXY_DETAILED_POSTINGS, max_retries=3, timeout=10, request_type=request_type_single)
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
    send_metrics_to_cloud_function(FOLDER_NAME, execution_time, cpu_usage, len(all_jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count)

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

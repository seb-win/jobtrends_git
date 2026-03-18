import logging
from bs4 import BeautifulSoup
import time
import psutil
from orchestrator.util_v2 import get_proxy, fetch_url, load_master_list, save_master_list, get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function

# Site-specific configuration
BUCKET_NAME = 'automotive_comp'
FOLDER_NAME = 'rivian_vw'
USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = True
USE_PAGINATION = True  # Boolean to choose between pagination and non-pagination
request_type_list = 'get'
request_type_single = 'get'
MAX_JOBS_PER_PAGE = 10  # Maximum number of jobs per page
PAGE_START = 1  # Starting page for pagination
key_name = 'limit'  # Key name for limit parameter
JOBS_LIST_KEY = ['jobs']  # Key path for extracting the list of job postings
TOTAL_JOBS_KEY = ['totalCount']  # Key path for extracting the total number of jobs

# Define job detail keys for customization, including optional fields
job_data_keys = {
    'created': ['data', 'posted_date'],
    'jobTitle': ['data', 'title'],
    'department': ['data', 'category', 0],
    'team': [],
    'location': ['data', 'city'],
    'country': ['data', 'country'],
    'contract': ['data', 'employment_type'],
    'id': ['data', 'req_id'],
    'link': [],
    'description': ['data', 'description'],
    'qualifications': ['data', 'qualifications'],
    'responsibilities': ['data', 'responsibilities'],
    'company': ['data', 'hiring_organization'],
    'salary min': ['data', 'tags5', 0],
    'salary max': ['data', 'tags6', 0]
}

# Define headers and daily job list URL unique to the site
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    # 'cookie': 'i18n=en-US; searchSource=external; session_id=557498b0-5cb8-4517-b888-968485ec99c9; jrasession=29582f89-3fe4-4277-8807-57f517c843e0; jasession=s%3A_4s8fqKCwNs3XVPr9ck_APDFeWm-q4aZ.o4Yz2X3LuekMw13sQUyhd6wBq7vyPhJgPxGfikbqqx0; _gid=GA1.2.897468421.1734091322; _janalytics_ses.0edf=*; OptanonAlertBoxClosed=2024-12-13T12:02:06.433Z; _uetsid=55982f70b94a11ef97772926a076420d; _uetvid=55982ed0b94a11efa4bfc342a395a772; _fbp=fb.1.1734091438874.399783770240861500; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Dec+13+2024+13%3A07%3A43+GMT%2B0100+(Mitteleurop%C3%A4ische+Normalzeit)&version=202401.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=8061075a-5343-47a3-a430-a325ecc94d76&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0007%3A1%2CC0009%3A1&geolocation=DE%3BBY&AwaitingReconsent=false; _gat_gtag_UA_35875149_15=1; _gat_UA-35875149-10=1; _janalytics_id.0edf=43fdcac0-46b3-4159-9de8-578a94eb3e05.1734091323.1.1734093233.1734091323.c1e69026-abf7-4273-aa00-bc2ba4ea14f7; _ga_D20QK619Y0=GS1.1.1734091322.1.1.1734093241.0.0.0; _ga_TQ72BH8CSS=GS1.1.1734091326.1.1.1734093241.0.0.0; _ga=GA1.1.1936504731.1734091322; _ga_5Y2BYGL910=GS1.1.1734091326.1.1.1734093242.46.0.0',
    'priority': 'u=1, i',
    'referer': 'https://careers.rivian.com/rivian-vw-group-technology/jobs',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

params = {
    'sortBy': 'relevance',
    'descending': 'false',
    'internal': 'false',
    'tags2': 'Rivian and VW Group Technology',
}
json = None
data = None
daily_job_url = 'https://careers.rivian.com/api/jobs'

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
            params['page'] = page  # Update params for pagination
            response = fetch_url(daily_job_url, headers=headers, params=params, json=json, data=data, use_proxy=USE_PROXY_DAILY_LIST, max_retries=3, timeout=10, request_type=request_type_list)
            if response:
                try:
                    job_data = response.json()  # Parse response to JSON
                    if page == PAGE_START:  # Extract total job count only on the first page
                        total_jobs_from_response = get_nested_value(job_data, TOTAL_JOBS_KEY)
                        print(total_jobs_from_response)
                    job_data = get_nested_value(job_data, JOBS_LIST_KEY)  # Extract the nested jobs list using the configured key
                    print(f'Page: {page}, Jobs count: {len(job_data)}')
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
        response = fetch_url(daily_job_url, headers=headers, params=params, json=json, data=data, use_proxy=USE_PROXY_DAILY_LIST, max_retries=3, timeout=10, request_type=request_type_list)
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

            # # Fetch job details from the job link using the adaptive retry mechanism (using proxy if needed)
            # job_link = job['link']
            # response = fetch_url(job_link, headers=headers, params=params, json=json, data=data, use_proxy=USE_PROXY_DETAILED_POSTINGS, max_retries=3, timeout=10, request_type=request_type_single)
            # if response:
            #     soup = BeautifulSoup(response.text, 'html.parser')
            #     job_text = soup.get_text()
            #     upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)

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

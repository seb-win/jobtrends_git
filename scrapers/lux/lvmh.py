import logging
from bs4 import BeautifulSoup
from orchestrator.util_v2 import get_proxy, fetch_url, load_master_list, save_master_list, get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
import time
import psutil

# Site-specific configuration
BUCKET_NAME = 'luxu_jobs'
FOLDER_NAME = 'lvmh'
USE_PROXY_DAILY_LIST = True
USE_PROXY_DETAILED_POSTINGS = True
USE_PAGINATION = True  # Boolean to choose between pagination and non-pagination
request_type_list = 'post'
request_type_single = 'get'
MAX_JOBS_PER_PAGE = 1000  # Maximum number of jobs per page
PAGE_START = 0  # Starting page for pagination
key_name = 'limit'  # Key name for limit parameter
JOBS_LIST_KEY = ['results', 0, 'hits']  # Key path for extracting the list of job postings
TOTAL_JOBS_KEY = ['results', 0, 'nbHits']  # Key path for extracting the total number of jobs

# Define job detail keys for customization, including optional fields
job_data_keys = {
    'created': ['publicationTimestamp'],
    'jobTitle': ['name'],
    'department': [],
    'team': [],
    'location': ['city'],
    'country': ['country'],
    'contract': ['contract'],
    'id': ['objectID'],
    'link': [],
    'career_level': [],
    'employment_type': [],
    'company': ['maison'],
    'description': ['description'],
    'responsibilities': ['jobResponsabilities'],
    'profile': ['profile'],
    'additionalInfo': ['additionalInformation']
}

# Define headers and daily job list URL unique to the site
headers = {
    'Accept': '*/*',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    # 'Cookie': 'bm_ss=ab8e18ef4e; OptanonAlertBoxClosed=2024-11-29T20:18:12.236Z; bm_mi=211C495C14FB8BFC0105160886A3CF24~YAAQjNAXAqZvLW2TAQAAq1yTeRmH/6WnwwoALbZGKtdDQj4HD8NHR1HjH4lFRwgEH2nXFWbL6PPEkqXAJts28a2LYggGVfyG8fUxYLyDTPXeI9C9l5SszCrf6fV5ToQ/dg1lCRZJiniKvtdPgjq0PqK/QX9mtggKIAJwETQrNwIeaOAjpV65IOVVtY1nT/4TwTd/HjgiupYPeP2XsiocCgpswcVUPV/xqIUV8Q3GU/Ve7KE5PiSTH/3V+lNC6WzhFyH28VxylahsQmu3s/RfdsYEmPogimbME6qE1JDDktXefDdCMrlxui6WQwHzQ0OEOEPzH5ltvr0+z3WWVRjkiMaFsw==~1; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Nov+29+2024+21%3A18%3A41+GMT%2B0100+(Mitteleurop%C3%A4ische+Normalzeit)&version=202411.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=c02d9319-f66e-49cc-98bf-638d0b2ce218&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0003%3A0%2CC0004%3A0%2CC0002%3A0%2CC0001%3A1&hosts=H95%3A0%2CH99%3A0%2CH750%3A0%2CH1440%3A0%2CH111%3A0&genVendors=&intType=2&geolocation=DE%3BBY&AwaitingReconsent=false; bm_lso=22EFB0B38E118780B7B61A2B8DA9F7349530FBA54C92C440EF970811D5E0BF9E~YAAQjNAXAqhvLW2TAQAArFyTeQGdf0fb1Z/gcPwFbUbIU1+HiHsLnc77hhuJ9wPk2BXeInSD8Mixx5Ur+kWyIPgtggtb7vr9RMVa6mJ3eex5y9kkdPcN8EDGu9bOX7NTNcgdfeMr6gvpmxEAvRbTlVvcDGMaF4zaOUJjMMr50lb3Gr71bFjB/Grx2Mf+NDO+PoAamBc049Q9cP+RF8Xz6oUJmjjfCUX6qviZ/PjlZwdzbEtmGz5oMgmCKbhbgaRnWYT53ZIPpxhS5ivv0RgdoBGllAQZVWGxK4bQ5D3Rhv9L7WlXmG62bh4/NB6MIunYX1Q1fjSkVlIp3xU5Wu4pkOMXlKnsj9w9aBb/B0v21UKbAlfrV0s/17JaWekeYMa0oNEdiEVPP00TMD8+7h83ccAtqauIn5Qa8nSgwS1cxihNbBFLp4dihv0bsmYQNJVLLgO6qzxZRSaDu73W^1732911521790; _abck=502112344392911F46EA26789D5A421E~0~YAAQjNAXAuVvLW2TAQAAlGGTeQwfmJOI3TyZAl0n5aG9wzXmht9S9d/Gxqhd4xYxQ2brLuva9HlVaZ9kWFG3e0J5ZfLKUoMruBuylq/fbGpRON9Qw+ngrSfwCkEt/ePWdb+pKOdezYjXOVMaE/l9IwH/yoWslWkuRrmdTiu8yeBR5/MYzemN7m1JzDLT718I/9OVGNkTje1O+fe+6RJee5DOkFdcuL0q3oCR8VMzERbRCnsh1LGD+YSTye9Qbox30VDxoRD1wNQ0q6u9MlgKsh5/r5i0ZlXUIpuEuSCXDvuKYh+vR+qt3/96ryOWseUA4MfuCs/uM3YmPNW2bG0JOz3f1CnjQgvkEnVA39gmSQKIMqNSvADBI/AGLzQd8/yCpkrtfEOdwZe0h9xSBc2VCkBZUE8rrVD8TgSm0CM7D+KCTWNcLhkeL4+W98ba0f4qAQUzIUetcwaZ3KW8rBL6Tzj7j1toYcdcMhr9fC1q~-1~||0||~-1; ak_bmsc=43702B1EFC65619075EF3E178B82069A~000000000000000000000000000000~YAAQjNAXAuhvLW2TAQAAJmKTeRkdJD1iLeKirDUgzDAepTj/VMBWOWbqk+KRU+CBoj2ve5IRrnjYqVOJ938THbJSSvjN/s8xVr8P1Ay8aapwtoSVlWMBPb/UMcNP8Y+80yGlbG4G45s8z8BYnDaScTN9Ewy+4B0KcYyQLkU/3uQymty6l8sZn8IoPHVDHUjrgaknSK7t+DvtX6FaXUKdsAyifmZ4g8xdfLo08lkrXMPZJTUF1ZmYRe8VWTMmUqiIeSFApBno2d708AvaZBhAzE7IIeUrz/IHeUYUCEpoBsV3ygY+BpgQxL9xWnc26ZD+C4If6I9bQc1gRsTLOKKHWMGdMafWIi2agV2mi/r1Xqd61IrDTfvma4chxvMuHcqkh0PHf7Oapmx562AsD4Q2cVh2yeGH0DpMlEiXayum3PCweG6AcptJdlP7mo2RWiYb3jGXmBboh/7ercJkacK3UB0Po43DI+uJNoPLPHXnm9l4Ao+4tmQHKkK9; bm_s=YAAQ3KDVF4RZGmmTAQAA1k6eeQKY5OuPxZkbeg/Q0G2M1hsYrxkZWgJKw3ZlPzwkJ3CAV4N/vFAnLcCDXjZtPodwiiQKLLQTTTxGe5m3LQmjSCpAoAQFOmbIQ56VK0Ti9hIx2CUMsIL4ReQhCyw7MggiCzYwlrkIDnjoH9/hlOn2YrAoErSqQxobVrsjFdZREn6cbXY12RvwNmEU8FKI5MW2ZVfUajW2cM2WHWsy0QwFgDMRnLx4vm1TKnpMVhpZIBmzEl+pMkNQjOLHY+WC0iNMho2O9+CQOeD8m0lyaFxbsIajnzbR42ro4dfaW6b667KvN5blHcljwEqiQw2zs3Lw; bm_so=EA2A7DFB0E215A530EC56ABE0A3EFB9F801E91AEB89D36902487C6E321BFBC94~YAAQ3KDVF4VZGmmTAQAA1k6eeQFfF5bO/25ieVcUy1usvEs98eA4EekK/+9SXgkEcw779YspfVCrTzAhxcyvu8mI2o56A3piK9bLBsW1ea5MQ/cGG52K/TAi5cIfqb6+ss4y0riJeVjGOga09OkpG+lvpWdKzk1U970mOj5877/wbt/wKAmwqVXzBgRniJt/P8Mip0ftIIgSxWAjXIp10g11R89iZvtaa5oHV6Ubx/GlZQAqVrw8NC8cs4xn7Yor4FIEKGqYKtMSvbs3utDO/q/5eu2Uor/UQAzgh4+85l4Q9eRGm54HLFKosSsSnYgj1VTMUApPdHPs+6XeQxd6yTSdZ+jOmFAjlVlNvEyepLVcAgLJ4373H855hej589TUWMmuoHcUZ6b/sjWW4DBuI+xyBM+rjnBAueW9JRTM4M8xAyoKbcxfo1w8rtg3NGnlfJDxbJP38JpIPOTa; bm_sv=DF38538BFEAA1318EA982D161FE641DA~YAAQ3KDVF4ZZGmmTAQAA1k6eeRkSM/EN9s/FpM2pPMPx/xKx/wPKlxxXr66UYTbrPc8DWsL6g/D5yFcRB77JyIpupVtg5kkiHAXSu2sWAmcqi5q+TyPSKSg4KIdPcwJapPhovqjXZaBh2u5+v6IIgcxih8+yw8jPAT6PdF6k6ZHKm7KGHIuVcREE2Rx91OpRdLbOsuLXwhbNvVeasUSumIztKck9gyPGH7xdlCcqW1cKHOcVEBCSd5z9OKPLdOE=~1; bm_sz=000543C8716610CDD185D437E5C74845~YAAQ3KDVF4dZGmmTAQAA1k6eeRkMzuhnibHsYIjvI2dykLX0uj7NkKZZ1nYi/rTlwWsgAPkD3JSYPjppZt/9tN65KkyOJ7Nw+XEB+A+sPo+Z1PXL7d+GQeYCch9aQUxUMDmdQR3MsMykUVxNK/8xzniRsUW0X/QfNIuezmmFHNiFlAABcUDlc30zv+4SNE0024BNhXNqPTBgnIfMwZlaNQLB9gqjaWlY0R8yQ+0QcirosmNgrdJub9RGnUUZTxOzCyhjy9UYc0/+k+qp/mIPO0gdcVvQt6/UAQVqdwGcpMDMOUTHndDTaZVqt1v8OP5CTk6szu2jyF9F30ZjDLzTJUsoF8uVmtnSa2uHieCWVHY/YjREabogqM0dHV7WTy4+nwwgo2NiHwjsEai/8kzOKjg=~3356227~3749177; RT="z=1&dm=www.lvmh.com&si=539244ef-12dc-40ed-a488-118842a94f16&ss=m436ryxm&sl=1&tt=w1&rl=1"',
    'Origin': 'https://www.lvmh.com',
    'Referer': 'https://www.lvmh.com/en/join-us/our-job-offers?PRD-en-us-timestamp-desc%5Bpage%5D=2',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'content-type': 'application/json',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}
params = None
data = None
json_data = {
    'queries': [
        {
            'indexName': 'PRD-en-us-timestamp-desc',
            'params': {
                'facets': [
                    'businessGroupFilter',
                    'cityFilter',
                    'contractFilter',
                    'countryRegionFilter',
                    'fullTimePartTimeFilter',
                    'functionFilter',
                    'geographicAreaFilter',
                    'maison',
                    'regionStateFilter',
                    'requiredExperienceFilter',
                    'workModeFilter',
                ],
                'filters': 'category:job',
                'highlightPostTag': '__/ais-highlight__',
                'highlightPreTag': '__ais-highlight__',
                'hitsPerPage': MAX_JOBS_PER_PAGE,
                'maxValuesPerFacet': 10,
                'query': '',
                'tagFilters': '',
            },
        },
    ],
}
daily_job_url = 'https://www.lvmh.com/api/search'

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
            print(page)
            json_data['queries'][0]['params']['page'] = page  # Update params for pagination
            response = fetch_url(daily_job_url, headers=headers, params=params, json=json_data, data=data, use_proxy=USE_PROXY_DAILY_LIST, max_retries=3, timeout=10, request_type=request_type_list)
            if response:
                try:
                    job_data = response.json()  # Parse response to JSON
                    if page == PAGE_START:  # Extract total job count only on the first page
                        total_jobs_from_response = get_nested_value(job_data, TOTAL_JOBS_KEY)
                        print(total_jobs_from_response)
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
                print(total_jobs_from_response)
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
            # job_link = job['id']
            # job_link = 'https://www.lvmh.com/join-us/our-job-offers/'+job_link
            # response = fetch_url(job_link, headers=headers, params=params, json=json_data, use_proxy=USE_PROXY_DETAILED_POSTINGS, max_retries=3, timeout=10, request_type=request_type_single)
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

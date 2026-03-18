import logging
import json
from bs4 import BeautifulSoup

from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list, 
    get_current_date, get_storage_client, update_job_status, upload_job_details_to_gcs, send_metrics_to_cloud_function
)
import time
import psutil

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'semi_comp'
FOLDER_NAME = 'arm'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = False
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 50
PAGE_START = 1
JOB_LIST_KEY = 'li.job-card'

HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json; charset=utf-8',
    # 'Cookie': 'CookieBehavior=DE|Asserted; AMCVS_E1D82C1653DA87700A490D4B%40AdobeOrg=1; s_ecid=MCMID%7C82298281462252654273848176551695591893; __spdt=ec0943a2b1f642ebb6565eac3e291077; _fbp=fb.1.1735742996421.863974024436296165; cmgvo=undefinedwww.google.deNatural%20Search; s_tbms_tbm14=1; s_cc=true; lo-uid=28932575-1735742996474-3c86bf94ffd1a56e; CookieConsent=0|1|2|3; SearchVisitorId=a0545a61-f274-d8b9-361e-c616c5bcb452; SearchSessionId={%22SearchSessionId%22:%22306094a3-1307-938c-bbcb-b81a4507d186%22%2C%22ImpressionParentId%22:%22%22%2C%22ViewParentId%22:%22%22%2C%22GoogleSearchRequestId%22:%22%22%2C%22GoogleJobId%22:%22%22%2C%22Created%22:%221735742997843%22}; _gid=GA1.2.1984018574.1735742998; _sp_id.a58f=68cc0dfb-80d3-47f8-85d4-5203a5a98eef.1735743061.1.1735743074.1735743061.10bb1ea7-c4ab-4a77-bd79-e08c682f9f30; _gcl_au=1.1.1962267134.1735742998.857509895.1735743061.1735743074; viewedJobs=IPTx7Gz05BWJNFWwQTttQauw1g%252b9Hs0us%252fPbNLuxbHzLfRDuXkTJquCKIhz%252bkZIrUtor8pgzYIAnHMAnD2HdhHcrb%252fwS%252bRftaDb2pLJPuLg%253d; AMCV_E1D82C1653DA87700A490D4B%40AdobeOrg=179643557%7CMCIDTS%7C20090%7CMCMID%7C82298281462252654273848176551695591893%7CMCAAMLH-1736367548%7C6%7CMCAAMB-1736367548%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1735769948s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.5.0; s_lv_s=Less%20than%201%20day; s_tbm=1; lo-visits=9; _ga_3RWZJB52SZ=GS1.1.1735766632.3.0.1735767016.0.0.0; _ga_8N0GS06FN6=GS1.1.1735766632.3.0.1735767016.0.0.0; _ga=GA1.1.268492422.1735742996; _ga_RGP61V6P6P=GS1.1.1735766632.3.1.1735767017.53.0.0; s_cmpCHL=%5B%5B%27search-jobs%27%2C%271735742999997%27%5D%2C%5B%27job%27%2C%271735743075048%27%5D%2C%5B%27search-jobs%27%2C%271735743176507%27%5D%2C%5B%27job%27%2C%271735743238819%27%5D%2C%5B%27search-jobs%27%2C%271735767017203%27%5D%5D; PersonalizationCookie=[{%22Locations%22:[{%22Path%22:%222921044-2905330-2938912-3220968-6553153-2925533%22%2C%22FacetType%22:4%2C%22GeolocationLatitude%22:50.1109%2C%22GeolocationLongitude%22:8.68213%2C%22LocationName%22:%22Frankfurt%2520am%2520Main%252C%2520Hesse%252C%2520Germany%22}]%2C%22Categories%22:[]%2C%22PersonalizationType%22:0%2C%22DateCreated%22:%222025-01-01T14:49:58.438Z%22%2C%22CustomFacets%22:[]%2C%22TenantId%22:33099%2C%22OnetCode%22:null%2C%22Served%22:true}%2C{%22Locations%22:[{%22Path%22:%221269750-1267701-1277331-1277333%22%2C%22FacetType%22:4}]%2C%22Categories%22:[%228081824%22]%2C%22PersonalizationType%22:1%2C%22DateCreated%22:%222025-01-01T14:51:15.018Z%22%2C%22CustomFacets%22:[{%22CustomFacetValue%22:%2213017%22%2C%22CustomFacetTerm%22:%22campaign%22}]%2C%22TenantId%22:33099}%2C{%22Locations%22:[{%22Path%22:%22ALL%22%2C%22FacetType%22:4}]%2C%22Categories%22:[]%2C%22PersonalizationType%22:1%2C%22DateCreated%22:%222025-01-01T21:30:17.217Z%22%2C%22CustomFacets%22:[{%22CustomFacetValue%22:%22Experienced%20Professional%22%2C%22CustomFacetTerm%22:%22custom_fields.ExperienceLevel%22}]%2C%22TenantId%22:33099}]; s_lv=1735767117343; s_sq=arm-global-prod%3D%2526c.%2526a.%2526activitymap.%2526page%253Dcareers%25253Asearch-jobs%2526link%253DNext%2526region%253Dpagination-bottom%2526pageIDType%253D1%2526.activitymap%2526.a%2526.c%2526pid%253Dcareers%25253Asearch-jobs%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Fcareers.arm.com%25252Fsearch-jobs%252526p%25253D2%2526ot%253DA; _gat=1; _ga_542Q2F1BB6=GS1.1.1735766023.3.1.1735767117.60.0.1321313968',
    'Referer': 'https://careers.arm.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

params = {
    'ActiveFacetID': '0',
    'CurrentPage': '1',
    'RecordsPerPage': '1500',
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

DAILY_JOB_URL = 'https://careers.arm.com/search-jobs/results'

# Define job detail keys for customization, including optional fields
JOB_DATA_KEYS = {
    'created': None,
    'jobTitle': "a.job-card__title",
    'department': "span.category",
    'team': None,
    'location': "span.location",
    'country': None,
    'contract': None,
    'id': 'data-job-id',
    'link': "a[href]",
    'career_level': None,
    'employment_type': None,
}

# Special extraction logic for certain keys if needed
extraction_logic = {
    'id': lambda el: el.get_text(strip=True)
    # Add more special cases if needed
}


def process_jobs(job_postings):
    """
    Extract relevant job details from job postings using JOB_DATA_KEYS.
    Handles both text content and attribute extraction.
    """
    jobs = []

    for i, job in enumerate(job_postings, 1):
        title = job.find("a", class_="job-card__title")
        id = title['data-job-id']
        link = 'https://careers.arm.com' + title['href']
        location = job.find("span", class_='location')
        category = job.find("span", class_="category")
        
        job_details = {
            'jobTitle': title.text,
            'department': category.text,
            'location': location.text,
            'id': id,
            'link': link,
            'scraping_date': None,
            'last_updated': None,
            'status': None,
            'keywords': []
        }
    
        #print(job)
        
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
        data = json.loads(response.text)
        results_html = data.get("results", "")
        if results_html:
            results_soup = BeautifulSoup(results_html, "html.parser")

        job_postings = results_soup.select(JOB_LIST_KEY)
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
            #params = {'page': page, 'limit': MAX_JOBS_PER_PAGE}
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
        #params = {'limit': MAX_JOBS_PER_PAGE}
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
            job_link = job.get('link')
            
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

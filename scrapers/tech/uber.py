import json
import logging
import re
import time
import psutil
from bs4 import BeautifulSoup
from orchestrator.schemas.job_detail_v1 import (
    build_full_text,
    clean_text,
    make_job_detail,
    make_section,
    map_section_name,
)
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'tech_jobs'
FOLDER_NAME = 'uber'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = False
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 1000
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'page'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['data', 'results']
TOTAL_JOBS_KEY = ['data', 'totalResults', 'low']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    'origin': 'https://www.uber.com',
    'priority': 'u=1, i',
    'referer': 'https://www.uber.com/hn/en/careers/list/?uclick_id=6b91d190-bbfe-4320-b6b1-8e6a8095f967',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'x-csrf-token': 'x',
    'x-uber-sites-page-edge-cache-enabled': 'true',
}

DAILY_JOB_URL = 'https://www.uber.com/api/loadSearchJobsResults'

JOB_DATA_KEYS = {
    'created': ['creationDate'],
    'jobTitle': ['title'],
    'department': ['department'],
    'team': ['team'],
    'location': ['location', 'city'],
    'country': ['location', 'countryName'],
    'contract': ['timeType'],
    'id': ['id'],
    'link': [],
    'career_level': [],
    'employment_type': [],
    'description': ['description']
}


def _location_values(raw_job):
    locations = raw_job.get('Locations')
    if isinstance(locations, list):
        values = []
        for location in locations:
            if not isinstance(location, dict):
                continue
            value = location.get('Address') or ', '.join(
                part for part in (
                    location.get('City'),
                    location.get('Region'),
                    location.get('Country'),
                )
                if part
            )
            if value:
                values.append(value)
        return values

    location = raw_job.get('location')
    if isinstance(location, dict):
        value = location.get('city') or location.get('name')
        return [value] if value else []

    return [location] if location else []


def _first_team(raw_job):
    teams = raw_job.get('Teams')
    if isinstance(teams, list) and teams:
        return teams[0]
    return raw_job.get('team')


def _split_additional_text(raw_job):
    additional_text = clean_text(raw_job.get('AdditionalText'))
    team = clean_text(_first_team(raw_job))
    if not additional_text:
        return None, team

    if team and additional_text.startswith(team):
        remainder = clean_text(additional_text[len(team):])
        return remainder, team

    return additional_text, team


def _uber_section_name(heading):
    normalized = clean_text(heading)
    if not normalized:
        return 'other'

    normalized = normalized.casefold().replace('’', "'")
    if normalized.startswith('about the role and team') or normalized.startswith('about the team'):
        return 'about'
    if normalized.startswith('about the role') or normalized.startswith('about the job'):
        return 'description'
    if "what you'll do" in normalized or 'what you will do' in normalized:
        return 'responsibilities'
    if normalized == 'ready to ride?':
        return 'additional_information'

    return map_section_name(heading)


def _paragraph_section_name(text):
    normalized = clean_text(text)
    if not normalized:
        return None

    normalized = normalized.casefold()
    if 'base salary range' in normalized or 'bonus program' in normalized:
        return 'compensation'
    if 'equal opportunity employer' in normalized:
        return 'equal_opportunity'
    if normalized.startswith('offices remain key'):
        return 'additional_information'
    return None


def _extract_description_sections(description_html):
    if not description_html:
        return []

    soup = BeautifulSoup(description_html, 'html.parser')
    sections = []
    current = None

    for element in soup.find_all(['p', 'ul'], recursive=False):
        if element.name == 'p':
            strong = element.find('strong')
            heading = clean_text(strong.get_text(' ', strip=True)) if strong else None
            paragraph_text = clean_text(element.get_text(' ', strip=True))

            if heading and paragraph_text == heading:
                current = {
                    'name': _uber_section_name(heading),
                    'heading': heading,
                    'text_parts': [],
                    'items': [],
                }
                sections.append(current)
                continue

            if paragraph_text:
                paragraph_section_name = _paragraph_section_name(paragraph_text)
                if paragraph_section_name:
                    if current and current['name'] == paragraph_section_name and current['heading'] is None:
                        current['text_parts'].append(paragraph_text)
                    else:
                        current = {
                            'name': paragraph_section_name,
                            'heading': None,
                            'text_parts': [paragraph_text],
                            'items': [],
                        }
                        sections.append(current)
                    continue

                if current is None:
                    current = {
                        'name': 'description',
                        'heading': None,
                        'text_parts': [],
                        'items': [],
                    }
                    sections.append(current)
                current['text_parts'].append(paragraph_text)

        elif element.name == 'ul':
            items = [clean_text(li.get_text(' ', strip=True)) for li in element.find_all('li', recursive=False)]
            items = [item for item in items if item]
            if items:
                if current is None:
                    current = {
                        'name': 'other',
                        'heading': None,
                        'text_parts': [],
                        'items': [],
                    }
                    sections.append(current)
                current['items'].extend(items)

    return [
        make_section(
            section['name'],
            heading=section['heading'],
            text='\n'.join(section['text_parts']) if section['text_parts'] else None,
            items=section['items'],
        )
        for section in sections
    ]


def _extract_compensation(raw_job):
    salary = raw_job.get('Salary')
    if not isinstance(salary, dict):
        return {}

    description = salary.get('Description')
    text = clean_text(description)
    compensation = {
        'raw': text,
        'currency': salary.get('Currency'),
        'min': salary.get('MinValue'),
        'max': salary.get('MaxValue'),
        'period': salary.get('Period'),
        'text': text,
    }

    if text:
        matches = re.findall(r'([A-Z]{3})\s+\$([0-9,]+)\s+per\s+year\s+-\s+([A-Z]{3})\s+\$([0-9,]+)\s+per\s+year', text)
        if matches:
            currencies = {match[0] for match in matches} | {match[2] for match in matches}
            mins = [int(match[1].replace(',', '')) for match in matches]
            maxes = [int(match[3].replace(',', '')) for match in matches]
            if len(currencies) == 1:
                compensation['currency'] = currencies.pop()
            compensation['min'] = min(mins)
            compensation['max'] = max(maxes)
            compensation['period'] = 'year'

    return compensation


def build_job_detail_v1_from_json(raw_job):
    """
    Build job_detail_v1 from the already aggregated Uber list entry.

    Uber's list API contains the detail description and metadata used here, so this
    function intentionally performs no standalone detail fetch.
    """
    job_family, department = _split_additional_text(raw_job)
    description = raw_job.get('Description') or raw_job.get('description')
    salary = raw_job.get('Salary') if isinstance(raw_job.get('Salary'), dict) else {}
    salary_description = salary.get('Description')
    full_text = build_full_text(description, salary_description)

    return make_job_detail(
        job_id=raw_job.get('Id') or raw_job.get('Reference') or raw_job.get('id'),
        title=raw_job.get('Title') or raw_job.get('title'),
        company='Uber',
        metadata={
            'department': department,
            'job_family': job_family,
            'employment_type': raw_job.get('ContractType') or raw_job.get('timeType'),
            'job_type': 'Remote' if raw_job.get('Remote') is True else raw_job.get('WorkPattern'),
            'experience_level': raw_job.get('ExperienceLevel'),
            'locations': _location_values(raw_job),
            'posted_at': raw_job.get('DisplayDate'),
        },
        full_text=full_text,
        sections=_extract_description_sections(description),
        compensation=_extract_compensation(raw_job),
    )


def build_job_detail_json(raw_job):
    detail_payload = build_job_detail_v1_from_json(raw_job)
    return json.dumps(detail_payload, ensure_ascii=False)


extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    'localeCode': 'en',
}
JSON_PAYLOAD = {
    'limit': 1000,
    'page': 0,
    'params': {},
}
DATA = None


def process_jobs(job_data, job_data_keys):
    """
    Extract relevant job details from job data using JOB_DATA_KEYS.
    This function uses get_nested_value() to retrieve values from the JSON structure.
    If a path is empty, that field is skipped.
    If extraction_logic defines a special extractor, it's applied to the retrieved value.
    """
    jobs = []
    for listing in job_data:
        job_details = {
            'scraping_date': None,
            'last_updated': None,
            'status': None,
            'keywords': [],
            '_raw_job': listing
        }

        for key, path in job_data_keys.items():
            if not path:
                # If path is empty, skip this field
                continue

            value = get_nested_value(listing, path)
            extractor = extraction_logic.get(key, lambda x: x)
            job_details[key] = extractor(value)

        jobs.append(job_details)
    return jobs


def fetch_job_list_page(page):
    """
    Fetch a single page of job listings in JSON format.
    Adjusts the request parameters based on PAGINATION_MODE.
    Returns a tuple (job_data_list, total_jobs) where job_data_list is a list of jobs 
    for that page and total_jobs is the total number of jobs in the entire dataset 
    (only set when page == PAGE_START).
    """


    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=PARAMS,
        json=JSON_PAYLOAD,
        data=DATA,
        use_proxy=USE_PROXY_DAILY_LIST,
        max_retries=3,
        timeout=10,
        request_type=REQUEST_TYPE_LIST
    )

    if not response:
        logging.error("Failed to fetch daily job list after multiple attempts.")
        return None, 0

    try:
        job_data = response.json()
    except ValueError as e:
        logging.error(f"Failed to parse response to JSON: {e}")
        return None, 0

    total_jobs = 0
    if page == PAGE_START:
        total_jobs = get_nested_value(job_data, TOTAL_JOBS_KEY)

    job_list = get_nested_value(job_data, JOBS_LIST_KEY)
    if not isinstance(job_list, list):
        logging.error(f"Unexpected response format: job data is not a list or doesn't contain '{JOBS_LIST_KEY}' key.")
        return None, total_jobs

    return job_list, total_jobs


def fetch_all_jobs():
    """
    Fetch all job postings (paginated or not) based on USE_PAGINATION.
    For pagination, it will loop through pages or offsets until it retrieves all jobs 
    or reaches a stopping condition.
    """
    all_jobs = []
    total_jobs_from_response = 0

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings...")
            job_data, total_jobs = fetch_job_list_page(page)
            if job_data is None:
                break

            # Set total_jobs_from_response if this is the first page
            if page == PAGE_START:
                total_jobs_from_response = total_jobs

            if not job_data:
                # No more jobs
                break

            all_jobs.extend(job_data)

            # Stop if we've retrieved all jobs
            if total_jobs_from_response and len(all_jobs) >= total_jobs_from_response:
                break

            page += 1
    else:
        job_data, total_jobs_from_response = fetch_job_list_page(PAGE_START)
        if job_data:
            all_jobs.extend(job_data)
            logging.info(f"Fetched {len(job_data)} jobs from the single page.")
        else:
            logging.info("No jobs found on the single page.")

    return all_jobs


def update_master_list_with_jobs(jobs, master_list):
    """
    Update the master list with new or existing jobs, fetch job details if needed,
    and mark old jobs as inactive.
    """
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
            # Add new job
            job['scraping_date'] = current_date
            job['last_updated'] = current_date
            job['status'] = 'active'
            master_list.append(job)
            new_jobs_count += 1

            # Fetch job details if a link is provided
            raw_job = job.get('_raw_job')
            if raw_job:
                job_detail = build_job_detail_json(raw_job)
                upload_job_details_to_gcs(job_detail, job_id, BUCKET_NAME, FOLDER_NAME)

    # Mark old jobs as inactive
    for entry in master_list:
        entry.pop('_raw_job', None)
        if entry['last_updated'] != current_date:
            entry['status'] = 'inactive'
            inactive_jobs_count += 1

    return new_jobs_count, inactive_jobs_count, skipped_jobs_count


def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")
    starting_time = time.time()
    cpu_usage = psutil.cpu_percent(interval=1)

    # Step 1: Fetch all jobs
    raw_job_data = fetch_all_jobs()

    # Step 2: Process jobs using JOB_DATA_KEYS
    jobs = process_jobs(raw_job_data, JOB_DATA_KEYS)

    # Step 3: Update master list
    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)
    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(jobs, master_list)

    # Step 4: Save the updated master list
    save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

    execution_time = time.time() - starting_time

    # Summary
    logging.info(f"Scraping completed successfully. {len(jobs)} jobs processed.")
    logging.info(f"{new_jobs_count} new jobs added.")
    logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(FOLDER_NAME, execution_time, cpu_usage, len(jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()

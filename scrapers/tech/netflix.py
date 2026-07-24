import html as html_lib
from html.parser import HTMLParser
import json
import logging
import re
import time
import psutil
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'tech_jobs'
FOLDER_NAME = 'netflix'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 10
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['positions']
TOTAL_JOBS_KEY = ['count']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'content-type': 'application/json',
    'priority': 'u=1, i',
    'referer': 'https://explore.jobs.netflix.net/careers',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sentry-trace': 'dc04550d252d4842a20ca67d165a2b4c-a5df919f96a05a5d-0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://explore.jobs.netflix.net/api/apply/v2/jobs'

JOB_DATA_KEYS = {
    'created': ['t_create'],
    'jobTitle': ['name'],
    'department': ['department'],
    'team': ['business_unit'],
    'location': ['location'],
    'country': [],
    'contract': [],
    'id': ['id'],
    'link': ['canonicalPositionUrl'],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = [
    ('domain', 'netflix.com'),
    ('start', '0'),
    ('num', '10'),
    ('domain', 'netflix.com'),
    ('sort_by', 'relevance'),
]
JSON_PAYLOAD = None
DATA = None




class JobDescriptionParser(HTMLParser):
    BLOCK_TAGS = {'p', 'div', 'li', 'br', 'h1', 'h2', 'h3'}
    HEADING_TAGS = {'h1', 'h2', 'h3'}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text_parts = []
        self.sections = []
        self.current = self.new_section('description', None)
        self.tag_stack = []
        self.capture_tag = None
        self.capture_parts = []
        self.li_parts = []

    @staticmethod
    def new_section(name, heading):
        return {'name': name, 'heading': heading, 'text_parts': [], 'items': []}

    def handle_starttag(self, tag, attrs):
        self.tag_stack.append(tag)
        if tag in self.BLOCK_TAGS:
            self.text_parts.append(' ')
        if tag == 'li':
            self.li_parts = []
        if tag in self.HEADING_TAGS or (tag in {'strong', 'b'} and self.capture_tag is None):
            self.capture_tag = tag
            self.capture_parts = []

    def handle_endtag(self, tag):
        if tag in self.BLOCK_TAGS:
            self.text_parts.append(' ')
        if self.capture_tag == tag:
            heading = normalize_text(' '.join(self.capture_parts))
            if heading and (tag in self.HEADING_TAGS or self.is_standalone_heading(heading)):
                self.flush_current()
                self.current = self.new_section(section_name_for_heading(heading), heading)
            self.capture_tag = None
            self.capture_parts = []
        if tag == 'li':
            item = normalize_text(' '.join(self.li_parts))
            if item:
                self.current['items'].append(item)
            self.li_parts = []
        if tag in self.tag_stack:
            self.tag_stack = self.tag_stack[:len(self.tag_stack) - 1 - self.tag_stack[::-1].index(tag)]

    def handle_data(self, data):
        self.text_parts.append(data)
        if self.capture_tag:
            self.capture_parts.append(data)
        if self.inside_tag('li'):
            self.li_parts.append(data)
        elif not self.capture_tag:
            self.current['text_parts'].append(data)

    def inside_tag(self, tag):
        return tag in self.tag_stack

    def is_standalone_heading(self, heading):
        return len(heading) <= 80 and not self.inside_tag('p') and not self.inside_tag('li')

    def flush_current(self):
        text = normalize_text(' '.join(self.current['text_parts']))
        if text or self.current['items']:
            self.sections.append({
                'name': self.current['name'],
                'heading': self.current['heading'],
                'text': text,
                'items': self.current['items']
            })

    def finish(self):
        self.flush_current()
        return normalize_text(' '.join(self.text_parts)), self.sections


def normalize_text(value):
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    text = html_lib.unescape(value)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text or None


def parse_job_description_html(value):
    if value is None:
        return None, []
    parser = JobDescriptionParser()
    parser.feed(str(value))
    parser.close()
    return parser.finish()


def clean_text(value):
    """Return normalized plain text from HTML or text fragments."""
    return normalize_text(value)


def first_custom_field(detail, field_name):
    values = get_nested_value(detail, ['custom_JD', 'data_fields', field_name])
    if isinstance(values, list):
        return values[0] if values else None
    return values


def unix_timestamp_to_date(value):
    if not value:
        return None
    try:
        return time.strftime('%Y-%m-%d', time.gmtime(int(value)))
    except (TypeError, ValueError, OSError):
        return None


def section_name_for_heading(heading):
    normalized = (heading or '').lower()
    if 'responsib' in normalized:
        return 'responsibilities'
    if 'qualification' in normalized or 'skills and experience' in normalized:
        return 'qualifications'
    if normalized in {'role', 'our team'} or 'team' in normalized:
        return 'about'
    if 'value' in normalized:
        return 'preferred_qualifications'
    if 'compensation' in normalized or 'salary' in normalized or 'benefit' in normalized:
        return 'compensation'
    if 'inclusion' in normalized or 'equal' in normalized or 'diversity' in normalized:
        return 'equal_opportunity'
    return 'other'


def extract_sections_from_html(html):
    return parse_job_description_html(html)[1]

def extract_compensation(full_text):
    compensation = {
        'raw': None,
        'currency': None,
        'min': None,
        'max': None,
        'period': None,
        'text': None,
        'locale': None,
        'location_id': None
    }
    if not full_text:
        return compensation

    match = re.search(r'(?:range for this role is\s*)?(\$[\d,]+(?:\.\d{2})?)\s*-\s*(\$[\d,]+(?:\.\d{2})?)', full_text)
    if not match:
        return compensation

    def money_to_float(value):
        return float(value.replace('$', '').replace(',', ''))

    compensation.update({
        'raw': match.group(0),
        'currency': 'USD',
        'min': money_to_float(match.group(1)),
        'max': money_to_float(match.group(2)),
        'period': 'year',
        'text': match.group(0),
        'locale': 'en-US'
    })
    return compensation


def build_job_detail_v1_from_json(detail):
    locations = detail.get('locations')
    if not isinstance(locations, list):
        locations = [detail.get('location')] if detail.get('location') else []

    full_text, sections = parse_job_description_html(detail.get('job_description'))
    return {
        'schema_version': 'job_detail_v1',
        'job': {
            'id': str(detail.get('id')) if detail.get('id') is not None else first_custom_field(detail, 'job_req_id'),
            'title': detail.get('posting_name') or detail.get('name'),
            'company': 'Netflix'
        },
        'metadata': {
            'department': detail.get('department') or first_custom_field(detail, 'team'),
            'job_family': detail.get('business_unit'),
            'role_type': None,
            'employment_type': None,
            'job_type': first_custom_field(detail, 'work_type'),
            'career_level': None,
            'experience_level': None,
            'required_travel': None,
            'locations': [clean_text(location) for location in locations if clean_text(location)],
            'created_at': unix_timestamp_to_date(detail.get('t_create')),
            'posted_at': first_custom_field(detail, 'posting_date'),
            'updated_at': unix_timestamp_to_date(detail.get('t_update'))
        },
        'content': {
            'full_text': full_text,
            'full_text_truncated': False,
            'sections': sections
        },
        'compensation': extract_compensation(full_text)
    }

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
            'keywords': []
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

    # # Copy existing params
    # params = {**PARAMS}

    # # Adjust pagination parameters based on PAGINATION_MODE
    # if PAGINATION_MODE == 'page':
    #     # Standard page-based pagination
    #     params['page'] = page
    # elif PAGINATION_MODE == 'offset':
        # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
    offset = (page * MAX_JOBS_PER_PAGE)
    params = dict(PARAMS)  # Converts list to dictionary (removes duplicates)
    params['start'] = offset  # Modify start
    params = list(params.items())  # Convert back to list
    # elif PAGINATION_MODE == 'firstItem':
    #     # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
    #     first_item = (page * MAX_JOBS_PER_PAGE) + 1
    #     params['firstItem'] = first_item
   
    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=params,
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
            job_link = 'https://explore.jobs.netflix.net/api/apply/v2/jobs/' + str(job_id)
            if job_link:
                response = fetch_url(
                    job_link,
                    headers=HEADERS,
                    params=PARAMS,
                    json=JSON_PAYLOAD,
                    data=DATA,
                    use_proxy=USE_PROXY_DETAILED_POSTINGS,
                    max_retries=3,
                    timeout=10,
                    request_type=REQUEST_TYPE_SINGLE
                )
                if response:
                    try:
                        detail_data = response.json()
                    except ValueError as e:
                        logging.error(f"Failed to parse job detail JSON for {job_id}: {e}")
                    else:
                        job_detail = build_job_detail_v1_from_json(detail_data)
                        job_text = json.dumps(job_detail, ensure_ascii=False)
                        upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)

    # Mark old jobs as inactive
    for entry in master_list:
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

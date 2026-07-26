import json
import logging
import re
import time
import psutil
from bs4 import BeautifulSoup
from orchestrator.schemas.section_extraction import looks_like_compliance_or_benefits_text, map_extracted_section_name
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'tech_jobs'
FOLDER_NAME = 'oracle'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 200
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offest'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ["items", 0, "requisitionList"]
TOTAL_JOBS_KEY = ["items", 0, "TotalJobsCount"]

HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en',
    'Connection': 'keep-alive',
    'Content-Type': 'application/vnd.oracle.adf.resourceitem+json;charset=utf-8',
    'Ora-Irc-Cx-UserId': 'b70d1dc8-9d8c-4a39-8557-68905de46fea',
    'Ora-Irc-Language': 'en',
    'Origin': 'https://careers.oracle.com',
    'Referer': 'https://careers.oracle.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0',
    'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Microsoft Edge";v="132"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

DAILY_JOB_URL = 'https://eeho.fa.us2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.workLocation,requisitionList.otherWorkLocations,requisitionList.secondaryLocations,flexFieldsFacet.values,requisitionList.requisitionFlexFields&finder=findReqs;siteNumber=CX_45001,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=200,sortBy=POSTING_DATES_DESC,offset=0'

JOB_DATA_KEYS = {
    'created': ['PostedDate'],
    'jobTitle': ['Title'],
    'department': [],
    'team': [],
    'location': [],
    'country': ['PrimaryLocation'],
    'contract': [],
    'id': ['Id'],
    'link': [],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {}
JSON_PAYLOAD = None
DATA = None


def clean_html_text(value):
    """Return source text with HTML tags/entities removed and whitespace normalized."""
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    soup = BeautifulSoup(value, 'html.parser')
    text = soup.get_text("\n")
    text = re.sub(r'\xa0|&nbsp;', ' ', text)
    text = re.sub(r'[ \t\r\f\v]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n', text)
    text = re.sub(r'\s*\n\s*', '\n', text)
    text = text.strip()
    return text or None


def clean_plain_text(value):
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    text = re.sub(r'\xa0|&nbsp;', ' ', value)
    text = re.sub(r'\s+', ' ', text).strip()
    return text or None


def get_flex_field(job, prompt):
    for field in job.get('requisitionFlexFields') or []:
        if field.get('Prompt') == prompt:
            return clean_plain_text(field.get('Value'))
    return None


def get_locations(job):
    locations = []
    primary = clean_plain_text(job.get('PrimaryLocation'))
    if primary:
        locations.append(primary)
    for location in job.get('secondaryLocations') or []:
        name = clean_plain_text(location.get('Name'))
        if name and name not in locations:
            locations.append(name)
    return locations


def html_list_items(value):
    if not value:
        return []
    soup = BeautifulSoup(value, 'html.parser')
    items = []
    for item in soup.find_all('li'):
        text = clean_plain_text(item.get_text(' '))
        if text:
            items.append(text)
    return items


def add_section(sections, name, heading, text=None, items=None):
    cleaned_text = clean_html_text(text) if text else None
    cleaned_items = [item for item in (items or []) if item]
    if cleaned_text or cleaned_items:
        sections.append({
            'name': name,
            'heading': heading,
            'text': cleaned_text,
            'items': cleaned_items,
        })


def oracle_section_name_for_heading(heading, default='responsibilities'):
    normalized = (clean_plain_text(heading) or '').casefold().rstrip(':')
    if normalized == 'experience':
        return 'qualifications'
    if 'responsibilit' in normalized or normalized in {'core responsibilities', 'key responsibilities'}:
        return 'responsibilities'
    if 'about the team' in normalized or normalized == 'who are we looking for?':
        return 'about'
    return map_extracted_section_name(heading, default=default)


def extract_oracle_sections_from_html(value, default_name, default_heading):
    if not value:
        return []

    soup = BeautifulSoup(value, 'html.parser')
    sections = []
    current = {
        'name': default_name,
        'heading': default_heading,
        'text_parts': [],
        'items': [],
    }

    def flush_current():
        nonlocal current
        text = '\n\n'.join(part for part in current['text_parts'] if part) or None
        add_section(sections, current['name'], current['heading'], text, current['items'])
        current = None

    def start_section(heading, remaining=None):
        nonlocal current
        if current:
            flush_current()
        current = {
            'name': oracle_section_name_for_heading(heading, default_name),
            'heading': clean_plain_text(heading),
            'text_parts': [remaining] if remaining else [],
            'items': [],
        }

    for element in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'ul', 'li'], recursive=True):
        if element.find_parent('li'):
            continue
        if element.name == 'p' and element.find(['p', 'ul']):
            continue

        if element.name == 'li':
            if element.find_parent('ul'):
                continue
            item = clean_plain_text(element.get_text(' '))
            if item and current:
                current['items'].append(item)
            continue

        if element.name == 'ul':
            items = [
                clean_plain_text(li.get_text(' '))
                for li in element.find_all('li', recursive=False)
            ]
            items = [item for item in items if item]
            if current:
                current['items'].extend(items)
            continue

        text = clean_plain_text(element.get_text(' '))
        if not text:
            continue

        heading = None
        if element.name in {'h1', 'h2', 'h3', 'h4'}:
            heading = text
        else:
            bold = element.find(['strong', 'b'])
            bold_text = clean_plain_text(bold.get_text(' ')) if bold else None
            if bold_text and (text == bold_text or text.startswith(bold_text)):
                heading = bold_text
            elif len(text) <= 80 and text.rstrip().endswith(':'):
                candidate = text.rstrip(':')
                mapped_name = oracle_section_name_for_heading(candidate, default=None)
                if mapped_name or default_name != 'description':
                    heading = text

        if heading:
            remaining = clean_plain_text(text[len(heading):]) if text.startswith(heading) else None
            start_section(heading.rstrip(':'), remaining)
        elif current:
            current['text_parts'].append(text)

    if current:
        flush_current()

    return [section for section in sections if section]


def parse_compensation(job):
    qualifications_text = clean_html_text(job.get('ExternalQualificationsStr'))
    if not qualifications_text:
        return {
            'raw': None,
            'currency': None,
            'min': None,
            'max': None,
            'period': None,
            'text': None,
            'locale': None,
            'location_id': None,
        }

    match = re.search(
        r'(US:\s*Hiring Range(?: in USD)?:?\s*from:?\s*\$([\d,]+)\s*to\s*\$([\d,]+)\s*per annum[^\n.]*(?:\.[^\n]*)?)',
        qualifications_text,
        re.IGNORECASE,
    )
    raw = match.group(1).strip() if match else None
    min_value = int(match.group(2).replace(',', '')) if match else None
    max_value = int(match.group(3).replace(',', '')) if match else None
    return {
        'raw': raw,
        'currency': 'USD' if match else None,
        'min': min_value,
        'max': max_value,
        'period': 'year' if match else None,
        'text': raw,
        'locale': 'US' if match else None,
        'location_id': None,
    }


def extract_detail_item(detail_data):
    if isinstance(detail_data, dict):
        items = detail_data.get('items')
        if isinstance(items, list) and items:
            return items[0]
    return detail_data if isinstance(detail_data, dict) else {}


def build_job_detail_v1_from_json(detail_data):
    job = extract_detail_item(detail_data)
    description_text = clean_html_text(job.get('ExternalDescriptionStr'))
    short_description = clean_plain_text(job.get('ShortDescriptionStr'))
    responsibilities_text = clean_html_text(job.get('ExternalResponsibilitiesStr'))
    qualifications_text = clean_html_text(job.get('ExternalQualificationsStr'))
    corporate_text = clean_html_text(job.get('CorporateDescriptionStr'))

    full_text_parts = [
        short_description,
        description_text,
        responsibilities_text,
        qualifications_text,
        corporate_text,
    ]
    full_text = '\n\n'.join(part for part in full_text_parts if part) or None

    sections = []
    sections.extend(extract_oracle_sections_from_html(job.get('ExternalDescriptionStr'), 'description', 'Description'))
    sections.extend(extract_oracle_sections_from_html(job.get('ExternalResponsibilitiesStr'), 'responsibilities', 'Responsibilities'))

    qualifications_name = 'qualifications'
    qualifications_heading = 'Qualifications'
    if looks_like_compliance_or_benefits_text(job.get('ExternalQualificationsStr')):
        qualifications_name = 'additional_information'
        qualifications_heading = 'Oracle qualifications field / compliance information'

    add_section(
        sections,
        qualifications_name,
        qualifications_heading,
        job.get('ExternalQualificationsStr'),
        html_list_items(job.get('ExternalQualificationsStr')),
    )
    add_section(sections, 'about', 'About Oracle', job.get('CorporateDescriptionStr'))

    return {
        'schema_version': 'job_detail_v1',
        'job': {
            'id': clean_plain_text(job.get('Id')),
            'title': clean_plain_text(job.get('Title')),
            'company': 'Oracle',
        },
        'metadata': {
            'department': clean_plain_text(job.get('JobFunction')) or clean_plain_text(job.get('JobFunctionCode')),
            'job_family': clean_plain_text(job.get('Category')),
            'role_type': get_flex_field(job, 'Role'),
            'employment_type': get_flex_field(job, 'Job Type'),
            'job_type': clean_plain_text(job.get('WorkplaceType')) or clean_plain_text(job.get('JobType')),
            'career_level': clean_plain_text(job.get('JobLevel')) or extract_career_level(description_text, qualifications_text),
            'experience_level': get_flex_field(job, 'Years'),
            'required_travel': clean_plain_text(job.get('InternationalTravelRequired')) or clean_plain_text(job.get('DomesticTravelRequired')),
            'locations': get_locations(job),
            'created_at': None,
            'posted_at': clean_plain_text(job.get('ExternalPostedStartDate')),
            'updated_at': None,
        },
        'content': {
            'full_text': full_text,
            'full_text_truncated': False,
            'sections': sections,
        },
        'compensation': parse_compensation(job),
    }


def extract_career_level(*texts):
    for text in texts:
        if not text:
            continue
        match = re.search(r'Career Level\s*-\s*([A-Z0-9]+)', text)
        if match:
            return match.group(1)
    return None


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

    # Copy existing params
    params = {**PARAMS}
    offset = (page * MAX_JOBS_PER_PAGE)
    daily = f'https://eeho.fa.us2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.workLocation,requisitionList.otherWorkLocations,requisitionList.secondaryLocations,flexFieldsFacet.values,requisitionList.requisitionFlexFields&finder=findReqs;siteNumber=CX_45001,facetsList=LOCATIONS%3BWORK_LOCATIONS%3BWORKPLACE_TYPES%3BTITLES%3BCATEGORIES%3BORGANIZATIONS%3BPOSTING_DATES%3BFLEX_FIELDS,limit=200,sortBy=POSTING_DATES_DESC,offset={offset}'

    response = fetch_url(
        daily,
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
            job_link = f'https://eeho.fa.us2.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails?expand=all&onlyData=true&finder=ById;Id={job_id},siteNumber=CX_45001'
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
                        job_detail_json = json.dumps(job_detail, ensure_ascii=False, indent=2)
                        upload_job_details_to_gcs(job_detail_json, job_id, BUCKET_NAME, FOLDER_NAME)

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

import logging
import time
import psutil
import json
import re
from bs4 import BeautifulSoup
from orchestrator.schemas.job_detail_v1 import (
    build_full_text,
    clean_text,
    make_job_detail,
    make_section,
    map_section_name,
)
from orchestrator.schemas.section_extraction import make_extracted_section
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'tech_jobs'
FOLDER_NAME = 'workday'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 20
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['jobPostings']
TOTAL_JOBS_KEY = ['total']

HEADERS = {
    'accept': 'application/json',
    'accept-language': 'en-US',
    'content-type': 'application/json',
    'origin': 'https://workday.wd5.myworkdayjobs.com',
    'priority': 'u=1, i',
    'referer': 'https://workday.wd5.myworkdayjobs.com/Workday/?source=Careers_Website',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://workday.wd5.myworkdayjobs.com/wday/cxs/workday/Workday/jobs'

JOB_DATA_KEYS = {
    'created': ['postedOn'],
    'jobTitle': ['title'],
    'department': [],
    'team': [],
    'location': ['locationsText'],
    'country': [],
    'contract': [],
    'id': ['bulletFields', 0],
    'link': ['externalPath'],
    'career_level': [],
    'employment_type': [],
    'skills': []
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = None
JSON_PAYLOAD = {
    'appliedFacets': {},
    'limit': 20,
    'offset': 0,
    'searchText': '',
}
DATA = None


def map_workday_section_name(heading):
    normalized = (clean_text(heading) or "").casefold()
    if "about the team" in normalized:
        return "about"
    if "about the role" in normalized or "responsible for" in normalized:
        return "responsibilities"
    if "about you" in normalized or "other qualification" in normalized:
        return "qualifications"
    if "flexible work" in normalized:
        return "benefits"
    if "fair chance" in normalized or "equal opportunity" in normalized:
        return "equal_opportunity"
    if "referred" in normalized or "privacy" in normalized or "never ask candidates" in normalized:
        return "additional_information"
    return map_section_name(heading)


def _extract_heading_text(element):
    if element.name in ("h1", "h2", "h3", "h4"):
        return clean_text(element.get_text(" ", strip=True))

    bold = element.find("b", recursive=False)
    if bold:
        return clean_text(bold.get_text(" ", strip=True))

    return None


def extract_workday_sections(job_description_html):
    if not job_description_html:
        return []

    soup = BeautifulSoup(job_description_html, "html.parser")
    sections = []
    intro_parts = []
    current = None

    def finish_current():
        nonlocal current
        if not current:
            return

        section = make_extracted_section(
            current["name"],
            heading=current["heading"],
            text=build_full_text(*current["text_parts"]),
            items=current["items"],
        )
        if section:
            sections.append(section)
        current = None

    for element in soup.contents:
        if isinstance(element, str):
            text = clean_text(element)
            if text:
                if current:
                    current["text_parts"].append(text)
                else:
                    intro_parts.append(text)
            continue

        if element.name not in ("p", "h1", "h2", "h3", "h4", "ul"):
            continue

        if element.name == "ul":
            items = [
                clean_text(li.get_text(" ", strip=True))
                for li in element.find_all("li", recursive=False)
            ]
            items = [item for item in items if item]
            if current:
                current["items"].extend(items)
            elif items:
                current = {
                    "name": "other",
                    "heading": None,
                    "text_parts": [],
                    "items": items,
                }
            continue

        heading = _extract_heading_text(element)
        text = clean_text(element.get_text(" ", strip=True))

        if heading and text == heading:
            finish_current()
            current = {
                "name": map_workday_section_name(heading),
                "heading": heading,
                "text_parts": [],
                "items": [],
            }
            continue

        if heading and text and text.startswith(heading):
            finish_current()
            remaining_text = clean_text(text[len(heading):])
            current = {
                "name": map_workday_section_name(heading),
                "heading": heading,
                "text_parts": [remaining_text] if remaining_text else [],
                "items": [],
            }
            continue

        if text:
            if current:
                current["text_parts"].append(text)
            else:
                intro_parts.append(text)

    finish_current()

    intro_text = build_full_text(*intro_parts)
    if intro_text:
        sections.insert(0, make_section("description", heading=None, text=intro_text))

    return [section for section in sections if section]


def _extract_compensation(full_text):
    if not full_text:
        return None

    primary_match = re.search(
        r"Primary Location Base Pay Range:\s*\$([\d,]+)\s*USD\s*-\s*\$([\d,]+)\s*USD",
        full_text,
        flags=re.IGNORECASE,
    )
    additional_match = re.search(
        r"Additional US Location\(s\) Base Pay Range:\s*\$([\d,]+)\s*USD\s*-\s*\$([\d,]+)\s*USD",
        full_text,
        flags=re.IGNORECASE,
    )
    raw_parts = []
    if primary_match:
        raw_parts.append(primary_match.group(0))
    if additional_match:
        raw_parts.append(additional_match.group(0))

    if not raw_parts:
        return None

    return {
        "raw": " | ".join(raw_parts),
        "currency": "USD",
        "min": int(primary_match.group(1).replace(",", "")) if primary_match else None,
        "max": int(primary_match.group(2).replace(",", "")) if primary_match else None,
        "period": "annual",
        "text": " | ".join(raw_parts),
        "locale": "US",
        "location_id": "primary_location" if primary_match else None,
    }


def build_job_detail_v1_from_json(detail_json):
    job = detail_json.get("jobPostingInfo", {}) if isinstance(detail_json, dict) else {}
    org = detail_json.get("hiringOrganization", {}) if isinstance(detail_json, dict) else {}
    job_description_html = job.get("jobDescription")
    full_text = clean_text(job_description_html, separator="\n")
    locations = [job.get("location"), *(job.get("additionalLocations") or [])]

    return make_job_detail(
        job_id=job.get("jobReqId") or job.get("id"),
        title=job.get("title"),
        company=org.get("name") or "Workday",
        metadata={
            "employment_type": job.get("timeType"),
            "job_type": job.get("remoteType"),
            "locations": locations,
            "posted_at": job.get("startDate"),
        },
        full_text=full_text,
        sections=extract_workday_sections(job_description_html),
        compensation=_extract_compensation(full_text),
    )


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
    json = {**JSON_PAYLOAD}

    # Adjust pagination parameters based on PAGINATION_MODE
    if PAGINATION_MODE == 'page':
        # Standard page-based pagination
        json['page'] = page
    elif PAGINATION_MODE == 'offset':
        # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
        offset = (page * MAX_JOBS_PER_PAGE)
        json['offset'] = offset
    elif PAGINATION_MODE == 'firstItem':
        # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
        first_item = (page * MAX_JOBS_PER_PAGE) + 1
        json['firstItem'] = first_item

    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=PARAMS,
        json=json,
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
            job_link = job.get('link')
            job_link = 'https://workday.wd5.myworkdayjobs.com/wday/cxs/workday/Workday' + job_link
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
                    json_text = json.loads(response.text)
                    job_detail = build_job_detail_v1_from_json(json_text)
                    job_json_string = json.dumps(job_detail, ensure_ascii=False)
                    upload_job_details_to_gcs(job_json_string, job_id, BUCKET_NAME, FOLDER_NAME)

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

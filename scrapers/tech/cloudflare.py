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
FOLDER_NAME = 'cloudflare'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = False
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 50
PAGE_START = 1

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'page'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['departments']
TOTAL_JOBS_KEY = ['departments']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'origin': 'https://www.cloudflare.com',
    'priority': 'u=1, i',
    'referer': 'https://www.cloudflare.com/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}


DAILY_JOB_URL = 'https://boards-api.greenhouse.io/v1/boards/cloudflare/departments/'

JOB_DATA_KEYS = {
    'created': ['first_published'],
    'jobTitle': ['title'],
    'department': ['department'],
    'team': ['metadata', 0, 'value'],
    'location': ['metadata', 2, 'value'],
    'country': [],
    'contract': [],
    'id': ['id'],
    'link': ['absolute_url'],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    'render_as': 'tree',
}

JSON_PAYLOAD = None
DATA = None



_COMPENSATION_RE = re.compile(
    r"Estimated annual salary of\s*\$([\d,]+)\s*-\s*\$([\d,]+)",
    flags=re.IGNORECASE,
)

_SECTION_NAME_OVERRIDES = {
    "available locations": "additional_information",
    "about the role": "description",
    "desirable skills, knowledge, and experience": "qualifications",
    "bonus points": "preferred_qualifications",
    "what makes cloudflare special?": "benefits",
}


def _direct_text(element):
    if not element:
        return None
    return clean_text(element.get_text(" ", strip=True))


def _extract_job_id_from_html(soup):
    form = soup.select_one('form[action*="/cloudflare/jobs/"]')
    if not form:
        return None

    action = form.get("action") or ""
    match = re.search(r"/jobs/(\d+)|gh_jid=(\d+)", action)
    if not match:
        return None

    return next((group for group in match.groups() if group), None)


def _section_name_for_heading(heading):
    normalized = (clean_text(heading) or "").casefold().rstrip(":")
    return _SECTION_NAME_OVERRIDES.get(normalized, map_section_name(heading))


def _extract_available_locations(sections):
    for section in sections:
        heading = (section.get("heading") or "").casefold().rstrip(":")
        if heading == "available locations":
            return section.get("items") or []
    return []


def _extract_cloudflare_sections(description):
    if not description:
        return []

    sections = []
    intro_parts = []
    current = None

    def finish_current():
        nonlocal current
        if not current:
            return

        section = make_section(
            current["name"],
            heading=current["heading"],
            text=build_full_text(*current["text_parts"]),
            items=current["items"],
        )
        if section:
            sections.append(section)
        current = None

    for element in description.find_all(["div", "p", "h2", "ul", "ol"], recursive=False):
        if element.name == "div" and element.find(["h2", "p", "ul", "ol"], recursive=False):
            element_items = element.find_all(["p", "h2", "ul", "ol"], recursive=False)
        else:
            element_items = [element]

        for child in element_items:
            if child.name in ("ul", "ol"):
                items = [
                    clean_text(li.get_text(" ", strip=True))
                    for li in child.find_all("li", recursive=False)
                ]
                items = [item for item in items if item]
                if current:
                    current["items"].extend(items)
                elif items:
                    current = {"name": "other", "heading": None, "text_parts": [], "items": items}
                continue

            heading = None
            if child.name == "h2":
                heading = _direct_text(child)
            elif child.name in ("p", "div"):
                strong = child.find("strong", recursive=False)
                if strong:
                    heading = _direct_text(strong)

            text = _direct_text(child)
            if heading and text == heading:
                finish_current()
                current = {"name": _section_name_for_heading(heading), "heading": heading, "text_parts": [], "items": []}
                continue

            if heading and text and text.startswith(heading):
                finish_current()
                remaining = clean_text(text[len(heading):])
                current = {
                    "name": _section_name_for_heading(heading),
                    "heading": heading,
                    "text_parts": [remaining] if remaining else [],
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

    match = _COMPENSATION_RE.search(full_text)
    if not match:
        return None

    raw = match.group(0)
    return {
        "raw": raw,
        "currency": "USD",
        "min": int(match.group(1).replace(",", "")),
        "max": int(match.group(2).replace(",", "")),
        "period": "annual",
        "text": raw,
        "locale": "US",
        "location_id": None,
    }


def build_job_detail_v1_from_html(html, job_metadata=None):
    soup = BeautifulSoup(html, "html.parser")
    metadata = job_metadata or {}
    description = soup.select_one("div.job__description.body")
    title = _direct_text(soup.select_one("div.job__title h1")) or metadata.get("jobTitle")
    location = _direct_text(soup.select_one("div.job__location div"))
    full_text = clean_text(description.get_text("\n", strip=True), separator="\n") if description else None
    sections = _extract_cloudflare_sections(description)
    locations = metadata.get("location") or _extract_available_locations(sections)

    return make_job_detail(
        job_id=metadata.get("id") or _extract_job_id_from_html(soup),
        title=title,
        company="Cloudflare",
        metadata={
            "department": metadata.get("team"),
            "job_family": metadata.get("department"),
            "job_type": location,
            "locations": locations,
            "created_at": metadata.get("created"),
            "career_level": metadata.get("career_level"),
            "employment_type": metadata.get("employment_type"),
        },
        full_text=full_text,
        sections=sections,
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

    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=None,
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
        json_obj = response.json()
    except ValueError as e:
        logging.error(f"Failed to parse response to JSON: {e}")
        return None, 0
    
    departments = json_obj['departments']
    total_jobs = 0

    job_data = []

    for dep in departments:
        department = dep['name']
        job_data_temp = dep['jobs']
        for job in job_data_temp:
            job['department'] = department

        job_data += job_data_temp

    return job_data, total_jobs


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
                    job_detail = build_job_detail_v1_from_html(response.text, job_metadata=job)
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

import logging
import json
import re
import time
import psutil
from bs4 import BeautifulSoup
from orchestrator.schemas.job_detail_v1 import build_full_text, clean_text, make_job_detail, make_section
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'tech_jobs'
FOLDER_NAME = 'spotify'

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
JOBS_LIST_KEY = ['result']
TOTAL_JOBS_KEY = []

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'origin': 'https://www.lifeatspotify.com',
    'priority': 'u=1, i',
    'referer': 'https://www.lifeatspotify.com/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://api-dot-new-spotifyjobs-com.nw.r.appspot.com/wp-json/animal/v1/job/search'

JOB_DATA_KEYS = {
    'created': [],
    'jobTitle': ['text'],
    'department': ['main_category', 'name'],
    'team': ['sub_category', 'name'],
    'location': ['locations'],
    'country': [],
    'contract': ['job_type', 'name'],
    'id': ['id'],
    'link': ['id'],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
}
JSON_PAYLOAD = None
DATA = None


def _clean_lines(container):
    if container is None:
        return []

    lines = []
    for line in container.get_text("\n", strip=True).splitlines():
        text = clean_text(line)
        if text:
            lines.append(text)
    return lines


def _extract_header_values(lines):
    apply_markers = {"Link copied to clipboard.", "Apply now", "Apply"}
    title = lines[0] if lines else None
    department = None
    metadata_lines = []
    seen_apply = False

    for line in lines[1:]:
        if line in apply_markers:
            seen_apply = True
            continue
        if seen_apply:
            metadata_lines.append(line)
            if len(metadata_lines) == 4:
                break
        elif department is None:
            department = line

    job_type = metadata_lines[2] if len(metadata_lines) > 3 else None
    locations = metadata_lines[3] if len(metadata_lines) > 3 else metadata_lines[2] if len(metadata_lines) > 2 else None

    return {
        "title": title,
        "department": department,
        "job_family": metadata_lines[0] if len(metadata_lines) > 0 else None,
        "role_type": None,
        "job_type": job_type,
        "locations": locations,
    }


def _section_name_for_heading(heading):
    normalized = (heading or "").casefold()
    if "what you'll do" in normalized or "what you will do" in normalized:
        return "responsibilities"
    if "who you are" in normalized:
        return "qualifications"
    if "where you'll be" in normalized or "where you will be" in normalized:
        return "additional_information"
    if "global benefits" in normalized:
        return "benefits"
    return "other"


def _extract_structured_sections(main):
    if main is None:
        return []

    sections = []
    section_blocks = main.select("div.singlejob_descriptionText__7hiF9, div.closingtext_container__0pdbw, div.perks_container__E7jyf")
    for block in section_blocks:
        heading = block.find(["h2", "h3"]) or block.find(
            ["p", "div"],
            class_=lambda value: value and "headline-3" in value,
        )
        if heading is None:
            continue
        heading_text = clean_text(heading.get_text(" ", strip=True))
        if not heading_text or heading_text in {"Quick clicks", "Learn about life at Spotify"}:
            continue

        texts = []
        items = []
        for element in block.find_all(["p", "div", "li"], recursive=True):
            if element is heading or heading in element.parents:
                continue
            if element.find(["p", "div", "li"]):
                continue
            text = clean_text(element.get_text(" ", strip=True))
            if not text or text == heading_text:
                continue
            if element.name == "li":
                items.append(text)
            elif "perks_text__" in " ".join(element.get("class", [])):
                items.append(text)
            else:
                texts.append(text)

        section = make_section(
            _section_name_for_heading(heading_text),
            heading=heading_text,
            text=" ".join(texts) if texts else None,
            items=items,
        )
        if section:
            sections.append(section)

    closing_text = main.select_one("div.closingtext_text__B9RMi")
    if closing_text:
        paragraphs = [clean_text(node.get_text(" ", strip=True)) for node in closing_text.find_all("div")]
        paragraphs = [text for text in paragraphs if text]
        if paragraphs:
            compensation_text = next((text for text in paragraphs if "$" in text), None)
            if compensation_text:
                section = make_section("compensation", heading="Compensation", text=compensation_text)
                if section:
                    sections.append(section)

            eeo_text = " ".join(
                text for text in paragraphs
                if "equal opportunity employer" in text.casefold()
            )
            if eeo_text:
                section = make_section("equal_opportunity", heading="Equal opportunity", text=eeo_text)
                if section:
                    sections.append(section)

    return sections


def _extract_description_text(main):
    if main is None:
        return None

    description = main.select_one("div.singlejob_descriptionTextNoPadding__j3OKM")
    if description is None:
        return None

    texts = []
    for element in description.find_all(["p", "div"], recursive=True):
        if element.find(["p", "div"]):
            continue
        text = clean_text(element.get_text(" ", strip=True))
        if text:
            texts.append(text)

    return "\n".join(texts) if texts else clean_text(description.get_text("\n", strip=True))


def _extract_header_from_dom(main):
    if main is None:
        return {}

    title_node = main.find("h1")
    department_node = main.find("h3")
    tag_nodes = main.select("div.tags_work-category__dLDLq p, div.tags_work-sub-category__eZ5y6 p")
    details = [clean_text(node.get_text(" ", strip=True)) for node in main.select("div.detail-3")]
    details = [text for text in details if text]

    return {
        "title": clean_text(title_node.get_text(" ", strip=True)) if title_node else None,
        "department": clean_text(department_node.get_text(" ", strip=True)) if department_node else None,
        "job_family": clean_text(tag_nodes[0].get_text(" ", strip=True)) if len(tag_nodes) > 0 else None,
        "role_type": None,
        "job_type": details[0] if len(details) > 1 else None,
        "locations": details[-1] if details else None,
    }


def _extract_compensation(full_text):
    if not full_text:
        return None

    match = re.search(
        r"(?:base range|salary range|base salary range)[^.]*?(?:US)?\$([\d,]+)\s*-\s*(?:US)?\$([\d,]+)[^.]*\.",
        full_text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    raw = clean_text(match.group(0))
    return {
        "raw": raw,
        "currency": "USD",
        "min": int(match.group(1).replace(",", "")),
        "max": int(match.group(2).replace(",", "")),
        "period": "year",
        "text": raw,
        "locale": "US",
        "location_id": None,
    }


def build_job_detail_v1_from_html(html_text, job_id=None, fallback_job=None):
    soup = BeautifulSoup(html_text or "", "html.parser")
    main = (
        soup.find("main", class_="main")
        or soup.find("main")
        or soup.select_one("div.singlejob_container__T16Px")
        or soup.select_one("div.container.block-container")
        or soup
    )
    lines = _clean_lines(main)
    header = {**_extract_header_values(lines), **_extract_header_from_dom(main)}
    fallback_job = fallback_job or {}
    sections = _extract_structured_sections(main)
    full_text = build_full_text(
        _extract_description_text(main),
        *(section.get("text") for section in sections),
        *(section.get("items") for section in sections),
    )

    return make_job_detail(
        job_id=job_id or fallback_job.get("id"),
        title=header.get("title") or fallback_job.get("jobTitle"),
        company="Spotify",
        metadata={
            "department": header.get("department") or fallback_job.get("team"),
            "job_family": header.get("job_family") or fallback_job.get("department"),
            "role_type": header.get("role_type"),
            "job_type": header.get("job_type") or fallback_job.get("contract"),
            "locations": header.get("locations") or fallback_job.get("location"),
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

    # Copy existing params
    params = {**PARAMS}

    # Adjust pagination parameters based on PAGINATION_MODE
    if PAGINATION_MODE == 'page':
        # Standard page-based pagination
        params['page'] = page
    elif PAGINATION_MODE == 'offset':
        # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
        offset = (page * MAX_JOBS_PER_PAGE)
        params['offset'] = offset
    elif PAGINATION_MODE == 'firstItem':
        # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
        first_item = (page * MAX_JOBS_PER_PAGE) + 1
        params['firstItem'] = first_item

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
            job_link = 'https://www.lifeatspotify.com/jobs/' + job.get('link')
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
                    job_detail = build_job_detail_v1_from_html(response.text, job_id=job_id, fallback_job=job)
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

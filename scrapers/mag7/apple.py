import logging
import time
import psutil
from bs4 import BeautifulSoup
import json
import gc
import html
import re

from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)
from orchestrator.schemas.hyd_extract import get_hydration_json

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'mag7'
FOLDER_NAME = 'apple'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 20
PAGE_START = 1
REQUESTS_PER_BLOCK = 5
DETAIL_FETCH_RETRIES = 3
DETAIL_FETCH_RETRY_SLEEP_S = 2
DETAIL_UPLOAD_RETRIES = 3
DETAIL_UPLOAD_RETRY_SLEEP_S = 3
MASTER_SAVE_RETRIES = 3
MASTER_SAVE_RETRY_SLEEP_S = 5

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'page'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['res','searchResults']
TOTAL_JOBS_KEY = ['res','totalRecords']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'browserlocale': 'de-de',
    'content-type': 'application/json',
    'locale': 'de_DE',
    'origin': 'https://jobs.apple.com',
    'priority': 'u=1, i',
    'referer': 'https://jobs.apple.com/en-us/search?sort=newest',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://jobs.apple.com/api/v1/search'

JOB_DATA_KEYS = {
    'created': ['postDateInGMT'],
    'jobTitle': ['postingTitle'],
    'department': ['team', 'teamName'],
    'team': [],
    'location': ['locations', '0', 'city'],
    'country': ['locations', '0', 'countryName'],
    'contract': [],
    'id': ['positionId'],
    'link': [],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = None
JSON_PAYLOAD = {
    'query': '',
    'filters': {},
    'page': PAGE_START,
    'locale': 'en-us',
    'sort': 'newest',
    'format': {
        'longDate': 'MMMM D, YYYY',
        'mediumDate': 'MMM D, YYYY',
    },
}
DATA = None


def _clean_text(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = "\n".join(str(item) for item in value if item is not None)
    elif not isinstance(value, str):
        value = str(value)

    soup = BeautifulSoup(html.unescape(value), "html.parser")
    text = soup.get_text("\n")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    return text or None


def _list_items_from_lines(value):
    text = _clean_text(value)
    if not text:
        return []

    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        item = re.sub(r"^[-*•]\s*", "", line).strip()
        if item:
            items.append(item)
    return items


def _bullet_items_from_lines(value):
    text = _clean_text(value)
    if not text:
        return []

    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line or not re.match(r"^[-*•]\s+", line):
            continue
        items.append(re.sub(r"^[-*•]\s*", "", line).strip())
    return [item for item in items if item]


def _section(name, heading, text=None, items=None):
    cleaned_text = _clean_text(text)
    cleaned_items = [_clean_text(item) for item in (items or [])]
    cleaned_items = [item for item in cleaned_items if item]
    if not cleaned_text and not cleaned_items:
        return None
    return {
        "name": name,
        "heading": heading,
        "text": cleaned_text,
        "items": cleaned_items,
    }


def _locations_from_jobs_data(jobs_data):
    locations = []
    for location in jobs_data.get("locations") or []:
        if not isinstance(location, dict):
            continue
        parts = [
            location.get("city") or location.get("name"),
            location.get("stateProvince"),
            location.get("countryName"),
        ]
        cleaned_location = _clean_text(", ".join(part for part in parts if part))
        if cleaned_location and cleaned_location not in locations:
            locations.append(cleaned_location)
    return locations


def _pay_data_from_jobs_data(jobs_data):
    ppld = jobs_data.get("postingPostLocationData", {}) or {}
    locale = "en_US" if "en_US" in ppld else (next(iter(ppld.keys())) if ppld else None)

    loc_id = None
    if jobs_data.get("locations"):
        loc_id = jobs_data["locations"][0].get("id")

    pay = None
    other = None
    if locale and loc_id:
        location_data = ppld.get(locale, {}).get(loc_id, {}) or {}
        pay = location_data.get("postingSupplementFooter")
        other = location_data.get("otherPostingSupplementFooter")

    pay_text = _clean_text((pay or {}).get("content"))
    other_text = _clean_text((other or {}).get("content"))
    if not pay_text:
        for footer in jobs_data.get("postingFooters") or []:
            localizations = (footer or {}).get("localizations", {}) or {}
            locale_entries = localizations.get(locale or "en_US") or []
            for entry in locale_entries:
                pay_text = _clean_text((entry or {}).get("content"))
                if pay_text:
                    break
            if pay_text:
                break
    compensation_text = "\n\n".join(text for text in [pay_text, other_text] if text) or None
    raw_label = _clean_text((pay or {}).get("label"))

    return {
        "raw": raw_label,
        "currency": None,
        "min": None,
        "max": None,
        "period": None,
        "text": compensation_text,
        "locale": locale if compensation_text else None,
        "location_id": loc_id if compensation_text else None,
    }


def build_job_detail_v1_from_json(data):
    job_details = data.get("loaderData", {}).get("jobDetails", {}) if isinstance(data, dict) else {}
    jobs_data = job_details.get("jobsData", {}) or {}
    compensation = _pay_data_from_jobs_data(jobs_data)

    sections = [
        _section("about", "Summary", jobs_data.get("jobSummary")),
        _section(
            "responsibilities",
            "Description",
            text=None if _bullet_items_from_lines(jobs_data.get("description")) else jobs_data.get("description"),
            items=_bullet_items_from_lines(jobs_data.get("description")),
        ),
        _section(
            "minimum_qualifications",
            "Minimum Qualifications",
            items=_list_items_from_lines(jobs_data.get("minimumQualifications")),
        ),
        _section(
            "preferred_qualifications",
            "Preferred Qualifications",
            items=_list_items_from_lines(jobs_data.get("preferredQualifications")),
        ),
        _section("compensation", "Pay & Benefits", compensation["text"]),
    ]
    sections = [section for section in sections if section]

    full_text_parts = [
        jobs_data.get("jobSummary"),
        jobs_data.get("description"),
        jobs_data.get("minimumQualifications"),
        jobs_data.get("preferredQualifications"),
        compensation["text"],
    ]
    full_text = _clean_text("\n\n".join(part for part in full_text_parts if part))

    team_names = jobs_data.get("teamNames") or []
    department = _clean_text(team_names[0]) if team_names else None

    return {
        "schema_version": "job_detail_v1",
        "job": {
            "id": jobs_data.get("positionId"),
            "title": _clean_text(jobs_data.get("postingTitle")),
            "company": "Apple",
        },
        "metadata": {
            "department": department,
            "job_family": None,
            "role_type": None,
            "employment_type": _clean_text(jobs_data.get("employmentType")),
            "job_type": _clean_text(jobs_data.get("jobType")),
            "career_level": None,
            "experience_level": None,
            "required_travel": None,
            "locations": _locations_from_jobs_data(jobs_data),
            "created_at": None,
            "posted_at": jobs_data.get("postDateInGMT"),
            "updated_at": None,
        },
        "content": {
            "full_text": full_text,
            "full_text_truncated": False,
            "sections": sections,
        },
        "compensation": compensation,
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

    # Copy existing params
    # params = {**PARAMS}

    # # Adjust pagination parameters based on PAGINATION_MODE
    # if PAGINATION_MODE == 'page':
    #     # Standard page-based pagination
    #     params['page'] = page
    # elif PAGINATION_MODE == 'offset':
    #     # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
    #     offset = (page * MAX_JOBS_PER_PAGE)
    #     params['offset'] = offset
    # elif PAGINATION_MODE == 'firstItem':
    #     # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
    #     first_item = (page * MAX_JOBS_PER_PAGE) + 1
    #     params['firstItem'] = first_item

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


def _save_master_list_with_retry(master_list):
    """Persist master list with retries so transient network errors do not abort the run."""
    for attempt in range(1, MASTER_SAVE_RETRIES + 1):
        try:
            save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)
            return True
        except Exception as exc:
            logging.warning(
                f"Master list save failed (attempt {attempt}/{MASTER_SAVE_RETRIES}): {exc}"
            )
            if attempt < MASTER_SAVE_RETRIES:
                time.sleep(MASTER_SAVE_RETRY_SLEEP_S)

    logging.error("Master list could not be saved after retries.")
    return False


def _upload_job_details_with_retry(job_text, job_id):
    """Upload one job detail document with retries and non-fatal failure."""
    for attempt in range(1, DETAIL_UPLOAD_RETRIES + 1):
        try:
            upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)
            return True
        except Exception as exc:
            logging.warning(
                f"Detail upload failed for job {job_id} "
                f"(attempt {attempt}/{DETAIL_UPLOAD_RETRIES}): {exc}"
            )
            if attempt < DETAIL_UPLOAD_RETRIES:
                time.sleep(DETAIL_UPLOAD_RETRY_SLEEP_S)

    logging.error(f"Skipping detail upload for job {job_id}: upload failed after retries.")
    return False


def _process_and_persist_block(raw_jobs_block, master_list, block_index):
    """
    Process one block of raw jobs, enrich/save details for new jobs, and persist immediately.
    """
    if not raw_jobs_block:
        return 0, 0, 0, 0

    jobs = process_jobs(raw_jobs_block, JOB_DATA_KEYS)
    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(jobs, master_list)
    _save_master_list_with_retry(master_list)

    logging.info(
        f"Saved block {block_index}: {len(jobs)} jobs processed, "
        f"{new_jobs_count} new, {inactive_jobs_count} marked inactive, {skipped_jobs_count} skipped."
    )

    raw_jobs_block.clear()
    gc.collect()

    return len(jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count


def fetch_all_jobs(master_list):
    """
    Fetch and process job postings in blocks of REQUESTS_PER_BLOCK pages.
    After each block (5 pages = 100 jobs), details are scraped and data is persisted.
    """
    total_jobs_from_response = 0
    total_processed_jobs = 0
    total_new_jobs = 0
    total_inactive_jobs = 0
    total_skipped_jobs = 0

    if USE_PAGINATION:
        page = PAGE_START
        raw_jobs_block = []
        requests_in_block = 0
        block_index = 1

        while True:
            logging.info(f"Fetching page {page} of job listings...")
            JSON_PAYLOAD['page'] = page
            job_data, total_jobs = fetch_job_list_page(page)
            if job_data is None:
                break

            if page == PAGE_START:
                total_jobs_from_response = total_jobs

            if not job_data:
                break

            raw_jobs_block.extend(job_data)
            requests_in_block += 1

            should_flush_block = requests_in_block >= REQUESTS_PER_BLOCK
            retrieved_all_jobs = total_jobs_from_response and (
                (total_processed_jobs + len(raw_jobs_block)) >= total_jobs_from_response
            )

            if should_flush_block or retrieved_all_jobs:
                processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                    raw_jobs_block, master_list, block_index
                )
                total_processed_jobs += processed
                total_new_jobs += new_jobs
                total_inactive_jobs += inactive_jobs
                total_skipped_jobs += skipped_jobs
                requests_in_block = 0
                block_index += 1

            if retrieved_all_jobs:
                break

            page += 1

        if raw_jobs_block:
            processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                raw_jobs_block, master_list, block_index
            )
            total_processed_jobs += processed
            total_new_jobs += new_jobs
            total_inactive_jobs += inactive_jobs
            total_skipped_jobs += skipped_jobs
    else:
        JSON_PAYLOAD['page'] = PAGE_START
        job_data, total_jobs_from_response = fetch_job_list_page(PAGE_START)
        if job_data:
            processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                list(job_data), master_list, 1
            )
            total_processed_jobs += processed
            total_new_jobs += new_jobs
            total_inactive_jobs += inactive_jobs
            total_skipped_jobs += skipped_jobs
            logging.info(f"Fetched and processed {len(job_data)} jobs from the single page.")
        else:
            logging.info("No jobs found on the single page.")

    return total_processed_jobs, total_new_jobs, total_inactive_jobs, total_skipped_jobs


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
            job_link = 'https://jobs.apple.com/en-us/details/' + job['id']
            if job_link:
                data = None
                for attempt in range(1, DETAIL_FETCH_RETRIES + 1):
                    try:
                        data = get_hydration_json(job_link, '__staticRouterHydrationData')
                        break
                    except Exception as exc:
                        logging.warning(
                            f"Detail fetch failed for job {job_id} (attempt {attempt}/{DETAIL_FETCH_RETRIES}): {exc}"
                        )
                        if attempt < DETAIL_FETCH_RETRIES:
                            time.sleep(DETAIL_FETCH_RETRY_SLEEP_S)

                if not isinstance(data, dict):
                    logging.error(f"Skipping detail upload for job {job_id}: no hydration data after retries.")
                    continue

                # if response:
                #     soup = BeautifulSoup(response.text, 'html.parser')
                #     job_text = soup.get_text()
                #     upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)
                out = build_job_detail_v1_from_json(data)

                # --- Als String für GCS ---
                out_json_str = json.dumps(out, ensure_ascii=False)
                _upload_job_details_with_retry(out_json_str, job_id)

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

    # Step 1: Load master list
    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)

    # Step 2: Fetch/process jobs in blocks and persist after each block
    jobs_processed, new_jobs_count, inactive_jobs_count, skipped_jobs_count = fetch_all_jobs(master_list)

    execution_time = time.time() - starting_time

    # Summary
    logging.info(f"Scraping completed successfully. {jobs_processed} jobs processed.")
    logging.info(f"{new_jobs_count} new jobs added.")
    logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(
        FOLDER_NAME,
        execution_time,
        cpu_usage,
        jobs_processed,
        new_jobs_count,
        inactive_jobs_count,
        skipped_jobs_count
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()

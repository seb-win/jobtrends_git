import logging
import time
import psutil
import json
import gc
import re
from dataclasses import dataclass, field
from datetime import datetime, UTC

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover - depends on runtime environment
    BeautifulSoup = None

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
BUCKET_NAME = 'mag7'
FOLDER_NAME = 'microsoft'

USE_PROXY_DAILY_LIST = True
USE_PROXY_DETAILED_POSTINGS = True
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 10
PAGE_START = 0
REQUESTS_PER_BLOCK = 10
DETAIL_REQUEST_SLEEP_SECONDS = 10
DETAIL_RETRY_ROUNDS = 3
DETAIL_RETRY_DELAY_SECONDS = 15

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

JOBS_LIST_KEY = ['data', 'positions']
TOTAL_JOBS_KEY = ['data', 'count']

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://apply.careers.microsoft.com/careers?start=0&sort_by=timestamp',
    #'request-id': '|68446d06fd3c4d47a8f3b7f71fb27a73.ad8f89d9321f43ad',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    #'sentry-trace': 'fdb8ee52b0134df59c32e7192d54fc4b-bab7636b52115bde-0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    #'x-browser-request-time': '1771963182.757',
    #'x-csrf-token': 'IjQ3Y2Y5OTU1MzJjMWRhOTc0MzU4N2YxMGNmMGMzODEyNDBkZGZlZDEi.HH-Urg.BmANZrWz3kDRT9I3PQBDxqw-veU',
}

DAILY_JOB_URL = 'https://apply.careers.microsoft.com/api/pcsx/search?domain=microsoft.com&query=&location=&start=0&sort_by=timestamp&'

JOB_DATA_KEYS = {
    'created': ['postedTs'],
    'jobTitle': ['name'],
    'department': ['department'],
    'team': [],
    'location': ['locations', 0],
    'country': [],
    'contract': [],
    'id': ['id'],
    'link': ['positionUrl'],
    'career_level': [],
    'employment_type': [],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = None
JSON_PAYLOAD = None
DATA = None


def format_time_ts(timestamp_raw):
    """Convert Microsoft epoch timestamps from seconds or milliseconds to UTC ISO."""
    if timestamp_raw in (None, ""):
        return None

    try:
        timestamp = float(timestamp_raw)
    except (TypeError, ValueError):
        logging.warning("Invalid timestamp value: %r", timestamp_raw)
        return None

    if timestamp > 100_000_000_000:
        timestamp /= 1000

    return datetime.fromtimestamp(timestamp, UTC).isoformat()


def first_or_none(value):
    if isinstance(value, list) and value:
        return value[0]
    return value


def _clean_direct_text(element):
    parts = []
    for child in element.children:
        if isinstance(child, str):
            parts.append(child)
        elif child.name == "br":
            parts.append("\n")
        elif child.name not in {"ul", "ol"}:
            parts.append(child.get_text(" ", strip=True))
    return clean_text(" ".join(parts))


def _collect_list_items(element):
    items = []
    for li in element.find_all("li"):
        item = clean_text(li.get_text(" ", strip=True))
        if item:
            items.append(item)
    return items


def _section_text_from_parts(parts):
    texts = []
    for part in parts:
        if part.name in {"ul", "ol"}:
            continue
        text = clean_text(part.get_text(" ", strip=True))
        if text:
            texts.append(text)
    return build_full_text(*texts)


def extract_job_description_sections(job_description_html):
    if not job_description_html:
        return []

    if BeautifulSoup is None:
        return extract_job_description_sections_fallback(job_description_html)

    soup = BeautifulSoup(job_description_html, "html.parser")
    sections = []
    intro_parts = []
    current_heading = None
    current_parts = []

    def flush_current():
        nonlocal current_heading, current_parts
        if current_heading:
            items = []
            for part in current_parts:
                if part.name in {"ul", "ol"}:
                    items.extend(_collect_list_items(part))
            section = make_section(
                map_microsoft_section_name(current_heading),
                heading=current_heading,
                text=_section_text_from_parts(current_parts),
                items=items,
            )
            if section:
                sections.append(section)
        elif current_parts:
            for part in current_parts:
                text = clean_text(part.get_text(" ", strip=True))
                if text:
                    intro_parts.append(text)

        current_heading = None
        current_parts = []

    for child in soup.contents:
        if isinstance(child, str):
            text = clean_text(child)
            if text and current_heading:
                current_parts.append(BeautifulSoup(text, "html.parser"))
            elif text:
                intro_parts.append(text)
            continue

        if child.name in {"b", "strong"}:
            heading = clean_text(child.get_text(" ", strip=True))
            if heading:
                flush_current()
                current_heading = heading
            continue

        if child.name == "p":
            bold = child.find(["b", "strong"], recursive=False)
            direct_text = _clean_direct_text(child)
            if bold and direct_text == clean_text(bold.get_text(" ", strip=True)):
                flush_current()
                current_heading = direct_text
                continue

        if child.name in {"p", "ul", "ol", "div"}:
            current_parts.append(child)

    flush_current()

    intro_text = build_full_text(*intro_parts)
    if intro_text:
        sections.insert(0, make_section("description", heading="Overview", text=intro_text))

    return [section for section in sections if section]


def map_microsoft_section_name(heading):
    normalized = (clean_text(heading) or "").casefold()
    if normalized == "qualifications":
        return "qualifications"
    return map_section_name(heading)


def extract_job_description_sections_fallback(job_description_html):
    sections = []
    chunks = re.split(
        r"<b>\s*(Overview|Responsibilities|Qualifications)\s*</b>\s*<br\s*/?>",
        job_description_html,
        flags=re.IGNORECASE,
    )
    for index in range(1, len(chunks), 2):
        heading = clean_text(chunks[index])
        html_chunk = chunks[index + 1]
        items = [
            clean_text(item)
            for item in re.findall(r"<li[^>]*>(.*?)</li>", html_chunk, flags=re.IGNORECASE | re.DOTALL)
        ]
        items = [item for item in items if item]
        text = clean_text(html_chunk, separator="\n")
        section = make_section(
            "description" if heading and heading.casefold() == "overview" else map_microsoft_section_name(heading),
            heading=heading,
            text=text,
            items=items,
        )
        if section:
            sections.append(section)
    return sections


def extract_compensation(full_text):
    if not full_text:
        return None

    match = re.search(
        r"(typical base pay range.*?USD\s*\$([\d,]+)\s*-\s*\$([\d,]+)\s*per\s+year.*?)"
        r"(?=\s+Certain roles|\s+This position|\s+Microsoft is an equal opportunity|$)",
        full_text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    raw = clean_text(match.group(1))
    return {
        "raw": raw,
        "currency": "USD",
        "min": int(match.group(2).replace(",", "")),
        "max": int(match.group(3).replace(",", "")),
        "period": "year",
        "text": raw,
        "locale": "US",
    }


def build_job_detail_v1_from_json(detail_json):
    data = detail_json.get("data", {}) if isinstance(detail_json, dict) else {}
    job_description_html = data.get("jobDescription")
    full_text = clean_text(job_description_html, separator="\n")

    return make_job_detail(
        job_id=data.get("id"),
        title=data.get("name"),
        company="Microsoft",
        metadata={
            "department": first_or_none(data.get("efcustomTextTaDisciplineName")) or data.get("department"),
            "job_family": first_or_none(data.get("efcustomTextCurrentProfession")),
            "role_type": first_or_none(data.get("efcustomTextRoletype")),
            "employment_type": first_or_none(data.get("efcustomTextEmploymentType")),
            "job_type": data.get("workLocationOption"),
            "required_travel": first_or_none(data.get("efcustomTextRequiredTravel")),
            "locations": data.get("locations") or data.get("location"),
            "created_at": format_time_ts(data.get("creationTs")),
            "posted_at": format_time_ts(data.get("postedTs")),
        },
        full_text=full_text,
        sections=extract_job_description_sections(job_description_html),
        compensation=extract_compensation(full_text),
    )


@dataclass
class DetailErrorCollector:
    failures: dict = field(default_factory=dict)

    def record(self, job, error_message, stage='detail_fetch'):
        job_id = job.get('id')
        if not job_id:
            return

        entry = self.failures.setdefault(
            job_id,
            {
                'job': job,
                'attempts': 0,
                'stage': stage,
                'errors': []
            }
        )
        entry['attempts'] += 1
        entry['stage'] = stage
        entry['job'] = job
        entry['errors'].append(str(error_message))
        logging.error("Detail scrape failed for job %s on attempt %s: %s", job_id, entry['attempts'], error_message)

    def resolve(self, job_id):
        self.failures.pop(job_id, None)

    def pending_jobs(self):
        return [entry['job'] for entry in self.failures.values()]

    def pending_count(self):
        return len(self.failures)

    def log_summary(self, prefix):
        if not self.failures:
            logging.info("%s: no pending detail failures.", prefix)
            return

        logging.warning("%s: %s pending detail failures.", prefix, len(self.failures))
        for job_id, entry in self.failures.items():
            last_error = entry['errors'][-1] if entry['errors'] else 'unknown error'
            logging.warning(
                "Pending detail failure for job %s after %s attempts. Last error: %s",
                job_id,
                entry['attempts'],
                last_error
            )


def fetch_and_store_job_details(job):
    job_id = job.get('id')
    if not job_id:
        return False, "missing job id"

    job_link = 'https://apply.careers.microsoft.com/api/pcsx/position_details'
    params = {
        'position_id': job_id,
        'domain': 'microsoft.com',
        'hl': 'en',
    }

    time.sleep(DETAIL_REQUEST_SLEEP_SECONDS)
    response = fetch_url(
        job_link,
        headers=HEADERS,
        params=params,
        json=JSON_PAYLOAD,
        data=DATA,
        use_proxy=USE_PROXY_DETAILED_POSTINGS,
        max_retries=3,
        timeout=10,
        request_type=REQUEST_TYPE_SINGLE
    )
    if not response:
        return False, "detail request returned no response"

    try:
        obj = json.loads(response.text)
    except json.JSONDecodeError as exc:
        return False, f"invalid detail JSON: {exc}"

    job_data = obj.get('data')
    if not isinstance(job_data, dict):
        return False, "detail response missing data object"

    job_detail = build_job_detail_v1_from_json(obj)
    job_text = json.dumps(job_detail, ensure_ascii=False)
    upload_success = upload_job_details_to_gcs(job_text, job_id, BUCKET_NAME, FOLDER_NAME)
    if not upload_success:
        return False, "upload to GCS failed"

    return True, None


def _update_detail_status(existing_entry, *, success, error_message=None):
    existing_entry['details_fetched'] = success
    existing_entry['detail_last_error'] = None if success else str(error_message)


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
    #     params['pg'] = page
    # elif PAGINATION_MODE == 'offset':
        # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
    #     offset = (page * MAX_JOBS_PER_PAGE)
    #     params['offset'] = offset
    # elif PAGINATION_MODE == 'firstItem':
    #     # firstItem-based pagination: firstItem = (page * MAX_JOBS_PER_PAGE) + 1
    #     first_item = (page * MAX_JOBS_PER_PAGE) + 1
    #     params['firstItem'] = first_item
    offset = (page * MAX_JOBS_PER_PAGE)
    time.sleep(3)
    response = fetch_url(
        f'https://apply.careers.microsoft.com/api/pcsx/search?domain=microsoft.com&query=&location=&start={offset}&sort_by=timestamp&',
        headers=HEADERS,
        params=None,
        json=JSON_PAYLOAD,
        data=DATA,
        use_proxy=USE_PROXY_DAILY_LIST,
        max_retries=3,
        timeout=20,
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


def update_master_list_with_jobs(jobs, master_list, error_collector):
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
            job['details_fetched'] = False
            job['detail_last_error'] = None
            master_list.append(job)
            new_jobs_count += 1

            success, error_message = fetch_and_store_job_details(job)
            if success:
                _update_detail_status(job, success=True)
            else:
                _update_detail_status(job, success=False, error_message=error_message)
                error_collector.record(job, error_message)

    # Mark old jobs as inactive
    for entry in master_list:
        if entry.get('last_updated') != current_date:
            if entry.get('status') != 'inactive':
                inactive_jobs_count += 1
            entry['status'] = 'inactive'

    return new_jobs_count, inactive_jobs_count, skipped_jobs_count


def retry_failed_job_details(master_list, error_collector):
    if error_collector.pending_count() == 0:
        return 0

    recovered_jobs = 0

    for retry_round in range(1, DETAIL_RETRY_ROUNDS + 1):
        pending_jobs = error_collector.pending_jobs()
        if not pending_jobs:
            break

        logging.warning(
            "Starting detail retry round %s with %s pending jobs.",
            retry_round,
            len(pending_jobs)
        )

        for job in pending_jobs:
            job_id = job.get('id')
            success, error_message = fetch_and_store_job_details(job)
            if success:
                error_collector.resolve(job_id)
                recovered_jobs += 1
                existing_entry = next((entry for entry in master_list if entry.get('id') == job_id), None)
                if existing_entry:
                    _update_detail_status(existing_entry, success=True)
                logging.info("Recovered detail scrape for job %s in retry round %s.", job_id, retry_round)
            else:
                existing_entry = next((entry for entry in master_list if entry.get('id') == job_id), None)
                if existing_entry:
                    _update_detail_status(existing_entry, success=False, error_message=error_message)
                error_collector.record(job, error_message)

        save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)
        error_collector.log_summary(f"After retry round {retry_round}")

        if error_collector.pending_count() == 0:
            break

        if retry_round < DETAIL_RETRY_ROUNDS:
            time.sleep(DETAIL_RETRY_DELAY_SECONDS)

    return recovered_jobs


def _process_and_persist_block(raw_jobs_block, master_list, block_index, error_collector):
    """
    Process one block of raw jobs, update the master list (including detail fetches for
    newly discovered jobs), and persist the master list immediately.
    """
    if not raw_jobs_block:
        return 0, 0, 0, 0

    jobs = process_jobs(raw_jobs_block, JOB_DATA_KEYS)
    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(jobs, master_list, error_collector)
    save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)
    logging.info(
        f"Saved block {block_index}: {len(jobs)} jobs processed, "
        f"{new_jobs_count} new, {inactive_jobs_count} marked inactive, {skipped_jobs_count} skipped."
    )

    raw_jobs_block.clear()
    gc.collect()

    return len(jobs), new_jobs_count, inactive_jobs_count, skipped_jobs_count


def fetch_all_jobs(master_list):
    """
    Fetch and process job postings in blocks of REQUESTS_PER_BLOCK requests.
    Each block is processed, enriched (new-job details), and the master list is saved
    immediately to minimize data loss if the script is interrupted.
    Returns aggregated counters for metrics/logging.
    """
    total_jobs_from_response = 0
    total_processed_jobs = 0
    total_new_jobs = 0
    total_inactive_jobs = 0
    total_skipped_jobs = 0
    error_collector = DetailErrorCollector()

    if USE_PAGINATION:
        page = PAGE_START
        raw_jobs_block = []
        requests_in_block = 0
        block_index = 1

        while True:
            logging.info(f"Fetching page {page} of job listings...")
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
                    raw_jobs_block, master_list, block_index, error_collector
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
                raw_jobs_block, master_list, block_index, error_collector
            )
            total_processed_jobs += processed
            total_new_jobs += new_jobs
            total_inactive_jobs += inactive_jobs
            total_skipped_jobs += skipped_jobs
    else:
        job_data, total_jobs_from_response = fetch_job_list_page(PAGE_START)
        if job_data:
            processed, new_jobs, inactive_jobs, skipped_jobs = _process_and_persist_block(
                list(job_data), master_list, 1, error_collector
            )
            total_processed_jobs += processed
            total_new_jobs += new_jobs
            total_inactive_jobs += inactive_jobs
            total_skipped_jobs += skipped_jobs
            logging.info(f"Fetched and processed {len(job_data)} jobs from the single page.")
        else:
            logging.info("No jobs found on the single page.")

    recovered_jobs = retry_failed_job_details(master_list, error_collector)
    error_collector.log_summary("Final detail retry status")
    logging.info("Recovered %s detail scrapes during retry phase.", recovered_jobs)

    return total_processed_jobs, total_new_jobs, total_inactive_jobs, total_skipped_jobs


def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")
    starting_time = time.time()
    cpu_usage = psutil.cpu_percent(interval=1)

    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)
    jobs_processed, new_jobs_count, inactive_jobs_count, skipped_jobs_count = fetch_all_jobs(master_list)

    execution_time = time.time() - starting_time

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

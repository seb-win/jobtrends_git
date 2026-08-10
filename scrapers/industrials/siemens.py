import logging
import re
import time
import psutil
from bs4 import BeautifulSoup
import html
import json
from urllib.parse import urljoin

from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, send_metrics_to_cloud_function
)

###
# v4: HTML scraper template aligned with scraper_json_v3 request logic.
#     Supports PARAMS, JSON_PAYLOAD, DATA and configurable pagination modes,
#     while keeping the job extraction logic BeautifulSoup-based.

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'industrials_jobs'
FOLDER_NAME = 'siemens'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'get'
REQUEST_TYPE_SINGLE = 'get'

MAX_JOBS_PER_PAGE = 6
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

KEY_NAME = 'folderRecordsPerPage'
JOB_LIST_SELECTOR = 'div.article__header__text'

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=0, i',
    'referer': 'https://jobs.siemens.com/de_DE/externaljobs/SearchJobs/?listFilterMode=1&folderRecordsPerPage=60&',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://jobs.siemens.com/de_DE/externaljobs/SearchJobs/'

PARAMS = {
    #'listFilterMode': '1',
    'folderRecordsPerPage': '6',
    'folderOffset': '0',
    #'ste_sid': '65f4b6f8917039b16d52f4e220e4e326',
}
JSON_PAYLOAD = None
DATA = None


def clean_text(value):
    if not value:
        return None

    value = html.unescape(str(value))
    text = BeautifulSoup(value, "html.parser").get_text(separator=" ", strip=True)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


SECTION_NAME_BY_HEADING = {
    "about the role": "description",
    "what you'll be doing": "responsibilities",
    "what you’ll be doing": "responsibilities",
    "what skills and experience you'll bring": "qualifications",
    "what skills and experience you’ll bring": "qualifications",
    "what's on offer": "benefits",
    "what’s on offer": "benefits",
    "who we are": "about",
    "how we work": "about",
    "ihre aufgaben und verantwortlichkeiten": "responsibilities",
    "ihre qualifikationen und erfahrung": "qualifications",
    "ihr profil und ihre fähigkeiten": "requirements",
    "unser globales team": "about",
    "unsere kultur": "about",
}


def extract_detail_fields(soup):
    fields = {}
    for field in soup.select("#section0__content .article__content__view__field"):
        label_element = field.select_one(".article__content__view__field__label")
        value_element = field.select_one(".article__content__view__field__value")
        label = clean_text(label_element.get_text(" ", strip=True)) if label_element else None
        if label and value_element:
            fields[label.rstrip(":")] = clean_text(value_element.get_text(" ", strip=True))
    return fields


def extract_job_title(soup):
    title_element = soup.select_one(".section__header__text__title")
    return clean_text(title_element.get_text(" ", strip=True)) if title_element else None


def extract_detail_sections(detail_element):
    if not detail_element:
        return []

    sections = []
    current = None

    for element in detail_element.find_all(["strong", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol"], recursive=True):
        heading = clean_text(element.get_text(" ", strip=True))
        if element.name in {"strong", "h1", "h2", "h3", "h4", "h5", "h6"}:
            if not heading:
                continue
            key = heading.rstrip(":").lower()
            section_name = SECTION_NAME_BY_HEADING.get(key)
            if section_name:
                current = {"name": section_name, "heading": heading.rstrip(":"), "text": None, "items": []}
                sections.append(current)
            continue

        if element.name in {"ul", "ol"} and current:
            items = [clean_text(item.get_text(" ", strip=True)) for item in element.find_all("li", recursive=False)]
            current["items"].extend([item for item in items if item])

    return [section for section in sections if section["text"] or section["items"]]


def split_locations(value):
    if not value:
        return []
    locations = [clean_text(part) for part in re.split(r"\s*;\s*", value)]
    return [location for location in locations if location]


def build_job_detail_v1_from_html(html_content):
    soup = BeautifulSoup(html_content or "", "html.parser")
    fields = extract_detail_fields(soup)
    detail_element = soup.select_one("#section1__content")
    full_text = clean_text(detail_element.get_text(" ", strip=True)) if detail_element else None

    return {
        "schema_version": "job_detail_v1",
        "job": {
            "id": fields.get("Job ID"),
            "title": extract_job_title(soup),
            "company": fields.get("Unternehmen") or "Siemens",
        },
        "metadata": {
            "department": fields.get("Organization"),
            "job_family": fields.get("Tätigkeitsbereich"),
            "role_type": None,
            "employment_type": fields.get("Beschäftigungsart"),
            "job_type": fields.get("Arbeitsmodell"),
            "career_level": fields.get("Erfahrungsniveau"),
            "experience_level": None,
            "required_travel": None,
            "locations": split_locations(fields.get("Standort(e)")),
            "created_at": None,
            "posted_at": fields.get("Veröffentlicht seit"),
            "updated_at": None,
        },
        "content": {
            "full_text": full_text,
            "full_text_truncated": False,
            "sections": extract_detail_sections(detail_element),
        },
        "compensation": {
            "raw": None,
            "currency": None,
            "min": None,
            "max": None,
            "period": None,
            "text": None,
            "locale": None,
            "location_id": None,
        },
    }


def process_jobs(job_postings):
    """
    Extract relevant job details from HTML job postings using BeautifulSoup.
    Adjust the selectors inside this function for the target website.
    """
    jobs = []
    base_url = "https://jobs.siemens.com"

    for job in job_postings:
        title_element = job.select_one("h3.article__header__text__title a.link")
        city_element = job.select_one(".list-item-jobCity")
        state_element = job.select_one(".list-item-jobState")
        country_element = job.select_one(".list-item-jobCountry")
        job_id_element = job.select_one(".list-item-jobId")
        family_element = job.select_one(".list-item-family")

        job_title = clean_text(title_element.get_text()) if title_element else None
        job_link = (
            urljoin(base_url, title_element.get("href"))
            if title_element and title_element.has_attr("href")
            else None
        )

        city = clean_text(city_element.get_text()) if city_element else None
        state = clean_text(state_element.get_text()) if state_element else None
        country = clean_text(country_element.get_text()) if country_element else None
        location = ", ".join(filter(None, [city, state])) or None

        job_id_text = clean_text(job_id_element.get_text()) if job_id_element else None
        job_id = job_id_text.replace("Job-ID:", "").strip() if job_id_text else None
        job_family = clean_text(family_element.get_text()) if family_element else None

        job_details = {
            'created': None,
            'updated': None,
            'jobTitle': job_title,
            'department': job_family,
            'team': None,
            'location': location,
            'country': country,
            'contract': None,
            'id': job_id,
            'link': job_link,
            'career_level': None,
            'employment_type': None,
            'scraping_date': None,
            'last_updated': None,
            'status': None,
            'keywords': []
        }

        jobs.append(job_details)
    return jobs


def build_params(page):
    """
    Build request parameters for the list endpoint/page.
    Keeps the same pagination behavior as scraper_json_v3.
    """
    params = {**PARAMS}

    if PAGINATION_MODE == 'page':
        params['page'] = page
    elif PAGINATION_MODE == 'offset':
        params['folderOffset'] = page * MAX_JOBS_PER_PAGE
    elif PAGINATION_MODE == 'firstItem':
        params['firstItem'] = (page * MAX_JOBS_PER_PAGE) + 1

    return params


def fetch_job_list_page(page):
    """
    Fetch a single page of job listings in HTML format.
    Returns a list of BeautifulSoup elements selected by JOB_LIST_SELECTOR.
    """
    params = build_params(page)

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
        logging.error("Failed to fetch job list page after multiple attempts.")
        return None

    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup.select(JOB_LIST_SELECTOR)
    except Exception as e:
        logging.error(f"Failed to parse response HTML: {e}")
        return None


def fetch_all_jobs():
    """
    Fetch all raw HTML job postings (paginated or not) based on USE_PAGINATION.
    Processing is handled separately in process_jobs(), mirroring scraper_json_v3.
    """
    all_job_postings = []

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings...")
            job_postings = fetch_job_list_page(page)
            if job_postings is None:
                break

            if not job_postings:
                logging.info("No more job postings found.")
                break

            all_job_postings.extend(job_postings)

            if len(job_postings) < MAX_JOBS_PER_PAGE:
                logging.info("Fewer jobs than MAX_JOBS_PER_PAGE found, ending pagination.")
                break

            page += 1
    else:
        logging.info("Fetching a single page of job listings...")
        job_postings = fetch_job_list_page(PAGE_START)
        if job_postings:
            all_job_postings.extend(job_postings)
            logging.info(f"Fetched {len(job_postings)} jobs from the single page.")
        else:
            logging.info("No job postings found.")

    return all_job_postings


def update_master_list_with_jobs(jobs, master_list):
    """
    Update the master list with new or existing jobs, fetch job details when needed,
    and mark old jobs as inactive.
    """
    current_date = get_current_date()
    new_jobs_count = 0
    inactive_jobs_count = 0
    skipped_jobs_count = 0
    processed_jobs_count = 0

    for job in jobs:
        job_id = job.get('id')
        if not job_id:
            skipped_jobs_count += 1
            continue

        existing_entry = next((entry for entry in master_list if entry['id'] == job_id), None)

        if existing_entry:
            update_job_status(existing_entry, current_date)
        else:
            job['scraping_date'] = current_date
            job['last_updated'] = current_date
            job['status'] = 'active'
            master_list.append(job)
            new_jobs_count += 1

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
                    job_detail_data = build_job_detail_v1_from_html(response.text)
                    job_detail_json = json.dumps(job_detail_data, ensure_ascii=False, indent=2)
                    upload_job_details_to_gcs(job_detail_json, job_id, BUCKET_NAME, FOLDER_NAME)

        processed_jobs_count += 1

        if processed_jobs_count % 100 == 0:
            logging.info(f"Saving master list after processing {processed_jobs_count} jobs...")
            save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

    for entry in master_list:
        if entry['last_updated'] != current_date:
            entry['status'] = 'inactive'
            inactive_jobs_count += 1

    return new_jobs_count, inactive_jobs_count, skipped_jobs_count


def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")
    starting_time = time.time()
    cpu_usage = psutil.cpu_percent(interval=1)

    # Step 1: Fetch all raw HTML job postings
    raw_job_postings = fetch_all_jobs()

    # Step 2: Process jobs using BeautifulSoup extraction logic
    jobs = process_jobs(raw_job_postings)

    # Step 3: Load and update master list
    master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)
    new_jobs_count, inactive_jobs_count, skipped_jobs_count = update_master_list_with_jobs(jobs, master_list)

    # Step 4: Save the updated master list
    save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

    execution_time = time.time() - starting_time

    logging.info(f"Scraping completed successfully. {len(jobs)} jobs processed.")
    logging.info(f"{new_jobs_count} new jobs added.")
    logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
    logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
    send_metrics_to_cloud_function(
        FOLDER_NAME,
        execution_time,
        cpu_usage,
        len(jobs),
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

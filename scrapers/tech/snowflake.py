import logging
import time
import psutil
from bs4 import BeautifulSoup
import re
import json
import requests
from html import unescape
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'tech_jobs'
FOLDER_NAME = 'snowflake'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'post'

MAX_JOBS_PER_PAGE = 500
PAGE_START = 0

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'offset'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['refineSearch', 'data', 'jobs']
TOTAL_JOBS_KEY = ['refineSearch', 'totalHits']

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'content-type': 'application/json',
    'origin': 'https://careers.snowflake.com',
    'priority': 'u=1, i',
    'referer': 'https://careers.snowflake.com/us/en/search-results',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'x-csrf-token': '3e90d194b7d84226b40af59cfa9a0c17',
}

DAILY_JOB_URL = 'https://careers.snowflake.com/widgets'

JOB_DATA_KEYS = {
    'created': ['dateCreated'],
    'jobTitle': ['title'],
    'department': ['category'],
    'team': ['subCategory'],
    'location': ['cityStateCountry'],
    'country': ['country'],
    'contract': ['contractType'],
    'id': ['jobId'],
    'seqNo': ['jobSeqNo'],
    'link': ['applyUrl'],
    'career_level': [],
    'employment_type': [],
    'skills': ['ml_skills'],
    'company': ['business'],
    'business_segment': ['businessSegment']
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = None
JSON_PAYLOAD = {
    'lang': 'en_us',
    'deviceType': 'desktop',
    'country': 'us',
    'pageName': 'search-results',
    'ddoKey': 'refineSearch',
    'sortBy': '',
    'subsearch': '',
    'from': 0,
    'jobs': True,
    'counts': True,
    'all_fields': [
        'category',
        'region',
        'location',
        'remote',
        'jobLevel',
    ],
    'size': 500,
    'clearAll': False,
    'jdsource': 'facets',
    'isSliderEnable': False,
    'pageId': 'page11',
    'siteType': 'external',
    'keywords': '',
    'global': True,
    'selected_fields': {},
    'locationData': {},
}
DATA = None


def clean_text(value):
    if value is None:
        return None

    text = BeautifulSoup(str(value), "html.parser").get_text(" ", strip=True)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def section_name_from_heading(heading):
    normalized = re.sub(r"[^a-z0-9]+", " ", (heading or "").lower()).strip()

    if normalized in {"in this role you will get to", "what you will do", "what youll do", "your role"}:
        return "responsibilities"
    if normalized in {"required skills", "requirements", "required qualifications"}:
        return "requirements"
    if normalized in {"minimum qualifications", "minimum requirements"}:
        return "minimum_qualifications"
    if normalized in {"preferred qualifications", "preferred requirements", "nice to have"}:
        return "preferred_qualifications"
    if normalized in {"benefits", "perks", "pay benefits"}:
        return "benefits"
    if normalized in {"salary", "compensation", "pay transparency"}:
        return "compensation"
    if normalized in {"additional information"}:
        return "additional_information"
    if normalized in {"about the job", "job description", "description"}:
        return "description"
    if normalized in {"about us", "about the team"}:
        return "about"

    return "other"


def extract_sections_from_description_html(description_html):
    if not description_html:
        return []

    soup = BeautifulSoup(description_html, "html.parser")
    sections = []
    current = None

    def finish_current():
        if not current:
            return

        text = clean_text(" ".join(current["text_parts"]))
        items = [item for item in current["items"] if item]
        if text or items:
            sections.append({
                "name": current["name"],
                "heading": current["heading"],
                "text": text,
                "items": items
            })

    for element in soup.find_all(["h1", "h2", "h3", "p", "ul", "ol"], recursive=True):
        if element.find_parent(["li"]):
            continue

        if element.name in {"h1", "h2", "h3"}:
            finish_current()
            heading = clean_text(element.get_text(" ", strip=True))
            current = {
                "name": section_name_from_heading(heading),
                "heading": heading,
                "text_parts": [],
                "items": []
            }
            continue

        if current is None:
            current = {
                "name": "description",
                "heading": None,
                "text_parts": [],
                "items": []
            }

        if element.name == "p":
            strong = element.find("strong", recursive=False)
            heading = clean_text(strong.get_text(" ", strip=True)) if strong else None
            paragraph_text = clean_text(element.get_text(" ", strip=True))
            if heading and paragraph_text and heading.rstrip(":") == paragraph_text.rstrip(":"):
                finish_current()
                current = {
                    "name": section_name_from_heading(heading),
                    "heading": heading,
                    "text_parts": [],
                    "items": []
                }
                continue

            text = clean_text(element.get_text(" ", strip=True))
            if text:
                current["text_parts"].append(text)
        elif element.name in {"ul", "ol"}:
            for li in element.find_all("li", recursive=False):
                item = clean_text(li.get_text(" ", strip=True))
                if item:
                    current["items"].append(item)

    finish_current()
    return sections


def parse_compensation(job_posting):
    raw = (
        job_posting.get("scrapeableCompensationSalarySummary")
        or job_posting.get("compensationTierSummary")
    )
    raw = clean_text(raw)

    compensation = {
        "raw": raw,
        "currency": None,
        "min": None,
        "max": None,
        "period": None,
        "text": raw,
        "locale": None,
        "location_id": None
    }

    if not raw:
        return compensation

    currency_match = re.search(r"[$€£]", raw)
    if currency_match:
        compensation["currency"] = {
            "$": "USD",
            "€": "EUR",
            "£": "GBP"
        }.get(currency_match.group(0))

    numbers = re.findall(r"(\d+(?:\.\d+)?)\s*([KkMm])?", raw)
    parsed_numbers = []
    for number, suffix in numbers:
        value = float(number)
        if suffix.lower() == "k":
            value *= 1000
        elif suffix.lower() == "m":
            value *= 1000000
        parsed_numbers.append(int(value) if value.is_integer() else value)

    if parsed_numbers:
        compensation["min"] = parsed_numbers[0]
    if len(parsed_numbers) > 1:
        compensation["max"] = parsed_numbers[1]

    return compensation


def build_job_detail_v1_from_json(job_json):
    job_posting = job_json.get("data", {}).get("jobPosting", job_json)
    if not isinstance(job_posting, dict):
        job_posting = {}

    description_html = job_posting.get("descriptionHtml")
    full_text = clean_text(description_html)
    team_names = job_posting.get("teamNames") or []
    secondary_locations = job_posting.get("secondaryLocationNames") or []
    locations = []

    primary_location = clean_text(job_posting.get("locationName"))
    if primary_location:
        locations.append(primary_location)
    for location in secondary_locations:
        cleaned_location = clean_text(location)
        if cleaned_location and cleaned_location not in locations:
            locations.append(cleaned_location)

    department_name = clean_text(job_posting.get("departmentExternalName") or job_posting.get("departmentName"))
    team_name = clean_text(team_names[0]) if team_names else None

    return {
        "schema_version": "job_detail_v1",
        "job": {
            "id": clean_text(job_posting.get("id")),
            "title": clean_text(job_posting.get("title")),
            "company": "Snowflake"
        },
        "metadata": {
            "department": team_name or department_name,
            "job_family": department_name if team_name else None,
            "role_type": None,
            "employment_type": clean_text(job_posting.get("employmentType")),
            "job_type": clean_text(job_posting.get("workplaceType")),
            "career_level": None,
            "experience_level": None,
            "required_travel": None,
            "locations": locations,
            "created_at": None,
            "posted_at": None,
            "updated_at": None
        },
        "content": {
            "full_text": full_text,
            "full_text_truncated": False,
            "sections": extract_sections_from_description_html(description_html)
        },
        "compensation": parse_compensation(job_posting)
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
    json = {**JSON_PAYLOAD}

    # Adjust pagination parameters based on PAGINATION_MODE
    if PAGINATION_MODE == 'page':
        # Standard page-based pagination
        json['page'] = page
    elif PAGINATION_MODE == 'offset':
        # Offset-based pagination: offset = page * MAX_JOBS_PER_PAGE
        offset = (page * MAX_JOBS_PER_PAGE)
        json['from'] = offset
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
            # Add new job
            job['scraping_date'] = current_date
            job['last_updated'] = current_date
            job['status'] = 'active'
            master_list.append(job)
            new_jobs_count += 1

            # Fetch job details if a link is provided
            job_link = job.get('link')
            if isinstance(job_link, str):
                # Remove trailing '/application' if present
                if job_link.endswith('/application'):
                    job_link = job_link.rsplit('/application', 1)[0]
                
                # Extract everything after 'snowflake/'
                if 'snowflake/' in job_link:
                    job_id = job_link.split('snowflake/', 1)[-1]


            header_single = {
                'accept': '*/*',
                'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
                'apollographql-client-name': 'frontend_non_user',
                'apollographql-client-version': '0.1.0',
                'content-type': 'application/json',
                'origin': 'https://jobs.ashbyhq.com',
                'priority': 'u=1, i',
                'referer': f'https://jobs.ashbyhq.com/snowflake/{job_id}',
                'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'traceparent': '00-0000000000000000786c23e6a73de38a-3051869e63617967-01',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
                'x-datadog-origin': 'rum',
                'x-datadog-parent-id': '3481712001764391271',
                'x-datadog-sampling-priority': '1',
                'x-datadog-trace-id': '8677350055591404426',
            }

            params = {
                'op': 'ApiJobPosting',
            }

            json_data = {
                'operationName': 'ApiJobPosting',
                'variables': {
                    'organizationHostedJobsPageName': 'snowflake',
                    'jobPostingId': job_id,
                },
                'query': 'query ApiJobPosting($organizationHostedJobsPageName: String!, $jobPostingId: String!) {\n  jobPosting(\n    organizationHostedJobsPageName: $organizationHostedJobsPageName\n    jobPostingId: $jobPostingId\n  ) {\n    id\n    title\n    departmentName\n    locationName\n    workplaceType\n    employmentType\n    descriptionHtml\n    isListed\n    isConfidential\n    teamNames\n    applicationForm {\n      ...FormRenderParts\n      __typename\n    }\n    surveyForms {\n      ...FormRenderParts\n      __typename\n    }\n    secondaryLocationNames\n    compensationTierSummary\n    compensationTiers {\n      id\n      title\n      tierSummary\n      __typename\n    }\n    applicationDeadline\n    compensationTierGuideUrl\n    scrapeableCompensationSalarySummary\n    compensationPhilosophyHtml\n    applicationLimitCalloutHtml\n    shouldAskForTextingConsent\n    candidateTextingPrivacyPolicyUrl\n    automatedProcessingLegalNotice {\n      automatedProcessingLegalNoticeRuleId\n      automatedProcessingLegalNoticeHtml\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment JSONBoxParts on JSONBox {\n  value\n  __typename\n}\n\nfragment FileParts on File {\n  id\n  filename\n  __typename\n}\n\nfragment FormFieldEntryParts on FormFieldEntry {\n  id\n  field\n  fieldValue {\n    ... on JSONBox {\n      ...JSONBoxParts\n      __typename\n    }\n    ... on File {\n      ...FileParts\n      __typename\n    }\n    ... on FileList {\n      files {\n        ...FileParts\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  isRequired\n  descriptionHtml\n  isHidden\n  __typename\n}\n\nfragment FormRenderParts on FormRender {\n  id\n  formControls {\n    identifier\n    title\n    __typename\n  }\n  errorMessages\n  sections {\n    title\n    descriptionHtml\n    fieldEntries {\n      ...FormFieldEntryParts\n      __typename\n    }\n    isHidden\n    __typename\n  }\n  sourceFormDefinitionId\n  __typename\n}',
            }
            
            if job_link:
                response = fetch_url(
                    'https://jobs.ashbyhq.com/api/non-user-graphql',
                    headers=header_single,
                    params=None,
                    json=json_data,
                    data=DATA,
                    use_proxy=USE_PROXY_DETAILED_POSTINGS,
                    max_retries=3,
                    timeout=10,
                    request_type=REQUEST_TYPE_SINGLE
                )
                if response:
                    job_json = json.loads(response.text)
                    job_detail = build_job_detail_v1_from_json(job_json)

                    if isinstance(job_detail, dict):
                        upload_job_details_to_gcs(
                            json.dumps(job_detail, ensure_ascii=False),
                            job_id,
                            BUCKET_NAME,
                            FOLDER_NAME
                        )
                    else:
                        print(f"Warning: job_detail is None or not a dict for job_id {job_id}")

        processed_jobs_count += 1

        # Save the master list after every 1000 processed jobs
        if processed_jobs_count % 100 == 0:
            logging.info(f"Saving master list after processing {processed_jobs_count} jobs...")
            save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

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

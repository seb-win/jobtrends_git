import logging
import time
import psutil
from bs4 import BeautifulSoup
import json
import html
import re

from orchestrator.schemas.job_detail_v1 import build_full_text, make_job_detail, make_section
from orchestrator.util_v2 import (
    get_proxy, fetch_url, load_master_list, save_master_list,
    get_current_date, get_storage_client, update_job_status,
    upload_job_details_to_gcs, get_nested_value, send_metrics_to_cloud_function
)

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = 'ai_comp_jobs'
FOLDER_NAME = 'deploy'

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = True
REQUEST_TYPE_LIST = 'post'
REQUEST_TYPE_SINGLE = 'post'

MAX_JOBS_PER_PAGE = 50
PAGE_START = 1

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = 'page'

KEY_NAME = 'limit'
JOBS_LIST_KEY = ['data', 'jobBoard', 'jobPostings']
TOTAL_JOBS_KEY = []

HEADERS = {
    'accept': '*/*',
    'accept-language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'apollographql-client-name': 'frontend_non_user',
    'apollographql-client-version': '0.1.0',
    'content-type': 'application/json',
    'origin': 'https://jobs.ashbyhq.com',
    'priority': 'u=1, i',
    'referer': 'https://jobs.ashbyhq.com/openai-deployment-company/',
    'sec-ch-ua': '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36',
}

DAILY_JOB_URL = 'https://jobs.ashbyhq.com/api/non-user-graphql'

JOB_DATA_KEYS = {
    'created': ['dummy_key'],
    'updated': [],
    'jobTitle': ['title'],
    'department': [],
    'team': ['teamId'],
    'location': ['locationName'],
    'country': [],
    'contract': [],
    'id': ['id'],
    'link': [],
    'career_level': [],
    'employment_type': ['employmentType'],
}

extraction_logic = {
    # If any specific fields need special handling, define them here.
}

PARAMS = {
    'op': 'ApiJobBoardWithTeams',
}
JSON_PAYLOAD = {
    'operationName': 'ApiJobBoardWithTeams',
    'variables': {
        'organizationHostedJobsPageName': 'openai-deployment-company',
    },
    'query': 'query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {\n  jobBoard: jobBoardWithTeams(\n    organizationHostedJobsPageName: $organizationHostedJobsPageName\n  ) {\n    teams {\n      id\n      name\n      externalName\n      parentTeamId\n      __typename\n    }\n    jobPostings {\n      id\n      title\n      teamId\n      locationId\n      locationName\n      workplaceType\n      employmentType\n      secondaryLocations {\n        ...JobPostingSecondaryLocationParts\n        __typename\n      }\n      compensationTierSummary\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment JobPostingSecondaryLocationParts on JobPostingSecondaryLocation {\n  locationId\n  locationName\n  __typename\n}',
}
DATA = None


JOB_DETAIL_QUERY = 'query ApiJobPosting($organizationHostedJobsPageName: String!, $jobPostingId: String!) {\n  jobPosting(\n    organizationHostedJobsPageName: $organizationHostedJobsPageName\n    jobPostingId: $jobPostingId\n  ) {\n    id\n    title\n    departmentName\n    departmentExternalName\n    locationName\n    locationAddress\n    workplaceType\n    employmentType\n    descriptionHtml\n    isListed\n    isConfidential\n    teamNames\n    applicationForm {\n      ...FormRenderParts\n      __typename\n    }\n    surveyForms {\n      ...FormRenderParts\n      __typename\n    }\n    secondaryLocationNames\n    compensationTierSummary\n    compensationTiers {\n      id\n      title\n      tierSummary\n      __typename\n    }\n    applicationDeadline\n    compensationTierGuideUrl\n    scrapeableCompensationSalarySummary\n    compensationPhilosophyHtml\n    applicationLimitCalloutHtml\n    shouldAskForTextingConsent\n    shouldAskForWhatsAppConsent\n    candidateTextingPrivacyPolicyUrl\n    candidateTextingTermsAndConditionsUrl\n    legalEntityNameForTextingConsent\n    automatedProcessingLegalNotice {\n      automatedProcessingLegalNoticeRuleId\n      automatedProcessingLegalNoticeHtml\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment JSONBoxParts on JSONBox {\n  value\n  __typename\n}\n\nfragment FileParts on File {\n  id\n  filename\n  __typename\n}\n\nfragment FormFieldEntryParts on FormFieldEntry {\n  id\n  field\n  fieldValue {\n    ... on JSONBox {\n      ...JSONBoxParts\n      __typename\n    }\n    ... on File {\n      ...FileParts\n      __typename\n    }\n    ... on FileList {\n      files {\n        ...FileParts\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  isRequired\n  descriptionHtml\n  isHidden\n  __typename\n}\n\nfragment FormRenderParts on FormRender {\n  id\n  formControls {\n    identifier\n    title\n    __typename\n  }\n  errorMessages\n  formErrors {\n    message\n    fieldEntryId\n    __typename\n  }\n  sections {\n    title\n    descriptionHtml\n    fieldEntries {\n      ...FormFieldEntryParts\n      __typename\n    }\n    isHidden\n    __typename\n  }\n  sourceFormDefinitionId\n  __typename\n}'

def clean_text(value):
    if not value:
        return None

    # HTML entities dekodieren
    value = html.unescape(value)

    # HTML entfernen
    text = BeautifulSoup(value, "html.parser").get_text(separator=" ", strip=True)

    # Geschützte Leerzeichen etc. normalisieren
    text = text.replace("\xa0", " ")

    # Mehrfache Whitespaces entfernen
    text = re.sub(r"\s+", " ", text).strip()

    return text


def _deploy_section_name(heading):
    heading_text = clean_text(heading)
    if not heading_text:
        return "other"

    normalized = heading_text.casefold().rstrip(":")
    if normalized == "about the company":
        return "about"
    if normalized == "about the role":
        return "description"
    if normalized == "in this role, you will":
        return "responsibilities"
    if normalized == "you may thrive in this role if you":
        return "qualifications"
    if "compensation" in normalized or "salary" in normalized:
        return "compensation"
    return "other"


def _html_to_sections(html_text):
    if not html_text:
        return []

    soup = BeautifulSoup(html.unescape(str(html_text)), "html.parser")
    sections = []
    current = {"name": "description", "heading": None, "text_parts": [], "items": []}

    def flush_current():
        section = make_section(
            current["name"],
            heading=current["heading"],
            text=build_full_text(*current["text_parts"]),
            items=current["items"],
        )
        if section:
            sections.append(section)

    for element in soup.find_all(["h1", "h2", "h3", "h4", "p", "li"], recursive=True):
        if element.name != "li" and element.find_parent("li"):
            continue

        text = clean_text(element.get_text(" ", strip=True))
        if not text:
            continue

        strong = element.find("strong")
        strong_text = clean_text(strong.get_text(" ", strip=True)) if strong else None
        is_heading = element.name in {"h1", "h2", "h3", "h4"} or (
            element.name == "p" and strong_text and text == strong_text
        )

        if is_heading:
            flush_current()
            heading = text.rstrip(":")
            current = {
                "name": _deploy_section_name(heading),
                "heading": heading,
                "text_parts": [],
                "items": [],
            }
            continue

        if element.name == "li":
            current["items"].append(text)
        else:
            current["text_parts"].append(text)

    flush_current()
    return sections


def _extract_locations(job_posting):
    locations = []
    for value in [job_posting.get("locationName"), *(job_posting.get("secondaryLocationNames") or [])]:
        text = clean_text(value)
        if text and text not in locations:
            locations.append(text)
    return locations


def _extract_compensation(job_posting):
    summary = clean_text(job_posting.get("compensationTierSummary"))
    salary_summary = clean_text(job_posting.get("scrapeableCompensationSalarySummary"))
    philosophy = clean_text(job_posting.get("compensationPhilosophyHtml"))
    text = build_full_text(summary, salary_summary, philosophy)

    source = salary_summary or summary
    currency = None
    locale = None
    min_value = None
    max_value = None

    if source:
        if "£" in source:
            currency = "GBP"
            locale = "UK"
        elif "€" in source:
            currency = "EUR"

        values = []
        for value, suffix in re.findall(r"[£€]\s*([0-9]+(?:\.[0-9]+)?)\s*([KkMm]?)", source):
            number = float(value)
            if suffix.casefold() == "k":
                number *= 1000
            elif suffix.casefold() == "m":
                number *= 1000000
            values.append(int(number))
        if values:
            min_value = values[0]
            max_value = values[1] if len(values) > 1 else None

    return {
        "raw": summary or salary_summary,
        "currency": currency,
        "min": min_value,
        "max": max_value,
        "period": "year" if min_value is not None else None,
        "text": text,
        "locale": locale,
        "location_id": None,
    }


def build_job_detail_v1_from_json(detail_json):
    job_posting = get_nested_value(detail_json, ["data", "jobPosting"]) or detail_json.get("jobPosting") or detail_json
    description_html = job_posting.get("descriptionHtml")
    compensation_html = job_posting.get("compensationPhilosophyHtml")

    sections = _html_to_sections(description_html)
    compensation_section = make_section(
        "compensation",
        heading="Compensation",
        text=build_full_text(job_posting.get("compensationTierSummary"), job_posting.get("scrapeableCompensationSalarySummary"), compensation_html),
    )
    if compensation_section:
        sections.append(compensation_section)

    team_names = job_posting.get("teamNames") or []
    job_family = clean_text(job_posting.get("departmentExternalName") or job_posting.get("departmentName"))
    department = clean_text(team_names[-1]) if team_names else clean_text(job_posting.get("departmentName"))

    return make_job_detail(
        job_id=job_posting.get("id"),
        title=job_posting.get("title"),
        company="OpenAI Deployment Company",
        metadata={
            "department": department,
            "job_family": job_family,
            "employment_type": job_posting.get("employmentType"),
            "job_type": job_posting.get("workplaceType"),
            "locations": _extract_locations(job_posting),
            "updated_at": job_posting.get("applicationDeadline"),
        },
        full_text=build_full_text(description_html, compensation_html, job_posting.get("applicationLimitCalloutHtml")),
        sections=sections,
        compensation=_extract_compensation(job_posting),
    )


def build_job_detail_json(detail_json):
    return json.dumps(build_job_detail_v1_from_json(detail_json), ensure_ascii=False, indent=2)

def build_team_lookup(teams):
    """Create a mapping from Ashby team IDs to team names."""
    if not isinstance(teams, list):
        return {}

    return {
        team.get('id'): team.get('name')
        for team in teams
        if team.get('id') and team.get('name')
    }


def process_jobs(job_data, job_data_keys, team_lookup=None):
    """
    Extract relevant job details from job data using JOB_DATA_KEYS.
    This function uses get_nested_value() to retrieve values from the JSON structure.
    If a path is empty, that field is skipped.
    If extraction_logic defines a special extractor, it's applied to the retrieved value.
    """
    team_lookup = team_lookup or {}
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
            if key == 'team':
                value = team_lookup.get(value, value)
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
    params = {**PARAMS} if PARAMS else {}

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
        return None, 0, []

    try:
        job_data = response.json()
    except ValueError as e:
        logging.error(f"Failed to parse response to JSON: {e}")
        return None, 0, []

    total_jobs = 0
    if page == PAGE_START:
        total_jobs = get_nested_value(job_data, TOTAL_JOBS_KEY)

    job_list = get_nested_value(job_data, JOBS_LIST_KEY)
    if not isinstance(job_list, list):
        logging.error(f"Unexpected response format: job data is not a list or doesn't contain '{JOBS_LIST_KEY}' key.")
        return None, total_jobs, []

    teams = get_nested_value(job_data, ['data', 'jobBoard', 'teams'])

    return job_list, total_jobs, teams


def fetch_all_jobs():
    """
    Fetch all job postings (paginated or not) based on USE_PAGINATION.
    For pagination, it will loop through pages or offsets until it retrieves all jobs 
    or reaches a stopping condition.
    """
    all_jobs = []
    all_teams = []
    total_jobs_from_response = 0

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings...")
            job_data, total_jobs, teams = fetch_job_list_page(page)
            if job_data is None:
                break

            if teams:
                all_teams = teams

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
        job_data, total_jobs_from_response, teams = fetch_job_list_page(PAGE_START)
        if teams:
            all_teams = teams

        if job_data:
            all_jobs.extend(job_data)
            logging.info(f"Fetched {len(job_data)} jobs from the single page.")
        else:
            logging.info("No jobs found on the single page.")

    return all_jobs, all_teams


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

            job_link = 'https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobPosting'
            params = {'op': 'ApiJobPosting'}
            json_data = {
                'operationName': 'ApiJobPosting',
                'variables': {
                    'organizationHostedJobsPageName': 'openai-deployment-company',
                    'jobPostingId': job_id,
                },
                'query': JOB_DETAIL_QUERY,
            }

            response = fetch_url(
                job_link,
                headers=HEADERS,
                params=params,
                json=json_data,
                data=DATA,
                use_proxy=USE_PROXY_DETAILED_POSTINGS,
                max_retries=3,
                timeout=10,
                request_type=REQUEST_TYPE_SINGLE
            )
            if response:
                try:
                    detail_json = response.json()
                except ValueError as e:
                    logging.error(f"Failed to parse job detail JSON for {job_id}: {e}")
                else:
                    job_detail = build_job_detail_json(detail_json)
                    upload_job_details_to_gcs(job_detail, job_id, BUCKET_NAME, FOLDER_NAME)

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
    raw_job_data, teams = fetch_all_jobs()

    # Step 2: Process jobs using JOB_DATA_KEYS
    team_lookup = build_team_lookup(teams)
    jobs = process_jobs(raw_job_data, JOB_DATA_KEYS, team_lookup)

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

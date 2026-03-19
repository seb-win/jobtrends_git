import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional

import psutil

from bs4 import BeautifulSoup


def _bootstrap_import_paths() -> None:
    current_file = Path(__file__).resolve()

    for candidate in [current_file.parent, *current_file.parents]:
        orchestrator_pkg = candidate / "orchestrator"
        if orchestrator_pkg.is_dir():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)
            return

        if (candidate / "extractors").is_dir() and (candidate / "schemas").is_dir():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)
            return


_bootstrap_import_paths()

try:
    from orchestrator.extractors.audi_detail_extractor import extract_detail_sections
    from orchestrator.DBNormalize import _now_utc_iso, _prepare_job_for_db
    from orchestrator.db_runs import (
        finish_run,
        get_conn,
        mark_jobs_inactive,
        start_run,
        touch_jobs_last_seen,
        update_stage,
        upsert_job_and_payload_from_master,
    )
    from orchestrator.schemas.builder import build_detailjob
    from orchestrator.RunMetrics import _update_http_columns_in_db
    from orchestrator.util_v5 import (
        fetch_url,
        get_current_date,
        get_nested_value,
        load_master_list,
        save_master_list,
        update_job_status,
        upload_detailjob_json_to_gcs,
    )
except ImportError:
    # Fallback for running directly inside the VM's orchestrator folder.
    from extractors.audi_detail_extractor import extract_detail_sections
    from DBNormalize import _now_utc_iso, _prepare_job_for_db
    from db_runs import (
        finish_run,
        get_conn,
        mark_jobs_inactive,
        start_run,
        touch_jobs_last_seen,
        update_stage,
        upsert_job_and_payload_from_master,
    )
    from schemas.builder import build_detailjob
    from RunMetrics import _update_http_columns_in_db
    from util_v5 import (
        fetch_url,
        get_current_date,
        get_nested_value,
        load_master_list,
        save_master_list,
        update_job_status,
        upload_detailjob_json_to_gcs,
    )

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = "automotive_comp"
FOLDER_NAME = "audi"
BASE_DETAIL_HOST = "https://careers.audi.com/desktop.html#/DETAILS/"

USE_PROXY_DAILY_LIST = True
USE_PROXY_DETAILED_POSTINGS = True

# Audi liefert aktuell alles in einem Call (bei dir), daher False.
USE_PAGINATION = False

REQUEST_TYPE_LIST = "get"
REQUEST_TYPE_SINGLE = "get"

# WICHTIG: du wolltest limit=500, damit weniger Risiko für "silent truncation".
MAX_JOBS_PER_PAGE = 500
PAGE_START = 1

# Set PAGINATION_MODE to one of the following:
# 'page': Uses page number pagination (existing logic)
# 'offset': Uses offset-based pagination (offset = page * MAX_JOBS_PER_PAGE)
# 'firstItem': Uses a firstItem-based pagination (firstItem = (page * MAX_JOBS_PER_PAGE) + 1)
PAGINATION_MODE = "page"

KEY_NAME = "limit"
JOBS_LIST_KEY = ["d", "results"]
TOTAL_JOBS_KEY = ["total_jobs"]

HEADERS = {
    "accept": "application/atomsvc+xml;q=0.8, application/json;odata=verbose;q=0.5, */*;q=0.1",
    "accept-language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "maxdataserviceversion": "2.0",
    "priority": "u=1, i",
    "referer": "https://careers.audi.com/desktop.html",
    "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "x-csrf-token": "undefined",
}

DAILY_JOB_URL = (
    "https://careers.audi.com/EREC_OPEN_DESTINATION/sap/opu/odata/sap/zaudi_ui_open_srv/JobSet"
    "?sap-client=200&sap-language=de"
    "&$select=JobID,Posting,Title,PostingAge,Location,HierarchyLevel,ContractType,FunctionalArea,Company,TravelRatio,"
    "JobDetailsUrl,ZLanguage,RefCode,ApplicationUrl"
    "&$expand=Location,HierarchyLevel,ContractType,FunctionalArea,Company"
)

JOB_DATA_KEYS = {
    "created": ["PostingAge"],
    "jobTitle": ["Title"],
    "department": ["FunctionalArea", "Text"],
    "team": [],
    "location": ["Location", "Text"],
    "country": [],
    "contract": ["ContractType", "Text"],
    "id": ["JobID"],
    "link": [],
    "career_level": ["HierarchyLevel", "Text"],
    "employment_type": [],
    "company": ["Company", "Text"],
}

# Speziallogik optional pro Key
extraction_logic = {
    # z.B. 'created': lambda x: x
}

PARAMS = {KEY_NAME: MAX_JOBS_PER_PAGE}
JSON_PAYLOAD = None
DATA = None

# -----------------------------------------------------------------------------
# HTTP tracking (aggregated per Scrape-Run)
# -----------------------------------------------------------------------------
HTTP_STATS = None


def _build_public_job_link(job_id: object) -> Optional[str]:
    if not job_id:
        return None
    return f"{BASE_DETAIL_HOST}{job_id}"

def _clean_text(text: Optional[str]) -> Optional[str]:
    """Local fallback cleaner (deterministic)."""
    if text is None:
        return None
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", t)
    t = t.strip()
    return t or None


def _fallback_fulltext_from_xml(xml_text: str) -> Optional[str]:
    """
    Fallback: Wenn extract_detail_sections() noch kein Mapping hat, holen wir Fulltext wie früher.
    Das ist deterministisch und LLM-frei.
    """
    try:
        soup = BeautifulSoup(xml_text, features="xml")
        text = soup.get_text(separator="\n")
        return _clean_text(text)
    except Exception:
        return None


def process_jobs(job_data, job_data_keys):
    """
    Extract relevant job details from job data using JOB_DATA_KEYS.
    Uses get_nested_value() to retrieve values from the JSON structure.
    """
    jobs = []
    for listing in job_data:
        job_details = {
            "scraping_date": None,
            "last_updated": None,
            "status": None,
            "keywords": [],
        }

        for key, path in job_data_keys.items():
            if not path:
                continue

            value = get_nested_value(listing, path)
            extractor = extraction_logic.get(key, lambda x: x)
            job_details[key] = extractor(value)

        job_details["link"] = _build_public_job_link(job_details.get("id"))

        jobs.append(job_details)
    return jobs


def fetch_job_list_page(page):
    """
    Fetch a single page of job listings in JSON format.
    Adjusts request parameters based on PAGINATION_MODE.
    Returns (job_data_list, total_jobs).
    """
    params = {**PARAMS}

    if PAGINATION_MODE == "page":
        params["page"] = page
    elif PAGINATION_MODE == "offset":
        params["offset"] = (page * MAX_JOBS_PER_PAGE)
    elif PAGINATION_MODE == "firstItem":
        params["firstItem"] = (page * MAX_JOBS_PER_PAGE) + 1

    response = fetch_url(
        DAILY_JOB_URL,
        headers=HEADERS,
        params=params,
        json=JSON_PAYLOAD,
        data=DATA,
        use_proxy=USE_PROXY_DAILY_LIST,
        max_retries=3,
        timeout=10,
        request_type=REQUEST_TYPE_LIST,
        http_stats=HTTP_STATS,
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
        total_jobs = get_nested_value(job_data, TOTAL_JOBS_KEY) or 0

    job_list = get_nested_value(job_data, JOBS_LIST_KEY)
    if not isinstance(job_list, list):
        logging.error(
            f"Unexpected response format: job data is not a list or doesn't contain '{JOBS_LIST_KEY}' key."
        )
        return None, total_jobs

    return job_list, total_jobs


def fetch_all_jobs():
    """
    Fetch all job postings (paginated or not) based on USE_PAGINATION.
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

            if page == PAGE_START:
                total_jobs_from_response = total_jobs

            if not job_data:
                break

            all_jobs.extend(job_data)

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

    DB:
      - touch last_seen_at for ALL jobs seen today (new + existing) in one batch
      - mirror inactive status to DB for jobs not seen today
    """
    current_date = get_current_date()

    new_jobs_count = 0
    inactive_jobs_count = 0
    skipped_jobs_count = 0
    db_jobs_written = 0
    db_jobs_failed = 0

    # NEW: collect all IDs seen today (new + existing) for DB batch touch
    seen_job_ids = []

    for job in jobs:
        job_id = job.get("id")
        if not job_id:
            skipped_jobs_count += 1
            continue

        job_id_str = str(job_id)
        seen_job_ids.append(job_id_str)
        detail_url_abs = _build_public_job_link(job_id_str)
        job["link"] = detail_url_abs

        existing_entry = next((entry for entry in master_list if str(entry.get("id")) == job_id_str), None)

        if existing_entry:
            previous_link = existing_entry.get("link")
            update_job_status(existing_entry, current_date)
            existing_entry["link"] = detail_url_abs

            if previous_link != detail_url_abs:
                try:
                    db_job = _prepare_job_for_db(existing_entry)
                    upsert_job_and_payload_from_master(FOLDER_NAME, db_job)
                except Exception as _db_e:
                    db_jobs_failed += 1
                    logging.exception(f"DB link backfill failed for {FOLDER_NAME} job_id={job_id_str}: {_db_e}")
        else:
            # Add new job to master list
            job["scraping_date"] = current_date
            job["last_updated"] = current_date
            job["status"] = "active"
            master_list.append(job)
            new_jobs_count += 1

            # Phase 1 DB-Ingest: only new jobs
            try:
                db_job = _prepare_job_for_db(job)
                upsert_job_and_payload_from_master(FOLDER_NAME, db_job)
                db_jobs_written += 1
            except Exception as _db_e:
                db_jobs_failed += 1
                logging.exception(f"DB ingest failed for {FOLDER_NAME} job_id={job_id_str}: {_db_e}")

            # Fetch job details and upload to GCS
            job_link = (
                "https://careers.audi.com/EREC_OPEN_DESTINATION/sap/opu/odata/sap/zaudi_ui_open_srv/"
                f"JobSet('{job_id_str}')?sap-client=200&sap-language=de&$expand=Location,HierarchyLevel,ContractType,FunctionalArea,Company"
            )

            response = fetch_url(
                job_link,
                headers=HEADERS,
                params=PARAMS,
                json=JSON_PAYLOAD,
                data=DATA,
                use_proxy=USE_PROXY_DETAILED_POSTINGS,
                max_retries=3,
                timeout=10,
                request_type=REQUEST_TYPE_SINGLE,
                http_stats=HTTP_STATS,
            )

            if response:
                # Build standardized detail object (schema v0.1) and upload JSON (no raw HTML/XML saved)
                detail_model = build_detailjob(
                    version="0.1",
                    job_id=job_id_str,
                    metadata={
                        "company_key": FOLDER_NAME,
                        "url": detail_url_abs,
                        "scraped_at": _now_utc_iso(),
                        "locale": "de_DE",
                    },
                    job_meta={
                        "title": job.get("jobTitle"),
                        "location_text": job.get("location"),
                        "employment_type": None,
                        "contract_type": job.get("contract"),
                        "career_level": job.get("career_level"),
                    },
                )

                # New extractor engine call (DOM/JSON hardcoded mapping via DETAIL_MAPPING_V01)
                sections = extract_detail_sections(raw=response.text, input_type="json")

                # Fulltext fallback (solange Mapping-Block noch leer/unvollständig ist)
                fulltext = sections.get("fulltext")
                if not fulltext:
                    fulltext = _fallback_fulltext_from_xml(response.text)

                detail_model.extracted.fulltext = fulltext
                detail_model.extracted.overview = sections.get("overview")
                detail_model.extracted.responsibilities.items = sections.get("responsibilities") or []
                detail_model.extracted.requirements.items = sections.get("requirements") or []
                detail_model.extracted.additional.items = sections.get("additional") or []
                detail_model.extracted.benefits.items = sections.get("benefits") or []
                detail_model.extracted.process = sections.get("process")

                upload_detailjob_json_to_gcs(
                    detail_model.model_dump(),
                    job_id_str,
                    BUCKET_NAME,
                    FOLDER_NAME,
                )

    # NEW: DB touch for all seen jobs (last_seen_at + status='active' + last_updated)
    # Assumes touch_jobs_last_seen(source, job_ids, seen_at) exists in Templates.db_runs.
    try:
        touch_jobs_last_seen(FOLDER_NAME, seen_job_ids, current_date)
    except Exception as _e:
        logging.exception(f"DB touch last_seen_at failed for {FOLDER_NAME}: {_e}")

    # Mark old jobs as inactive (master list) + NEW: collect inactive ids for DB mirror
    inactive_ids = []
    for entry in master_list:
        if entry.get("last_updated") != current_date:
            entry["status"] = "inactive"
            inactive_jobs_count += 1
            if entry.get("id"):
                inactive_ids.append(str(entry["id"]))

    # NEW: Mirror inactive status to DB
    try:
        mark_jobs_inactive(FOLDER_NAME, inactive_ids)
    except Exception as _e:
        logging.exception(f"DB mark inactive failed for {FOLDER_NAME}: {_e}")

    return new_jobs_count, inactive_jobs_count, skipped_jobs_count, db_jobs_written, db_jobs_failed


def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")

    run_id = start_run(
        company_key=FOLDER_NAME,
        meta={
            "bucket": BUCKET_NAME,
            "use_proxy_daily_list": USE_PROXY_DAILY_LIST,
            "use_proxy_detailed": USE_PROXY_DETAILED_POSTINGS,
            "pagination": USE_PAGINATION,
            "pagination_mode": PAGINATION_MODE,
            "limit": MAX_JOBS_PER_PAGE,
        },
    )

    global HTTP_STATS
    HTTP_STATS = {}

    start_time = time.time()
    cpu_usage = None

    try:
        update_stage(run_id, "started")

        try:
            cpu_usage = psutil.cpu_percent(interval=1)
        except Exception:
            cpu_usage = None

        update_stage(run_id, "fetch_list")
        raw_job_data = fetch_all_jobs()

        update_stage(run_id, "parse_list")
        jobs = process_jobs(raw_job_data or [], JOB_DATA_KEYS)

        update_stage(run_id, "load_master")
        master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)

        update_stage(run_id, "update_master")
        new_jobs_count, inactive_jobs_count, skipped_jobs_count, db_jobs_written, db_jobs_failed = update_master_list_with_jobs(
            jobs, master_list
        )

        update_stage(run_id, "save_master")
        save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

        execution_time = time.time() - start_time

        finish_run(
            run_id=run_id,
            status="success",
            execution_time_sec=execution_time,
            cpu_usage_pct=cpu_usage,
            jobs_fetched=len(raw_job_data) if raw_job_data else 0,
            jobs_processed=len(jobs) if jobs else 0,
            new_jobs=new_jobs_count,
            inactive_jobs=inactive_jobs_count,
            skipped_jobs=skipped_jobs_count,
            meta={
                "note": "master_json_still_enabled",
                "http": HTTP_STATS,
                "db_jobs_written": db_jobs_written,
                "db_jobs_failed": db_jobs_failed,
            },
        )

        try:
            _update_http_columns_in_db(run_id, HTTP_STATS, conn_factory=get_conn)
        except Exception as _e:
            logging.warning(f"Could not update http_* columns for run {run_id}: {_e}")

        update_stage(run_id, "finished_success")

        logging.info(f"Scraping completed successfully. {len(jobs)} jobs processed.")
        logging.info(f"{new_jobs_count} new jobs added.")
        logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
        logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
        logging.info(f"DB ingest: {db_jobs_written} jobs written, {db_jobs_failed} failed")

    except Exception as e:
        execution_time = time.time() - start_time

        update_stage(run_id, "failed", meta={"exception_type": type(e).__name__})

        finish_run(
            run_id=run_id,
            status="failed",
            execution_time_sec=execution_time,
            cpu_usage_pct=cpu_usage,
            jobs_fetched=len(raw_job_data) if "raw_job_data" in locals() and raw_job_data else None,
            jobs_processed=len(jobs) if "jobs" in locals() and jobs else None,
            error_message=str(e),
            meta={
                "exception_type": type(e).__name__,
                "http": HTTP_STATS,
            },
        )

        try:
            _update_http_columns_in_db(run_id, HTTP_STATS or {}, conn_factory=get_conn)
        except Exception as _e:
            logging.warning(f"Could not update http_* columns for run {run_id}: {_e}")

        logging.exception("Scraping failed.")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()

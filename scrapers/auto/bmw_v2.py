import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional

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
    from orchestrator.extractors.bmw_detail_extractor import extract_detail_sections
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
        load_master_list,
        save_master_list,
        update_job_status,
        upload_detailjob_json_to_gcs,
    )
except ImportError:
    # Fallback for running directly inside the VM's orchestrator folder.
    from extractors.bmw_detail_extractor import extract_detail_sections
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
        load_master_list,
        save_master_list,
        update_job_status,
        upload_detailjob_json_to_gcs,
    )

# MONITORING: Optional psutil import for CPU tracking
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# --------------------------------------
# Configuration and Constants
# --------------------------------------
BUCKET_NAME = "automotive_comp"
FOLDER_NAME = "bmw"
COMPANY_KEY = "bmw"

USE_PROXY_DAILY_LIST = False
USE_PROXY_DETAILED_POSTINGS = False
USE_PAGINATION = False
REQUEST_TYPE_LIST = "get"
REQUEST_TYPE_SINGLE = "get"

MAX_JOBS_PER_PAGE = 50
PAGE_START = 1
JOB_LIST_KEY = "div.grp-jobfinder__wrapper"

HEADERS = {
    'sec-ch-ua-platform': '"macOS"',
    'Referer': 'https://www.bmwgroup.jobs/de/de.html',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'sec-ch-ua-mobile': '?0',
}

PARAMS = {
    "rowIndex": "0",
    "blockCount": "5000",
}

DAILY_JOB_URL = "https://www.bmwgroup.jobs/de/de/_jcr_content/main/layoutcontainer/jobfinder30_copy.jobfinder_table.content.html"

JOB_DATA_KEYS = {
    # "created" ist eigentlich posting date – kommt stabil als data-posting-date:
    'created': 'div.grp-jobfinder-cell-refno[data-posting-date]',

    # Titel / Bereich / Ort lieber aus data-* (stabiler als sichtbare Zellen):
    'jobTitle': 'div.grp-jobfinder-cell-refno[data-job-title]',
    'department': 'div.grp-jobfinder-cell-refno[data-job-field]',
    'location': 'div.grp-jobfinder-cell-refno[data-job-location]',
    'company': 'div.grp-jobfinder-cell-refno[data-job-legal-entity]',

    # ID: Textinhalt der refno-Zelle
    'id': 'div.grp-jobfinder-cell-refno',

    # Link bleibt wie gehabt
    'link': 'a.grp-jobfinder__link-jobdescription[href]',

    # career_level kommt als data-job-type
    'career_level': 'div.grp-jobfinder-cell-refno[data-job-type]',

    # optional: sichtbares Published-Label (falls du es speichern willst)
    'published_label': 'div.grp-jobfinder-cell-job-title-group__published',
}

extraction_logic = {
    'id': lambda el: el.get_text(strip=True),

    'career_level': lambda el: el.get('data-job-type'),

    'created': lambda el: el.get('data-posting-date'),          # "20260216"
    'jobTitle': lambda el: el.get('data-job-title'),
    'department': lambda el: el.get('data-job-field'),
    'location': lambda el: el.get('data-job-location'),
    'legal_entity': lambda el: el.get('data-job-legal-entity'),

}

# -----------------------------------------------------------------------------
# HTTP tracking (aggregated per Scrape-Run)
# -----------------------------------------------------------------------------
HTTP_STATS = None


def _clean_text(text: Optional[str]) -> Optional[str]:
    """Local fallback cleaner (deterministic)."""
    if text is None:
        return None
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", t)
    t = t.strip()
    return t or None


def _fallback_fulltext_from_html(html_text: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html_text, features="lxml")
        text = soup.get_text(separator="\n")
        return _clean_text(text)
    except Exception:
        return None


def process_jobs(job_postings):
    jobs = []
    for job in job_postings:
        job_details = {
            "scraping_date": None,
            "last_updated": None,
            "status": None,
            "keywords": [],
        }

        for key, selector in JOB_DATA_KEYS.items():
            if not selector:
                continue

            element = job.select_one(selector)
            if not element:
                continue

            match = re.search(r"\[(.*?)\]", selector)
            if match:
                attribute_name = match.group(1)
                extractor = extraction_logic.get(key, lambda el: el.get(attribute_name, None))
                job_details[key] = extractor(element)
            else:
                extractor = extraction_logic.get(key, lambda el: el.get_text(strip=True))
                job_details[key] = extractor(element)

        jobs.append(job_details)
    return jobs


def fetch_job_list_page(url, headers, params, use_proxy=False, http_stats=None):
    response = fetch_url(
        url,
        headers=headers,
        params=params,
        json=None,
        data=None,
        use_proxy=use_proxy,
        max_retries=3,
        timeout=10,
        request_type=REQUEST_TYPE_LIST,
        http_stats=http_stats,
    )
    if not response:
        logging.error("Failed to fetch job list page after multiple attempts.")
        return None

    try:
        soup = BeautifulSoup(response.text, "html.parser")
        return soup.select(JOB_LIST_KEY)
    except Exception as e:
        logging.error(f"Failed to parse response HTML: {e}")
        return None


def fetch_all_jobs(http_stats=None):
    all_jobs = []

    if USE_PAGINATION:
        page = PAGE_START
        while True:
            logging.info(f"Fetching page {page} of job listings...")
            job_postings = fetch_job_list_page(
                DAILY_JOB_URL,
                HEADERS,
                PARAMS,
                use_proxy=USE_PROXY_DAILY_LIST,
                http_stats=http_stats,
            )
            if not job_postings:
                logging.info("No more job postings found, or failed to retrieve job postings.")
                break

            jobs = process_jobs(job_postings)
            all_jobs.extend(jobs)

            if len(job_postings) < MAX_JOBS_PER_PAGE:
                logging.info("Fewer jobs than MAX_JOBS_PER_PAGE found, ending pagination.")
                break

            page += 1
    else:
        logging.info("Fetching a single page of job listings...")
        job_postings = fetch_job_list_page(
            DAILY_JOB_URL,
            HEADERS,
            PARAMS,
            use_proxy=USE_PROXY_DAILY_LIST,
            http_stats=http_stats,
        )
        if job_postings:
            jobs = process_jobs(job_postings)
            all_jobs.extend(jobs)
        else:
            logging.info("No job postings found.")

    return all_jobs


def update_master_list_with_jobs(all_jobs, master_list):
    current_date = get_current_date()

    new_jobs_count = 0
    inactive_jobs_count = 0
    skipped_jobs_count = 0

    db_jobs_written = 0
    db_jobs_failed = 0
    db_jobs_touched = 0           # NEW: rows updated (last_seen_at/status/last_updated)
    db_jobs_touch_failed = 0      # NEW: touch call failed (0/1)

    seen_job_ids = []             # NEW: collect all IDs seen in today's crawl (new + existing)

    for job in all_jobs:
        job_id = job.get("id")
        if not job_id:
            skipped_jobs_count += 1
            continue

        # Normalize to str for DB array handling + consistent IDs
        job_id_str = str(job_id)
        seen_job_ids.append(job_id_str)

        existing_entry = next((entry for entry in master_list if str(entry.get("id")) == job_id_str), None)

        if existing_entry:
            # Master list: touch only
            update_job_status(existing_entry, current_date)
        else:
            # Master list: add new job
            job["scraping_date"] = current_date
            job["last_updated"] = current_date
            job["status"] = "active"
            master_list.append(job)
            new_jobs_count += 1

            # Phase 1 DB-Ingest: only new jobs (keeps payload writes low)
            try:
                db_job = _prepare_job_for_db(job)
                upsert_job_and_payload_from_master(FOLDER_NAME, db_job)
                db_jobs_written += 1
            except Exception as _db_e:
                db_jobs_failed += 1
                logging.exception(f"DB ingest failed for {FOLDER_NAME} job_id={job_id_str}: {_db_e}")

            # Detail fetch / extract / upload (only for new jobs, as before)
            job_link = job.get("link")
            if job_link and job_link.startswith("/"):
                job_link = "https://www.bmwgroup.jobs" + job_link

            if job_link:
                response = fetch_url(
                    job_link,
                    headers=HEADERS,
                    params=None,
                    json=None,
                    data=None,
                    use_proxy=USE_PROXY_DETAILED_POSTINGS,
                    max_retries=3,
                    timeout=10,
                    request_type=REQUEST_TYPE_SINGLE,
                    http_stats=HTTP_STATS,
                )
                if response:
                    detail_model = build_detailjob(
                        version="0.1",
                        job_id=job_id_str,
                        metadata={
                            "company_key": FOLDER_NAME,
                            "url": job_link,
                            "scraped_at": _now_utc_iso(),
                            "locale": "en_US",
                        },
                        job_meta={
                            "title": job.get("jobTitle"),
                            "location_text": job.get("location"),
                            "employment_type": job.get("employment_type"),
                            "contract_type": job.get("contract"),
                            "career_level": job.get("career_level"),
                        },
                    )

                    sections = extract_detail_sections(raw=response.text, input_type="html")

                    fulltext = sections.get("fulltext")
                    if not fulltext:
                        fulltext = _fallback_fulltext_from_html(response.text)

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

    # NEW: DB touch for all seen jobs (updates last_seen_at + status/last_updated in one batch)
    # Assumes you added touch_jobs_last_seen in db_runs.py already.
    try:
        # Import locally to avoid import errors in environments where db_runs is stubbed/mocked.
        db_jobs_touched = touch_jobs_last_seen(FOLDER_NAME, seen_job_ids, current_date)
    except Exception as _e:
        db_jobs_touch_failed = 1
        logging.exception(f"DB touch last_seen_at failed for {FOLDER_NAME}: {_e}")

    # Mark jobs as inactive in master list (as before)
    inactive_ids = []  # NEW: collect ids for optional DB inactive update
    for entry in master_list:
        if entry.get("last_updated") != current_date:
            entry["status"] = "inactive"
            inactive_jobs_count += 1
            if entry.get("id"):
                inactive_ids.append(str(entry["id"]))

    try:
        _ = mark_jobs_inactive(FOLDER_NAME, inactive_ids)
    except Exception as _e:
        logging.exception(f"DB mark inactive failed for {FOLDER_NAME}: {_e}")

    # Keep return signature stable (original 5 values), but you can extend if you want.
    # If you want to include db_jobs_touched/db_jobs_touch_failed, add them to the return.
    return new_jobs_count, inactive_jobs_count, skipped_jobs_count, db_jobs_written, db_jobs_failed



def main():
    logging.info(f"Starting job scraping process for {FOLDER_NAME}")

    run_id = start_run(
        company_key=COMPANY_KEY,
        meta={
            "bucket": BUCKET_NAME,
            "folder": FOLDER_NAME,
            "company_key": COMPANY_KEY,
            "use_proxy_daily_list": USE_PROXY_DAILY_LIST,
            "use_proxy_detailed_postings": USE_PROXY_DETAILED_POSTINGS,
            "use_pagination": USE_PAGINATION,
            "pagination_mode": "enabled" if USE_PAGINATION else "single_page",
            "note": "master_json_still_enabled",
        },
    )

    global HTTP_STATS
    HTTP_STATS = {}

    start_time = time.time()
    cpu_usage = None

    try:
        update_stage(run_id, "started")

        if PSUTIL_AVAILABLE:
            try:
                cpu_usage = psutil.cpu_percent(interval=1)
            except Exception:
                cpu_usage = None

        update_stage(run_id, "fetch_list")
        all_jobs = fetch_all_jobs(http_stats=HTTP_STATS)

        update_stage(run_id, "load_master")
        master_list = load_master_list(BUCKET_NAME, FOLDER_NAME)

        update_stage(run_id, "update_master")
        new_jobs_count, inactive_jobs_count, skipped_jobs_count, db_jobs_written, db_jobs_failed = update_master_list_with_jobs(
            all_jobs,
            master_list,
        )

        update_stage(run_id, "save_master")
        save_master_list(BUCKET_NAME, FOLDER_NAME, master_list)

        execution_time_sec = time.time() - start_time

        finish_run(
            run_id=run_id,
            status="success",
            jobs_fetched=len(all_jobs) if all_jobs else 0,
            jobs_processed=len(all_jobs) if all_jobs else 0,
            new_jobs=new_jobs_count,
            inactive_jobs=inactive_jobs_count,
            skipped_jobs=skipped_jobs_count,
            execution_time_sec=execution_time_sec,
            cpu_usage_pct=cpu_usage,
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

        logging.info(f"Scraping completed successfully. {len(all_jobs)} jobs processed.")
        logging.info(f"{new_jobs_count} new jobs added.")
        logging.info(f"{inactive_jobs_count} jobs marked as inactive.")
        logging.info(f"Total jobs skipped due to missing IDs: {skipped_jobs_count}")
        logging.info(f"DB ingest: {db_jobs_written} jobs written, {db_jobs_failed} failed")

    except Exception as e:
        execution_time_sec = time.time() - start_time

        update_stage(run_id, "failed", meta={"exception_type": type(e).__name__})

        finish_run(
            run_id=run_id,
            status="failed",
            error_message=str(e),
            jobs_fetched=len(all_jobs) if "all_jobs" in locals() and all_jobs else None,
            jobs_processed=len(all_jobs) if "all_jobs" in locals() and all_jobs else None,
            new_jobs=new_jobs_count if "new_jobs_count" in locals() else None,
            inactive_jobs=inactive_jobs_count if "inactive_jobs_count" in locals() else None,
            skipped_jobs=skipped_jobs_count if "skipped_jobs_count" in locals() else None,
            execution_time_sec=execution_time_sec,
            cpu_usage_pct=cpu_usage,
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

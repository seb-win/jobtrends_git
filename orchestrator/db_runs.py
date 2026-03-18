# db_runs.py
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import json

import psycopg
from psycopg.rows import dict_row


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def get_conn():
    """
    PostgreSQL-Connection (Supabase / Cloud SQL kompatibel)

    Erwartete ENV-Vars:
    - DB_HOST
    - DB_PORT (optional, default 5432)
    - DB_NAME
    - DB_USER
    - DB_PASSWORD
    - DB_SSLMODE (optional, default 'require')
    """
    return psycopg.connect(
        host="aws-1-eu-west-1.pooler.supabase.com",
        port=5432,
        dbname="postgres",
        user="postgres.yvrdgwaokadtapcvecqa",
        password="Mon_m0d3na$ever",
        sslmode="require",
        row_factory=dict_row,
    )


def start_run(company_key: str, meta: Optional[Dict[str, Any]] = None) -> str:
    """
    Startet einen Scrape-Run und legt ihn als 'running' an.
    """
    run_id = str(uuid.uuid4())
    started_at = _now_utc()

    sql = """
    insert into scrape_runs (
        run_id,
        company_key,
        started_at,
        status,
        meta
    )
    values (%s, %s, %s, %s, %s::jsonb);
    """

    meta_json = json.dumps(meta or {})

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    run_id,
                    company_key,
                    started_at,
                    "running",
                    meta_json,
                ),
            )

    return run_id


def finish_run(
    run_id: str,
    status: str,
    execution_time_sec: Optional[float] = None,
    cpu_usage_pct: Optional[float] = None,
    jobs_fetched: Optional[int] = None,
    jobs_processed: Optional[int] = None,
    new_jobs: Optional[int] = None,
    inactive_jobs: Optional[int] = None,
    skipped_jobs: Optional[int] = None,
    error_message: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Schließt einen Scrape-Run (success / failed) und schreibt Metriken.
    """
    finished_at = _now_utc()

    sql = """
    update scrape_runs
       set finished_at = %s,
           status = %s,
           execution_time_sec = %s,
           cpu_usage_pct = %s,
           jobs_fetched = %s,
           jobs_processed = %s,
           new_jobs = %s,
           inactive_jobs = %s,
           skipped_jobs = %s,
           error_message = %s,
           meta = coalesce(meta, '{}'::jsonb) || coalesce(%s::jsonb, '{}'::jsonb)
     where run_id = %s;
    """

    meta_json = json.dumps(meta or {})

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    finished_at,
                    status,
                    execution_time_sec,
                    cpu_usage_pct,
                    jobs_fetched,
                    jobs_processed,
                    new_jobs,
                    inactive_jobs,
                    skipped_jobs,
                    error_message,
                    meta_json,
                    run_id,
                ),
            )


def update_stage(run_id: str, stage: str, meta: Optional[Dict[str, Any]] = None) -> None:
    import json
    sql = """
    update scrape_runs
       set stage = %s,
           meta = coalesce(meta, '{}'::jsonb) || coalesce(%s::jsonb, '{}'::jsonb)
     where run_id = %s;
    """
    meta_json = json.dumps(meta or {})
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (stage, meta_json, run_id))


def upsert_job_and_payload_from_master(source: str, job: Dict[str, Any]) -> str:
    """
    Upsert in 'jobs' (kanonisches Schema) + upsert in 'job_payloads' in einer Transaktion.
    Quelle für die Daten ist *genau* das Job-Dict, das auch in die Masterliste geschrieben wird.

    Hinweis: Diese Funktion ist so gebaut, dass fehlende Felder zu NULL (None) werden.
    Sie ist robust genug für Phase 1 (nur neue Jobs werden aufgerufen), funktioniert aber auch
    wenn sie später versehentlich doppelt gerufen wird (ON CONFLICT schützt).

    Returns:
        job_pk (UUID as str)
    """
    if not source:
        raise ValueError("source must not be empty")
    job_id = job.get("id")
    if not job_id:
        raise ValueError("job['id'] (job_id) is required")

    # Map Master-Job-Dict -> DB-Spalten (fehlende Werte => None)
    title = job.get("jobTitle")
    location_text = job.get("location")
    country = job.get("country")
    company = job.get("company")
    department = job.get("department")
    team = job.get("team")
    career_level = job.get("career_level")

    employment_type = job.get("employment_type")
    contract = job.get("contract")

    url = job.get("link")
    created_raw = job.get("created")

    scraping_date = job.get("scraping_date")
    last_updated = job.get("last_updated")
    status = job.get("status")

    keywords = job.get("keywords")
    if keywords is None:
        keywords = []
    if not isinstance(keywords, (list, tuple)):
        keywords = [keywords]

    # Optional tracking columns (Phase 1: wir setzen sie auf current_date, wenn verfügbar)
    first_seen_at = job.get("first_seen_at") or scraping_date
    last_seen_at = job.get("last_seen_at") or last_updated

    # Für Phase 1 schreiben wir als "payload" einfach die bereits gemappte Job-Struct.
    payload_obj = job

    sql_jobs = """
    insert into jobs (
        source,
        job_id,
        scraping_date,
        last_updated,
        status,
        created_raw,
        title,
        location_text,
        country,
        company,
        department,
        team,
        career_level,
        employment_type,
        contract,
        url,
        keywords,
        first_seen_at,
        last_seen_at
    )
    values (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s::jsonb, %s, %s
    )
    on conflict (source, job_id) do update
       set scraping_date   = coalesce(jobs.scraping_date, excluded.scraping_date),
           last_updated    = excluded.last_updated,
           status          = excluded.status,
           created_raw     = coalesce(excluded.created_raw, jobs.created_raw),
           title           = coalesce(excluded.title, jobs.title),
           location_text   = coalesce(excluded.location_text, jobs.location_text),
           country         = coalesce(excluded.country, jobs.country),
           company         = coalesce(excluded.company, jobs.company),
           department      = coalesce(excluded.department, jobs.department),
           team            = coalesce(excluded.team, jobs.team),
           career_level    = coalesce(excluded.career_level, jobs.career_level),
           employment_type = coalesce(excluded.employment_type, jobs.employment_type),
           contract        = coalesce(excluded.contract, jobs.contract),
           url             = coalesce(excluded.url, jobs.url),
           keywords        = coalesce(excluded.keywords, jobs.keywords),
           first_seen_at   = coalesce(jobs.first_seen_at, excluded.first_seen_at),
           last_seen_at    = coalesce(excluded.last_seen_at, jobs.last_seen_at)
    returning job_pk;
    """

    sql_payloads = """
    insert into job_payloads (
        job_pk,
        source,
        job_id,
        payload,
        ingested_at
    )
    values (
        %s, %s, %s, %s::jsonb, %s
    )
    on conflict (job_pk) do update
       set source = excluded.source,
           job_id = excluded.job_id,
           payload = excluded.payload,
           ingested_at = excluded.ingested_at;
    """

    ingested_at = _now_utc()
    keywords_json = json.dumps(list(keywords))
    payload_json = json.dumps(payload_obj)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql_jobs,
                (
                    source,
                    str(job_id),
                    scraping_date,
                    last_updated,
                    status,
                    created_raw,
                    title,
                    location_text,
                    country,
                    company,
                    department,
                    team,
                    career_level,
                    employment_type,
                    contract,
                    url,
                    keywords_json,
                    first_seen_at,
                    last_seen_at,
                ),
            )
            row = cur.fetchone()
            if not row or "job_pk" not in row:
                raise RuntimeError("jobs upsert did not return job_pk")
            job_pk = str(row["job_pk"])

            cur.execute(
                sql_payloads,
                (
                    job_pk,
                    source,
                    str(job_id),
                    payload_json,
                    ingested_at,
                ),
            )

    return job_pk

def touch_jobs_last_seen(source: str, job_ids: list[str], seen_at) -> int:
    """
    Aktualisiert last_seen_at (und sinnvollerweise status/last_updated) für alle heute gesehenen Jobs.
    Returns: Anzahl geupdateter Zeilen (rowcount).
    """
    if not job_ids:
        return 0

    sql = """
    update jobs
       set last_seen_at = %s,
           last_updated = %s,
           status = 'active'
     where source = %s
       and job_id = any(%s);
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (seen_at, seen_at, source, job_ids))
            return cur.rowcount


def mark_jobs_inactive(source: str, job_ids: list[str]) -> int:
    """Optional: setzt status='inactive' für Jobs, die heute nicht gesehen wurden."""
    if not job_ids:
        return 0

    sql = """
    update jobs
       set status = 'inactive'
     where source = %s
       and job_id = any(%s);
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source, job_ids))
            return cur.rowcount
"""DBNormalize

Generische Normalisierungs-Utilities für JobTrends Scraper, die sicherstellen,
dass Masterlisten-Job-Dicts stabil in eine DB geschrieben werden können.

- Keine company-spezifische Logik
- Keine Abhängigkeit zu euren Fetchern/Parsern
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional


__all__ = [
    "_now_utc_iso",
    "_safe_json_scalar",
    "_prepare_job_for_db",
]


def _now_utc_iso() -> str:
    """ISO timestamp string (UTC), DB-castbar zu timestamptz/timestamp."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_json_scalar(value: Any) -> Optional[str]:
    """
    Gibt einen JSON-String zurück, der als jsonb gecastet werden kann.

    Beispiele:
      12        -> "12" (json number 12)
      "foo"     -> "\"foo\"" (json string)
      None      -> None
      {"a": 1}  -> "{\"a\": 1}" (json object)
    """
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        # Fallback: repr/str als JSON-String speichern
        return json.dumps(str(value), ensure_ascii=False)


def _prepare_job_for_db(master_job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase-1 Prinzip: DB bekommt das Masterlisten-Job-Dict.
    Aber: wir normalisieren Typen, damit DB Inserts stabil sind.

    Erwartet:
      - job['id'] (Pflicht)
    Normalisiert u.a.:
      - created -> JSON-Scalar (für jsonb)
      - keywords -> Liste
      - scraping_date/last_updated/first_seen_at/last_seen_at -> ISO UTC Strings
    """
    job = dict(master_job)

    if not job.get("id"):
        raise ValueError("job['id'] is required for DB ingest")

    # jsonb-fähig machen (auch wenn created bereits Text ist)
    job["created"] = _safe_json_scalar(job.get("created"))

    # keywords immer als list
    kw = job.get("keywords")
    if kw is None:
        job["keywords"] = []
    elif isinstance(kw, (list, tuple)):
        job["keywords"] = list(kw)
    else:
        job["keywords"] = [kw]

    now_iso = _now_utc_iso()

    job["scraping_date"] = job.get("scraping_date") or now_iso
    job["last_updated"] = job.get("last_updated") or now_iso

    # Optional, aber in euren DB-Tabellen sehr hilfreich
    job["first_seen_at"] = job.get("first_seen_at") or job["scraping_date"]
    job["last_seen_at"] = job.get("last_seen_at") or job["last_updated"]

    return job

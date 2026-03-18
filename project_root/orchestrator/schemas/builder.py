from __future__ import annotations

from typing import Optional, Dict, Any, Literal, Union

from .v0_1 import DetailJobV01

SupportedVersion = Literal["0.1"]
DetailJobModel = Union[DetailJobV01]


def build_detailjob(
    *,
    version: SupportedVersion,
    job_id: Optional[str],
    metadata: Dict[str, Any],
    job_meta: Optional[Dict[str, Any]] = None,
) -> DetailJobModel:
    """
    Versioned factory that returns a valid DetailJob model for the requested schema version.

    metadata expected keys: company_key, url, scraped_at, locale
    job_meta is typically taken from the daily list/master record (safe, non-guessing source).
    """
    if version == "0.1":
        obj = DetailJobV01(
            job_id=job_id,
            company_key=metadata.get("company_key"),
            url=metadata.get("url"),
            scraped_at=metadata.get("scraped_at"),
            locale=metadata.get("locale"),
        )

        if job_meta:
            # Only set if present; empty strings are normalized to None by validators.
            obj.meta.title = job_meta.get("title") or job_meta.get("meta", {}).get("title")
            obj.meta.location_text = job_meta.get("location_text") or job_meta.get("meta", {}).get("location_text")
            obj.meta.employment_type = job_meta.get("employment_type") or job_meta.get("meta", {}).get("employment_type")
            obj.meta.contract_type = job_meta.get("contract_type") or job_meta.get("meta", {}).get("contract_type")
            obj.meta.career_level = job_meta.get("career_level") or job_meta.get("meta", {}).get("career_level")
            # posting_date/salary_text intentionally left unless explicitly available in your source.

        return obj

    raise ValueError(f"Unsupported schema version: {version}")

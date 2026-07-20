from __future__ import annotations

import html
import re
from html.parser import HTMLParser
from typing import Any, Iterable, Mapping, Optional

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover - depends on runtime environment
    BeautifulSoup = None

SCHEMA_VERSION = "job_detail_v1"

SECTION_NAMES = {
    "description",
    "about",
    "responsibilities",
    "minimum_qualifications",
    "preferred_qualifications",
    "basic_qualifications",
    "qualifications",
    "requirements",
    "benefits",
    "compensation",
    "equal_opportunity",
    "additional_information",
    "other",
}

_HEADING_NAME_RULES = (
    ("minimum_qualifications", ("minimum qualification", "minimum requirement")),
    ("preferred_qualifications", ("preferred qualification", "preferred requirement", "nice to have")),
    ("basic_qualifications", ("basic qualification",)),
    ("responsibilities", ("responsibilit", "what you'll do", "what you will do", "your role")),
    ("qualifications", ("who you are", "about you", "what we're looking for", "what we are looking for")),
    ("requirements", ("requirement", "required skill", "required qualification")),
    ("benefits", ("benefit", "perk", "pay & benefit", "pay and benefit")),
    ("compensation", ("salary", "compensation", "pay transparency", "base pay")),
    ("equal_opportunity", ("equal opportunity", "eeo", "diversity statement")),
    ("additional_information", ("additional information",)),
    ("description", ("about the job", "job description", "description")),
    ("about", ("about us", "about the team")),
)


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)

    def get_text(self, separator: str) -> str:
        return separator.join(part.strip() for part in self.parts if part.strip())


def _strip_html_tags(value: str, separator: str) -> str:
    if BeautifulSoup is not None:
        return BeautifulSoup(value, "html.parser").get_text(separator=separator, strip=True)

    parser = _HTMLTextExtractor()
    parser.feed(value)
    parser.close()
    return parser.get_text(separator)


def clean_text(value: Any, *, separator: str = " ") -> Optional[str]:
    """Return cleaned text with HTML removed, entities unescaped, and whitespace collapsed."""
    if value is None:
        return None

    if isinstance(value, (list, tuple, set)):
        parts = [clean_text(item, separator=separator) for item in value]
        text = separator.join(part for part in parts if part)
    else:
        text = str(value)

    if not text.strip():
        return None

    text = html.unescape(text)
    if "<" in text and ">" in text:
        text = _strip_html_tags(text, separator)
        text = html.unescape(text)

    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def clean_list(values: Any) -> list[str]:
    """Clean real source list values; non-list scalar values are treated as one item."""
    if values is None:
        return []

    if isinstance(values, str):
        iterable: Iterable[Any] = [values]
    elif isinstance(values, Iterable):
        iterable = values
    else:
        iterable = [values]

    cleaned: list[str] = []
    seen: set[str] = set()
    for value in iterable:
        text = clean_text(value)
        if text and text not in seen:
            cleaned.append(text)
            seen.add(text)
    return cleaned


def map_section_name(heading: Any, default: str = "other") -> str:
    """Map a source heading to a canonical section name."""
    cleaned = clean_text(heading)
    if not cleaned:
        return default if default in SECTION_NAMES else "other"

    normalized = cleaned.casefold()
    for name, needles in _HEADING_NAME_RULES:
        if any(needle in normalized for needle in needles):
            return name

    return default if default in SECTION_NAMES else "other"


def make_section(
    name: Optional[str] = None,
    *,
    heading: Any = None,
    text: Any = None,
    items: Any = None,
) -> Optional[dict[str, Any]]:
    """Build a fixed-shape content.sections entry, or None if it has no content."""
    section_name = name if name in SECTION_NAMES else map_section_name(heading, default=name or "other")
    section_heading = clean_text(heading)
    section_text = clean_text(text)
    section_items = clean_list(items)

    if not section_text and not section_items:
        return None

    return {
        "name": section_name,
        "heading": section_heading,
        "text": section_text,
        "items": section_items,
    }


def build_full_text(*parts: Any) -> Optional[str]:
    """Join cleaned relevant job-posting text blocks while avoiding adjacent duplicates."""
    cleaned_parts: list[str] = []
    seen: set[str] = set()
    for part in parts:
        text = clean_text(part, separator="\n")
        if text and text not in seen:
            cleaned_parts.append(text)
            seen.add(text)

    return "\n\n".join(cleaned_parts) if cleaned_parts else None


def _clean_scalar(value: Any) -> Optional[str]:
    return clean_text(value)


def _clean_locations(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if item not in (None, "", [])]
    cleaned = clean_text(value)
    return [cleaned] if cleaned else []


def make_job_detail(
    *,
    job_id: Any = None,
    title: Any = None,
    company: Any = None,
    metadata: Optional[Mapping[str, Any]] = None,
    full_text: Any = None,
    sections: Optional[Iterable[Optional[Mapping[str, Any]]]] = None,
    compensation: Optional[Mapping[str, Any]] = None,
    full_text_truncated: bool = False,
) -> dict[str, Any]:
    """Build a job_detail_v1 dict with stable keys and cleaned scalar text."""
    meta = dict(metadata or {})
    comp = dict(compensation or {})

    detail = {
        "schema_version": SCHEMA_VERSION,
        "job": {
            "id": _clean_scalar(job_id),
            "title": _clean_scalar(title),
            "company": _clean_scalar(company),
        },
        "metadata": {
            "department": _clean_scalar(meta.get("department")),
            "team": _clean_scalar(meta.get("team")),
            "business_category": _clean_scalar(meta.get("business_category")),
            "job_category": _clean_scalar(meta.get("job_category")),
            "profession": _clean_scalar(meta.get("profession")),
            "role_type": _clean_scalar(meta.get("role_type")),
            "employment_type": _clean_scalar(meta.get("employment_type")),
            "job_type": _clean_scalar(meta.get("job_type")),
            "career_level": _clean_scalar(meta.get("career_level")),
            "experience_level": _clean_scalar(meta.get("experience_level")),
            "required_travel": _clean_scalar(meta.get("required_travel")),
            "locations": _clean_locations(meta.get("locations")),
            "created_at": _clean_scalar(meta.get("created_at")),
            "posted_at": _clean_scalar(meta.get("posted_at")),
            "updated_at": _clean_scalar(meta.get("updated_at")),
        },
        "content": {
            "full_text": clean_text(full_text, separator="\n"),
            "full_text_truncated": bool(full_text_truncated),
            "sections": [],
        },
        "compensation": {
            "raw": _clean_scalar(comp.get("raw")),
            "currency": _clean_scalar(comp.get("currency")),
            "min": comp.get("min"),
            "max": comp.get("max"),
            "period": _clean_scalar(comp.get("period")),
            "text": _clean_scalar(comp.get("text")),
            "locale": _clean_scalar(comp.get("locale")),
            "location_id": _clean_scalar(comp.get("location_id")),
        },
    }

    if sections:
        for section in sections:
            if not section:
                continue
            fixed = make_section(
                section.get("name"),
                heading=section.get("heading"),
                text=section.get("text"),
                items=section.get("items"),
            )
            if fixed:
                detail["content"]["sections"].append(fixed)

    return detail

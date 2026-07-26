from __future__ import annotations

import html
import re
from html.parser import HTMLParser
from typing import Any, Iterable, Optional

from orchestrator.schemas.job_detail_v1 import clean_text, make_section, map_section_name


_BULLET_RE = re.compile(r"(?:^|\n|<br\s*/?>|\s)([-*•])\s+", re.IGNORECASE)
_NUMBERED_RE = re.compile(r"(?:^|\n|<br\s*/?>|\s)(\d+[.)])\s+")


def split_structured_items(value: Any, *, min_items: int = 2) -> list[str]:
    """Extract only clearly source-structured items from arrays, HTML lists, or bullet text."""
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        return _dedupe(clean_text(item) for item in value)

    text = str(value)
    html_items = extract_html_list_items(text)
    if len(html_items) >= min_items:
        return html_items

    br_items = _split_br_or_newline_items(text)
    if len(br_items) >= min_items:
        return br_items

    bullet_items = _split_inline_bullets(text)
    if len(bullet_items) >= min_items:
        return bullet_items

    return []


def make_extracted_section(
    name: Optional[str] = None,
    *,
    heading: Any = None,
    text: Any = None,
    items: Any = None,
    keep_text_with_items: bool = False,
):
    """Create a section while preserving real/clearly formatted source list items."""
    extracted_items = split_structured_items(items)
    if not extracted_items:
        extracted_items = split_structured_items(text)

    section_text = text if keep_text_with_items or not extracted_items else None
    return make_section(name, heading=heading, text=section_text, items=extracted_items)


def extract_html_list_items(value: Any) -> list[str]:
    """Return cleaned text from HTML <li> elements."""
    if not value:
        return []
    parser = _ListItemParser()
    parser.feed(html.unescape(str(value)))
    parser.close()
    return _dedupe(parser.items)


def looks_like_compliance_or_benefits_text(value: Any) -> bool:
    """Detect boilerplate/pay/benefits text that should not become requirements."""
    text = (clean_text(value) or "").casefold()
    if not text:
        return False

    signals = (
        "range and benefit information",
        "hiring range",
        "medical, dental, and vision",
        "401(k)",
        "flexible spending accounts",
        "paid time off",
        "oracle maintains broad salary ranges",
        "eligible for bonus, equity",
        "immunization/occupational health mandates",
        "drug testing requirements",
        "oracle uses artificial intelligence in our recruiting process",
    )
    return sum(1 for signal in signals if signal in text) >= 2


def map_extracted_section_name(heading: Any, default: str = "other") -> str:
    """Map extracted source headings, including skill headings, to canonical names."""
    cleaned = clean_text(heading)
    if not cleaned:
        return default

    normalized = cleaned.casefold().rstrip(":")
    if "preferred skill" in normalized or "nice to have skill" in normalized:
        return "preferred_qualifications"
    if "required skill" in normalized:
        return "requirements"
    if (
        normalized in {"skills", "key skills", "core skills", "technical skills"}
        or "technical skill" in normalized
    ):
        return "qualifications"
    if normalized in {"highly desirable", "nice to have"}:
        return "preferred_qualifications"

    mapped = map_section_name(cleaned, default=default)
    return mapped or default


def extract_heading_list_sections(html_text: Any, *, heading_map: Optional[dict[str, str]] = None) -> list[dict[str, Any]]:
    """Extract sections from heading blocks followed by HTML lists."""
    if not html_text:
        return []

    parser = _BlockParser()
    parser.feed(html.unescape(str(html_text)))
    parser.close()

    sections = []
    current = None
    for block in parser.blocks:
        block_type = block.get("type")
        if block_type in {"h1", "h2", "h3", "h4", "strong", "b"}:
            heading = block.get("text")
            if not heading:
                continue
            if current:
                section = _finish_block_section(current)
                if section:
                    sections.append(section)
            current = {"heading": heading, "text_parts": [], "items": [], "heading_map": heading_map}
            continue

        if not current:
            continue

        if block_type == "ul":
            current["items"].extend(block.get("items") or [])
        elif block.get("text"):
            current["text_parts"].append(block["text"])

    if current:
        section = _finish_block_section(current)
        if section:
            sections.append(section)

    return sections


def _finish_block_section(current: dict[str, Any]):
    heading = current.get("heading")
    name = _mapped_name(heading, current.get("heading_map"))
    return make_section(
        name,
        heading=heading,
        text="\n".join(current.get("text_parts") or []) or None,
        items=current.get("items") or [],
    )


def _mapped_name(heading: Any, heading_map: Optional[dict[str, str]]) -> str:
    cleaned = clean_text(heading) or ""
    normalized = cleaned.casefold()
    for needle, name in (heading_map or {}).items():
        if needle.casefold() in normalized:
            return name
    return map_extracted_section_name(cleaned)


def _split_br_or_newline_items(value: str) -> list[str]:
    text = html.unescape(value)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>\s*<p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    parts = [clean_text(part) for part in re.split(r"\n+", text)]
    parts = [part for part in parts if part]
    if len(parts) < 2:
        return []

    bulletish = [
        re.sub(r"^([-*•]|\d+[.)])\s+", "", part).strip()
        for part in parts
        if re.match(r"^([-*•]|\d+[.)])\s+\S", part)
    ]
    if len(bulletish) >= 2:
        return _dedupe(bulletish)
    return []


def _split_inline_bullets(value: str) -> list[str]:
    text = html.unescape(value)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = clean_text(text) or ""
    if not text:
        return []

    marker_re = _BULLET_RE if len(_BULLET_RE.findall(text)) >= 2 else _NUMBERED_RE
    matches = list(marker_re.finditer(text))
    if len(matches) < 2:
        return []

    items = []
    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        item = clean_text(text[start:end])
        if item:
            items.append(item)
    return _dedupe(items)


def _dedupe(values: Iterable[Optional[str]]) -> list[str]:
    cleaned = []
    seen = set()
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            cleaned.append(text)
            seen.add(text)
    return cleaned


class _ListItemParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.items: list[str] = []
        self._in_li = False
        self._parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "li":
            self._in_li = True
            self._parts = []
        elif tag == "br" and self._in_li:
            self._parts.append(" ")

    def handle_endtag(self, tag):
        if tag == "li" and self._in_li:
            item = clean_text(" ".join(self._parts))
            if item:
                self.items.append(item)
            self._in_li = False
            self._parts = []

    def handle_data(self, data):
        if self._in_li and data:
            self._parts.append(data)


class _BlockParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[dict[str, Any]] = []
        self._current_tag: Optional[str] = None
        self._current_parts: list[str] = []
        self._list_items: Optional[list[str]] = None
        self._li_parts: Optional[list[str]] = None

    def handle_starttag(self, tag, attrs):
        if tag in {"p", "h1", "h2", "h3", "h4", "strong", "b"} and self._current_tag is None:
            self._current_tag = tag
            self._current_parts = []
        elif tag == "ul" and self._list_items is None:
            self._list_items = []
        elif tag == "li" and self._list_items is not None:
            self._li_parts = []
        elif tag == "br":
            if self._li_parts is not None:
                self._li_parts.append(" ")
            elif self._current_tag is not None:
                self._current_parts.append(" ")

    def handle_endtag(self, tag):
        if tag == self._current_tag:
            text = clean_text(" ".join(self._current_parts))
            if text:
                self.blocks.append({"type": tag, "text": text})
            self._current_tag = None
            self._current_parts = []
        elif tag == "li" and self._li_parts is not None:
            item = clean_text(" ".join(self._li_parts))
            if item:
                self._list_items.append(item)
            self._li_parts = None
        elif tag == "ul" and self._list_items is not None:
            self.blocks.append({"type": "ul", "items": self._list_items})
            self._list_items = None

    def handle_data(self, data):
        if self._li_parts is not None:
            self._li_parts.append(data)
        elif self._current_tag is not None:
            self._current_parts.append(data)

from __future__ import annotations

import re
import json
from typing import Any, Dict, List, Optional, Union

from bs4 import BeautifulSoup


# ============================================================
# 1) PASTE-IN BLOCK (wird später von deinem Prompt erzeugt)
#    -> Du ersetzt NUR dieses Dict (DETAIL_MAPPING_V01).
# ============================================================

DETAIL_MAPPING_V01 = {
    "schema_version": "0.1",
    "supported_input_types": ["json"],

    "json": {
        "paths": {
            "fulltext": ["d"],
            "overview": ["d", "CompanyDesc"],
            "process": ["d", "TaskDesc"],

            "responsibilities_items": ["d", "DepartmentDesc"],
            "requirements_items": ["d", "ProjectDesc"],
            "additional_items": None,
            "benefits_items": None,
        },
        "html_fields": ["fulltext", "overview", "process"],
        "html_item_fields": [
            "responsibilities_items",
            "requirements_items",
            "additional_items",
            "benefits_items",
        ],
    },

    "dom": {
        "root_selector": None,
        "fields": {
            "fulltext": {"selector": None, "mode": "text"},
            "overview": {"selector": None, "mode": "text"},
            "process": {"selector": None, "mode": "text"},
        },
        "lists": {
            "responsibilities_items": {"selector": None, "item_selector": "li", "mode": "text"},
            "requirements_items": {"selector": None, "item_selector": "li", "mode": "text"},
            "additional_items": {"selector": None, "item_selector": "li", "mode": "text"},
            "benefits_items": {"selector": None, "item_selector": "li", "mode": "text"},
        },
        "remove_selectors": [],
    },
}


# ============================================================
# 2) Engine: Deterministisch (nicht anfassen, nur Mapping anpassen)
# ============================================================

def _clean_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", t)
    t = t.strip()
    return t or None


def _html_to_text(html: str) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator="\n")
    return _clean_text(text)


def _get_json_path(obj: Any, path: Optional[List[Union[str, int]]]) -> Any:
    if path is None:
        return None
    cur = obj
    for key in path:
        if cur is None:
            return None
        try:
            if isinstance(key, int) and isinstance(cur, list):
                cur = cur[key]
            elif isinstance(key, str) and isinstance(cur, dict):
                cur = cur.get(key)
            else:
                return None
        except Exception:
            return None
    return cur


def _ensure_list_of_str(val: Any, treat_as_html: bool = False) -> List[str]:
    if val is None:
        return []
    out: List[str] = []
    if isinstance(val, list):
        for x in val:
            if x is None:
                continue
            if isinstance(x, str):
                s = _html_to_text(x) if treat_as_html else _clean_text(x)
                if s:
                    out.append(s)
            else:
                # keine Fantasie: nicht-string ignorieren
                continue
    elif isinstance(val, str):
        s = _html_to_text(val) if treat_as_html else _clean_text(val)
        if s:
            out.append(s)
    return out


def _dom_select_root(soup: BeautifulSoup, root_selector: Optional[str]) -> BeautifulSoup:
    if not root_selector:
        return soup
    root = soup.select_one(root_selector)
    return root if root else soup


def _dom_remove(soup: BeautifulSoup, selectors: List[str]) -> None:
    for sel in selectors or []:
        for node in soup.select(sel):
            node.decompose()


def _dom_extract_field(root: BeautifulSoup, selector: Optional[str], mode: str) -> Optional[str]:
    if not selector:
        return None
    node = root.select_one(selector)
    if not node:
        return None
    if mode == "html":
        return _html_to_text(str(node))
    # default: text
    return _clean_text(node.get_text(separator="\n"))


def _dom_extract_list(root: BeautifulSoup, selector: Optional[str], item_selector: str, mode: str) -> List[str]:
    if not selector:
        return []
    container = root.select_one(selector)
    if not container:
        return []
    items = container.select(item_selector) if item_selector else [container]
    out: List[str] = []
    for it in items:
        if mode == "html":
            s = _html_to_text(str(it))
        else:
            s = _clean_text(it.get_text(separator="\n"))
        if s:
            out.append(s)
    return out


def extract_detail_sections(
    *,
    raw: Union[str, Dict[str, Any]],
    input_type: str,
) -> Dict[str, Any]:
    """
    Gibt zurück:
    {
      "overview": str|None,
      "responsibilities": List[str],
      "requirements": List[str],
      "additional": List[str],
      "benefits": List[str],
      "process": str|None
    }
    """
    sections = {
        "overview": None,
        "responsibilities": [],
        "requirements": [],
        "additional": [],
        "benefits": [],
        "process": None,
        "fulltext": None,
    }

    mapping = DETAIL_MAPPING_V01

    if input_type not in mapping.get("supported_input_types", []):
        return sections

    # -------- JSON --------
    if input_type == "json":
        if isinstance(raw, str):
            try:
                raw_obj = json.loads(raw)
            except Exception:
                # not valid JSON -> return empty sections
                return sections
        elif isinstance(raw, dict):
            raw_obj = raw
        else:
            return sections

        jcfg = mapping.get("json", {})
        paths = jcfg.get("paths", {})
        html_fields = set(jcfg.get("html_fields", []))
        html_item_fields = set(jcfg.get("html_item_fields", []))

        for field_name in ["fulltext", "overview", "process"]:
            p = paths.get(field_name)
            val = _get_json_path(raw_obj, p)
            if isinstance(val, str):
                sections[field_name] = _html_to_text(val) if field_name in html_fields else _clean_text(val)

        list_field_map = {
            "responsibilities_items": "responsibilities",
            "requirements_items": "requirements",
            "additional_items": "additional",
            "benefits_items": "benefits",
        }
        for src_field, dst_field in list_field_map.items():
            p = paths.get(src_field)
            val = _get_json_path(raw_obj, p)
            items = _ensure_list_of_str(val, treat_as_html=(src_field in html_item_fields))
            sections[dst_field] = items

        return sections

    # -------- HTML/XML --------
    if input_type in ("html", "xml") and isinstance(raw, str):
        # xml: wenn Audi XML liefert, parse es als xml, sonst html
        soup = BeautifulSoup(raw, "xml" if input_type == "xml" else "lxml")

        dcfg = mapping.get("dom", {})
        _dom_remove(soup, dcfg.get("remove_selectors", []))
        root = _dom_select_root(soup, dcfg.get("root_selector"))

        # fields
        fields = dcfg.get("fields", {})
        for field_name in ["fulltext", "overview", "process"]:
            spec = fields.get(field_name) or {}
            sections[field_name] = _dom_extract_field(root, spec.get("selector"), spec.get("mode", "text"))

        # lists
        lists = dcfg.get("lists", {})
        list_field_map = {
            "responsibilities_items": "responsibilities",
            "requirements_items": "requirements",
            "additional_items": "additional",
            "benefits_items": "benefits",
        }
        for src_field, dst_field in list_field_map.items():
            spec = lists.get(src_field) or {}
            sections[dst_field] = _dom_extract_list(
                root,
                spec.get("selector"),
                spec.get("item_selector", "li"),
                spec.get("mode", "text"),
            )

        return sections

    return sections

    return sections

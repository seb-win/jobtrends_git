from __future__ import annotations

from typing import List, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator

SchemaVersion = Literal["0.1"]


def _empty_str_to_none(v):
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


class ItemsContainer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: List[str] = Field(default_factory=list)

    @field_validator("items", mode="before")
    @classmethod
    def _clean_items(cls, v):
        if v is None:
            return []
        if not isinstance(v, list):
            return []
        cleaned: List[str] = []
        for x in v:
            if x is None:
                continue
            if isinstance(x, str):
                s = x.strip()
                if s:
                    cleaned.append(s)
        return cleaned


class MetaBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Optional[str] = None
    location_text: Optional[str] = None
    posting_date: Optional[str] = None  # YYYY-MM-DD, if available
    employment_type: Optional[str] = None
    contract_type: Optional[str] = None
    career_level: Optional[str] = None
    salary_text: Optional[str] = None

    _empty_to_none = field_validator(
        "title",
        "location_text",
        "posting_date",
        "employment_type",
        "contract_type",
        "career_level",
        "salary_text",
        mode="before",
    )(_empty_str_to_none)


class ExtractedBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fulltext: Optional[str] = None
    overview: Optional[str] = None
    responsibilities: ItemsContainer = Field(default_factory=ItemsContainer)
    requirements: ItemsContainer = Field(default_factory=ItemsContainer)
    additional: ItemsContainer = Field(default_factory=ItemsContainer)
    benefits: ItemsContainer = Field(default_factory=ItemsContainer)
    process: Optional[str] = None

    _empty_to_none = field_validator("fulltext", "overview", "process", mode="before")(_empty_str_to_none)


class DetailJobV01(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: SchemaVersion = "0.1"
    job_id: Optional[str] = None
    company_key: Optional[str] = None
    url: Optional[str] = None
    scraped_at: Optional[str] = None
    locale: Optional[str] = None
    meta: MetaBlock = Field(default_factory=MetaBlock)
    extracted: ExtractedBlock = Field(default_factory=ExtractedBlock)

    _empty_to_none = field_validator("job_id", "company_key", "url", "scraped_at", "locale", mode="before")(_empty_str_to_none)

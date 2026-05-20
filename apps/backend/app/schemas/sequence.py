"""Schemas for public sequence list/detail APIs."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class SequenceListItem(BaseModel):
    """One sequence row shown in search results."""

    accession: str
    version: str | None = None
    locus: str | None = None
    scientific_name: str | None = None
    genus: str | None = None
    seq_status_desc: str
    seq_length: int | None = None
    submit_time: datetime | None = None
    operate_count: int = 0


class SequenceSearchResponse(BaseModel):
    """Paginated response for sequence search API."""

    items: list[SequenceListItem]
    page: int
    page_size: int
    total: int


class SequenceDetailResponse(BaseModel):
    """Detailed sequence profile for accession page."""

    accession: str
    version: str | None = None
    locus: str | None = None
    definition: str | None = None
    organism_id: int | None = None
    scientific_name: str | None = None
    genus: str | None = None
    seq_status: int | None = None
    seq_status_desc: str | None = None
    submitter: str | None = None
    submit_time: datetime | None = None
    seq_length: int | None = None
    sequence: str | None = None
    features: list[dict[str, Any]]
    references: list[dict[str, Any]]

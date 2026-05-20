"""Schemas for sequence change request submission and review."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


SequenceActionType = Literal["CREATE", "UPDATE", "DELETE"]
SequenceRequestStatus = Literal["PENDING", "APPROVED", "REJECTED"]
ReviewDecision = Literal["APPROVE", "REJECT"]


class SequenceRequestCreate(BaseModel):
    """Payload submitted by user/admin to request sequence mutations."""

    action_type: SequenceActionType
    target_accession: str = Field(min_length=1, max_length=50)
    payload_json: dict[str, Any] | None = None
    reason: str | None = Field(default=None, max_length=255)


class SequenceRequestItem(BaseModel):
    """Sequence mutation request entry."""

    id: int
    action_type: str
    target_accession: str
    payload_json: dict[str, Any] | None
    reason: str | None
    status: SequenceRequestStatus
    requester_id: int
    reviewer_id: int | None
    review_comment: str | None
    created_at: datetime
    reviewed_at: datetime | None


class SequenceReviewRequest(BaseModel):
    """Payload used by admin to approve or reject requests."""

    decision: ReviewDecision
    review_comment: str | None = Field(default=None, max_length=255)


class SequenceReviewResponse(BaseModel):
    """Review outcome summary for client confirmation."""

    request_id: int
    status: SequenceRequestStatus
    execution_summary: str

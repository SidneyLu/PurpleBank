"""Schemas for admin-managed user change request workflow."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


UserActionType = Literal["CREATE", "DELETE"]
UserRequestStatus = Literal["PENDING", "APPROVED", "REJECTED"]
UserReviewDecision = Literal["APPROVE", "REJECT"]


class UserRequestCreate(BaseModel):
    """Payload for admin-submitted user mutation request."""

    action_type: UserActionType
    payload_json: dict[str, Any]
    reason: str | None = Field(default=None, max_length=255)


class UserRequestItem(BaseModel):
    """User management request item for review console."""

    id: int
    action_type: str
    payload_json: dict[str, Any] | None
    reason: str | None
    status: UserRequestStatus
    requester_id: int
    reviewer_id: int | None
    review_comment: str | None
    created_at: datetime
    reviewed_at: datetime | None


class UserReviewRequest(BaseModel):
    """Review command for user management requests."""

    decision: UserReviewDecision
    review_comment: str | None = Field(default=None, max_length=255)


class UserSummary(BaseModel):
    """Admin-facing user list row."""

    id: int
    username: str
    role: str
    is_active: bool
    created_at: datetime

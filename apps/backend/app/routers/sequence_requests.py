"""Sequence request APIs for submission and admin review."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.database import get_db_session
from app.dependencies.auth import get_current_user, require_admin
from app.schemas.auth import UserProfile
from app.schemas.sequence_request import (
    SequenceRequestCreate,
    SequenceRequestItem,
    SequenceReviewRequest,
    SequenceReviewResponse,
)
from app.services.sequence_request_service import (
    SequenceWorkflowError,
    create_sequence_request,
    list_sequence_requests,
    review_sequence_request,
)

router = APIRouter(prefix="/sequence-requests", tags=["sequence-requests"])
admin_router = APIRouter(prefix="/admin/sequence-requests", tags=["admin-sequence-reviews"])


@router.post("", response_model=SequenceRequestItem, status_code=status.HTTP_201_CREATED)
def submit_request(
    payload: SequenceRequestCreate,
    current_user: UserProfile = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> SequenceRequestItem:
    """Submit a pending sequence mutation request.

    Permission:
        Authenticated user/admin.

    Errors:
        400: Invalid payload.
        401: Unauthenticated.
    """

    try:
        return create_sequence_request(db, current_user.id, payload)
    except SequenceWorkflowError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/my", response_model=list[SequenceRequestItem])
def my_requests(
    current_user: UserProfile = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> list[SequenceRequestItem]:
    """List sequence mutation requests created by current user."""

    return list_sequence_requests(db, requester_id=current_user.id)


@admin_router.get("", response_model=list[SequenceRequestItem])
def all_requests(
    _: UserProfile = Depends(require_admin),
    db: Session = Depends(get_db_session),
) -> list[SequenceRequestItem]:
    """List all sequence mutation requests for admin review console.

    Permission:
        Admin only.

    Errors:
        403: Non-admin access.
    """

    return list_sequence_requests(db, requester_id=None)


@admin_router.post("/{request_id}/review", response_model=SequenceReviewResponse)
def review_request(
    request_id: int,
    payload: SequenceReviewRequest,
    admin_user: UserProfile = Depends(require_admin),
    db: Session = Depends(get_db_session),
) -> SequenceReviewResponse:
    """Approve or reject a sequence change request.

    Permission:
        Admin only.

    Errors:
        400: Invalid state/payload or execution failure.
        403: Non-admin access.
        404: Request not found.
    """

    try:
        return review_sequence_request(db, request_id, admin_user.id, payload)
    except SequenceWorkflowError as exc:
        message = str(exc)
        status_code = status.HTTP_404_NOT_FOUND if message == "Request not found" else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=status_code, detail=message) from exc

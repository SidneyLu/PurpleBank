"""Admin user list and user-request review APIs."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.database import get_db_session
from app.dependencies.auth import require_admin
from app.schemas.auth import UserProfile
from app.schemas.user_request import UserRequestCreate, UserRequestItem, UserReviewRequest, UserSummary
from app.services.auth_service import list_all_users
from app.services.user_request_service import UserWorkflowError, create_user_request, list_user_requests, review_user_request

router = APIRouter(prefix="/admin", tags=["admin-users"])


@router.get("/users", response_model=list[UserSummary])
def users(
    _: UserProfile = Depends(require_admin),
    db: Session = Depends(get_db_session),
) -> list[UserSummary]:
    """Return all users for admin management table.

    Permission:
        Admin only.

    Errors:
        403: Non-admin access.
    """

    return [UserSummary(**profile.model_dump()) for profile in list_all_users(db)]


@router.post("/user-requests", response_model=UserRequestItem, status_code=status.HTTP_201_CREATED)
def submit_user_request(
    payload: UserRequestCreate,
    admin_user: UserProfile = Depends(require_admin),
    db: Session = Depends(get_db_session),
) -> UserRequestItem:
    """Submit admin user-create/user-delete request.

    Permission:
        Admin only.

    Errors:
        400: Invalid payload.
        403: Non-admin access.
    """

    try:
        return create_user_request(db, admin_user.id, payload)
    except UserWorkflowError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/user-requests", response_model=list[UserRequestItem])
def get_user_requests(
    _: UserProfile = Depends(require_admin),
    db: Session = Depends(get_db_session),
) -> list[UserRequestItem]:
    """List all user management requests for admin review."""

    return list_user_requests(db)


@router.post("/user-requests/{request_id}/review")
def review_user_req(
    request_id: int,
    payload: UserReviewRequest,
    admin_user: UserProfile = Depends(require_admin),
    db: Session = Depends(get_db_session),
) -> dict[str, str | int]:
    """Approve or reject admin user management request.

    Permission:
        Admin only.

    Errors:
        400: Invalid state/payload.
        404: Request not found.
        403: Non-admin access.
    """

    try:
        return review_user_request(db, request_id, admin_user.id, payload)
    except UserWorkflowError as exc:
        message = str(exc)
        status_code = status.HTTP_404_NOT_FOUND if message == "Request not found" else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=status_code, detail=message) from exc

"""Authentication API routes.

Exposes register/login/me endpoints for JWT-based auth flows.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.database import get_db_session
from app.core.security import create_access_token
from app.dependencies.auth import get_current_user
from app.schemas.auth import AuthTokenResponse, LoginRequest, RegisterRequest, UserProfile
from app.services.auth_service import AuthError, authenticate_user, register_user

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=AuthTokenResponse, status_code=status.HTTP_201_CREATED)
def register(payload: RegisterRequest, db: Session = Depends(get_db_session)) -> AuthTokenResponse:
    """Register a standard user account.

    Permission:
        Public endpoint.

    Errors:
        400: Username exists or registration precondition fails.
    """

    try:
        user = register_user(db, payload.username, payload.password)
    except AuthError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    token = create_access_token(str(user.id), {"role": user.role})
    return AuthTokenResponse(access_token=token, user=user)


@router.post("/login", response_model=AuthTokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db_session)) -> AuthTokenResponse:
    """Authenticate by username/password and issue access token.

    Permission:
        Public endpoint.

    Errors:
        401: Invalid credentials or inactive account.
    """

    try:
        user = authenticate_user(db, payload.username, payload.password)
    except AuthError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    token = create_access_token(str(user.id), {"role": user.role})
    return AuthTokenResponse(access_token=token, user=user)


@router.get("/me", response_model=UserProfile)
def me(current_user: UserProfile = Depends(get_current_user)) -> UserProfile:
    """Return profile for current bearer token.

    Permission:
        Authenticated user.

    Errors:
        401: Missing/invalid token.
    """

    return current_user

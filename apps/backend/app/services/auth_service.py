"""Authentication and account query service functions.

This service owns account creation and authentication checks so routers remain
thin and focused on HTTP concerns. Database writes here intentionally avoid
implicit commits to let calling routes decide transaction scope.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.security import hash_password, verify_password
from app.schemas.auth import UserProfile


class AuthError(Exception):
    """Raised when authentication or registration preconditions fail."""


def _row_to_profile(row: object) -> UserProfile:
    mapping = row._mapping
    return UserProfile(
        id=int(mapping["id"]),
        username=str(mapping["username"]),
        role=str(mapping["role"]),
        is_active=bool(mapping["is_active"]),
        created_at=mapping.get("created_at"),
    )


def register_user(db: Session, username: str, password: str) -> UserProfile:
    """Create a new active user with default role `user`."""

    exists = db.execute(
        text("SELECT id FROM app_user WHERE username = :username LIMIT 1"),
        {"username": username},
    ).first()
    if exists:
        raise AuthError("Username already exists")

    password_hash = hash_password(password)
    db.execute(
        text(
            """
            INSERT INTO app_user (username, password_hash, role, is_active)
            VALUES (:username, :password_hash, 'user', 1)
            """
        ),
        {"username": username, "password_hash": password_hash},
    )
    db.commit()

    row = db.execute(
        text(
            """
            SELECT id, username, role, is_active, created_at
            FROM app_user
            WHERE username = :username
            LIMIT 1
            """
        ),
        {"username": username},
    ).first()
    if not row:
        raise AuthError("User creation failed")
    return _row_to_profile(row)


def authenticate_user(db: Session, username: str, password: str) -> UserProfile:
    """Validate credentials and return user profile on success."""

    row = db.execute(
        text(
            """
            SELECT id, username, password_hash, role, is_active, created_at
            FROM app_user
            WHERE username = :username
            LIMIT 1
            """
        ),
        {"username": username},
    ).first()
    if not row:
        raise AuthError("Invalid username or password")

    mapping = row._mapping
    if not verify_password(password, str(mapping["password_hash"])):
        raise AuthError("Invalid username or password")
    if not bool(mapping["is_active"]):
        raise AuthError("User is inactive")

    return UserProfile(
        id=int(mapping["id"]),
        username=str(mapping["username"]),
        role=str(mapping["role"]),
        is_active=bool(mapping["is_active"]),
        created_at=mapping.get("created_at"),
    )


def get_user_by_id(db: Session, user_id: int) -> UserProfile | None:
    """Load user profile by primary key."""

    row = db.execute(
        text(
            """
            SELECT id, username, role, is_active, created_at
            FROM app_user
            WHERE id = :user_id
            LIMIT 1
            """
        ),
        {"user_id": user_id},
    ).first()
    if not row:
        return None
    return _row_to_profile(row)


def list_all_users(db: Session) -> list[UserProfile]:
    """Return all users for admin list page."""

    rows = db.execute(
        text(
            """
            SELECT id, username, role, is_active, created_at
            FROM app_user
            ORDER BY created_at DESC
            """
        )
    ).fetchall()
    return [_row_to_profile(row) for row in rows]

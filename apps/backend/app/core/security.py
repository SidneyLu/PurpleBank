"""Security helpers for password hashing and JWT authentication.

The backend uses passlib PBKDF2 hashing for compatibility with existing
seed data and JWT bearer tokens for stateless API authorization.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from passlib.hash import pbkdf2_sha256

from app.core.config import get_settings

settings = get_settings()


def hash_password(password: str) -> str:
    """Return secure hash string for a plaintext password."""

    return pbkdf2_sha256.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    """Verify plaintext password against stored hash."""

    return pbkdf2_sha256.verify(password, password_hash)


def create_access_token(subject: str, extra_payload: dict[str, Any] | None = None) -> str:
    """Create a signed JWT token for authenticated users."""

    now = datetime.now(tz=UTC)
    payload: dict[str, Any] = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=settings.jwt_exp_minutes)).timestamp()),
    }
    if extra_payload:
        payload.update(extra_payload)
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> dict[str, Any]:
    """Decode and validate a JWT token.

    Raises jwt exceptions when token is invalid or expired.
    """

    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])

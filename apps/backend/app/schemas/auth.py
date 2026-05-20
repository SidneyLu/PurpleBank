"""Authentication-related request and response schemas."""

from datetime import datetime

from pydantic import BaseModel, Field


class RegisterRequest(BaseModel):
    """Payload for public user registration."""

    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=8, max_length=128)


class LoginRequest(BaseModel):
    """Payload for username/password login."""

    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=8, max_length=128)


class UserProfile(BaseModel):
    """Public-safe user profile returned to clients."""

    id: int
    username: str
    role: str
    is_active: bool
    created_at: datetime | None = None


class AuthTokenResponse(BaseModel):
    """Bearer token response for successful authentication."""

    access_token: str
    token_type: str = "bearer"
    user: UserProfile

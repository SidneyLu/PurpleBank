"""Unit tests for password hashing and JWT helpers."""

from app.core.security import create_access_token, decode_access_token, hash_password, verify_password


def test_password_hash_and_verify() -> None:
    """Password verification should pass for original input only."""

    password = "StrongPass!234"
    hashed = hash_password(password)

    assert hashed != password
    assert verify_password(password, hashed)
    assert not verify_password("wrong", hashed)


def test_token_round_trip() -> None:
    """Created JWT should decode back to subject payload."""

    token = create_access_token("123", {"role": "admin"})
    payload = decode_access_token(token)

    assert payload["sub"] == "123"
    assert payload["role"] == "admin"

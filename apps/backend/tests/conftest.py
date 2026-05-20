"""pytest fixtures for backend tests."""

from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from app.core.database import get_db_session
from app.main import app


class DummySession:
    """Minimal DB session stub for routes that should not touch database."""

    def execute(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("DummySession.execute should be monkeypatched in this test")

    def close(self) -> None:
        return None


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    """Provide FastAPI test client with DB dependency overridden."""

    def override_db() -> Generator[DummySession, None, None]:
        yield DummySession()

    app.dependency_overrides[get_db_session] = override_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()

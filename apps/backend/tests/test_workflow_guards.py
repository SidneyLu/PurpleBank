"""Workflow validation tests for service-level guards."""

import pytest

from app.services.sequence_request_service import SequenceWorkflowError, _execute_sequence_action
from app.services.user_request_service import UserWorkflowError, _execute_user_action


class NoopDb:
    """Minimal stub with execute method for guarded code paths."""

    def execute(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None


def test_sequence_create_requires_fields() -> None:
    """CREATE action should reject missing required payload fields."""

    with pytest.raises(SequenceWorkflowError):
        _execute_sequence_action(NoopDb(), "CREATE", "NC_X", {})


def test_user_create_requires_password() -> None:
    """User CREATE action should require password in payload."""

    with pytest.raises(UserWorkflowError):
        _execute_user_action(NoopDb(), "CREATE", {"username": "demo"})

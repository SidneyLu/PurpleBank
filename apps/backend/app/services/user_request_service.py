"""Admin user change request workflow service.

Admin account mutations still require explicit review. This module handles
request creation and review execution with transactional guarantees.
"""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.security import hash_password
from app.schemas.user_request import UserRequestCreate, UserRequestItem, UserReviewRequest


class UserWorkflowError(Exception):
    """Raised when user request workflow validation fails."""


def _map_request_row(row: object) -> UserRequestItem:
    mapping = row._mapping
    payload_value = mapping.get("payload_json")
    if isinstance(payload_value, str):
        try:
            payload_value = json.loads(payload_value)
        except ValueError:
            payload_value = {"raw": payload_value}

    return UserRequestItem(
        id=int(mapping["id"]),
        action_type=str(mapping["action_type"]),
        payload_json=payload_value,
        reason=mapping.get("reason"),
        status=str(mapping["status"]),
        requester_id=int(mapping["requester_id"]),
        reviewer_id=int(mapping["reviewer_id"]) if mapping.get("reviewer_id") is not None else None,
        review_comment=mapping.get("review_comment"),
        created_at=mapping["created_at"],
        reviewed_at=mapping.get("reviewed_at"),
    )


def create_user_request(db: Session, requester_id: int, payload: UserRequestCreate) -> UserRequestItem:
    """Create a pending admin user mutation request."""

    db.execute(
        text(
            """
            INSERT INTO user_change_request (
                action_type,
                payload_json,
                reason,
                status,
                requester_id
            ) VALUES (
                :action_type,
                CAST(:payload_json AS JSON),
                :reason,
                'PENDING',
                :requester_id
            )
            """
        ),
        {
            "action_type": payload.action_type,
            "payload_json": json.dumps(payload.payload_json),
            "reason": payload.reason,
            "requester_id": requester_id,
        },
    )
    db.commit()

    row = db.execute(
        text(
            """
            SELECT id, action_type, payload_json, reason, status,
                   requester_id, reviewer_id, review_comment, created_at, reviewed_at
            FROM user_change_request
            WHERE requester_id = :requester_id
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {"requester_id": requester_id},
    ).first()
    if not row:
        raise UserWorkflowError("Failed to create user request")
    return _map_request_row(row)


def list_user_requests(db: Session) -> list[UserRequestItem]:
    """List all user management requests for admin review."""

    rows = db.execute(
        text(
            """
            SELECT id, action_type, payload_json, reason, status,
                   requester_id, reviewer_id, review_comment, created_at, reviewed_at
            FROM user_change_request
            ORDER BY created_at DESC, id DESC
            """
        )
    ).fetchall()
    return [_map_request_row(row) for row in rows]


def review_user_request(
    db: Session,
    request_id: int,
    reviewer_id: int,
    payload: UserReviewRequest,
) -> dict[str, str | int]:
    """Approve/reject user mutation request and execute side effects."""

    try:
        row = db.execute(
            text(
                """
                SELECT id, action_type, payload_json, status
                FROM user_change_request
                WHERE id = :request_id
                FOR UPDATE
                """
            ),
            {"request_id": request_id},
        ).first()
        if not row:
            raise UserWorkflowError("Request not found")

        mapping = row._mapping
        if mapping["status"] != "PENDING":
            raise UserWorkflowError("Only pending requests can be reviewed")

        if payload.decision == "REJECT":
            db.execute(
                text(
                    """
                    UPDATE user_change_request
                    SET status = 'REJECTED',
                        reviewer_id = :reviewer_id,
                        review_comment = :review_comment,
                        reviewed_at = CURRENT_TIMESTAMP
                    WHERE id = :request_id
                    """
                ),
                {
                    "request_id": request_id,
                    "reviewer_id": reviewer_id,
                    "review_comment": payload.review_comment,
                },
            )
            db.commit()
            return {
                "request_id": request_id,
                "status": "REJECTED",
                "execution_summary": "User request rejected.",
            }

        raw_payload = mapping["payload_json"]
        mutation_payload = json.loads(raw_payload) if isinstance(raw_payload, str) else (raw_payload or {})
        action_type = str(mapping["action_type"])

        summary = _execute_user_action(db, action_type, mutation_payload)

        db.execute(
            text(
                """
                UPDATE user_change_request
                SET status = 'APPROVED',
                    reviewer_id = :reviewer_id,
                    review_comment = :review_comment,
                    reviewed_at = CURRENT_TIMESTAMP
                WHERE id = :request_id
                """
            ),
            {
                "request_id": request_id,
                "reviewer_id": reviewer_id,
                "review_comment": payload.review_comment,
            },
        )

        db.commit()
        return {
            "request_id": request_id,
            "status": "APPROVED",
            "execution_summary": summary,
        }
    except Exception:
        # Roll back review execution on any failure to keep queue/user state consistent.
        db.rollback()
        raise


def _execute_user_action(db: Session, action_type: str, payload: dict[str, object]) -> str:
    """Execute approved user create/delete action."""

    if action_type == "CREATE":
        username = str(payload.get("username") or "").strip()
        password = str(payload.get("password") or "").strip()
        role = str(payload.get("role") or "user").strip().lower()
        if not username or not password:
            raise UserWorkflowError("CREATE payload must include username and password")
        if role not in {"user", "admin"}:
            raise UserWorkflowError("Role must be user or admin")

        existing = db.execute(
            text("SELECT id FROM app_user WHERE username = :username LIMIT 1"),
            {"username": username},
        ).first()
        if existing:
            raise UserWorkflowError("Username already exists")

        db.execute(
            text(
                """
                INSERT INTO app_user (username, password_hash, role, is_active)
                VALUES (:username, :password_hash, :role, 1)
                """
            ),
            {
                "username": username,
                "password_hash": hash_password(password),
                "role": role,
            },
        )
        return f"User {username} created with role {role}."

    if action_type == "DELETE":
        target_user_id = payload.get("target_user_id")
        if target_user_id is None:
            raise UserWorkflowError("DELETE payload must include target_user_id")

        row = db.execute(
            text("SELECT id, username FROM app_user WHERE id = :id LIMIT 1"),
            {"id": int(target_user_id)},
        ).first()
        if not row:
            raise UserWorkflowError("Target user does not exist")

        # Logical deletion preserves foreign key references and audit history.
        db.execute(
            text("UPDATE app_user SET is_active = 0 WHERE id = :id"),
            {"id": int(target_user_id)},
        )
        username = str(row._mapping["username"])
        return f"User {username} deactivated."

    raise UserWorkflowError(f"Unsupported action_type: {action_type}")

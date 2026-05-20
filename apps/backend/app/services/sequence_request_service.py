"""Sequence change request workflow service.

This module enforces the rule that all sequence mutations must be requested,
reviewed, and then executed inside explicit transactions.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.sequence_request import (
    SequenceRequestCreate,
    SequenceRequestItem,
    SequenceReviewRequest,
    SequenceReviewResponse,
)


class SequenceWorkflowError(Exception):
    """Raised when request payload, status, or execution is invalid."""


def _map_request_row(row: object) -> SequenceRequestItem:
    mapping = row._mapping
    payload_value = mapping.get("payload_json")
    if isinstance(payload_value, str):
        try:
            import json

            payload_value = json.loads(payload_value)
        except ValueError:
            payload_value = {"raw": payload_value}

    return SequenceRequestItem(
        id=int(mapping["id"]),
        action_type=str(mapping["action_type"]),
        target_accession=str(mapping["target_accession"]),
        payload_json=payload_value,
        reason=mapping.get("reason"),
        status=str(mapping["status"]),
        requester_id=int(mapping["requester_id"]),
        reviewer_id=int(mapping["reviewer_id"]) if mapping.get("reviewer_id") is not None else None,
        review_comment=mapping.get("review_comment"),
        created_at=mapping["created_at"],
        reviewed_at=mapping.get("reviewed_at"),
    )


def create_sequence_request(db: Session, requester_id: int, payload: SequenceRequestCreate) -> SequenceRequestItem:
    """Persist a new pending sequence change request."""

    import json

    db.execute(
        text(
            """
            INSERT INTO sequence_change_request (
                action_type,
                target_accession,
                payload_json,
                reason,
                status,
                requester_id
            ) VALUES (
                :action_type,
                :target_accession,
                CAST(:payload_json AS JSON),
                :reason,
                'PENDING',
                :requester_id
            )
            """
        ),
        {
            "action_type": payload.action_type,
            "target_accession": payload.target_accession,
            "payload_json": json.dumps(payload.payload_json or {}),
            "reason": payload.reason,
            "requester_id": requester_id,
        },
    )
    db.commit()

    row = db.execute(
        text(
            """
            SELECT id, action_type, target_accession, payload_json, reason, status,
                   requester_id, reviewer_id, review_comment, created_at, reviewed_at
            FROM sequence_change_request
            WHERE requester_id = :requester_id
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {"requester_id": requester_id},
    ).first()
    if not row:
        raise SequenceWorkflowError("Failed to create sequence request")
    return _map_request_row(row)


def list_sequence_requests(db: Session, requester_id: int | None = None) -> list[SequenceRequestItem]:
    """List sequence requests for one requester or all requests for admin."""

    if requester_id is None:
        rows = db.execute(
            text(
                """
                SELECT id, action_type, target_accession, payload_json, reason, status,
                       requester_id, reviewer_id, review_comment, created_at, reviewed_at
                FROM sequence_change_request
                ORDER BY created_at DESC, id DESC
                """
            )
        ).fetchall()
    else:
        rows = db.execute(
            text(
                """
                SELECT id, action_type, target_accession, payload_json, reason, status,
                       requester_id, reviewer_id, review_comment, created_at, reviewed_at
                FROM sequence_change_request
                WHERE requester_id = :requester_id
                ORDER BY created_at DESC, id DESC
                """
            ),
            {"requester_id": requester_id},
        ).fetchall()

    return [_map_request_row(row) for row in rows]


def review_sequence_request(
    db: Session,
    request_id: int,
    reviewer_id: int,
    payload: SequenceReviewRequest,
) -> SequenceReviewResponse:
    """Approve/reject a sequence request and execute mutation on approval."""

    try:
        row = db.execute(
            text(
                """
                SELECT id, action_type, target_accession, payload_json, status
                FROM sequence_change_request
                WHERE id = :request_id
                FOR UPDATE
                """
            ),
            {"request_id": request_id},
        ).first()
        if not row:
            raise SequenceWorkflowError("Request not found")

        mapping = row._mapping
        if mapping["status"] != "PENDING":
            raise SequenceWorkflowError("Only pending requests can be reviewed")

        if payload.decision == "REJECT":
            db.execute(
                text(
                    """
                    UPDATE sequence_change_request
                    SET status = 'REJECTED',
                        reviewer_id = :reviewer_id,
                        review_comment = :comment,
                        reviewed_at = CURRENT_TIMESTAMP
                    WHERE id = :request_id
                    """
                ),
                {
                    "request_id": request_id,
                    "reviewer_id": reviewer_id,
                    "comment": payload.review_comment,
                },
            )
            db.commit()
            return SequenceReviewResponse(
                request_id=request_id,
                status="REJECTED",
                execution_summary="Request rejected by admin review.",
            )

        action_type = str(mapping["action_type"])
        target_accession = str(mapping["target_accession"])
        raw_payload = mapping.get("payload_json")
        if isinstance(raw_payload, str):
            import json

            mutation_payload: dict[str, Any] = json.loads(raw_payload) if raw_payload else {}
        elif isinstance(raw_payload, dict):
            mutation_payload = raw_payload
        else:
            mutation_payload = {}

        execution_summary = _execute_sequence_action(db, action_type, target_accession, mutation_payload)

        db.execute(
            text(
                """
                UPDATE sequence_change_request
                SET status = 'APPROVED',
                    reviewer_id = :reviewer_id,
                    review_comment = :comment,
                    reviewed_at = CURRENT_TIMESTAMP
                WHERE id = :request_id
                """
            ),
            {
                "request_id": request_id,
                "reviewer_id": reviewer_id,
                "comment": payload.review_comment,
            },
        )

        db.commit()
        return SequenceReviewResponse(
            request_id=request_id,
            status="APPROVED",
            execution_summary=execution_summary,
        )
    except Exception:
        # All review-side writes must be atomic; rollback guarantees no partial state.
        db.rollback()
        raise


def _execute_sequence_action(
    db: Session,
    action_type: str,
    target_accession: str,
    mutation_payload: dict[str, Any],
) -> str:
    """Execute approved sequence mutation using action-specific rules."""

    if action_type == "CREATE":
        accession = str(mutation_payload.get("accession") or target_accession)
        required_fields = ["organism_id", "sequence"]
        missing = [field for field in required_fields if not mutation_payload.get(field)]
        if missing:
            raise SequenceWorkflowError(f"CREATE payload missing fields: {', '.join(missing)}")

        db.execute(
            text(
                """
                INSERT INTO Sequence (
                    accession,
                    version,
                    locus,
                    definition,
                    organism_id,
                    mol_type,
                    sequence
                ) VALUES (
                    :accession,
                    :version,
                    :locus,
                    :definition,
                    :organism_id,
                    :mol_type,
                    :sequence
                )
                """
            ),
            {
                "accession": accession,
                "version": mutation_payload.get("version"),
                "locus": mutation_payload.get("locus"),
                "definition": mutation_payload.get("definition"),
                "organism_id": mutation_payload.get("organism_id"),
                "mol_type": mutation_payload.get("mol_type"),
                "sequence": mutation_payload.get("sequence"),
            },
        )
        return f"CREATE executed for accession {accession}."

    if action_type == "UPDATE":
        db.execute(
            text(
                """
                CALL sp_update_sequence_reviewed(
                    :accession,
                    :version,
                    :locus,
                    :definition,
                    :organism_id,
                    :mol_type,
                    :sequence,
                    :feature_gene,
                    :feature_product,
                    :feature_location,
                    :feature_note
                )
                """
            ),
            {
                "accession": target_accession,
                "version": mutation_payload.get("version"),
                "locus": mutation_payload.get("locus"),
                "definition": mutation_payload.get("definition"),
                "organism_id": mutation_payload.get("organism_id"),
                "mol_type": mutation_payload.get("mol_type"),
                "sequence": mutation_payload.get("sequence"),
                "feature_gene": mutation_payload.get("feature_gene"),
                "feature_product": mutation_payload.get("feature_product"),
                "feature_location": mutation_payload.get("feature_location"),
                "feature_note": mutation_payload.get("feature_note"),
            },
        )
        return f"UPDATE executed for accession {target_accession}."

    if action_type == "DELETE":
        # Deletion is multi-table and must remain atomic; one transaction avoids
        # orphan rows and ensures either the whole accession is removed or nothing changes.
        db.execute(
            text(
                """
                DELETE ra
                FROM `Ref_Author` AS ra
                JOIN `Reference` AS r ON r.ref_id = ra.ref_id
                WHERE r.accession = :accession
                """
            ),
            {"accession": target_accession},
        )
        db.execute(text("DELETE FROM `Ref_Sequence` WHERE accession = :accession"), {"accession": target_accession})
        db.execute(text("DELETE FROM `Reference` WHERE accession = :accession"), {"accession": target_accession})
        db.execute(text("DELETE FROM `Feature` WHERE accession = :accession"), {"accession": target_accession})
        db.execute(text("DELETE FROM `DNA` WHERE accession = :accession"), {"accession": target_accession})
        db.execute(text("DELETE FROM `RNA` WHERE accession = :accession"), {"accession": target_accession})
        db.execute(text("DELETE FROM sequence_operation_log WHERE accession = :accession"), {"accession": target_accession})
        db.execute(text("DELETE FROM sequence_review WHERE accession = :accession"), {"accession": target_accession})
        db.execute(text("DELETE FROM `Sequence` WHERE accession = :accession"), {"accession": target_accession})
        return f"DELETE executed for accession {target_accession}."

    raise SequenceWorkflowError(f"Unsupported action_type: {action_type}")

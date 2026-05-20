"""Public read-only sequence API routes for guests and authenticated users."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.core.database import get_db_session
from app.schemas.sequence import SequenceDetailResponse, SequenceSearchResponse
from app.services.sequence_service import get_sequence_detail, search_sequences

router = APIRouter(prefix="/sequences", tags=["sequences"])


@router.get("/search", response_model=SequenceSearchResponse)
def search(
    keyword: str | None = Query(default=None, max_length=100),
    status_filter: str | None = Query(default=None, alias="status", max_length=20),
    genus: str | None = Query(default=None, max_length=100),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=50),
    sort: str = Query(default="submit_time"),
    order: str = Query(default="desc"),
    db: Session = Depends(get_db_session),
) -> SequenceSearchResponse:
    """Search sequence data from reporting view with pagination.

    Permission:
        Public endpoint (guest readable).

    Errors:
        400: Invalid filter/sort combination.
    """

    if status_filter and status_filter not in {"pending", "approved", "rejected"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="status must be pending/approved/rejected")

    return search_sequences(db, keyword, status_filter, genus, page, page_size, sort, order)


@router.get("/{accession}", response_model=SequenceDetailResponse)
def detail(accession: str, db: Session = Depends(get_db_session)) -> SequenceDetailResponse:
    """Return complete sequence details for one accession.

    Permission:
        Public endpoint (guest readable).

    Errors:
        404: Accession not found.
    """

    result = get_sequence_detail(db, accession)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sequence not found")
    return result

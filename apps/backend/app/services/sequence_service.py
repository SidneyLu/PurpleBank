"""Public sequence querying services.

These functions back guest-visible endpoints. They expose read-only access to
search and detail data while keeping SQL concerns out of route handlers.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.sequence import SequenceDetailResponse, SequenceListItem, SequenceSearchResponse

ALLOWED_SORT_FIELDS = {
    "submit_time": "submit_time",
    "seq_length": "seq_length",
    "operate_count": "operate_count",
    "accession": "accession",
}


def search_sequences(
    db: Session,
    keyword: str | None,
    status: str | None,
    genus: str | None,
    page: int,
    page_size: int,
    sort: str,
    order: str,
) -> SequenceSearchResponse:
    """Search sequence rows from reporting view with pagination."""

    filters: list[str] = []
    params: dict[str, object] = {}

    if keyword:
        filters.append(
            """
            (
                accession LIKE :keyword
                OR locus LIKE :keyword
                OR definition LIKE :keyword
                OR scientific_name LIKE :keyword
            )
            """
        )
        params["keyword"] = f"%{keyword}%"

    if status:
        filters.append("seq_status_desc = :status")
        params["status"] = status

    if genus:
        filters.append("genus = :genus")
        params["genus"] = genus

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    sort_column = ALLOWED_SORT_FIELDS.get(sort, "submit_time")
    sort_order = "DESC" if order.lower() == "desc" else "ASC"

    total = db.execute(
        text(f"SELECT COUNT(*) AS total FROM v_nucleotide_sequence_info {where_clause}"),
        params,
    ).scalar_one()

    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size

    rows = db.execute(
        text(
            f"""
            SELECT accession, version, locus, scientific_name, genus,
                   seq_status_desc, seq_length, submit_time, operate_count
            FROM v_nucleotide_sequence_info
            {where_clause}
            ORDER BY {sort_column} {sort_order}
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).fetchall()

    items = [
        SequenceListItem(
            accession=row._mapping["accession"],
            version=row._mapping["version"],
            locus=row._mapping["locus"],
            scientific_name=row._mapping["scientific_name"],
            genus=row._mapping["genus"],
            seq_status_desc=row._mapping["seq_status_desc"],
            seq_length=row._mapping["seq_length"],
            submit_time=row._mapping["submit_time"],
            operate_count=int(row._mapping["operate_count"] or 0),
        )
        for row in rows
    ]

    return SequenceSearchResponse(items=items, page=page, page_size=page_size, total=int(total))


def get_sequence_detail(db: Session, accession: str) -> SequenceDetailResponse | None:
    """Load detailed sequence information and related feature/reference data."""

    row = db.execute(
        text(
            """
            SELECT v.accession,
                   v.version,
                   v.locus,
                   v.definition,
                   v.organism_id,
                   v.scientific_name,
                   v.genus,
                   v.seq_status,
                   v.seq_status_desc,
                   v.submitter,
                   v.submit_time,
                   v.seq_length,
                   s.sequence
            FROM v_nucleotide_sequence_info AS v
            JOIN `Sequence` AS s ON s.accession = v.accession
            WHERE v.accession = :accession
            LIMIT 1
            """
        ),
        {"accession": accession},
    ).first()
    if not row:
        return None

    feature_rows = db.execute(
        text(
            """
            SELECT feature_id, `key`, location, gene, product, translation, note
            FROM `Feature`
            WHERE accession = :accession
            ORDER BY feature_id ASC
            """
        ),
        {"accession": accession},
    ).fetchall()

    reference_rows = db.execute(
        text(
            """
            SELECT ref_id, title, journal, year, pmid
            FROM `Reference`
            WHERE accession = :accession
            ORDER BY ref_id ASC
            """
        ),
        {"accession": accession},
    ).fetchall()

    mapping = row._mapping
    return SequenceDetailResponse(
        accession=str(mapping["accession"]),
        version=mapping["version"],
        locus=mapping["locus"],
        definition=mapping["definition"],
        organism_id=mapping["organism_id"],
        scientific_name=mapping["scientific_name"],
        genus=mapping["genus"],
        seq_status=mapping["seq_status"],
        seq_status_desc=mapping["seq_status_desc"],
        submitter=mapping["submitter"],
        submit_time=mapping["submit_time"],
        seq_length=mapping["seq_length"],
        sequence=mapping["sequence"],
        features=[dict(item._mapping) for item in feature_rows],
        references=[dict(item._mapping) for item in reference_rows],
    )

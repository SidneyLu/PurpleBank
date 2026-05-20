"""API route behavior tests with lightweight dependency stubs."""

from app.schemas.sequence import SequenceDetailResponse, SequenceSearchResponse


def test_health_endpoint(client) -> None:
    """Health endpoint should always be reachable."""

    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_admin_users_requires_auth(client) -> None:
    """Admin endpoint should reject unauthenticated users."""

    response = client.get("/api/v1/admin/users")
    assert response.status_code == 401


def test_public_search_works_with_monkeypatched_service(client, monkeypatch) -> None:
    """Public search route should return service payload for guests."""

    from app.routers import public_sequences

    def fake_search(*args, **kwargs):  # noqa: ANN002, ANN003
        return SequenceSearchResponse(items=[], page=1, page_size=10, total=0)

    monkeypatch.setattr(public_sequences, "search_sequences", fake_search)

    response = client.get("/api/v1/sequences/search?page=1&page_size=10")
    assert response.status_code == 200
    assert response.json()["total"] == 0


def test_public_detail_not_found(client, monkeypatch) -> None:
    """Detail endpoint returns 404 when service reports missing accession."""

    from app.routers import public_sequences

    def fake_detail(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    monkeypatch.setattr(public_sequences, "get_sequence_detail", fake_detail)

    response = client.get("/api/v1/sequences/UNKNOWN")
    assert response.status_code == 404


def test_public_detail_success(client, monkeypatch) -> None:
    """Detail endpoint should relay service detail payload."""

    from app.routers import public_sequences

    def fake_detail(*args, **kwargs):  # noqa: ANN002, ANN003
        return SequenceDetailResponse(
            accession="NC_TEST",
            version="1",
            locus="L_TEST",
            definition="demo",
            organism_id=1,
            scientific_name="Test species",
            genus="Testus",
            seq_status=1,
            seq_status_desc="approved",
            submitter="seed",
            submit_time=None,
            seq_length=12,
            sequence="ATCGATCGATCG",
            features=[],
            references=[],
        )

    monkeypatch.setattr(public_sequences, "get_sequence_detail", fake_detail)

    response = client.get("/api/v1/sequences/NC_TEST")
    assert response.status_code == 200
    assert response.json()["accession"] == "NC_TEST"

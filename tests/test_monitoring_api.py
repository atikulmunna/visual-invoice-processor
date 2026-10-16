from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.monitoring_api import create_monitoring_app


@pytest.fixture
def operator(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """A single-operator deployment without sign-in, whose review queue functions note each call."""
    monkeypatch.delenv("ALPHA_AUTH_ENABLED", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_USERNAME", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_PASSWORD", raising=False)
    return TestClient(create_monitoring_app(postgres_dsn="postgresql://example"), raise_server_exceptions=False)


def test_operator_routes_require_basic_auth_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPHA_AUTH_ENABLED", raising=False)
    monkeypatch.setenv("DASHBOARD_BASIC_AUTH_USERNAME", "admin")
    monkeypatch.setenv("DASHBOARD_BASIC_AUTH_PASSWORD", "secret-pass")
    monkeypatch.setattr("app.monitoring_api.pending_review_items", lambda dsn, **kwargs: [{}, {}])
    client = TestClient(create_monitoring_app(postgres_dsn="postgresql://example"))

    assert client.get("/health").status_code == 200
    assert client.get("/backlog").status_code == 401
    assert client.get("/backlog", auth=("admin", "secret-pass")).json() == {"review_queue_total": 2}


def test_approving_passes_the_correction_and_note(operator: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, Any] = {}

    def resolve(dsn: str, document_id: str, **kwargs: Any) -> dict[str, Any]:
        called.update(dsn=dsn, document_id=document_id, **kwargs)
        return {"storage_result": {"status": "appended", "row_id": 10}, "review_item": {"status": "RESOLVED_STORED"}}

    monkeypatch.setattr("app.monitoring_api.resolve_review_item", resolve)

    response = operator.post(
        "/review-items/doc-2/resolve",
        json={"note": "approved", "corrected_record": {"vendor_name": "Gamma", "total_amount": 45.0}},
    )

    assert response.status_code == 200
    assert response.json()["review_status"] == "RESOLVED_STORED"
    assert called == {"dsn": "postgresql://example", "document_id": "doc-2", "note": "approved",
                      "record_override": {"vendor_name": "Gamma", "total_amount": 45.0}, "org_id": None}


def test_rejecting_or_marking_a_duplicate_dismisses_the_item(operator: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str | None]] = []

    def dismiss(dsn: str, document_id: str, *, resolution_status: str, note: str | None, org_id: str | None) -> dict[str, Any]:
        calls.append((document_id, resolution_status, note))
        return {"storage_result": {"status": "dismissed"}, "review_item": {"status": resolution_status}}

    monkeypatch.setattr("app.monitoring_api.dismiss_review_item", dismiss)

    duplicate = operator.post("/review-items/doc-3/resolve", json={"action": "duplicate", "note": "already stored"})
    rejected = operator.post("/review-items/doc-3/resolve", json={"action": "reject", "note": "invalid document"})
    unknown = operator.post("/review-items/doc-3/resolve", json={"action": "archive"})

    assert duplicate.json()["review_status"] == "RESOLVED_DUPLICATE_MANUAL"
    assert rejected.json()["review_status"] == "REJECTED"
    assert (unknown.status_code, unknown.json()["detail"]) == (400, "Unsupported review action: archive")
    assert calls == [("doc-3", "RESOLVED_DUPLICATE_MANUAL", "already stored"), ("doc-3", "REJECTED", "invalid document")]


@pytest.mark.parametrize(
    ("failure", "status", "detail"),
    [
        (ValueError("Fill in the Total before approving this document"), 400,
         "Fill in the Total before approving this document"),
        (FileNotFoundError("Review item not found: doc-2"), 404, "Review item not found: doc-2"),
        (RuntimeError("connection to server at 10.0.0.5 failed: password authentication failed"), 500, None),
    ],
)
def test_review_resolve_explains_reviewer_mistakes_but_hides_server_faults(
    operator: TestClient, monkeypatch: pytest.MonkeyPatch, failure: Exception, status: int, detail: str | None
) -> None:
    def failing(*_: object, **__: object) -> dict[str, object]:
        raise failure

    monkeypatch.setattr("app.monitoring_api.resolve_review_item", failing)

    response = operator.post("/review-items/doc-2/resolve", json={})

    assert response.status_code == status
    assert "10.0.0.5" not in response.text
    if detail:
        assert response.json()["detail"] == detail

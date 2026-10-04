from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.alpha_store import AlphaAuthenticationError, AlphaUser, Organization
from app.monitoring_api import create_monitoring_app

PASSWORD = "A-strong-alpha-password"
ORG_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
USERS = {
    "owner.one": AlphaUser(
        "11111111-1111-1111-1111-111111111111", "owner.one", 20, 5, True,
        org_id=ORG_ID, org_name="Acme Traders", role="owner",
    ),
    "member.two": AlphaUser(
        "22222222-2222-2222-2222-222222222222", "member.two", 20, 0, True,
        org_id=ORG_ID, org_name="Acme Traders", role="member",
    ),
}


class _FakeAlphaStore:
    base_currency = "BDT"

    def __init__(self, _: str) -> None:
        pass

    def authenticate(self, username: str, password: str) -> AlphaUser:
        if username not in USERS or password != PASSWORD:
            raise AlphaAuthenticationError("Invalid credentials")
        return USERS[username]

    def authenticate_session(self, token: str) -> AlphaUser:
        raise AlphaAuthenticationError("Invalid session")

    def get_organization(self, org_id: str) -> Organization:
        assert org_id == ORG_ID
        return Organization(ORG_ID, "Acme Traders", base_currency=_FakeAlphaStore.base_currency)

    def set_base_currency(self, org_id: str, currency: str) -> str:
        code = currency.strip().upper()
        if len(code) != 3 or not code.isalpha():
            raise ValueError("Base currency must be a three-letter ISO code such as BDT or USD")
        _FakeAlphaStore.base_currency = code
        return code


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setenv("ALPHA_AUTH_ENABLED", "true")
    monkeypatch.setattr("app.monitoring_api.AlphaStore", _FakeAlphaStore)
    monkeypatch.setattr("app.workspace_api.AlphaStore", _FakeAlphaStore)
    _FakeAlphaStore.base_currency = "BDT"
    return TestClient(create_monitoring_app(postgres_dsn="postgresql://example", frontend_dist=tmp_path / "none"))


def test_me_describes_the_user_organization_and_quota(client: TestClient) -> None:
    response = client.get("/api/me", auth=("owner.one", PASSWORD))

    assert response.status_code == 200
    assert response.json() == {
        "username": "owner.one",
        "organization": {"id": ORG_ID, "name": "Acme Traders", "role": "owner", "base_currency": "BDT"},
        "documents_remaining": 15,
        "document_limit": 20,
    }


def test_me_requires_a_session(client: TestClient) -> None:
    assert client.get("/api/me").status_code == 401


def test_me_without_tenants_has_no_organization(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ALPHA_AUTH_ENABLED", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_USERNAME", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_PASSWORD", raising=False)
    client = TestClient(create_monitoring_app(frontend_dist=tmp_path / "none"))

    body = client.get("/api/me").json()

    assert body == {"username": "anonymous", "organization": None, "documents_remaining": None, "document_limit": None}


def test_owner_can_change_the_base_currency(client: TestClient) -> None:
    response = client.put("/api/organization", auth=("owner.one", PASSWORD), json={"base_currency": "usd"})

    assert response.status_code == 200
    assert response.json()["base_currency"] == "USD"
    assert _FakeAlphaStore.base_currency == "USD"


def test_members_cannot_change_settings(client: TestClient) -> None:
    response = client.put("/api/organization", auth=("member.two", PASSWORD), json={"base_currency": "USD"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Only organization owners can change settings"
    assert _FakeAlphaStore.base_currency == "BDT"


def test_invalid_currency_is_rejected_with_a_readable_message(client: TestClient) -> None:
    response = client.put("/api/organization", auth=("owner.one", PASSWORD), json={"base_currency": "dollars"})

    assert response.status_code == 400
    assert "three-letter" in response.json()["detail"]


def test_accounts_without_an_organization_cannot_change_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ALPHA_AUTH_ENABLED", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_USERNAME", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_PASSWORD", raising=False)
    client = TestClient(create_monitoring_app(frontend_dist=tmp_path / "none"))

    assert client.put("/api/organization", json={"base_currency": "USD"}).status_code == 403


def _built_frontend(root: Path) -> Path:
    dist = root / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<!doctype html><div id=root></div>", encoding="utf-8")
    (dist / "assets" / "index-abc.js").write_text("console.log('ledgerly')", encoding="utf-8")
    return dist


def test_frontend_serves_index_for_client_routes_and_real_assets(tmp_path: Path) -> None:
    client = TestClient(create_monitoring_app(frontend_dist=_built_frontend(tmp_path)))

    root = client.get("/app")
    deep_link = client.get("/app/records?q=acme")
    asset = client.get("/app/assets/index-abc.js")
    missing_asset = client.get("/app/assets/missing.js")

    assert root.status_code == 200
    assert "<div id=root>" in root.text
    assert root.headers["cache-control"] == "no-cache"
    assert deep_link.status_code == 200
    assert deep_link.text == root.text
    assert asset.status_code == 200
    assert "ledgerly" in asset.text
    assert missing_asset.status_code == 404


def test_frontend_routes_are_absent_when_not_built(tmp_path: Path) -> None:
    client = TestClient(create_monitoring_app(frontend_dist=tmp_path / "missing"))

    assert client.get("/app").status_code == 404
    assert client.get("/health").status_code == 200


def test_existing_api_routes_are_not_shadowed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPHA_AUTH_ENABLED", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_USERNAME", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_PASSWORD", raising=False)
    client = TestClient(create_monitoring_app(frontend_dist=_built_frontend(tmp_path), review_queue_dir=tmp_path / "q"))

    response: Any = client.get("/backlog")

    assert response.status_code == 200
    assert "review_queue_total" in response.json()


def _job(**overrides: Any) -> dict[str, Any]:
    job: dict[str, Any] = {
        "id": "33333333-3333-3333-3333-333333333333",
        "org_id": ORG_ID,
        "original_name": "invoice.pdf",
        "content_type": "application/pdf",
        "declared_size": 2048,
        "status": "STORED",
        "attempts": 1,
        "page_count": 1,
        "result": {
            "record": {"vendor_name": "RYANS", "invoice_date": "2022-05-17", "total_amount": 1954.0,
                       "currency": "BDT", "currency_assumed": True},
        },
        "error_code": None,
        "error_message": None,
        "authorized_at_utc": None,
        "completed_at_utc": None,
    }
    job.update(overrides)
    return job


def test_job_view_summarizes_a_stored_record() -> None:
    from app.workspace_api import job_view

    view = job_view(_job())

    assert view["summary"] == {
        "vendor_name": "RYANS",
        "invoice_date": "2022-05-17",
        "total_amount": 1954.0,
        "currency": "BDT",
        "currency_assumed": True,
    }
    assert view["retryable"] is False
    assert view["reason_codes"] == []


def test_job_view_keeps_review_reasons_and_hides_failure_details() -> None:
    from app.workspace_api import job_view

    review = job_view(_job(status="REVIEW_REQUIRED", result={"record": {"vendor_name": None}, "reason_codes": ["missing_vendor"]}))
    failed = job_view(_job(status="FAILED", result={}, error_code="provider_request_failed",
                           error_message="OpenRouter failed with status 402: {...}"))
    exhausted = job_view(_job(status="FAILED", attempts=3, result={}))
    rejected = job_view(_job(status="REJECTED", result={}, error_code="page_limit_exceeded",
                             error_message="PDFs may contain at most 5 pages"))
    duplicate = job_view(_job(status="DUPLICATE", result={"status": "SKIPPED_DUPLICATE"}))

    assert review["reason_codes"] == ["missing_vendor"]
    assert failed["error_code"] == "provider_request_failed"
    assert failed["error_message"] is None
    assert failed["retryable"] is True
    assert exhausted["retryable"] is False
    assert rejected["error_message"] == "PDFs may contain at most 5 pages"
    assert duplicate["summary"] is None


class _JobStore(_FakeAlphaStore):
    seen: dict[str, Any] = {}

    def list_jobs(self, *, org_id: str, limit: int = 20) -> list[dict[str, Any]]:
        _JobStore.seen = {"org_id": org_id, "limit": limit}
        return [_job()]

    def get_job(self, job_id: str, *, org_id: str) -> dict[str, Any]:
        from app.alpha_store import AlphaNotFoundError

        if job_id != _job()["id"] or org_id != ORG_ID:
            raise AlphaNotFoundError("Processing job not found")
        return _job()


@pytest.fixture
def jobs_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setenv("ALPHA_AUTH_ENABLED", "true")
    monkeypatch.setattr("app.monitoring_api.AlphaStore", _JobStore)
    monkeypatch.setattr("app.workspace_api.AlphaStore", _JobStore)
    return TestClient(create_monitoring_app(postgres_dsn="postgresql://example", frontend_dist=tmp_path / "none"))


def test_recent_jobs_are_listed_for_the_signed_in_organization(jobs_client: TestClient) -> None:
    response = jobs_client.get("/api/jobs?limit=5", auth=("owner.one", PASSWORD))

    assert response.status_code == 200
    assert [job["name"] for job in response.json()["jobs"]] == ["invoice.pdf"]
    assert _JobStore.seen == {"org_id": ORG_ID, "limit": 5}
    assert jobs_client.get("/api/jobs?limit=0", auth=("owner.one", PASSWORD)).status_code == 422
    assert jobs_client.get("/api/jobs?limit=51", auth=("owner.one", PASSWORD)).status_code == 422


def test_single_job_status_is_scoped_to_the_organization(jobs_client: TestClient) -> None:
    found = jobs_client.get(f"/api/jobs/{_job()['id']}", auth=("owner.one", PASSWORD))
    missing = jobs_client.get("/api/jobs/44444444-4444-4444-4444-444444444444", auth=("owner.one", PASSWORD))

    assert found.status_code == 200
    assert found.json()["status"] == "STORED"
    assert missing.status_code == 404


def test_jobs_require_an_organization(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("ALPHA_AUTH_ENABLED", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_USERNAME", raising=False)
    monkeypatch.delenv("DASHBOARD_BASIC_AUTH_PASSWORD", raising=False)
    client = TestClient(create_monitoring_app(frontend_dist=tmp_path / "none"))

    assert client.get("/api/jobs").status_code == 403


def test_upload_limits_come_from_configuration(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGESTION_BACKEND", "s3")
    monkeypatch.setenv("S3_BUCKET_NAME", "alpha-invoices")
    monkeypatch.setenv("LEDGER_BACKEND", "postgres")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://example")
    monkeypatch.setenv("MAX_UPLOAD_BYTES", "1048576")
    monkeypatch.setenv("MAX_PDF_PAGES", "3")
    monkeypatch.setenv("ALLOWED_MIME_TYPES", "application/pdf,image/png")

    response = client.get("/api/upload-limits", auth=("owner.one", PASSWORD))

    assert response.json() == {
        "max_upload_bytes": 1048576,
        "max_pdf_pages": 3,
        "allowed_types": ["application/pdf", "image/png"],
    }


class _ReviewStorage:
    """Stands in for S3 when preparing review document links."""

    existing = {"archive/u/j/receipt.pdf"}
    broken = False

    def archive_key_for(self, object_key: str) -> str:
        return "archive/" + object_key.removeprefix("inbox/")

    def object_exists(self, key: str) -> bool:
        if _ReviewStorage.broken:
            raise RuntimeError("S3 is unreachable")
        return key in _ReviewStorage.existing

    def create_presigned_download(self, key: str, *, content_type: str, expires_seconds: int = 300) -> str:
        return f"https://bucket.example/{key}?type={content_type}"


class _ReviewStore(_FakeAlphaStore):
    def find_job_by_object_key(self, object_key: str, *, org_id: str) -> dict[str, Any] | None:
        if object_key == "inbox/u/j/receipt.pdf" and org_id == ORG_ID:
            return {"original_name": "Receipt-2734.pdf", "content_type": "application/pdf"}
        return None


def _write_review(queue: Path, document_id: str, org_id: str, source: str) -> None:
    import json

    queue.mkdir(exist_ok=True)
    (queue / f"{document_id}.json").write_text(json.dumps({
        "document_id": document_id,
        "status": "REVIEW_REQUIRED",
        "org_id": org_id,
        "created_at_utc": "2026-10-04T08:00:00+00:00",
        "reason_codes": ["missing_vendor", "validation_failed"],
        "metadata": {
            "source_file_id": source,
            "file_hash": f"hash-{document_id}",
            "violations": [{"code": "amount_mismatch", "severity": "error"}],
            "normalized_record": {"vendor_name": None, "invoice_date": "2026-03-02", "currency": "BDT",
                                  "total_amount": 1954.0, "line_items": []},
        },
    }), encoding="utf-8")


@pytest.fixture
def review_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    for key, value in {"ALPHA_AUTH_ENABLED": "true", "INGESTION_BACKEND": "s3", "S3_BUCKET_NAME": "alpha",
                       "LEDGER_BACKEND": "postgres", "POSTGRES_DSN": "postgresql://example"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("app.monitoring_api.AlphaStore", _ReviewStore)
    monkeypatch.setattr("app.workspace_api.AlphaStore", _ReviewStore)
    monkeypatch.setattr("app.monitoring_api._resolved_file_hashes", lambda dsn, org_id=None: set())
    monkeypatch.setattr("app.workspace_api.ObjectStorageService.from_settings", lambda settings: _ReviewStorage())
    _ReviewStorage.broken = False
    queue = tmp_path / "queue"
    _write_review(queue, "doc-mine", ORG_ID, "inbox/u/j/receipt.pdf")
    _write_review(queue, "doc-gone", ORG_ID, "inbox/u/old/expired.png")
    _write_review(queue, "doc-other", "cccccccc-cccc-cccc-cccc-cccccccccccc", "inbox/x/y/other.pdf")
    return TestClient(create_monitoring_app(
        postgres_dsn="postgresql://example", review_queue_dir=queue, frontend_dist=tmp_path / "none"))


def test_review_queue_lists_only_the_organization_items(review_client: TestClient) -> None:
    response = review_client.get("/api/review", auth=("owner.one", PASSWORD))

    items = response.json()["items"]
    assert response.status_code == 200
    assert {item["document_id"] for item in items} == {"doc-mine", "doc-gone"}
    assert items[0]["reason_codes"] == ["missing_vendor", "validation_failed"]
    assert items[0]["total_amount"] == 1954.0


def test_review_detail_includes_the_record_reasons_and_a_document_link(review_client: TestClient) -> None:
    body = review_client.get("/api/review/doc-mine", auth=("owner.one", PASSWORD)).json()

    assert body["record"]["vendor_name"] is None
    assert body["violation_codes"] == ["amount_mismatch"]
    assert body["document"] == {
        "name": "Receipt-2734.pdf",
        "content_type": "application/pdf",
        "url": "https://bucket.example/archive/u/j/receipt.pdf?type=application/pdf",
    }


def test_review_detail_reports_a_deleted_or_unreachable_document_without_failing(review_client: TestClient) -> None:
    gone = review_client.get("/api/review/doc-gone", auth=("owner.one", PASSWORD)).json()["document"]
    _ReviewStorage.broken = True
    unreachable = review_client.get("/api/review/doc-mine", auth=("owner.one", PASSWORD))

    assert gone == {"name": "expired.png", "content_type": "image/png", "url": None}
    assert unreachable.status_code == 200
    assert unreachable.json()["document"]["url"] is None


def test_review_detail_hides_other_organizations(review_client: TestClient) -> None:
    assert review_client.get("/api/review/doc-other", auth=("owner.one", PASSWORD)).status_code == 404
    assert review_client.get("/api/review/../secrets", auth=("owner.one", PASSWORD)).status_code == 404


def test_invalid_corrections_get_a_readable_message(review_client: TestClient) -> None:
    corrected = {
        "document_type": "invoice", "vendor_name": "Acme", "invoice_date": "2026-03-02", "currency": "BDT",
        "subtotal": 10, "tax_amount": 0, "total_amount": 10, "model_confidence": 0.9, "validation_score": 0.9,
        "line_items": [{"description": "Paper", "quantity": 0, "unit_price": 10, "line_total": 10}],
    }

    response = review_client.post(
        "/review-items/doc-mine/resolve", auth=("owner.one", PASSWORD), json={"corrected_record": corrected})

    assert response.status_code == 400
    assert response.json()["detail"] == "line item 1 quantity: Input should be greater than 0"

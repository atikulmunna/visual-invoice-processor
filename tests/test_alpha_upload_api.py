from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from app.alpha_store import AlphaAuthenticationError, AlphaNotFoundError, AlphaQuotaError, AlphaUser, Organization
from app.monitoring_api import create_monitoring_app

PASSWORD = "A-strong-alpha-password"
ORG_ONE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ORG_TWO = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
USERS = {
    "tester.one": AlphaUser(
        "11111111-1111-1111-1111-111111111111", "tester.one", 20, 3, True,
        org_id=ORG_ONE, org_name="tester.one", role="owner",
    ),
    "tester.two": AlphaUser(
        "22222222-2222-2222-2222-222222222222", "tester.two", 20, 0, True,
        org_id=ORG_TWO, org_name="tester.two", role="owner",
    ),
}


class _FakeAlphaStore:
    jobs: dict[str, dict[str, Any]] = {}
    sessions: dict[str, str] = {}

    def __init__(self, _: str) -> None:
        pass

    def authenticate(self, username: str, password: str) -> AlphaUser:
        if username not in USERS or password != PASSWORD:
            raise AlphaAuthenticationError("Invalid credentials")
        return USERS[username]

    def create_session(self, user: AlphaUser) -> str:
        self.sessions["test-session-token"] = user.username
        return "test-session-token"

    def authenticate_session(self, token: str) -> AlphaUser:
        if token not in self.sessions:
            raise AlphaAuthenticationError("Invalid session")
        return USERS[self.sessions[token]]

    def delete_session(self, token: str) -> None:
        self.sessions.pop(token, None)

    def authorize_upload(self, user: AlphaUser, **kwargs: Any) -> str:
        job_id = kwargs["object_key"].split("/")[-2]
        self.jobs[job_id] = {"id": job_id, "org_id": user.org_id, "status": "AUTHORIZED"}
        return job_id

    def get_job(self, job_id: str, *, org_id: str) -> dict[str, Any]:
        job = self.jobs.get(job_id)
        if job is None or job["org_id"] != org_id:
            raise AlphaNotFoundError("Processing job not found")
        return job

    def retry_job(self, job_id: str, *, org_id: str) -> str:
        job = self.jobs.get(job_id)
        if job is None or job["org_id"] != org_id or job["status"] != "FAILED":
            raise AlphaQuotaError("This job cannot be retried")
        return f"inbox/{job_id}/file.pdf"

    def complete_job(self, job_id: str, **kwargs: Any) -> None:
        self.jobs[job_id]["status"] = kwargs["status"]

    def get_organization(self, org_id: str) -> Organization:
        return Organization(org_id, "Test organization")


class _FakeStorage:
    def create_presigned_upload(self, object_key: str, **kwargs: Any) -> dict[str, Any]:
        return {
            "url": "https://s3.example",
            "fields": {"key": object_key, "Content-Type": kwargs["content_type"]},
        }


def _configure(monkeypatch: Any) -> None:
    monkeypatch.setenv("ALPHA_AUTH_ENABLED", "true")
    monkeypatch.setenv("INGESTION_BACKEND", "s3")
    monkeypatch.setenv("S3_BUCKET_NAME", "alpha-invoices")
    monkeypatch.setenv("S3_REGION", "ap-southeast-1")
    monkeypatch.setenv("LEDGER_BACKEND", "postgres")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://example")
    monkeypatch.setenv("MAX_UPLOAD_BYTES", "5242880")
    monkeypatch.setattr("app.monitoring_api.AlphaStore", _FakeAlphaStore)
    monkeypatch.setattr("app.workspace_api.AlphaStore", _FakeAlphaStore)
    monkeypatch.setattr(
        "app.monitoring_api.ObjectStorageService.from_settings",
        lambda settings: _FakeStorage(),
    )


def test_presign_and_owner_scoped_status(monkeypatch: Any) -> None:
    _configure(monkeypatch)
    client = TestClient(create_monitoring_app(postgres_dsn="postgresql://example"))
    auth = ("tester.one", "A-strong-alpha-password")

    response = client.post(
        "/uploads/presign",
        auth=auth,
        json={"filename": "invoice.pdf", "content_type": "application/pdf", "size": 1200},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["object_key"].startswith("inbox/11111111-1111-1111-1111-111111111111/")
    assert payload["documents_remaining"] == 16
    status = client.get(f"/uploads/{payload['job_id']}", auth=auth)
    assert status.status_code == 200
    assert status.json()["status"] == "AUTHORIZED"


def test_alpha_dashboard_shows_user_quota_and_result_workspace(monkeypatch: Any) -> None:
    _configure(monkeypatch)
    client = TestClient(create_monitoring_app(postgres_dsn="postgresql://example"))

    response = client.get("/dashboard", auth=("tester.one", "A-strong-alpha-password"))

    assert response.status_code == 200
    assert '<div class="alpha-user">tester.one</div>' in response.text
    assert '<span id="documentsRemaining">17</span>' in response.text
    assert 'id="resultPanel"' in response.text
    assert "terminalJobStatuses" in response.text


def test_alpha_login_creates_secure_session_and_logout_clears_it(monkeypatch: Any) -> None:
    _configure(monkeypatch)
    _FakeAlphaStore.sessions.clear()
    client = TestClient(
        create_monitoring_app(postgres_dsn="postgresql://example"),
        base_url="https://testserver",
    )

    root = client.get("/", follow_redirects=False)
    login_page = client.get("/login", follow_redirects=False)
    login = client.post("/api/session", json={"username": "tester.one", "password": PASSWORD})
    dashboard = client.get("/dashboard")
    me = client.get("/api/me")
    logout = client.post("/logout", follow_redirects=False)
    dashboard_after_logout = client.get("/dashboard")

    assert root.status_code == 307
    assert root.headers["location"] == "/login"
    assert login_page.status_code == 307
    assert login_page.headers["location"] == "/app/login"
    assert login.status_code == 200
    assert login.json() == {"username": "tester.one"}
    cookie = login.headers["set-cookie"].lower()
    assert "invoice_alpha_session=test-session-token" in cookie
    assert "httponly" in cookie
    assert "secure" in cookie
    assert "samesite=lax" in cookie
    assert dashboard.status_code == 200
    assert "tester.one" in dashboard.text
    assert me.json()["username"] == "tester.one"
    assert logout.status_code == 303
    assert logout.headers["location"] == "/login"
    assert "invoice_alpha_session=" in logout.headers["set-cookie"]
    assert dashboard_after_logout.status_code == 401


def test_sign_in_rejects_wrong_and_oversized_credentials(monkeypatch: Any) -> None:
    _configure(monkeypatch)
    client = TestClient(create_monitoring_app(postgres_dsn="postgresql://example"), base_url="https://testserver")

    wrong = client.post("/api/session", json={"username": "tester.one", "password": "not-the-password"})
    unknown = client.post("/api/session", json={"username": "nobody", "password": PASSWORD})
    oversized = client.post("/api/session", json={"username": "tester.one", "password": "x" * 257})

    assert wrong.status_code == 401
    assert wrong.json()["detail"] == "The username or password is incorrect."
    assert "set-cookie" not in wrong.headers
    assert unknown.status_code == 401
    assert oversized.status_code == 422


def test_sign_in_is_unavailable_without_a_database(monkeypatch: Any) -> None:
    _configure(monkeypatch)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    client = TestClient(create_monitoring_app(postgres_dsn=None), base_url="https://testserver")

    response = client.post("/api/session", json={"username": "tester.one", "password": PASSWORD})

    assert response.status_code == 503


def test_legacy_login_address_keeps_the_return_path(monkeypatch: Any) -> None:
    _configure(monkeypatch)
    client = TestClient(create_monitoring_app(postgres_dsn="postgresql://example"))

    response = client.get("/login?next=/app/review", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/app/login?next=/app/review"


def test_presign_rejects_unsupported_and_oversized_files(monkeypatch: Any) -> None:
    _configure(monkeypatch)
    client = TestClient(create_monitoring_app(postgres_dsn="postgresql://example"))
    auth = ("tester.one", "A-strong-alpha-password")

    unsupported = client.post(
        "/uploads/presign",
        auth=auth,
        json={"filename": "invoice.txt", "content_type": "text/plain", "size": 20},
    )
    oversized = client.post(
        "/uploads/presign",
        auth=auth,
        json={"filename": "invoice.pdf", "content_type": "application/pdf", "size": 5242881},
    )

    assert unsupported.status_code == 400
    assert oversized.status_code == 400


def _write_review_item(queue: Path, document_id: str, org_id: str, vendor: str) -> None:
    (queue / f"{document_id}.json").write_text(
        json.dumps(
            {
                "document_id": document_id,
                "status": "REVIEW_REQUIRED",
                "org_id": org_id,
                "metadata": {
                    "file_hash": f"hash-{document_id}",
                    "source_file_id": f"inbox/{document_id}.pdf",
                    "normalized_record": {"vendor_name": vendor, "total_amount": 10.0},
                },
            }
        ),
        encoding="utf-8",
    )


def test_jobs_are_isolated_between_organizations(monkeypatch: Any) -> None:
    _configure(monkeypatch)
    _FakeAlphaStore.jobs.clear()
    client = TestClient(create_monitoring_app(postgres_dsn="postgresql://example"))
    one = ("tester.one", PASSWORD)
    two = ("tester.two", PASSWORD)

    job_id = client.post(
        "/uploads/presign",
        auth=one,
        json={"filename": "invoice.pdf", "content_type": "application/pdf", "size": 1200},
    ).json()["job_id"]
    _FakeAlphaStore.jobs[job_id]["status"] = "FAILED"

    assert client.get(f"/uploads/{job_id}", auth=two).status_code == 404
    assert client.post(f"/uploads/{job_id}/retry", auth=two).status_code == 409
    assert _FakeAlphaStore.jobs[job_id]["status"] == "FAILED"
    assert client.get(f"/uploads/{job_id}", auth=one).status_code == 200


def test_review_queue_is_isolated_between_organizations(tmp_path: Path, monkeypatch: Any) -> None:
    _configure(monkeypatch)
    monkeypatch.setattr("app.monitoring_api._resolved_file_hashes", lambda dsn, org_id=None: set())
    queue = tmp_path / "review_queue"
    queue.mkdir()
    _write_review_item(queue, "doc-one", ORG_ONE, "Acme")
    _write_review_item(queue, "doc-two", ORG_TWO, "Globex")
    client = TestClient(create_monitoring_app(postgres_dsn="postgresql://example", review_queue_dir=queue))
    one = ("tester.one", PASSWORD)
    two = ("tester.two", PASSWORD)

    items = client.get("/review-items", auth=one).json()
    assert [item["document_id"] for item in items["items"]] == ["doc-one"]
    assert client.get("/stats", auth=one).json()["review_queue_total"] == 1
    assert client.get("/backlog", auth=two).json()["review_queue_total"] == 1

    for action in ("approve", "reject", "duplicate"):
        response = client.post("/review-items/doc-two/resolve", auth=one, json={"action": action})
        assert response.status_code == 404
    assert json.loads((queue / "doc-two.json").read_text(encoding="utf-8"))["status"] == "REVIEW_REQUIRED"

    rejected = client.post("/review-items/doc-one/resolve", auth=one, json={"action": "reject"})
    assert rejected.status_code == 200
    assert client.get("/review-history", auth=one).json()["count"] == 1
    assert client.get("/review-history", auth=two).json()["count"] == 0


def test_operator_logs_are_hidden_from_organizations(tmp_path: Path, monkeypatch: Any) -> None:
    _configure(monkeypatch)
    monkeypatch.setattr("app.monitoring_api._resolved_file_hashes", lambda dsn, org_id=None: set())
    dead = tmp_path / "dead_letter.jsonl"
    dead.write_text(json.dumps({"document_id": "x", "status": "FAILED", "error_message": "other tenant"}) + "\n")
    metrics = tmp_path / "metrics.jsonl"
    metrics.write_text(json.dumps({"metric": "documents_processed_total", "value": 9}) + "\n")
    client = TestClient(
        create_monitoring_app(
            postgres_dsn="postgresql://example",
            metrics_path=metrics,
            dead_letter_path=dead,
            review_queue_dir=tmp_path / "review_queue",
        )
    )
    one = ("tester.one", PASSWORD)

    stats = client.get("/stats", auth=one).json()
    assert "documents_processed_total" not in stats
    assert stats["dead_letter_total"] == 0
    assert client.get("/failures", auth=one).json()["count"] == 0
    assert client.get("/backlog", auth=one).json()["attention_total"] == 0


def test_dashboard_data_is_scoped_to_the_active_organization(tmp_path: Path, monkeypatch: Any) -> None:
    _configure(monkeypatch)
    seen: list[tuple[str, str | None]] = []

    def _fake_query(dsn: str | None, *, limit: int, org_id: str | None = None) -> dict[str, Any]:
        seen.append(("records", org_id))
        return {"kpis": {}, "recent_records": []}

    def _fake_hashes(dsn: str | None, *, org_id: str | None = None) -> set[str]:
        seen.append(("hashes", org_id))
        return set()

    monkeypatch.setattr("app.monitoring_api._query_dashboard_data", _fake_query)
    monkeypatch.setattr("app.monitoring_api._resolved_file_hashes", _fake_hashes)
    client = TestClient(
        create_monitoring_app(postgres_dsn="postgresql://example", review_queue_dir=tmp_path / "review_queue")
    )

    response = client.get("/dashboard/data", auth=("tester.two", PASSWORD))

    assert response.status_code == 200
    assert seen == [("records", ORG_TWO), ("hashes", ORG_TWO)]


def test_account_without_organization_is_refused(tmp_path: Path, monkeypatch: Any) -> None:
    _configure(monkeypatch)
    monkeypatch.setitem(
        USERS,
        "tester.orphan",
        AlphaUser("33333333-3333-3333-3333-333333333333", "tester.orphan", 20, 0, True),
    )
    client = TestClient(
        create_monitoring_app(postgres_dsn="postgresql://example", review_queue_dir=tmp_path / "review_queue")
    )
    auth = ("tester.orphan", PASSWORD)

    assert client.get("/review-items", auth=auth).status_code == 403
    assert client.get("/dashboard/data", auth=auth).status_code == 403
    assert client.post(
        "/uploads/presign",
        auth=auth,
        json={"filename": "invoice.pdf", "content_type": "application/pdf", "size": 1200},
    ).status_code == 403

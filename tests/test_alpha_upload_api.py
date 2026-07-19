from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from app.alpha_store import AlphaUser
from app.monitoring_api import create_monitoring_app


class _FakeAlphaStore:
    jobs: dict[str, dict[str, Any]] = {}
    sessions: set[str] = set()

    def __init__(self, _: str) -> None:
        pass

    def authenticate(self, username: str, password: str) -> AlphaUser:
        assert username == "tester.one"
        assert password == "A-strong-alpha-password"
        return AlphaUser("11111111-1111-1111-1111-111111111111", username, 20, 3, True)

    def create_session(self, user: AlphaUser) -> str:
        assert user.username == "tester.one"
        self.sessions.add("test-session-token")
        return "test-session-token"

    def authenticate_session(self, token: str) -> AlphaUser:
        if token not in self.sessions:
            from app.alpha_store import AlphaAuthenticationError

            raise AlphaAuthenticationError("Invalid session")
        return AlphaUser("11111111-1111-1111-1111-111111111111", "tester.one", 20, 3, True)

    def delete_session(self, token: str) -> None:
        self.sessions.discard(token)

    def authorize_upload(self, user: AlphaUser, **kwargs: Any) -> str:
        job_id = kwargs["object_key"].split("/")[-2]
        self.jobs[job_id] = {"id": job_id, "user_id": user.id, "status": "AUTHORIZED"}
        return job_id

    def get_job(self, job_id: str, *, user_id: str | None = None) -> dict[str, Any]:
        job = self.jobs[job_id]
        assert job["user_id"] == user_id
        return job


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
    login_page = client.get("/login")
    login = client.post(
        "/login",
        data={"username": "tester.one", "password": "A-strong-alpha-password"},
        follow_redirects=False,
    )
    dashboard = client.get("/dashboard")
    logout = client.post("/logout", follow_redirects=False)
    dashboard_after_logout = client.get("/dashboard")

    assert root.status_code == 307
    assert root.headers["location"] == "/login"
    assert login_page.status_code == 200
    assert "Enter workspace" in login_page.text
    assert '<link rel="icon" type="image/png" href="/assets/icon.png" />' in login_page.text
    assert '<img src="/assets/icon.png" alt="" />' in login_page.text
    assert "Invoice intelligence" not in login_page.text
    assert "rgba(241, 90, 36, 0.16)" in login_page.text
    assert "--ink-950: #f7f4ee" in login_page.text
    assert login.status_code == 303
    assert login.headers["location"] == "/dashboard"
    assert "httponly" in login.headers["set-cookie"].lower()
    assert "secure" in login.headers["set-cookie"].lower()
    assert "samesite=lax" in login.headers["set-cookie"].lower()
    assert dashboard.status_code == 200
    assert "tester.one" in dashboard.text
    assert logout.status_code == 303
    assert logout.headers["location"] == "/login"
    assert dashboard_after_logout.status_code == 401


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

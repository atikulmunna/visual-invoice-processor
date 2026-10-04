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

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app import vendor_store
from app.alpha_store import AlphaAuthenticationError, AlphaUser
from app.monitoring_api import create_monitoring_app

PASSWORD = "A-strong-alpha-password"
ORG_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
USERS = {
    "member.one": AlphaUser("11111111-1111-1111-1111-111111111111", "member.one", 20, 0, True,
                            org_id=ORG_ID, org_name="Acme Traders", role="member"),
    "orphan": AlphaUser("33333333-3333-3333-3333-333333333333", "orphan", 20, 0, True),
}
AUTH = ("member.one", PASSWORD)


class _FakeAlphaStore:
    def __init__(self, _: str) -> None:
        pass

    def authenticate(self, username: str, password: str) -> AlphaUser:
        if username not in USERS or password != PASSWORD:
            raise AlphaAuthenticationError("Invalid credentials")
        return USERS[username]

    def authenticate_session(self, token: str) -> AlphaUser:
        raise AlphaAuthenticationError("Invalid session")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[TestClient, list]:
    """A signed-in workspace with stand-ins for the vendor queries, which note each call."""
    calls: list[tuple[str, dict[str, Any]]] = []
    vendors = [
        {"id": 1, "name": "RYANS", "records": 1},
        {"id": 2, "name": "RYANS Computers", "records": 4},
        {"id": 3, "name": "Shwapno", "records": 2},
    ]

    def vendor_detail(dsn: str, **kwargs: Any) -> dict[str, Any]:
        calls.append(("detail", kwargs))
        if kwargs["vendor_id"] == 404:
            raise vendor_store.VendorNotFound(404)
        return {"id": kwargs["vendor_id"], "name": "RYANS Computers"}

    def update_vendor(dsn: str, **kwargs: Any) -> None:
        calls.append(("update", kwargs))
        if kwargs["name"] == "Shwapno":
            raise vendor_store.VendorConflict("Shwapno already goes by this name. Merge the two vendors instead.", 3)
        if kwargs["default_currency"] == "TK":
            raise ValueError("Use a three-letter currency code, such as BDT or USD")

    def merge_vendors(dsn: str, **kwargs: Any) -> None:
        calls.append(("merge", kwargs))
        if kwargs["merge_ids"] == [kwargs["keep_id"]]:
            raise ValueError("Choose at least one other vendor to merge")

    monkeypatch.setenv("ALPHA_AUTH_ENABLED", "true")
    monkeypatch.setattr("app.monitoring_api.AlphaStore", _FakeAlphaStore)
    monkeypatch.setattr("app.workspace_api.AlphaStore", _FakeAlphaStore)
    monkeypatch.setattr("app.vendor_api.vendor_store.list_vendors", lambda dsn, **kwargs: vendors)
    monkeypatch.setattr("app.vendor_api.vendor_store.vendor_detail", vendor_detail)
    monkeypatch.setattr("app.vendor_api.vendor_store.update_vendor", update_vendor)
    monkeypatch.setattr("app.vendor_api.vendor_store.merge_vendors", merge_vendors)
    monkeypatch.setattr("app.vendor_api._today", lambda: date(2026, 10, 5))
    app = create_monitoring_app(postgres_dsn="postgresql://example", frontend_dist=tmp_path / "none")
    return TestClient(app), calls


def test_vendor_list_comes_with_merge_suggestions(client: tuple[TestClient, list]) -> None:
    http, _ = client

    body = http.get("/api/vendors", auth=AUTH).json()

    assert [vendor["name"] for vendor in body["vendors"]] == ["RYANS", "RYANS Computers", "Shwapno"]
    assert body["suggestions"] == [{"keep": 2, "merge": 1, "keep_name": "RYANS Computers", "merge_name": "RYANS"}]


def test_vendor_detail_is_scoped_to_the_organization(client: tuple[TestClient, list]) -> None:
    http, calls = client

    found = http.get("/api/vendors/2", auth=AUTH)
    missing = http.get("/api/vendors/404", auth=AUTH)

    assert found.json() == {"id": 2, "name": "RYANS Computers"}
    assert calls[0] == ("detail", {"org_id": ORG_ID, "vendor_id": 2, "today": date(2026, 10, 5)})
    assert missing.status_code == 404
    assert http.get("/api/vendors/0", auth=AUTH).status_code == 422


def test_editing_a_vendor(client: tuple[TestClient, list]) -> None:
    http, calls = client

    saved = http.put("/api/vendors/2", auth=AUTH, json={"name": "RYANS Computers Ltd", "tax_id": "001", "default_currency": "BDT"})
    taken = http.put("/api/vendors/2", auth=AUTH, json={"name": "Shwapno"})
    bad_currency = http.put("/api/vendors/2", auth=AUTH, json={"name": "RYANS", "default_currency": "TK"})

    assert saved.status_code == 200
    assert calls[0] == ("update", {"org_id": ORG_ID, "vendor_id": 2, "name": "RYANS Computers Ltd", "tax_id": "001",
                                   "default_currency": "BDT"})
    assert taken.status_code == 409
    assert taken.json()["detail"] == "Shwapno already goes by this name. Merge the two vendors instead."
    assert bad_currency.status_code == 400
    assert http.put("/api/vendors/2", auth=AUTH, json={"name": ""}).status_code == 422


def test_merging_vendors_into_one(client: tuple[TestClient, list]) -> None:
    http, calls = client

    merged = http.post("/api/vendors/2/merge", auth=AUTH, json={"vendor_ids": [1]})
    into_itself = http.post("/api/vendors/2/merge", auth=AUTH, json={"vendor_ids": [2]})

    assert merged.status_code == 200
    assert calls[0] == ("merge", {"org_id": ORG_ID, "keep_id": 2, "merge_ids": [1]})
    assert into_itself.status_code == 400
    assert http.post("/api/vendors/2/merge", auth=AUTH, json={"vendor_ids": []}).status_code == 422


def test_vendors_need_a_session_and_an_organization(client: tuple[TestClient, list]) -> None:
    http, calls = client

    assert http.get("/api/vendors").status_code == 401
    assert http.get("/api/vendors", auth=("orphan", PASSWORD)).status_code == 403
    assert http.post("/api/vendors/2/merge", auth=("orphan", PASSWORD), json={"vendor_ids": [1]}).status_code == 403
    assert calls == []

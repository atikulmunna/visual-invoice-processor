from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from app.alpha_store import AlphaStore
from app.config import Settings
from app.duplicates import check_for_duplicate, find_possible_duplicate, normalize_invoice_number
from app.storage_service import PostgresStorageService

PASSWORD = "A-strong-alpha-password"


@pytest.mark.parametrize(
    ("value", "expected"),
    [("INV-0042", "INV0042"), ("inv 0042", "INV0042"), (" 0042/A ", "0042A"), ("-", None), (None, None)],
)
def test_invoice_numbers_compare_without_spacing_case_or_punctuation(value: Any, expected: str | None) -> None:
    assert normalize_invoice_number(value) == expected


def _org(dsn: str) -> str:
    return AlphaStore(dsn).create_user(f"dupes-{uuid4().hex[:8]}", PASSWORD, max_users=1000).org_id


def _store(dsn: str, org_id: str, **record: Any) -> int:
    base = {"vendor_name": "RYANS Computers", "invoice_number": None, "invoice_date": "2026-09-10",
            "currency": "BDT", "total_amount": 1954.0}
    result = PostgresStorageService(dsn, "ledger_records").append_record(
        record={**base, **record},
        metadata={"drive_file_id": f"inbox/{uuid4()}.pdf", "file_hash": uuid4().hex, "org_id": org_id},
    )
    return result["row_id"]


def _check(dsn: str, org_id: str, **record: Any) -> dict[str, Any] | None:
    base = {"vendor_name": "RYANS Computers", "invoice_number": None, "invoice_date": "2026-09-10",
            "currency": "BDT", "total_amount": 1954.0}
    return find_possible_duplicate(dsn, org_id=org_id, record={**base, **record})


def test_same_vendor_and_invoice_number_is_a_duplicate_whatever_the_amount(dsn: str) -> None:
    org = _org(dsn)
    stored = _store(dsn, org, invoice_number="INV-0042", total_amount=500.0)

    match = _check(dsn, org, vendor_name="ryans computers ltd", invoice_number="inv 0042", invoice_date="2026-01-01")

    assert match == {
        "record_id": stored,
        "match": "invoice_number",
        "vendor_name": "RYANS Computers",
        "invoice_number": "INV-0042",
        "invoice_date": "2026-09-10",
        "currency": "BDT",
        "total_amount": 500.0,
        "added_at": match["added_at"],
    }


@pytest.mark.parametrize(
    ("change", "matches"),
    [
        ({"invoice_date": "2026-09-17"}, True),  # 7 days later: still inside the window
        ({"invoice_date": "2026-09-03"}, True),
        ({"invoice_date": "2026-09-18"}, False),  # 8 days: a separate bill
        ({"total_amount": 1954.005}, True),
        ({"total_amount": 1955.0}, False),
        ({"currency": "USD"}, False),
        ({"invoice_date": None}, False),
        ({"vendor_name": "Star Tech"}, False),
    ],
)
def test_same_vendor_and_amount_counts_only_within_a_week(dsn: str, change: dict[str, Any], matches: bool) -> None:
    org = _org(dsn)
    _store(dsn, org)
    _store(dsn, org, vendor_name="Star Tech", total_amount=99.0)

    match = _check(dsn, org, **change)

    assert (match is not None and match["match"] == "amount_and_date") is matches


def test_an_invoice_number_match_wins_over_an_amount_match(dsn: str) -> None:
    org = _org(dsn)
    same_amount = _store(dsn, org, invoice_number="A-1")
    same_number = _store(dsn, org, invoice_number="B-2", total_amount=10.0, invoice_date="2025-01-01")

    match = _check(dsn, org, invoice_number="B-2")

    assert (match["record_id"], match["match"]) == (same_number, "invoice_number")
    assert same_amount != same_number


def test_new_vendors_and_other_organizations_have_no_duplicates(dsn: str) -> None:
    org, other = _org(dsn), _org(dsn)
    _store(dsn, org, invoice_number="INV-9")

    assert _check(dsn, org, vendor_name="Brand New Vendor", invoice_number="INV-9") is None
    assert _check(dsn, other, invoice_number="INV-9") is None
    assert _check(dsn, org, vendor_name=None, invoice_number="INV-9") is None


def test_the_worker_check_only_runs_on_postgres_and_never_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_: Any, **__: Any) -> None:
        raise RuntimeError("database unreachable")

    monkeypatch.setattr("app.duplicates.find_possible_duplicate", broken)
    sheets = Settings(ledger_backend="sheets")
    postgres = Settings(ledger_backend="postgres", postgres_dsn="postgresql://example")

    assert check_for_duplicate(sheets, {"vendor_name": "A"}, "org") is None
    assert check_for_duplicate(postgres, {"vendor_name": "A"}, None) is None
    assert check_for_duplicate(postgres, {"vendor_name": "A"}, "org") is None

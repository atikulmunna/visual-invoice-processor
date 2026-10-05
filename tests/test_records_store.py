from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.alpha_store import AlphaStore
from app.records_store import (
    ExportTooLarge,
    RecordFilters,
    export_records,
    get_record,
    overview,
    pick_currency,
    record_facets,
    search_records,
    trend_months,
)

PASSWORD = "A-strong-alpha-password"
START = datetime(2026, 10, 1, tzinfo=timezone.utc)

# vendor, invoice number, invoice date, currency, total, flagged, approved by a reviewer
ROWS = [
    ("Acme Supplies", "INV-100", "2026-08-03", "BDT", 1000.0, False, False),
    ("Acme Supplies", "INV-101", "2026-09-10", "BDT", 2500.0, True, False),
    ("Daraz", "D_1", "2026-10-01", "BDT", 500.0, False, True),
    ("Cloud Co", "DX1", "2026-09-20", "USD", 40.0, False, False),
    ("Old Vendor", "O-1", "2025-01-15", "BDT", 900.0, False, False),
]


def _insert(conn, org_id: str, index: int, row: tuple) -> None:
    vendor, number, invoice_date, currency, total, flagged, reviewed = row
    record = {
        "vendor_name": vendor,
        "invoice_number": number,
        "invoice_date": invoice_date,
        "currency": currency,
        "total_amount": total,
        "needs_review": flagged,
    }
    metadata = {"resolution_source": "manual_review"} if reviewed else {}
    conn.execute(
        """
        INSERT INTO ledger_records(drive_file_id, file_hash, status, record_json, metadata_json, processed_at_utc, org_id)
        VALUES (%s, %s, 'STORED', %s, %s, %s, %s)
        """,
        (f"inbox/{org_id}/{index}/{number}.pdf", f"hash-{org_id}-{index}", json.dumps(record),
         json.dumps(metadata), START + timedelta(hours=index), org_id),
    )


@pytest.fixture(scope="module")
def orgs(dsn: str) -> tuple[str, str]:
    import psycopg

    store = AlphaStore(dsn)
    mine = store.create_user(f"mine-{uuid4().hex[:8]}", PASSWORD, max_users=1000).org_id
    other = store.create_user(f"other-{uuid4().hex[:8]}", PASSWORD, max_users=1000).org_id
    with psycopg.connect(dsn) as conn:
        for index, row in enumerate(ROWS):
            _insert(conn, mine, index, row)
        _insert(conn, other, 0, ("Acme Supplies", "INV-999", "2026-09-01", "BDT", 99999.0, False, False))
    return mine, other


def _numbers(dsn: str, org_id: str, **kwargs) -> list[str]:
    filters = kwargs.pop("filters", RecordFilters())
    return [row["invoice_number"] for row in search_records(dsn, org_id=org_id, filters=filters, **kwargs)["items"]]


def test_records_are_newest_first_and_only_the_organization_own(dsn: str, orgs: tuple[str, str]) -> None:
    mine, other = orgs
    found = search_records(dsn, org_id=mine, filters=RecordFilters())

    assert found["total"] == 5
    assert [row["invoice_number"] for row in found["items"]] == ["O-1", "DX1", "D_1", "INV-101", "INV-100"]
    assert found["items"][2] == {
        "id": found["items"][2]["id"],
        "added_at": (START + timedelta(hours=2)).isoformat(),
        "vendor_name": "Daraz",
        "invoice_number": "D_1",
        "invoice_date": "2026-10-01",
        "currency": "BDT",
        "total_amount": 500.0,
        "flagged": False,
        "reviewed": True,
    }
    assert _numbers(dsn, other) == ["INV-999"]


def test_search_matches_vendor_or_invoice_number_and_treats_wildcards_literally(dsn: str, orgs: tuple[str, str]) -> None:
    mine, _ = orgs

    assert sorted(_numbers(dsn, mine, filters=RecordFilters(query="acme"))) == ["INV-100", "INV-101"]
    assert _numbers(dsn, mine, filters=RecordFilters(query="d_1")) == ["D_1"]
    assert _numbers(dsn, mine, filters=RecordFilters(query="%")) == []


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        (RecordFilters(currency="usd"), ["DX1"]),
        (RecordFilters(vendor="Acme Supplies"), ["INV-100", "INV-101"]),
        (RecordFilters(date_from=date(2026, 9, 1), date_to=date(2026, 9, 30)), ["DX1", "INV-101"]),
        (RecordFilters(date_from=date(2026, 10, 1)), ["D_1"]),
        (RecordFilters(flagged=True), ["INV-101"]),
        (RecordFilters(reviewed=True), ["D_1"]),
        (RecordFilters(currency="BDT", flagged=True, query="acme"), ["INV-101"]),
    ],
)
def test_filters_narrow_the_list(dsn: str, orgs: tuple[str, str], filters: RecordFilters, expected: list[str]) -> None:
    assert sorted(_numbers(dsn, orgs[0], filters=filters)) == sorted(expected)


def test_sorting_and_pages(dsn: str, orgs: tuple[str, str]) -> None:
    mine, _ = orgs

    assert _numbers(dsn, mine, sort="total", descending=False) == ["DX1", "D_1", "O-1", "INV-100", "INV-101"]
    assert _numbers(dsn, mine, sort="invoice_date") == ["D_1", "DX1", "INV-101", "INV-100", "O-1"]
    assert _numbers(dsn, mine, sort="vendor", descending=False)[:2] == ["INV-100", "INV-101"]
    last_page = search_records(dsn, org_id=mine, filters=RecordFilters(), page=3, page_size=2)
    assert last_page["total"] == 5
    assert [row["invoice_number"] for row in last_page["items"]] == ["INV-100"]
    assert search_records(dsn, org_id=mine, filters=RecordFilters(), page=9, page_size=2) == {"items": [], "total": 5}


def test_facets_list_the_organization_currencies_and_vendors(dsn: str, orgs: tuple[str, str]) -> None:
    assert record_facets(dsn, org_id=orgs[0]) == {
        "currencies": [{"code": "BDT", "count": 4}, {"code": "USD", "count": 1}],
        "vendors": ["Acme Supplies", "Cloud Co", "Daraz", "Old Vendor"],
    }


def test_export_holds_every_matching_record_in_list_order(dsn: str, orgs: tuple[str, str]) -> None:
    mine, other = orgs

    everything = export_records(dsn, org_id=mine, filters=RecordFilters(), sort="total", descending=False)
    bdt_flagged = export_records(dsn, org_id=mine, filters=RecordFilters(currency="BDT", flagged=True))

    assert [row["record"]["invoice_number"] for row in everything] == ["DX1", "D_1", "O-1", "INV-100", "INV-101"]
    assert everything[2]["added_at"] == START + timedelta(hours=4)
    assert (everything[1]["reviewed"], everything[1]["flagged"]) == (True, False)
    assert everything[1]["record"]["vendor_name"] == "Daraz"
    assert [row["record"]["invoice_number"] for row in bdt_flagged] == ["INV-101"]
    assert [row["record"]["total_amount"] for row in export_records(dsn, org_id=other, filters=RecordFilters())] == [
        99999.0
    ]


def test_export_refuses_more_records_than_it_may_hold(dsn: str, orgs: tuple[str, str]) -> None:
    with pytest.raises(ExportTooLarge) as refused:
        export_records(dsn, org_id=orgs[0], filters=RecordFilters(), limit=4)

    assert (refused.value.total, refused.value.limit) == (5, 4)
    assert len(export_records(dsn, org_id=orgs[0], filters=RecordFilters(), limit=5)) == 5


def test_a_record_is_found_only_within_its_organization(dsn: str, orgs: tuple[str, str]) -> None:
    mine, other = orgs
    record_id = search_records(dsn, org_id=mine, filters=RecordFilters(query="D_1"))["items"][0]["id"]

    found = get_record(dsn, org_id=mine, record_id=record_id)

    assert found is not None
    assert found["source_key"] == f"inbox/{mine}/2/D_1.pdf"
    assert found["record"]["vendor_name"] == "Daraz"
    assert (found["flagged"], found["reviewed"]) == (False, True)
    assert get_record(dsn, org_id=other, record_id=record_id) is None
    assert get_record(dsn, org_id=mine, record_id=10**12) is None


def test_overview_totals_one_currency_by_invoice_month(dsn: str, orgs: tuple[str, str]) -> None:
    summary = overview(dsn, org_id=orgs[0], currency=None, base_currency="BDT", today=date(2026, 10, 4))

    assert summary["currency"] == "BDT"
    assert summary["currencies"] == ["BDT", "USD"]
    assert (summary["records_total"], summary["flagged_total"]) == (5, 1)
    months = {row["month"]: row for row in summary["months"]}
    assert summary["months"][0]["month"] == "2025-11"
    assert summary["months"][-1] == {"month": "2026-10", "total": 500.0, "count": 1}
    assert months["2026-09"] == {"month": "2026-09", "total": 2500.0, "count": 1}
    assert months["2026-08"]["total"] == 1000.0
    # The January 2025 invoice falls outside the twelve-month window.
    assert sum(row["count"] for row in summary["months"]) == 3
    assert summary["top_vendors"] == [
        {"vendor_id": None, "vendor_name": "Acme Supplies", "total": 3500.0, "count": 2},
        {"vendor_id": None, "vendor_name": "Daraz", "total": 500.0, "count": 1},
    ]


def test_overview_switches_currency_and_falls_back_when_asked_for_one_it_lacks(dsn: str, orgs: tuple[str, str]) -> None:
    usd = overview(dsn, org_id=orgs[0], currency="usd", base_currency="BDT", today=date(2026, 10, 4))
    missing = overview(dsn, org_id=orgs[0], currency="EUR", base_currency="BDT", today=date(2026, 10, 4))

    assert usd["currency"] == "USD"
    assert usd["top_vendors"] == [{"vendor_id": None, "vendor_name": "Cloud Co", "total": 40.0, "count": 1}]
    assert missing["currency"] == "BDT"


def test_overview_of_an_empty_organization(dsn: str) -> None:
    org_id = AlphaStore(dsn).create_user(f"empty-{uuid4().hex[:8]}", PASSWORD, max_users=1000).org_id

    summary = overview(dsn, org_id=org_id, currency=None, base_currency="BDT", today=date(2026, 10, 4))

    assert summary["currency"] is None
    assert summary["records_total"] == 0
    assert all(row["total"] == 0 for row in summary["months"])
    assert summary["top_vendors"] == []


def test_trend_months_end_with_the_current_month_and_cross_years() -> None:
    assert trend_months(date(2026, 2, 10), 3) == ["2025-12", "2026-01", "2026-02"]
    assert len(trend_months(date(2026, 10, 4))) == 12


@pytest.mark.parametrize(
    ("requested", "base", "expected"),
    [
        ("usd", "BDT", "USD"),
        ("EUR", "BDT", "BDT"),
        (None, "BDT", "BDT"),
        (None, "EUR", "USD"),
        (None, None, "USD"),
    ],
)
def test_pick_currency_prefers_the_request_then_the_base_then_the_most_used(
    requested: str | None, base: str | None, expected: str
) -> None:
    assert pick_currency(["USD", "BDT"], requested, base) == expected


def test_pick_currency_without_records() -> None:
    assert pick_currency([], "BDT", "BDT") is None

from __future__ import annotations

import json
from datetime import date
from uuid import uuid4

import pytest

from app.alpha_store import AlphaStore
from app.records_store import RecordFilters, overview, search_records
from app.storage_service import PostgresStorageService
from app.vendor_store import (
    VendorConflict,
    VendorNotFound,
    link_unlinked_records,
    list_vendors,
    merge_vendors,
    normalize_tax_id,
    normalize_vendor_name,
    suggest_merges,
    update_vendor,
    vendor_detail,
)

PASSWORD = "A-strong-alpha-password"


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Roboflow, Inc", "roboflow"),
        ("RYANS Computers Ltd.", "ryans computers"),
        ("Star Tech Pvt. Ltd.", "star tech"),
        ("DARAZ BANGLADESH LIMITED", "daraz bangladesh"),
        ("Fish & Co", "fish and"),
        ("Ltd", "ltd"),  # a name that is only a suffix is kept
        ("স্বপ্ন সুপারশপ", "স্বপ্ন সুপারশপ"),  # Bangla vowel signs stay part of their word
        ("  ", None),
        ("...", None),
        (None, None),
    ],
)
def test_vendor_names_are_compared_without_case_punctuation_or_legal_form(name: str | None, expected: str | None) -> None:
    assert normalize_vendor_name(name) == expected


def test_tax_ids_ignore_spacing_and_dashes() -> None:
    assert normalize_tax_id(" 001234567-0101 ") == "0012345670101"
    assert normalize_tax_id("-") is None
    assert normalize_tax_id(None) is None


def test_merge_suggestions_pair_look_alike_names_and_keep_the_busier_vendor() -> None:
    vendors = [
        {"id": 1, "name": "RYANS", "records": 1},
        {"id": 2, "name": "RYANS Computers", "records": 5},
        {"id": 3, "name": "Star Tech", "records": 2},
        {"id": 4, "name": "Shwapno", "records": 3},
        {"id": 5, "name": "Agora Super Store", "records": 1},
        {"id": 6, "name": "Agora Superstore", "records": 1},
    ]

    assert suggest_merges(vendors) == [
        {"keep": 2, "merge": 1, "keep_name": "RYANS Computers", "merge_name": "RYANS"},
        {"keep": 5, "merge": 6, "keep_name": "Agora Super Store", "merge_name": "Agora Superstore"},
    ]


def _org(dsn: str) -> str:
    return AlphaStore(dsn).create_user(f"vendors-{uuid4().hex[:8]}", PASSWORD, max_users=1000).org_id


def _store(dsn: str, org_id: str, **record: object) -> int:
    base = {"vendor_name": None, "vendor_tax_id": None, "currency": "BDT", "invoice_date": "2026-09-10",
            "total_amount": 100.0}
    result = PostgresStorageService(dsn, "ledger_records").append_record(
        record={**base, **record},
        metadata={"drive_file_id": f"inbox/{uuid4()}.pdf", "file_hash": uuid4().hex, "org_id": org_id},
    )
    return result["row_id"]


def _vendor_of(dsn: str, record_id: int) -> int | None:
    import psycopg

    with psycopg.connect(dsn) as conn:
        return conn.execute("select vendor_id from ledger_records where id = %s", (record_id,)).fetchone()[0]


def test_stored_records_link_to_one_vendor_per_spelling_or_tax_id(dsn: str) -> None:
    org = _org(dsn)

    first = _store(dsn, org, vendor_name="RYANS Computers", total_amount=1954.0)
    same_name = _store(dsn, org, vendor_name="ryans computers ltd.")
    same_bin = _store(dsn, org, vendor_name="RYANS", vendor_tax_id="0012-345")
    by_bin_again = _store(dsn, org, vendor_name="Ryans IT", vendor_tax_id="0012345")
    no_vendor = _store(dsn, org)

    vendor = _vendor_of(dsn, first)
    assert vendor is not None
    assert _vendor_of(dsn, same_name) == vendor
    # The tax ID belongs to no vendor yet, so "RYANS" starts its own; the second BIN match joins it.
    assert _vendor_of(dsn, same_bin) != vendor
    assert _vendor_of(dsn, by_bin_again) == _vendor_of(dsn, same_bin)
    assert _vendor_of(dsn, no_vendor) is None
    names = {row["name"]: row for row in list_vendors(dsn, org_id=org)}
    # Spellings that normalize alike are one name, so only real variants show as aliases.
    assert names["RYANS Computers"]["aliases"] == []
    assert names["RYANS"]["tax_id"] == "0012345"
    assert names["RYANS"]["aliases"] == ["Ryans IT"]


def test_vendors_never_cross_organizations(dsn: str) -> None:
    mine, other = _org(dsn), _org(dsn)

    first = _store(dsn, mine, vendor_name="Shwapno", vendor_tax_id="99")
    second = _store(dsn, other, vendor_name="Shwapno", vendor_tax_id="99")

    assert _vendor_of(dsn, first) != _vendor_of(dsn, second)
    with pytest.raises(VendorNotFound):
        vendor_detail(dsn, org_id=other, vendor_id=_vendor_of(dsn, first), today=date(2026, 10, 5))
    with pytest.raises(VendorNotFound):
        merge_vendors(dsn, org_id=mine, keep_id=_vendor_of(dsn, first), merge_ids=[_vendor_of(dsn, second)])


def test_backfill_links_older_records_and_keeps_the_first_spelling(dsn: str) -> None:
    import psycopg

    org = _org(dsn)
    with psycopg.connect(dsn) as conn:
        for index, name in enumerate(["Agora Superstore", "AGORA SUPERSTORE LTD", None]):
            conn.execute(
                """
                insert into ledger_records (drive_file_id, file_hash, status, record_json, metadata_json,
                                            processed_at_utc, org_id)
                values (%s, %s, 'STORED', %s, '{}', now() - make_interval(days => %s), %s)
                """,
                (f"legacy-{uuid4()}", uuid4().hex, json.dumps({"vendor_name": name, "currency": "bdt"}), 3 - index, org),
            )

    assert link_unlinked_records(dsn, org_id=org) == 2
    assert link_unlinked_records(dsn, org_id=org) == 0
    vendors = list_vendors(dsn, org_id=org)
    assert [(row["name"], row["records"], row["default_currency"]) for row in vendors] == [
        ("Agora Superstore", 2, "BDT")
    ]


def test_vendor_detail_totals_each_currency_and_charts_the_main_one(dsn: str) -> None:
    org = _org(dsn)
    for month, amount in (("2026-08-03", 1000.0), ("2026-09-10", 2500.5), ("2025-01-15", 900.0)):
        _store(dsn, org, vendor_name="Star Tech", invoice_date=month, total_amount=amount)
    record = _store(dsn, org, vendor_name="Star Tech", currency="USD", total_amount=40.0)

    detail = vendor_detail(dsn, org_id=org, vendor_id=_vendor_of(dsn, record), today=date(2026, 10, 5))

    assert detail["records"] == 4
    assert detail["totals"] == [{"currency": "BDT", "total": 4400.5}, {"currency": "USD", "total": 40.0}]
    assert detail["currency"] == "BDT"
    assert (detail["first_invoice_date"], detail["last_invoice_date"]) == ("2025-01-15", "2026-09-10")
    months = {row["month"]: row for row in detail["months"]}
    assert months["2026-09"] == {"month": "2026-09", "total": 2500.5, "count": 1}
    assert sum(row["count"] for row in detail["months"]) == 2  # January 2025 is outside the window


def test_merging_moves_records_and_names_onto_the_vendor_kept(dsn: str) -> None:
    org = _org(dsn)
    kept = _vendor_of(dsn, _store(dsn, org, vendor_name="RYANS Computers", total_amount=10.0))
    merged_record = _store(dsn, org, vendor_name="RYANS", vendor_tax_id="777", total_amount=5.0)
    merged = _vendor_of(dsn, merged_record)

    merge_vendors(dsn, org_id=org, keep_id=kept, merge_ids=[merged])

    vendors = list_vendors(dsn, org_id=org)
    assert [(row["id"], row["records"], row["tax_id"], row["aliases"]) for row in vendors] == [
        (kept, 2, "777", ["RYANS"])
    ]
    assert _vendor_of(dsn, merged_record) == kept
    # A later document printed "RYANS" now links straight to the kept vendor.
    assert _vendor_of(dsn, _store(dsn, org, vendor_name="Ryans")) == kept
    with pytest.raises(ValueError):
        merge_vendors(dsn, org_id=org, keep_id=kept, merge_ids=[kept])


def test_renaming_adds_an_alias_and_refuses_another_vendors_name_or_tax_id(dsn: str) -> None:
    org = _org(dsn)
    star = _vendor_of(dsn, _store(dsn, org, vendor_name="Star Tech", vendor_tax_id="111"))
    shwapno = _vendor_of(dsn, _store(dsn, org, vendor_name="Shwapno"))

    update_vendor(dsn, org_id=org, vendor_id=star, name="Star Tech & Engineering", tax_id="111",
                  default_currency="usd")

    renamed = vendor_detail(dsn, org_id=org, vendor_id=star, today=date(2026, 10, 5))
    assert (renamed["name"], renamed["default_currency"]) == ("Star Tech & Engineering", "USD")
    assert renamed["aliases"] == ["Star Tech"]
    with pytest.raises(VendorConflict, match="Star Tech & Engineering already goes by this name"):
        update_vendor(dsn, org_id=org, vendor_id=shwapno, name="star tech", tax_id=None, default_currency=None)
    with pytest.raises(VendorConflict, match="already has this tax ID"):
        update_vendor(dsn, org_id=org, vendor_id=shwapno, name="Shwapno", tax_id="1-1-1", default_currency=None)
    with pytest.raises(ValueError):
        update_vendor(dsn, org_id=org, vendor_id=shwapno, name="Shwapno", tax_id=None, default_currency="taka")
    with pytest.raises(VendorNotFound):
        update_vendor(dsn, org_id=_org(dsn), vendor_id=shwapno, name="X", tax_id=None, default_currency=None)


def test_records_and_overview_show_the_chosen_vendor_name(dsn: str) -> None:
    org = _org(dsn)
    kept = _vendor_of(dsn, _store(dsn, org, vendor_name="RYANS Computers", total_amount=300.0))
    merged = _vendor_of(dsn, _store(dsn, org, vendor_name="RYANS", total_amount=200.0))
    merge_vendors(dsn, org_id=org, keep_id=kept, merge_ids=[merged])

    found = search_records(dsn, org_id=org, filters=RecordFilters(query="ryans"))
    by_printed_name = search_records(dsn, org_id=org, filters=RecordFilters(vendor="RYANS Computers"))
    summary = overview(dsn, org_id=org, currency=None, base_currency="BDT", today=date(2026, 10, 5))

    assert {row["vendor_name"] for row in found["items"]} == {"RYANS Computers"}
    assert by_printed_name["total"] == 2
    assert summary["top_vendors"] == [{"vendor_id": kept, "vendor_name": "RYANS Computers", "total": 500.0, "count": 2}]

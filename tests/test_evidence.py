from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from app.evidence import assess_fields, evidence_reasons

TODAY = date(2026, 10, 16)

DARAZ_TEXT = """Order # 1001872388
03-03-2025
Invoice # 1001866868
Black Printed Ramie-Cotton Fatua 0080000104060 Tk 1,246.51 1 Tk 1,246.51
Subtotal: Tk 4,722.62
VAT Tk 472.26
Grand Total Tk 5,274.88"""

HIFI_TEXT = """INVOICE
From HiFi Heaven Shop-615, Level-6, North Tower, House Building, Uttara.
Invoice no: 16713
Order date: 09-19-2026
Total 2,16,500.00৳"""


def _statuses(record: dict[str, Any], text: str | None) -> dict[str, str]:
    return {field: result["status"] for field, result in assess_fields(record, text, TODAY)["fields"].items()}


def test_values_printed_in_the_document_are_found_whatever_their_format() -> None:
    daraz = {"vendor_name": "Daraz", "invoice_number": "1001866868", "invoice_date": "2025-03-03", "currency": "BDT",
             "subtotal": 4722.62, "tax_amount": 472.26, "total_amount": 5274.88}
    hifi = {"vendor_name": "HiFi Heaven Ltd", "invoice_number": "16713", "invoice_date": "2026-09-19", "currency": "BDT",
            "total_amount": 216500.0}

    assert _statuses(daraz, DARAZ_TEXT) == {
        "vendor_name": "not_found",  # the document never names Daraz
        "invoice_number": "found",
        "invoice_date": "found",
        "currency": "found",
        "subtotal": "found",
        "tax_amount": "found",
        "total_amount": "found",
    }
    assert set(_statuses(hifi, HIFI_TEXT).values()) == {"found"}


@pytest.mark.parametrize(
    ("printed", "value"),
    [
        ("placed on May 17, 2022", "2022-05-17"),
        ("Date: Mar 06 2012", "2012-03-06"),
        ("Date paid October 1st, 2026", "2026-10-01"),
        ("3/2/26 1:59 PM", "2026-02-03"),
        ("Dated 2026-03-04", "2026-03-04"),
        ("17.05.2022", "2022-05-17"),
    ],
)
def test_dates_are_found_in_common_shapes(printed: str, value: str) -> None:
    assert _statuses({"invoice_date": value}, f"Receipt\n{printed}\nThank you") == {"invoice_date": "found"}


@pytest.mark.parametrize(
    ("printed", "total"),
    [("Grand Total 1,954", 1954.0), ("Total $50.10", 50.1), ("BDT 291.54", 291.54), ("Summe 1.234,56 EUR", 1234.56)],
)
def test_amounts_are_found_with_any_grouping(printed: str, total: float) -> None:
    assert _statuses({"total_amount": total}, printed) == {"total_amount": "found"}


def test_values_the_document_does_not_show_are_flagged() -> None:
    record = {"vendor_name": "Star Tech", "invoice_number": "INV-777", "invoice_date": "2026-01-05", "total_amount": 999.0}

    evidence = assess_fields(record, DARAZ_TEXT, TODAY)

    assert {field: result["status"] for field, result in evidence["fields"].items()} == dict.fromkeys(record, "not_found")
    assert evidence["fields"]["total_amount"]["confidence"] == 0.3
    assert evidence_reasons(evidence) == ["value_not_on_document"]


def test_short_numbers_must_stand_alone() -> None:
    assert _statuses({"invoice_number": "12"}, "Invoice 12 issued") == {"invoice_number": "found"}
    assert _statuses({"invoice_number": "12"}, "Phone 01712345678") == {"invoice_number": "not_found"}


def test_without_text_nothing_is_verified_and_no_reason_is_raised() -> None:
    evidence = assess_fields({"vendor_name": "Uber", "total_amount": 291.54, "invoice_date": "2026-03-02"}, None, TODAY)

    assert evidence["text_available"] is False
    assert {result["status"] for result in evidence["fields"].values()} == {"unverified"}
    assert {result["confidence"] for result in evidence["fields"].values()} == {None}
    assert evidence_reasons(evidence) == []


def test_impossible_dates_are_caught_even_without_text() -> None:
    future = assess_fields({"invoice_date": "2027-01-01"}, None, TODAY)
    ancient = assess_fields({"invoice_date": "1999-12-31"}, None, TODAY)
    backwards = assess_fields({"invoice_date": "2026-05-10", "due_date": "2026-04-01"}, None, TODAY)
    tomorrow = assess_fields({"invoice_date": "2026-10-17"}, None, TODAY)  # time zones can run a day ahead

    assert future["fields"]["invoice_date"]["status"] == "implausible"
    assert ancient["fields"]["invoice_date"]["status"] == "implausible"
    assert backwards["fields"]["due_date"]["status"] == "implausible"
    assert tomorrow["fields"]["invoice_date"]["status"] == "unverified"
    assert evidence_reasons(future) == ["implausible_date"]


def test_empty_values_zero_amounts_and_assumed_currency_are_not_checked() -> None:
    record = {"vendor_name": None, "invoice_number": "", "tax_amount": 0.0, "currency": "BDT", "currency_assumed": True,
              "total_amount": 1954.0}

    assert _statuses(record, "Grand Total 1,954") == {"total_amount": "found"}

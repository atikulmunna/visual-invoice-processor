from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from app.alpha_store import AlphaStore
from app.payables import attention, check_payment, initial_payment, outstanding, set_payment
from app.storage_service import append_record

PASSWORD = "A-strong-alpha-password"
TODAY = date(2026, 10, 16)


def test_receipts_start_paid_and_invoices_wait() -> None:
    assert initial_payment({"document_type": "receipt", "invoice_date": "2026-10-14"}) == ("paid", "2026-10-14")
    assert initial_payment({"document_type": "invoice", "invoice_date": "2026-10-14"}) == ("unpaid", None)
    assert initial_payment({"document_type": "receipt"}) == ("unpaid", None)


@pytest.mark.parametrize(
    ("status", "given", "stored"),
    [
        ("unpaid", date(2026, 10, 1), None),
        ("paid", None, TODAY),
        ("paid", date(2026, 10, 2), date(2026, 10, 2)),
        ("scheduled", date(2026, 11, 1), date(2026, 11, 1)),
    ],
)
def test_each_status_keeps_the_date_that_fits_it(status: str, given: date | None, stored: date | None) -> None:
    assert check_payment(status, given, TODAY) == stored


@pytest.mark.parametrize(
    ("status", "given", "message"),
    [
        ("scheduled", None, "planned for"),
        ("paid", date(2026, 10, 17), "cannot be in the future"),
        ("written off", None, "must be one of"),
    ],
)
def test_payment_changes_that_make_no_sense_are_refused(status: str, given: date | None, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        check_payment(status, given, TODAY)


def _org(dsn: str) -> str:
    return AlphaStore(dsn).create_user(f"pay-{uuid4().hex[:8]}", PASSWORD, max_users=1000).org_id


def _bill(dsn: str, org_id: str, **record: Any) -> int:
    base = {"document_type": "invoice", "vendor_name": "Star Tech", "invoice_number": uuid4().hex[:6],
            "invoice_date": "2026-10-01", "due_date": None, "currency": "BDT", "total_amount": 1000.0}
    result = append_record(
        dsn,
        record={**base, **record},
        metadata={"drive_file_id": f"inbox/{uuid4()}.pdf", "file_hash": uuid4().hex, "org_id": org_id},
    )
    return result["row_id"]


def test_bills_still_to_pay_come_earliest_first_with_their_due_dates(dsn: str) -> None:
    org, other = _org(dsn), _org(dsn)
    later = _bill(dsn, org, invoice_date="2026-10-01", due_date="2026-10-30")
    on_receipt = _bill(dsn, org, invoice_date="2026-10-05")
    planned = _bill(dsn, org, invoice_date="2026-09-01", due_date="2026-09-15")
    _bill(dsn, org, document_type="receipt", invoice_date="2026-10-02")
    _bill(dsn, other)
    set_payment(dsn, org_id=org, record_id=planned, status="scheduled", payment_date=date(2026, 10, 20), today=TODAY)

    items = outstanding(dsn, org_id=org)

    assert [item["id"] for item in items] == [on_receipt, planned, later]
    assert items[0]["payment"] == {"status": "unpaid", "date": None, "due_date": "2026-10-05", "due_printed": False}
    assert items[1]["payment"] == {"status": "scheduled", "date": "2026-10-20", "due_date": "2026-09-15",
                                   "due_printed": True}
    assert (items[2]["vendor_name"], items[2]["currency"], items[2]["total_amount"]) == ("Star Tech", "BDT", 1000.0)


def test_overdue_and_due_soon_count_only_bills_not_yet_paid(dsn: str) -> None:
    org = _org(dsn)
    _bill(dsn, org, invoice_date="2026-10-10")  # due on receipt, already past
    _bill(dsn, org, invoice_date="2026-10-01", due_date="2026-10-16")  # due today
    _bill(dsn, org, invoice_date="2026-10-01", due_date="2026-10-22")  # in six days
    _bill(dsn, org, invoice_date="2026-10-01", due_date="2026-10-23")  # a week away
    paid = _bill(dsn, org, invoice_date="2026-09-01")
    late_plan = _bill(dsn, org, invoice_date="2026-10-01", due_date="2026-10-30")
    set_payment(dsn, org_id=org, record_id=paid, status="paid", payment_date=None, today=TODAY)
    set_payment(dsn, org_id=org, record_id=late_plan, status="scheduled", payment_date=date(2026, 10, 12), today=TODAY)

    assert attention(dsn, org_id=org, today=TODAY) == {"overdue": 2, "due_soon": 2}
    assert attention(dsn, org_id=_org(dsn), today=TODAY) == {"overdue": 0, "due_soon": 0}


def test_marking_a_bill_paid_takes_it_off_the_list_and_only_in_its_organization(dsn: str) -> None:
    org, other = _org(dsn), _org(dsn)
    bill = _bill(dsn, org)

    assert set_payment(dsn, org_id=other, record_id=bill, status="paid", payment_date=None, today=TODAY) is None
    paid = set_payment(dsn, org_id=org, record_id=bill, status="paid", payment_date=None, today=TODAY)
    assert paid == {"status": "paid", "date": "2026-10-16", "due_date": "2026-10-01", "due_printed": False}
    assert outstanding(dsn, org_id=org) == []

    reopened = set_payment(dsn, org_id=org, record_id=bill, status="unpaid", payment_date=date(2026, 10, 3), today=TODAY)
    assert (reopened["status"], reopened["date"]) == ("unpaid", None)


def test_existing_receipts_become_paid_when_the_migration_runs(tmp_path: Path, empty_database: str) -> None:
    import psycopg

    from app.db_migrate import DEFAULT_MIGRATIONS_DIR, apply_migrations, migration_files

    before = tmp_path / "before"
    before.mkdir()
    for path in migration_files(DEFAULT_MIGRATIONS_DIR):
        if path.name < "009":
            shutil.copy(path, before / path.name)
    apply_migrations(empty_database, before)
    with psycopg.connect(empty_database) as conn:
        for kind in ("receipt", "invoice"):
            conn.execute(
                "insert into ledger_records(drive_file_id, file_hash, status, record_json, metadata_json) "
                "values (%s, %s, 'STORED', %s, '{}')",
                (f"inbox/{kind}.pdf", kind, json.dumps({"document_type": kind, "invoice_date": "2026-07-01"})),
            )

    apply_migrations(empty_database)

    with psycopg.connect(empty_database) as conn:
        rows = {row[0]: row[1:] for row in conn.execute("select file_hash, payment_status, payment_date from ledger_records")}
    assert rows["receipt"] == ("paid", date(2026, 7, 1))
    assert rows["invoice"] == ("unpaid", None)

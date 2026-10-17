"""Bills to pay: each stored record is unpaid, scheduled, or paid, and overdue once its due date passes.

A bill with no printed due date is due on its invoice date (payable on receipt). Overdue is never
stored: it is an unpaid bill past its due date, or a scheduled one whose planned date has passed.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import psycopg

PAYMENT_STATUSES = ("unpaid", "scheduled", "paid")
DUE_SOON_DAYS = 7

# The date a bill falls due, and the date it is expected to be settled: the planned date once scheduled.
_DUE = "coalesce(nullif(lr.record_json ->> 'due_date', ''), lr.record_json ->> 'invoice_date')::date"
_SETTLE_BY = f"coalesce(lr.payment_date, {_DUE})"


def initial_payment(record: dict[str, Any]) -> tuple[str, str | None]:
    """The status a newly stored bill starts with: a receipt is proof of payment, anything else waits."""
    if record.get("document_type") == "receipt" and record.get("invoice_date"):
        return "paid", str(record["invoice_date"])
    return "unpaid", None


def payment_view(record: dict[str, Any], status: str, payment_date: date | None) -> dict[str, Any]:
    """How a record stands for payment, with its due date and whether that date was printed."""
    printed = record.get("due_date") or None
    return {
        "status": status,
        "date": payment_date.isoformat() if payment_date else None,
        "due_date": printed or record.get("invoice_date"),
        "due_printed": printed is not None,
    }


def outstanding(dsn: str, *, org_id: str) -> list[dict[str, Any]]:
    """Unpaid and scheduled bills, by the date they should be settled, earliest first."""
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        rows = conn.execute(
            f"""
            select lr.id, coalesce(v.name, lr.record_json ->> 'vendor_name'), lr.record_json ->> 'invoice_number',
                   lr.record_json, lr.payment_status, lr.payment_date, upper(lr.record_json ->> 'currency'),
                   (lr.record_json ->> 'total_amount')::numeric
            from public.ledger_records lr
            left join public.vendors v on v.id = lr.vendor_id
            where lr.org_id = %s and lr.payment_status <> 'paid'
            order by {_SETTLE_BY} nulls last, lr.id
            """,
            (org_id,),
        ).fetchall()
    return [
        {
            "id": record_id,
            "vendor_name": vendor,
            "invoice_number": number,
            "invoice_date": record.get("invoice_date"),
            "payment": payment_view(record, status, paid_or_planned),
            "currency": currency,
            "total_amount": float(total) if total is not None else None,
        }
        for record_id, vendor, number, record, status, paid_or_planned, currency, total in rows
    ]


def attention(dsn: str, *, org_id: str | None, today: date) -> dict[str, int]:
    """Bills past their settle-by date, and bills due within the next week. org_id None counts every organization."""
    org_sql, params = ("", ()) if org_id is None else ("and lr.org_id = %s", (org_id,))
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        overdue, due_soon = conn.execute(
            f"""
            select count(*) filter (where {_SETTLE_BY} < %s),
                   count(*) filter (where {_SETTLE_BY} between %s and %s)
            from public.ledger_records lr
            where lr.payment_status <> 'paid' {org_sql}
            """,
            (today, today, today + timedelta(days=DUE_SOON_DAYS - 1), *params),
        ).fetchone()
    return {"overdue": overdue, "due_soon": due_soon}


def check_payment(status: str, payment_date: date | None, today: date) -> date | None:
    """The date to store with a status, or ValueError when the combination makes no sense."""
    if status not in PAYMENT_STATUSES:
        raise ValueError(f"Payment status must be one of: {', '.join(PAYMENT_STATUSES)}")
    if status == "unpaid":
        return None
    if status == "scheduled" and payment_date is None:
        raise ValueError("Choose the date the payment is planned for")
    if status == "paid" and payment_date is not None and payment_date > today:
        raise ValueError("A payment date cannot be in the future; schedule it instead")
    return payment_date or today


def set_payment(
    dsn: str, *, org_id: str, record_id: int, status: str, payment_date: date | None, today: date
) -> dict[str, Any] | None:
    """Records how a bill stands for payment. None when the organization has no such record."""
    stored_date = check_payment(status, payment_date, today)
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        row = conn.execute(
            """
            update public.ledger_records set payment_status = %s, payment_date = %s
            where org_id = %s and id = %s
            returning record_json, payment_status, payment_date
            """,
            (status, stored_date, org_id, record_id),
        ).fetchone()
    return payment_view(row[0] or {}, row[1], row[2]) if row else None

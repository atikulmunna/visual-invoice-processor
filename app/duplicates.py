"""Finds stored records a new document probably repeats, so it waits for review instead of being stored twice.

Identical files are already caught by their hash. This catches the same bill arriving as a different
file: a scan and a PDF, or a copy forwarded by someone else. A match needs the same vendor, plus either
the same invoice number or the same amount in the same currency within a few days.
"""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Any

import psycopg

from app.config import Settings
from app.vendor_store import find_vendor

DATE_WINDOW_DAYS = 7
AMOUNT_TOLERANCE = 0.01

logger = logging.getLogger(__name__)

_SUMMARY = """
    select id, record_json ->> 'vendor_name', record_json ->> 'invoice_number', record_json ->> 'invoice_date',
           upper(record_json ->> 'currency'), (record_json ->> 'total_amount')::numeric, processed_at_utc
    from public.ledger_records
"""


def normalize_invoice_number(value: Any) -> str | None:
    """INV-0042, inv 0042, and INV0042 are the same number."""
    cleaned = re.sub(r"[^0-9A-Z]", "", str(value or "").upper())
    return cleaned or None


def _summary(row: tuple[Any, ...], match: str) -> dict[str, Any]:
    return {
        "record_id": row[0],
        "match": match,
        "vendor_name": row[1],
        "invoice_number": row[2],
        "invoice_date": row[3],
        "currency": row[4],
        "total_amount": float(row[5]) if row[5] is not None else None,
        "added_at": row[6].isoformat(),
    }


def _iso_day(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def find_possible_duplicate(dsn: str, *, org_id: str, record: dict[str, Any]) -> dict[str, Any] | None:
    """The stored record this one most likely repeats, or None. Invoice number matches win over amount matches."""
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            vendor_id = find_vendor(cur, org_id=org_id, name=record.get("vendor_name"), tax_id=record.get("vendor_tax_id"))
        if vendor_id is None:
            return None  # every stored record with a vendor is linked, so a new vendor has no history to repeat
        number = normalize_invoice_number(record.get("invoice_number"))
        if number:
            row = conn.execute(
                _SUMMARY + """
                where org_id = %s and vendor_id = %s
                  and regexp_replace(upper(record_json ->> 'invoice_number'), '[^0-9A-Z]', '', 'g') = %s
                order by id desc limit 1
                """,
                (org_id, vendor_id, number),
            ).fetchone()
            if row:
                return _summary(row, "invoice_number")
        day, total = _iso_day(record.get("invoice_date")), record.get("total_amount")
        currency = str(record.get("currency") or "").upper()
        if day is None or not isinstance(total, (int, float)) or not currency:
            return None
        row = conn.execute(
            _SUMMARY + """
            where org_id = %s and vendor_id = %s and upper(record_json ->> 'currency') = %s
              and abs((record_json ->> 'total_amount')::numeric - %s) <= %s
              and record_json ->> 'invoice_date' between %s and %s
            order by id desc limit 1
            """,
            (org_id, vendor_id, currency, total, AMOUNT_TOLERANCE,
             (day - timedelta(days=DATE_WINDOW_DAYS)).isoformat(), (day + timedelta(days=DATE_WINDOW_DAYS)).isoformat()),
        ).fetchone()
    return _summary(row, "amount_and_date") if row else None


def check_for_duplicate(settings: Settings, record: dict[str, Any], org_id: str | None) -> dict[str, Any] | None:
    """The worker's check. It only advises, so a failed lookup is logged and the document carries on."""
    if settings.ledger_backend != "postgres" or not settings.postgres_dsn or not org_id:
        return None
    try:
        return find_possible_duplicate(settings.postgres_dsn, org_id=org_id, record=record)
    except Exception:  # noqa: BLE001
        logger.exception("Duplicate check failed; continuing without it")
        return None
